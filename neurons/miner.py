import os
import time
import json
import threading
import typing
from queue import PriorityQueue
import bittensor as bt
import shutil

from neza.protocol import VideoTask, TaskStatusCheck

# Import ComfyUI API module
from neza.api.comfy_api import ComfyAPI

# import base miner class which takes care of most of the boilerplate
from neza.base.miner import BaseMinerNeuron

from neza.utils.http import http_get_request_sync, http_put_request_sync
from neza.utils.tools import _parse_env_servers

import traceback

# Load environment variables
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file


class VideoMiner(BaseMinerNeuron):
    """
    Text-to-Video Miner Implementation
    Responsible for receiving video generation tasks, processing them, and uploading results to S3
    """

    def __init__(self, config=None):
        super(VideoMiner, self).__init__(config=config)
        # Initialize ComfyUI instance
        servers = _parse_env_servers(os.environ.get("COMFYUI_SERVERS", ""))
        self.comfy_api = ComfyAPI(servers, clear_queue=False)

        # Task queue and status management
        self.task_queue = PriorityQueue()  # Priority queue
        self.completed_tasks = {}  # Completed tasks dictionary: task_id -> task_info
        self.task_lock = threading.Lock()  # Thread lock to protect task data structures

        # In-memory task storage
        self.tasks = {}  # task_id -> task_info

        # IP blacklist
        self.ip_blacklist = []

        # Start task processing thread
        self.task_processor_thread = threading.Thread(
            target=self._process_tasks, daemon=True
        )
        self.task_processor_thread.start()

        # Register protocol handlers
        self.axon.attach(
            forward_fn=self.forward_video_task,
        )

        bt.logging.info("Video Miner initialized and ready to process tasks")

    async def forward(self, synapse: bt.Synapse) -> bt.Synapse:
        """
        Generic forward method, required by abstract class
        This method won't actually be called as we've registered specialized handlers for each request type
        """
        bt.logging.warning(
            f"Default forward method called with synapse type: {type(synapse)}"
        )
        return synapse

    async def forward_video_task(self, synapse: VideoTask) -> VideoTask:
        """Handle VideoTask requests"""
        bt.logging.info(f"Received VideoTask request: {synapse.task_id}")

        # Check IP blacklist
        is_blacklisted, reason = await self.blacklist(synapse)
        if is_blacklisted:
            bt.logging.warning(f"Request from blacklisted source: {reason}")
            synapse.status_code = 403
            synapse.error = f"Request rejected: {reason}"
            return synapse

        uid = self.metagraph.hotkeys.index(synapse.dendrite.hotkey)
        stake = self.metagraph.S[uid].item()
        network = self.config.subtensor.network
        if network != "test" and stake < 10000:
            bt.logging.warning(
                f"Blacklisting request from {synapse.dendrite.hotkey} [uid={uid}], not enough stake -- {stake}"
            )
            synapse.status_code = 403
            synapse.error = f"Stake below minimum: {stake}"
            return synapse

        if synapse.task_id and synapse.workflow_params:
            # Calculate task priority
            priority = await self.priority(synapse)

            # Check if upload URL is provided
            has_upload_url = bool(synapse.upload_url)

            if has_upload_url:
                bt.logging.info(
                    f"Received upload URL for task {synapse.task_id}: {synapse.upload_url}"
                )
                # Use the provided upload URL
                video_url = synapse.upload_url
                # Reserved for metadata URL
                metadata_url = ""
            else:
                bt.logging.info(f"No upload URL provided for task {synapse.task_id}")
                # Build default video URL
                video_url = ""
                metadata_url = ""

            # Add task to memory storage
            with self.task_lock:
                if synapse.task_id in self.tasks:
                    bt.logging.warning(f"Task {synapse.task_id} already exists")
                    synapse.status_code = 400
                    synapse.error = "Task already exists"
                    return synapse

                self.tasks[synapse.task_id] = {
                    "task_id": synapse.task_id,
                    "secret_key": synapse.secret_key,
                    "workflow_params": synapse.workflow_params,
                    "priority": priority,
                    "upload_url": synapse.upload_url,
                    "status": "pending",
                    "progress": 0,
                    "error_message": "",
                    "estimated_time": 0,
                    "created_at": time.time(),
                    "updated_at": time.time(),
                    "s3_video_url": video_url,
                    "s3_metadata_url": metadata_url,
                    "has_custom_upload": has_upload_url,
                    "output_file": None,
                }

                # Add to queue
                self.task_queue.put((1.0 / priority, synapse.task_id))

            # Set response
            synapse.status_code = 200
            synapse.video_url = video_url
            synapse.metadata_url = metadata_url

            bt.logging.info(f"VideoTask {synapse.task_id} accepted and queued")
            return synapse
        else:
            bt.logging.warning(
                "Invalid VideoTask request: missing task_id or workflow_params"
            )
            synapse.status_code = 400
            synapse.error = "Missing task_id or workflow_params"
            return synapse

    async def blacklist(self, synapse: bt.Synapse) -> typing.Tuple[bool, str]:
        """
        Decide whether to reject a specific task

        Args:
            synapse: Request containing task information

        Returns:
            Tuple[bool, str]: (whether to reject, rejection reason)
        """
        # Check IP blacklist
        if hasattr(synapse, "dendrite") and hasattr(synapse.dendrite, "ip"):
            sender_ip = synapse.dendrite.ip
            if sender_ip in self.ip_blacklist:
                return True, f"IP {sender_ip} is blacklisted"

        # Check workflow parameters
        if isinstance(synapse, VideoTask):
            # Check if workflow parameters are empty
            if not synapse.workflow_params:
                return True, "Empty workflow parameters"

        return False, ""

    async def priority(self, synapse: bt.Synapse) -> float:
        """
        Calculate task priority

        Args:
            synapse: Request containing task information

        Returns:
            float: Priority score, higher means higher priority
        """
        # Default priority
        priority = 1.0

        # Adjust priority based on sender's weight
        if hasattr(synapse, "dendrite") and hasattr(synapse.dendrite, "hotkey"):
            sender_uid = (
                self.metagraph.hotkeys.index(synapse.dendrite.hotkey)
                if synapse.dendrite.hotkey in self.metagraph.hotkeys
                else None
            )
            if sender_uid is not None:
                # Higher weight means higher priority
                weight = float(self.metagraph.S[sender_uid])
                priority *= 1.0 + weight

        return priority

    def _process_tasks(self):
        """Task processing thread"""
        while not self.should_exit:
            try:
                # Check if there are tasks in the queue
                if self.task_queue.empty():
                    time.sleep(1)
                    continue

                # Get next task from queue
                _, task_id = self.task_queue.get()

                with self.task_lock:
                    if task_id not in self.tasks:
                        continue

                    task = self.tasks[task_id]
                    workflow_params = task["workflow_params"]

                    # Update task status
                    task["status"] = "processing"
                    task["updated_at"] = time.time()

                bt.logging.info(f"Processing task {task_id}")

                # Call workflow parameters
                success = self._run_comfy_workflow(task_id, workflow_params)

                if not success:
                    with self.task_lock:
                        if task_id in self.tasks:
                            self.tasks[task_id]["status"] = "failed"
                            self.tasks[task_id][
                                "error_message"
                            ] = "Failed to run comfy workflow"
                            self.tasks[task_id]["updated_at"] = time.time()

                    bt.logging.error(f"Task {task_id} failed: comfy workflow error")
                    continue

                # Complete task
                with self.task_lock:
                    if task_id in self.tasks:
                        self.tasks[task_id]["status"] = "completed"
                        self.tasks[task_id]["progress"] = 100
                        self.tasks[task_id]["updated_at"] = time.time()

                        # Move to completed tasks
                        self.completed_tasks[task_id] = self.tasks[task_id]

                bt.logging.info(f"Task {task_id} completed")

            except Exception as e:
                bt.logging.error(f"Error processing task: {str(e)}")
                time.sleep(1)

    def _run_comfy_workflow(self, task_id, workflow_params):
        """Run comfy workflow

        Args:
            task_id: Task ID
            workflow_params: Complete workflow configuration

        Returns:
            bool: Whether successful
        """
        try:
            bt.logging.info(f"Running comfy workflow for task {task_id}")

            # Update task status to processing
            with self.task_lock:
                if task_id in self.tasks:
                    self.tasks[task_id]["status"] = "processing"
                    self.tasks[task_id]["updated_at"] = time.time()

            bt.logging.info(f"Executing ComfyUI workflow for task {task_id}")

            # Get task details including upload URL and content type
            with self.task_lock:
                if task_id not in self.tasks:
                    bt.logging.error(f"Task {task_id} not found in memory")
                    return False

                task_info = self.tasks[task_id]

            # Check if there's a custom upload URL
            has_custom_upload = task_info.get("has_custom_upload", False)
            upload_url = task_info.get("upload_url", "")

            # Execute ComfyUI workflow
            success, output_file, server_info, _ = self.comfy_api.execute_workflow(
                workflow_params, task_id
            )

            if not success:
                bt.logging.error(
                    f"ComfyUI workflow execution failed for task {task_id}"
                )
                with self.task_lock:
                    if task_id in self.tasks:
                        self.tasks[task_id]["status"] = "failed"
                        self.tasks[task_id][
                            "error_message"
                        ] = "ComfyUI workflow execution failed"
                        self.tasks[task_id]["updated_at"] = time.time()
                return False

            bt.logging.info(
                f"ComfyUI workflow executed successfully for task {task_id}, output file: {output_file}"
            )

            # If there's a custom upload URL, upload video to specified URL
            if has_custom_upload and upload_url:
                bt.logging.info(f"Uploading video to custom URL for task {task_id}...")

                # Maximum retry count
                max_retries = 3
                retry_count = 0
                upload_success = False

                while retry_count < max_retries and not upload_success:
                    try:
                        # Get correct ComfyUI server address from server_info
                        if (
                            server_info
                            and "host" in server_info
                            and "port" in server_info
                        ):
                            host = server_info["host"]
                            port = server_info["port"]
                            comfy_url = f"http://{host}:{port}"
                        else:
                            # If no server info is obtained, use environment variable or default value
                            comfy_url = os.environ.get(
                                "COMFYUI_HOST", "http://127.0.0.1:8188"
                            )

                        filename = os.path.basename(output_file)
                        video_download_url = (
                            f"{comfy_url}/view?filename={filename}&type=output"
                        )

                        bt.logging.info(f"Downloading video from ComfyUI")

                        # Increase timeout setting when downloading videos
                        video_data, status_code = http_get_request_sync(
                            video_download_url, timeout=120
                        )

                        if status_code != 200 or video_data is None:
                            bt.logging.error(
                                f"Failed to download video from ComfyUI: {status_code}"
                            )
                            retry_count += 1
                            time.sleep(2)
                            continue

                        # Check the size of the downloaded data.
                        data_size = len(video_data) if video_data else 0
                        bt.logging.info(f"Downloaded video size: {data_size} bytes")

                        if data_size == 0:
                            bt.logging.error("Downloaded video data is empty")
                            retry_count += 1
                            time.sleep(2)
                            continue

                        # Add timeout settings and retries when uploading videos.
                        bt.logging.info(
                            f"Uploading video to URL, size: {data_size} bytes"
                        )
                        response_text, status_code = http_put_request_sync(
                            upload_url, data=video_data, headers={}, timeout=180
                        )

                        if status_code in [200, 201, 204]:
                            bt.logging.info(
                                f"Video uploaded successfully for task {task_id}"
                            )
                            upload_success = True
                        else:
                            bt.logging.error(
                                f"Failed to upload video for task {task_id}: status code {status_code}, response: {response_text}"
                            )
                            retry_count += 1
                            time.sleep(5)
                    except Exception as e:
                        bt.logging.error(
                            f"Error uploading video for task {task_id}: {str(e)}"
                        )
                        bt.logging.error(traceback.format_exc())
                        retry_count += 1
                        time.sleep(5)

                if not upload_success:
                    bt.logging.error(
                        f"Failed to upload video after {max_retries} attempts for task {task_id}"
                    )
            else:
                bt.logging.info(
                    f"No custom upload URL for task {task_id} or URL is empty"
                )

            # Update task status to completed
            with self.task_lock:
                if task_id in self.tasks:
                    self.tasks[task_id]["status"] = "completed"
                    self.tasks[task_id]["progress"] = 100
                    self.tasks[task_id]["output_file"] = output_file
                    self.tasks[task_id]["updated_at"] = time.time()

            bt.logging.info(f"Task {task_id} completed successfully")
            return True

        except Exception as e:
            bt.logging.error(f"Error running comfy workflow: {str(e)}")
            bt.logging.error(traceback.format_exc())

            # Update task status to failed
            try:
                with self.task_lock:
                    if task_id in self.tasks:
                        self.tasks[task_id]["status"] = "failed"
                        self.tasks[task_id]["error_message"] = str(e)
                        self.tasks[task_id]["updated_at"] = time.time()
            except:
                pass

            return False


# The main function parses the configuration and runs the miner.
if __name__ == "__main__":
    with VideoMiner() as miner:
        while True:
            bt.logging.info(f"Miner running... {time.time()}")
            time.sleep(60)
