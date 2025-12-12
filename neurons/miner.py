import os
import time
import json
import threading
import asyncio
import typing
from queue import PriorityQueue
import bittensor as bt
import shutil

from neza.protocol import VideoTask, TaskStatusCheck

# Import ComfyUI WebSocket API module
from neza.api.comfy_ws_api import ComfyWSAPI

# import base miner class which takes care of most of the boilerplate
from neza.base.miner import BaseMinerNeuron

from neza.utils.http import (
    http_get_request_sync,
    http_put_request_sync,
    batch_download_and_package_outputs,
    register_miner_api_keys,
)
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
        # Initialize ComfyUI WebSocket API instance
        servers = _parse_env_servers(os.environ.get("COMFYUI_SERVERS", ""))
        self.comfy_api = ComfyWSAPI(servers, clear_queue=False)

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

        # Start API registration thread
        self.api_registration_thread = threading.Thread(
            target=self._maintain_api_registration, daemon=True
        )
        self.api_registration_thread.start()

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
        """Task processing thread - polls for task status"""
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

                # Submit task to ComfyUI and start polling
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
        """Run comfy workflow and batch process all outputs"""
        try:
            bt.logging.info(f"Running comfy workflow for task {task_id}")
            with self.task_lock:
                if task_id not in self.tasks:
                    bt.logging.error(f"Task {task_id} not found in memory")
                    return False
                task_info = self.tasks[task_id]
            has_custom_upload = task_info.get("has_custom_upload", False)
            upload_url = task_info.get("upload_url", "")
            # submit task to comfyui
            success, task_id, server_info = self.comfy_api.execute_workflow(
                workflow_params, task_id
            )
            if not success:
                bt.logging.error(f"Failed to submit task to ComfyUI: {task_id}")
                return False

            # Poll for task completion
            success = self._poll_task_completion(
                task_id, upload_url if has_custom_upload else None, server_info
            )
            if not success:
                bt.logging.error(f"Failed to poll for task completion: {task_id}")
                return False

            return True

        except Exception as e:
            bt.logging.error(f"Error submitting task {task_id}: {str(e)}")
            bt.logging.error(traceback.format_exc())
            try:
                with self.task_lock:
                    if task_id in self.tasks:
                        self.tasks[task_id]["status"] = "failed"
                        self.tasks[task_id]["error_message"] = str(e)
                        self.tasks[task_id]["updated_at"] = time.time()
            except:
                pass
            return False

    def _poll_task_completion(self, task_id, upload_url, server_info):
        """Poll for task completion status

        Args:
            task_id: Task ID
            upload_url: Custom upload URL if any
            server_info: Server info

        Returns:
            bool: Whether successful
        """
        try:
            bt.logging.info(f"Starting to poll for task completion: {task_id}")

            # Polling configuration
            max_poll_time = 3600  # 1 hour max
            poll_interval = 2  # 2 seconds
            start_time = time.time()

            while time.time() - start_time < max_poll_time:
                # Get task status from ComfyUI
                task_status = self.comfy_api.get_task_status(task_id, server_info["id"])

                if not task_status:
                    bt.logging.warning(
                        f"Task status not found for {task_id}, retrying..."
                    )
                    time.sleep(poll_interval)
                    continue

                # Update local task progress
                progress = task_status.get("progress", 0)
                with self.task_lock:
                    if task_id in self.tasks:
                        self.tasks[task_id]["progress"] = progress
                        self.tasks[task_id]["updated_at"] = time.time()

                # Check if task is completed
                if task_status.get("status") == "completed":
                    bt.logging.info(f"Task {task_id} completed successfully")

                    comfy_url = f"http://{server_info['host']}:{server_info['port']}"
                    token = server_info.get("token")

                    success, zip_path, error_msg = batch_download_and_package_outputs(
                        task_status.get("output_info", {}),
                        comfy_url,
                        task_id,
                        upload_url,
                        token=token,
                    )

                    if not success:
                        bt.logging.error(
                            f"Batch download and package failed: {error_msg}"
                        )
                        with self.task_lock:
                            if task_id in self.tasks:
                                self.tasks[task_id]["status"] = "failed"
                                self.tasks[task_id]["error_message"] = error_msg
                                self.tasks[task_id]["updated_at"] = time.time()
                        return False
                    # Update task status
                    with self.task_lock:
                        if task_id in self.tasks:
                            self.tasks[task_id]["status"] = "completed"
                            self.tasks[task_id]["progress"] = 100
                            self.tasks[task_id]["output_file"] = zip_path
                            self.tasks[task_id]["updated_at"] = time.time()

                    bt.logging.info(f"Task {task_id} completed successfully")
                    bt.logging.info(
                        f"Batch download and package outputs: {zip_path} and upload_url: {upload_url} upload_status: {success}"
                    )

                    return True

                # Check if task failed
                elif task_status.get("status") == "failed":
                    error_msg = task_status.get("error_message", "Unknown error")
                    bt.logging.error(f"Task {task_id} failed: {error_msg}")

                    with self.task_lock:
                        if task_id in self.tasks:
                            self.tasks[task_id]["status"] = "failed"
                            self.tasks[task_id]["error_message"] = error_msg
                            self.tasks[task_id]["updated_at"] = time.time()

                    return False

                # Task still processing, wait and continue
                time.sleep(poll_interval)

            # Timeout reached
            bt.logging.error(
                f"Task {task_id} polling timeout after {max_poll_time} seconds"
            )

            with self.task_lock:
                if task_id in self.tasks:
                    self.tasks[task_id]["status"] = "failed"
                    self.tasks[task_id]["error_message"] = "Task polling timeout"
                    self.tasks[task_id]["updated_at"] = time.time()

            return False

        except Exception as e:
            bt.logging.error(f"Error polling task {task_id}: {str(e)}")
            bt.logging.error(traceback.format_exc())

            with self.task_lock:
                if task_id in self.tasks:
                    self.tasks[task_id]["status"] = "failed"
                    self.tasks[task_id]["error_message"] = str(e)
                    self.tasks[task_id]["updated_at"] = time.time()

            return False

    def _upload_completed_video(self, task_id, upload_url):
        """Upload completed video to specified URL

        Args:
            task_id: Task ID
            upload_url: Upload URL

        Returns:
            bool: Whether upload successful
        """
        try:
            bt.logging.info(f"Uploading video for task {task_id} to {upload_url}")

            # Get task info
            with self.task_lock:
                if task_id not in self.tasks:
                    bt.logging.error(f"Task {task_id} not found for upload")
                    return False

                task_info = self.tasks[task_id]
                server_info = task_info.get("server_info", {})

            # Maximum retry count
            max_retries = 3
            retry_count = 0
            upload_success = False

            while retry_count < max_retries and not upload_success:
                try:
                    # Get correct ComfyUI server address from server_info
                    if server_info and "host" in server_info and "port" in server_info:
                        host = server_info["host"]
                        port = server_info["port"]
                        comfy_url = f"http://{host}:{port}"
                    else:
                        # If no server info is obtained, use environment variable or default value
                        comfy_url = os.environ.get(
                            "COMFYUI_HOST", "http://127.0.0.1:8188"
                        )

                    # Get output file path for the task
                    output_file = self.comfy_api.get_task_output_file(task_id)
                    if not output_file:
                        bt.logging.error(f"No output file found for task {task_id}")
                        retry_count += 1
                        time.sleep(2)
                        continue

                    bt.logging.info(f"Found output file: {output_file}")

                    filename = os.path.basename(output_file)
                    video_download_url = (
                        f"{comfy_url}/view?filename={filename}&type=output"
                    )

                    bt.logging.info(
                        f"Downloading video from ComfyUI: {video_download_url}"
                    )

                    headers = {}
                    token = server_info.get("token") if server_info else None
                    if token:
                        headers["Authorization"] = f"Bearer {token}"

                    # Download video from ComfyUI
                    video_data, status_code = http_get_request_sync(
                        video_download_url, timeout=120, headers=headers
                    )

                    if status_code != 200 or video_data is None:
                        bt.logging.error(
                            f"Failed to download video from ComfyUI: {status_code}"
                        )
                        retry_count += 1
                        time.sleep(2)
                        continue

                    # Check the size of the downloaded data
                    data_size = len(video_data) if video_data else 0
                    bt.logging.info(f"Downloaded video size: {data_size} bytes")

                    if data_size == 0:
                        bt.logging.error("Downloaded video data is empty")
                        retry_count += 1
                        time.sleep(2)
                        continue

                    # Upload video to specified URL
                    bt.logging.info(f"Uploading video to URL, size: {data_size} bytes")
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
                return False

            return True

        except Exception as e:
            bt.logging.error(
                f"Error in _upload_completed_video for task {task_id}: {str(e)}"
            )
            bt.logging.error(traceback.format_exc())
            return False

    def cleanup_comfy_tasks(self):
        """Clean up completed ComfyUI tasks"""
        try:
            # Clean up completed tasks from ComfyUI API
            cleaned_count = self.comfy_api.cleanup_completed_tasks(max_age_hours=1)
            if cleaned_count > 0:
                bt.logging.info(f"Cleaned up {cleaned_count} completed ComfyUI tasks")
        except Exception as e:
            bt.logging.error(f"Error cleaning up ComfyUI tasks: {str(e)}")

    def get_comfy_server_status(self):
        """Get status of all ComfyUI servers"""
        try:
            status_info = {}
            for server in self.comfy_api.servers:
                server_id = server["id"]
                status = self.comfy_api.get_server_status(server_id)
                if status:
                    status_info[server_id] = status

            return status_info
        except Exception as e:
            bt.logging.error(f"Error getting ComfyUI server status: {str(e)}")
            return {}

    def _maintain_api_registration(self):
        """Register API keys with Owner on startup"""
        try:
            # Load channels from .keys.json
            channels = None
            keys_json_path = os.path.join(os.getcwd(), ".keys.json")
            if os.path.exists(keys_json_path):
                try:
                    with open(keys_json_path, "r", encoding="utf-8") as f:
                        channels = json.load(f)
                except Exception as e:
                    bt.logging.error(f"Error loading .keys.json: {str(e)}")

            api_models_str = os.environ.get("API_MODELS", "").strip()
            api_models = {}
            if api_models_str:
                try:
                    groups = api_models_str.split(";")
                    for group in groups:
                        group = group.strip()
                        if not group:
                            continue
                        if "|" not in group:
                            bt.logging.warning(
                                f"Invalid format in group: {group}, missing '|' separator"
                            )
                            continue

                        parts = group.split("|", 1)
                        if len(parts) != 2:
                            bt.logging.warning(
                                f"Invalid format in group: {group}, expected 'models|keys'"
                            )
                            continue

                        models_part, keys_part = parts
                        model_names = [
                            m.strip() for m in models_part.split(",") if m.strip()
                        ]
                        keys = [k.strip() for k in keys_part.split(",") if k.strip()]

                        if not model_names or not keys:
                            bt.logging.warning(
                                f"Invalid format in group: {group} (empty models or keys)"
                            )
                            continue

                        for model_name in model_names:
                            if model_name in api_models:
                                existing_keys = api_models[model_name]
                                for key in keys:
                                    if key not in existing_keys:
                                        existing_keys.append(key)
                                bt.logging.info(
                                    f"Added keys to existing model '{model_name}': {keys}"
                                )
                            else:
                                api_models[model_name] = keys.copy()

                    if api_models:
                        bt.logging.info(
                            f"Loaded {len(api_models)} models from API_MODELS"
                        )
                except Exception as e:
                    bt.logging.error(f"Error parsing API_MODELS: {str(e)}")

            if not channels and not api_models:
                bt.logging.info("No API keys found, skipping registration")
                return

            # Register API keys
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                success = loop.run_until_complete(
                    register_miner_api_keys(
                        self.wallet,
                        channels=channels,
                        models_dict=api_models if api_models else None,
                    )
                )
                if success:
                    bt.logging.info("Successfully registered API keys with Owner")
                else:
                    bt.logging.warning("Failed to register API keys with Owner")
            finally:
                loop.close()

        except Exception as e:
            bt.logging.error(f"Error in API registration: {str(e)}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up resources when miner is shut down"""
        try:
            # Shutdown ComfyUI API
            if hasattr(self, "comfy_api"):
                self.comfy_api.shutdown()
                bt.logging.info("ComfyUI API shutdown completed")
        except Exception as e:
            bt.logging.error(f"Error during miner shutdown: {str(e)}")

        # Call parent cleanup
        super().__exit__(exc_type, exc_val, exc_tb)


# The main function parses the configuration and runs the miner.
if __name__ == "__main__":
    with VideoMiner() as miner:
        while True:
            bt.logging.info(f"Miner running... {time.time()}")
            time.sleep(60)
