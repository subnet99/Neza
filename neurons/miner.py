import os
import time
import json
import threading
import asyncio
import typing
from queue import PriorityQueue
import bittensor as bt
import socket
import traceback
import urllib.parse

from neza.protocol import VideoTask, ComfySupport

# Import ComfyUI WebSocket API module
from neza.api.comfy_ws_api import ComfyWSAPI

# import base miner class which takes care of most of the boilerplate
from neza.base.miner import BaseMinerNeuron

from neza.utils.http import (
    http_get_request_sync,
    http_put_request_sync,
    batch_download_and_package_outputs,
    cleanup_task_files,
    register_miner_api_keys,
)
from neza.utils.tools import _parse_env_servers
from neza.utils.proxy_auth import ProxyAuthManager
from neza.utils.task_mapping_manager import TaskMappingManager
from neza.utils.ws_proxy import WSProxyService
from neza.utils.miner_http_server import MinerHTTPServer


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

        self.proxy_auth = ProxyAuthManager(self.wallet.hotkey.ss58_address)

        self.task_mapping_manager = TaskMappingManager(
            hotkey=self.wallet.hotkey.ss58_address,
            cleanup_interval=int(os.getenv("TASK_MAPPING_CLEANUP_INTERVAL", "300")),
            default_expiration=int(os.getenv("TASK_MAPPING_EXPIRATION", "7200")),
        )

        self.proxy_port = self.config.axon.port + 1
        self.http_port = self.config.axon.port + 2

        self.ws_proxy_service = WSProxyService(
            self.task_mapping_manager,
            self.proxy_auth,
            proxy_port=self.proxy_port,
            comfy_api=self.comfy_api,
        )

        # Register message callback for proxy forwarding
        self.comfy_api.add_message_callback(self.ws_proxy_service.forward_message_sync)

        self.http_server = MinerHTTPServer(
            self.task_mapping_manager, port=self.http_port
        )

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

        # Start WebSocket proxy service
        self._start_proxy_service()
        # Start API registration thread
        self.api_registration_thread = threading.Thread(
            target=self._maintain_api_registration, daemon=True
        )
        self.api_registration_thread.start()

        # Register protocol handlers
        self.axon.attach(
            forward_fn=self.forward_video_task,
        )
        self.axon.attach(
            forward_fn=self.forward_comfy_support,
        )

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
            video_url = synapse.upload_url or ""
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
                    "has_custom_upload": bool(synapse.upload_url),
                    "output_file": None,
                    "prompt_id": None,
                    "server_info": None,
                }

                best_server = self.comfy_api._get_best_server()

                if not best_server:
                    bt.logging.error("No available ComfyUI servers")
                    synapse.status_code = 500
                    synapse.error = "No available ComfyUI servers"
                    return synapse

                worker_id = self.comfy_api.get_worker_id_by_server(best_server)

                if worker_id is None:
                    bt.logging.error(
                        f"Could not find worker_id for server {best_server['id']}"
                    )
                    synapse.status_code = 500
                    synapse.error = "Could not find worker_id for server"
                    return synapse

                success, prompt_id, server_info = self.comfy_api.execute_workflow(
                    synapse.workflow_params, synapse.task_id, worker_id
                )

                if not success or not prompt_id:
                    bt.logging.error(
                        f"Failed to submit task {synapse.task_id} to ComfyUI"
                    )
                    synapse.status_code = 500
                    synapse.error = "Failed to submit task to ComfyUI"
                    return synapse

                comfy_instance = f"{server_info['host']}:{server_info['port']}"

                client_id = server_info.get("client_id")
                token = server_info.get("token")
                self.task_mapping_manager.add_mapping(
                    task_id=synapse.task_id,
                    prompt_id=prompt_id,
                    comfy_instance=comfy_instance,
                    client_id=client_id,
                    token=token,
                )

                self.tasks[synapse.task_id]["status"] = "submitted"
                self.tasks[synapse.task_id]["prompt_id"] = prompt_id
                self.tasks[synapse.task_id]["server_info"] = server_info
                self.tasks[synapse.task_id]["updated_at"] = time.time()
                self.task_queue.put((1.0 / priority, synapse.task_id))

            proxy_info = self._generate_proxy_connection_info(synapse.task_id)

            synapse.status_code = 200
            synapse.video_url = video_url
            synapse.metadata_url = metadata_url

            if proxy_info:
                synapse.proxy_comfy_url = proxy_info.get("proxy_url", "")
                synapse.proxy_signature = proxy_info.get("credential", "")
                synapse.proxy_task_id = proxy_info.get("task_id", "")
                synapse.prompt_id = prompt_id
                synapse.http_view_url = proxy_info.get("http_view_url", "")
            else:
                bt.logging.warning(
                    f"Failed to generate proxy info for task {synapse.task_id}"
                )
            return synapse
        else:
            bt.logging.warning(
                "Invalid VideoTask request: missing task_id or workflow_params"
            )
            synapse.status_code = 400
            synapse.error = "Missing task_id or workflow_params"
            return synapse

    async def forward_comfy_support(self, synapse: ComfySupport) -> ComfySupport:
        """Handle ComfySupport query requests"""
        servers = getattr(self.comfy_api, "servers", None) if self.comfy_api else None
        synapse.supports_comfy = bool(
            servers and sum(1 for s in servers if s.get("available", False)) > 0
        )
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
        sender_ip = getattr(getattr(synapse, "dendrite", None), "ip", None)
        if sender_ip and sender_ip in self.ip_blacklist:
            return True, f"IP {sender_ip} is blacklisted"

        # Check workflow parameters
        if isinstance(synapse, VideoTask) and not synapse.workflow_params:
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
        priority = 1.0

        # Adjust priority based on sender's weight
        hotkey = getattr(getattr(synapse, "dendrite", None), "hotkey", None)
        if hotkey and hotkey in self.metagraph.hotkeys:
            sender_uid = self.metagraph.hotkeys.index(hotkey)
            weight = float(self.metagraph.S[sender_uid])
            priority *= 1.0 + weight

        return priority

    def _update_task_status(self, task_id, status=None, error_message=None, **kwargs):
        """Update task status with lock protection"""
        with self.task_lock:
            if task_id not in self.tasks:
                return False
            if status:
                self.tasks[task_id]["status"] = status
                if status in ["completed", "failed"]:
                    try:
                        self.task_mapping_manager.update_status(task_id, status)
                    except Exception as e:
                        bt.logging.error(f"Error updating mapping status: {e}")
            if error_message is not None:
                self.tasks[task_id]["error_message"] = error_message
            self.tasks[task_id]["updated_at"] = time.time()
            for key, value in kwargs.items():
                self.tasks[task_id][key] = value
            return True

    def _process_tasks(self):
        """Task processing thread - polls for task status"""
        while not self.should_exit:
            try:
                # Check if there are tasks in the queue
                if self.task_queue.empty():
                    time.sleep(1)
                    continue

                _, task_id = self.task_queue.get()
                success = self._run_comfy_workflow(task_id)

                if not success:
                    self._update_task_status(
                        task_id,
                        status="failed",
                        error_message="Failed to run comfy workflow",
                    )
                    bt.logging.error(f"Task {task_id} failed: comfy workflow error")
                    continue

                # Complete task
                self._update_task_status(task_id, status="completed", progress=100)
                with self.task_lock:
                    if task_id in self.tasks:
                        self.completed_tasks[task_id] = self.tasks[task_id]

            except Exception as e:
                bt.logging.error(f"Error processing task: {str(e)}")
                time.sleep(1)

    def _run_comfy_workflow(self, task_id):
        """Run comfy workflow and batch process all outputs"""
        try:
            with self.task_lock:
                if task_id not in self.tasks:
                    return False
                task_info = self.tasks[task_id]

                if task_info.get("status") == "submitted" and task_info.get(
                    "prompt_id"
                ):
                    server_info = task_info["server_info"]
                    has_custom_upload = task_info.get("has_custom_upload", False)
                    upload_url = task_info.get("upload_url", "")
                else:
                    return False

            success = self._poll_task_completion(
                task_id, upload_url if has_custom_upload else None, server_info
            )
            cleanup_task_files(task_id)
            return success

        except Exception as e:
            bt.logging.error(f"Error submitting task {task_id}: {str(e)}")
            bt.logging.error(traceback.format_exc())
            self._update_task_status(task_id, status="failed", error_message=str(e))
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
                self._update_task_status(task_id, progress=progress)

                if task_status.get("status") == "completed":
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
                        self._update_task_status(
                            task_id, status="failed", error_message=error_msg
                        )
                        return False
                    # Update task status
                    self._update_task_status(
                        task_id, status="completed", progress=100, output_file=zip_path
                    )

                    return True

                # Check if task failed
                elif task_status.get("status") == "failed":
                    error_msg = task_status.get("error_message", "Unknown error")
                    bt.logging.error(f"Task {task_id} failed: {error_msg}")
                    self._update_task_status(
                        task_id, status="failed", error_message=error_msg
                    )
                    return False

                # Task still processing, wait and continue
                time.sleep(poll_interval)

            # Timeout reached
            bt.logging.error(
                f"Task {task_id} polling timeout after {max_poll_time} seconds"
            )
            self._update_task_status(
                task_id, status="failed", error_message="Task polling timeout"
            )
            return False

        except Exception as e:
            bt.logging.error(f"Error polling task {task_id}: {str(e)}")
            bt.logging.error(traceback.format_exc())
            self._update_task_status(task_id, status="failed", error_message=str(e))
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
                    host = server_info.get("host") if server_info else None
                    port = server_info.get("port") if server_info else None
                    if host and port:
                        comfy_url = f"http://{host}:{port}"
                    else:
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

                    filename = os.path.basename(output_file)
                    video_download_url = (
                        f"{comfy_url}/view?filename={filename}&type=output"
                    )
                    headers = {}
                    token = server_info.get("token") if server_info else None
                    if token:
                        headers["Authorization"] = f"Bearer {token}"

                    # Download video from ComfyUI
                    video_data, status_code = http_get_request_sync(
                        video_download_url, timeout=120, headers=headers
                    )

                    if status_code != 200 or not video_data:
                        bt.logging.error(
                            f"Failed to download video from ComfyUI: status={status_code}, data_empty={not video_data}"
                        )
                        retry_count += 1
                        time.sleep(2)
                        continue

                    response_text, status_code = http_put_request_sync(
                        upload_url, data=video_data, headers={}, timeout=180
                    )

                    if status_code in [200, 201, 204]:
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
            self.comfy_api.cleanup_completed_tasks(max_age_hours=1)
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

    def _start_proxy_service(self):
        """Start the WebSocket proxy and HTTP services"""
        try:
            # Start WebSocket proxy service in a separate thread
            ws_proxy_thread = threading.Thread(
                target=self._run_ws_proxy_service, daemon=True
            )
            ws_proxy_thread.start()
            http_server_thread = threading.Thread(
                target=self._run_http_server, daemon=True
            )
            http_server_thread.start()
        except Exception as e:
            bt.logging.error(f"Failed to start services: {str(e)}")

    def _run_ws_proxy_service(self):
        """Run the WebSocket proxy service"""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Start WebSocket proxy service
            loop.run_until_complete(self.ws_proxy_service.start())

            # Keep the loop running
            loop.run_forever()
        except Exception as e:
            bt.logging.error(f"WebSocket proxy service error: {str(e)}")
        finally:
            loop.close()

    def _run_http_server(self):
        """Run the HTTP server"""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.http_server.start())
            loop.run_forever()
        except Exception as e:
            bt.logging.error(f"HTTP server error: {str(e)}")
        finally:
            loop.close()

    def _generate_proxy_connection_info(self, task_id: str) -> dict:
        """
        Generate proxy connection information for validator

        Args:
            task_id: Task ID

        Returns:
            dict: Proxy connection information
        """
        try:
            public_ip = self._get_public_ip()

            mapping = self.task_mapping_manager.get_mapping(task_id)
            if not mapping:
                bt.logging.error(f"No mapping found for task {task_id}")
                return {}

            prompt_id = mapping.get("prompt_id")
            comfy_instance = mapping.get("comfy_instance")
            client_id = mapping.get("client_id")

            if not prompt_id or not comfy_instance:
                bt.logging.error(f"Invalid mapping for task {task_id}")
                return {}

            proxy_info = self.proxy_auth.create_simple_proxy_connection_info(
                task_id=task_id,
                client_id=client_id,
            )

            encoded_credential = urllib.parse.quote(proxy_info["credential"])
            proxy_info["proxy_url"] = (
                f"ws://{public_ip}:{self.proxy_port}/{task_id}/{encoded_credential}"
            )

            proxy_info["http_view_url"] = (
                f"http://{public_ip}:{self.http_port}/view/{task_id}"
            )
            return proxy_info

        except Exception as e:
            bt.logging.error(f"Error generating proxy connection info: {str(e)}")
            return {}

    def _get_public_ip(self) -> str:
        """Get public IP address for proxy service"""
        config_ip = getattr(self.config, "axon", {}).get("external_ip", None)
        if config_ip and config_ip != "0.0.0.0":
            return config_ip

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            s.close()
            return local_ip
        except Exception:
            return "127.0.0.1"

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
                            else:
                                api_models[model_name] = keys.copy()
                except Exception as e:
                    bt.logging.error(f"Error parsing API_MODELS: {str(e)}")

            if not channels and not api_models:
                return

            # Register API keys
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(
                    register_miner_api_keys(
                        self.wallet,
                        channels=channels,
                        models_dict=api_models if api_models else None,
                    )
                )
            finally:
                loop.close()

        except Exception as e:
            bt.logging.error(f"Error in API registration: {str(e)}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up resources when miner is shut down"""
        ws_proxy = getattr(self, "ws_proxy_service", None)
        if ws_proxy:
            try:
                asyncio.run(ws_proxy.stop())
            except Exception as e:
                bt.logging.error(
                    f"Error shutting down WebSocket proxy service: {str(e)}"
                )

        http_server = getattr(self, "http_server", None)
        if http_server:
            try:
                asyncio.run(http_server.stop())
            except Exception as e:
                bt.logging.error(f"Error shutting down HTTP server: {str(e)}")

        comfy_api = getattr(self, "comfy_api", None)
        if comfy_api:
            try:
                comfy_api.shutdown()
            except Exception as e:
                bt.logging.error(f"Error during miner shutdown: {str(e)}")

        super().__exit__(exc_type, exc_val, exc_tb)


# The main function parses the configuration and runs the miner.
if __name__ == "__main__":
    with VideoMiner() as miner:
        while True:
            time.sleep(60)
