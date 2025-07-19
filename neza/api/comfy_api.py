import time
import json
import uuid
import os
import traceback
import threading
import requests
import bittensor as bt
from typing import List, Dict, Any, Tuple, Optional


class ComfyAPI:
    """
    ComfyUI API wrapper class
    """

    def __init__(self, servers: List[Dict[str, str]] = None, clear_queue=False):
        """
        Initialize ComfyAPI class

        Args:
            servers: List of ComfyUI servers, format: [{"host": "127.0.0.1", "port": "8188"}, ...]
                    If None, servers will be retrieved from environment variables
        """
        self.servers = []
        # Process each server configuration
        for server in servers:
            host = server.get("host", "127.0.0.1")
            port = server.get("port", "8188")

            # Ensure host doesn't include protocol prefix
            if host.startswith("http://"):
                host = host[7:]
            elif host.startswith("https://"):
                host = host[8:]

            # Check if hostname contains port
            if ":" in host:
                host_parts = host.split(":")
                host = host_parts[0]
                if len(host_parts) > 1 and host_parts[1]:
                    port = host_parts[1]

            self.servers.append(
                {
                    "host": host,
                    "port": port,
                    "client_id": str(uuid.uuid4()),
                    "task_results": {},  # Task result dictionary, prompt_id -> result
                    "available": False,  # Whether server is available
                }
            )

        # Check if ComfyUI servers are available
        for server in self.servers:
            self._check_server_availability(server, clear_queue)

    def _check_server_availability(
        self, server: Dict[str, Any], clear_queue: bool = False
    ) -> None:
        """
        Check if a ComfyUI server is available by attempting to connect to its queue API endpoint.
        """
        try:
            host_for_http = server["host"]
            if not host_for_http.startswith(("http://", "https://")):
                host_for_http = f"http://{host_for_http}"

            # Add timeout setting
            response = requests.get(
                f"{host_for_http}:{server['port']}/queue", timeout=30
            )
            if response.status_code == 200:
                bt.logging.info(f"ComfyUI server ***:{server['port']} is available.")
                server["available"] = True
                if clear_queue:
                    self.clear_server_queue(server)
                    self.interrupt_server_processing(server)
            else:
                bt.logging.warning(
                    f"ComfyUI server ***:{server['port']} is not available. Status code: {response.status_code}"
                )
                server["available"] = False
        except requests.exceptions.RequestException as e:
            bt.logging.warning(
                f"ComfyUI server ***:{server['port']} is not available due to network error: {str(e)}"
            )
            server["available"] = False
        except Exception as e:
            bt.logging.warning(
                f"ComfyUI server ***:{server['port']} is not available due to an unexpected error: {str(e)}"
            )
            server["available"] = False

    def _get_server_load(self, server: Dict[str, Any]) -> float:
        """
        Get current queue size of the server

        Args:
            server: Server configuration

        Returns:
            float: Queue size, or infinity if unable to get
        """
        try:
            host_for_http = server["host"]
            if not host_for_http.startswith(("http://", "https://")):
                host_for_http = f"http://{host_for_http}"

            # Add timeout setting
            response = requests.get(
                f"{host_for_http}:{server['port']}/queue", timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                # Return the number of tasks in the queue
                queue_running = data.get("queue_running", 0)
                queue_pending = data.get("queue_pending", 0)

                # Ensure returned values are numeric
                if isinstance(queue_running, list):
                    queue_running = len(queue_running)
                elif not isinstance(queue_running, (int, float)):
                    print(
                        f"Unexpected queue_running type: {type(queue_running)}, value: {queue_running}"
                    )
                    queue_running = 0

                if isinstance(queue_pending, list):
                    queue_pending = len(queue_pending)
                elif not isinstance(queue_pending, (int, float)):
                    print(
                        f"Unexpected queue_pending type: {type(queue_pending)}, value: {queue_pending}"
                    )
                    queue_pending = 0

                return float(queue_running + queue_pending)
            else:
                print(f"Failed to get queue info: {response.status_code}")
                return float("inf")  # Return infinity to indicate server is unavailable
        except Exception as e:
            print(f"Error getting queue info: {str(e)}")
            print(traceback.format_exc())
            return float("inf")

    def _get_server_by_worker_id(
        self, worker_id: int = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get server by worker ID
        """
        if worker_id is None:
            return self._get_best_server()
        else:
            return self.servers[worker_id]

    def _get_best_server(self) -> Optional[Dict[str, Any]]:
        """
        Get server with lowest load

        Returns:
            Optional[Dict[str, Any]]: Server configuration with lowest load, or None if no servers available
        """
        min_load = float("inf")
        best_server = None

        for server in self.servers:
            # If server is not available, try to check availability again
            if not server["available"]:
                self._check_server_availability(server)

            # If server is available, check load
            if server["available"]:
                load = self._get_server_load(server)
                if load < min_load:
                    min_load = load
                    best_server = server

        return best_server

    def get_video(
        self, server: Dict[str, Any], prompt_id: str, timeout: int = 1800
    ) -> Tuple[str, Optional[float]]:
        """
        Get video generation result using periodic polling of history instead of WebSocket events

        Args:
            server: Server configuration
            prompt_id: Prompt ID
            timeout: Timeout in seconds (default 1800s = 30min)

        Returns:
            str: Video filename, empty string if failed
        """
        # If result is already in task_results, return it directly
        if prompt_id in server["task_results"]:
            bt.logging.debug(f"Found video result in cache for prompt_id: {prompt_id}")
            return server["task_results"][prompt_id]

        # Calculate number of polling attempts based on timeout
        # Poll every 30 seconds, with a maximum of 60 attempts (30 minutes)
        poll_interval = 30  # seconds
        max_attempts = min(60, max(1, int(timeout / poll_interval)))

        bt.logging.info(
            f"Starting polling for result of prompt_id: {prompt_id}, will check every {poll_interval}s for up to {max_attempts} times"
        )

        # Poll history at regular intervals
        for attempt in range(max_attempts):
            try:
                # Sleep first to give time for the task to start processing
                if attempt > 0:  # Don't sleep on first attempt
                    bt.logging.debug(
                        f"Polling attempt {attempt+1}/{max_attempts} for prompt_id: {prompt_id}"
                    )
                    time.sleep(poll_interval)

                # Get history
                history_result, execution_time = self.get_history(server, prompt_id)
                if history_result:
                    bt.logging.info(
                        f"Found result in history for prompt_id: {prompt_id} on attempt {attempt+1}/{max_attempts}"
                    )
                    server["task_results"][prompt_id] = history_result[0]
                    return history_result[0], execution_time

                # Check if server is available
                if not server["available"]:
                    bt.logging.warning(
                        f"Server ***:{server['port']} not available, checking availability"
                    )
                    self._check_server_availability(server)

            except Exception as e:
                bt.logging.error(f"Error during polling attempt {attempt+1}: {str(e)}")
                # Continue polling despite errors

        # If we get here, all polling attempts failed
        bt.logging.error(
            f"Failed to get video result for prompt_id: {prompt_id} after {max_attempts} polling attempts"
        )

        # Final attempt to get history
        try:
            bt.logging.info(
                f"Making final attempt to get history for prompt_id: {prompt_id}"
            )
            history_result, execution_time = self.get_history(server, prompt_id)
            if history_result:
                bt.logging.info(
                    f"Found result in history on final attempt for prompt_id: {prompt_id}"
                )
                server["task_results"][prompt_id] = history_result[0]
                return history_result[0], execution_time
        except Exception as e:
            bt.logging.error(f"Error in final history check: {str(e)}")

        return "", None

    def get_history(
        self, server: Dict[str, Any], prompt_id: str
    ) -> Tuple[List[str], Optional[float]]:
        """
        Get video filenames from history and extract execution time if available

        Args:
            server: Server configuration
            prompt_id: Prompt ID

        Returns:
            Tuple[List[str], Optional[float]]: (List of video filenames, execution time in seconds if available)
        """
        try:
            # Ensure host doesn't include protocol prefix for HTTP requests
            host_for_http = server["host"]
            if not host_for_http.startswith(("http://", "https://")):
                host_for_http = f"http://{host_for_http}"

            # Add timeout setting
            response = requests.get(
                f"{host_for_http}:{server['port']}/history/{prompt_id}", timeout=60
            )
            if response.status_code == 200:
                response_data = response.json()

                # Skip if history is None
                if prompt_id not in response_data:
                    bt.logging.info(f"History is None for prompt_id: {prompt_id}")
                    return [], None

                history = response_data[prompt_id]

                execution_time = None
                if "status" in history and "messages" in history["status"]:
                    messages = history["status"]["messages"]
                    start_timestamp = None
                    end_timestamp = None

                    for msg in messages:
                        if len(msg) >= 2:
                            msg_type = msg[0]
                            msg_data = msg[1]

                            if (
                                msg_type == "execution_start"
                                and "timestamp" in msg_data
                            ):
                                start_timestamp = msg_data["timestamp"]
                            elif (
                                msg_type == "execution_success"
                                and "timestamp" in msg_data
                            ):
                                end_timestamp = msg_data["timestamp"]

                    if start_timestamp is not None and end_timestamp is not None:
                        execution_time = (end_timestamp - start_timestamp) / 1000.0
                        bt.logging.info(
                            f"ComfyUI Execution time: {execution_time:.2f}s for prompt_id: {prompt_id}"
                        )

                # Extract all gif filenames
                filenames = []
                for node_id in history["outputs"]:
                    node_output = history["outputs"][node_id]
                    if "gifs" in node_output:
                        for gif in node_output["gifs"]:
                            filenames.append(gif["filename"])

                return filenames, execution_time
            else:
                bt.logging.info(f"Failed to get history: {response.status_code}")
                return [], None
        except Exception as e:
            bt.logging.error(f"Failed to get history: {str(e)}")
            return [], None

    def execute_comfy_workflow(
        self, workflow: Dict[str, Any], task_id: str = None, worker_id: int = None
    ) -> Tuple[bool, str, Dict[str, Any], Optional[float]]:
        """
        Execute ComfyUI workflow and return output filename

        Args:
            workflow: Complete workflow configuration
            task_id: Task ID, used for logging
            worker_id: Worker ID for server selection

        Returns:
            Tuple[bool, str, Dict[str, Any]]: (whether successful, output filename, server info), returns (False, "", None) if failed
        """
        # Get server with lowest load
        server = self._get_server_by_worker_id(worker_id)

        if server is None:
            bt.logging.error("No available ComfyUI servers")
            return False, "", None, None

        # Double-check server availability
        if not server["available"]:
            bt.logging.warning(
                f"Server ***:{server['port']} not available, checking availability"
            )
            self._check_server_availability(server)
            if not server["available"]:
                bt.logging.error(f"Server ***:{server['port']} is still not available")
                return False, "", None, None

        try:
            # Ensure host doesn't include protocol prefix for HTTP requests
            host_for_http = server["host"]
            if not host_for_http.startswith(("http://", "https://")):
                host_for_http = f"http://{host_for_http}"

            # Log task information
            task_log = f"task_id: {task_id}" if task_id else "no task_id"
            bt.logging.info(
                f"Executing ComfyUI workflow ({task_log}) on server ***:{server['port']}"
            )

            # Add timeout setting
            try:
                start_time = time.time()
                response = requests.post(
                    f"{host_for_http}:{server['port']}/prompt",
                    json={
                        "prompt": workflow,
                        "client_id": server["client_id"],
                    },
                    timeout=30,
                )

                if response.status_code != 200:
                    bt.logging.error(
                        f"ComfyUI server ***:{server['port']} returned error status code: {response.status_code}"
                    )
                    bt.logging.error(f"Response content: {response.text}")
                    # Mark server as unavailable if it returns an error
                    server["available"] = False
                    return False, "", None, None

                response_json = response.json()
                if "prompt_id" not in response_json:
                    bt.logging.error(
                        f"Missing prompt_id in ComfyUI response: {response_json}"
                    )
                    return False, "", None, None

                prompt_id = response_json["prompt_id"]
                bt.logging.info(
                    f"Workflow submitted successfully, prompt_id: {prompt_id}"
                )

            except requests.exceptions.RequestException as e:
                bt.logging.error(f"Error sending request to ComfyUI server: {str(e)}")
                # Mark server as unavailable if there's a connection error
                server["available"] = False
                return False, "", None, None
            except json.JSONDecodeError as e:
                bt.logging.error(f"Error parsing ComfyUI response: {str(e)}")
                bt.logging.error(f"Response content: {response.text}")
                return False, "", None, None

            # Get video result using polling
            bt.logging.info(f"Waiting for video result, prompt_id: {prompt_id}")
            video_result, execution_time = self.get_video(server, prompt_id)

            if not video_result:
                bt.logging.error(
                    f"Failed to get video result from ComfyUI, prompt_id: {prompt_id}"
                )
                # Try to get from history one more time with increased timeout
                bt.logging.info("Making final attempt to get result from history")
                history_result, comfy_execution_time = self.get_history(
                    server, prompt_id
                )
                if history_result:
                    elapsed_time = time.time() - start_time
                    if comfy_execution_time is not None:
                        elapsed_time = comfy_execution_time
                    # Return server info
                    server_info = {"host": server["host"], "port": server["port"]}
                    return True, history_result[0], server_info, comfy_execution_time

                elapsed_time = time.time() - start_time
                bt.logging.error(
                    f"Workflow execution failed after {elapsed_time:.2f}s, prompt_id: {prompt_id}"
                )
                return False, "", None, None

            # Success
            elapsed_time = (
                execution_time if execution_time else (time.time() - start_time)
            )
            bt.logging.info(
                f"Workflow execution completed successfully in {elapsed_time:.2f}s, prompt_id: {prompt_id}"
            )

            # Return server info
            server_info = {"host": server["host"], "port": server["port"]}
            return True, video_result, server_info, execution_time

        except Exception as e:
            bt.logging.error(f"Error executing ComfyUI workflow: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False, "", None, None

    def execute_workflow(
        self, workflow: Dict[str, Any], task_id: str = None
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Alias method for execute_comfy_workflow, maintains backward compatibility

        Args:
            workflow: Complete workflow configuration
            task_id: Task ID, used for logging

        Returns:
            Tuple[bool, str, Dict[str, Any]]: (whether successful, output filename, server info)
        """
        return self.execute_comfy_workflow(workflow, task_id)

    def clear_server_queue(self, server: Dict[str, Any]) -> bool:
        """
        Clear the queue of a ComfyUI server

        Args:
            server: Server configuration

        Returns:
            bool: Whether clearing was successful
        """
        try:
            host_for_http = server["host"]
            if not host_for_http.startswith(("http://", "https://")):
                host_for_http = f"http://{host_for_http}"

            response = requests.post(
                f"{host_for_http}:{server['port']}/queue",
                json={"clear": True},
                timeout=20,
            )

            if response.status_code == 200:
                bt.logging.info(
                    f"Successfully cleared queue for server ***:{server['port']}"
                )
                return True
            else:
                bt.logging.warning(
                    f"Failed to clear queue for server ***:{server['port']}, status code: {response.status_code}"
                )
                return False

        except Exception as e:
            bt.logging.error(
                f"Error clearing queue for server ***:{server['port']}: {str(e)}"
            )
            return False

    def interrupt_server_processing(self, server: Dict[str, Any]) -> bool:
        """
        Interrupt current processing on a ComfyUI server

        Args:
            server: Server configuration

        Returns:
            bool: Whether interruption was successful
        """
        try:
            host_for_http = server["host"]
            if not host_for_http.startswith(("http://", "https://")):
                host_for_http = f"http://{host_for_http}"

            response = requests.post(
                f"{host_for_http}:{server['port']}/interrupt", timeout=20
            )

            if response.status_code == 200:
                bt.logging.info(
                    f"Successfully interrupted processing for server ***:{server['port']}"
                )
                return True
            else:
                bt.logging.warning(
                    f"Failed to interrupt processing for server ***:{server['port']}, status code: {response.status_code}"
                )
                return False

        except Exception as e:
            bt.logging.error(
                f"Error interrupting processing for server ***:{server['port']}: {str(e)}"
            )
            return False
