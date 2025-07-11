import json
import os
import time
import uuid
import threading
from typing import Any, Dict, List, Optional, Tuple, Callable
import bittensor as bt
import requests
from websocket import (
    WebSocket,
    WebSocketTimeoutException,
    WebSocketConnectionClosedException,
)
import traceback


class ComfyAPI:
    def __init__(self, servers: List[Dict[str, str]] = None):
        """
        Initialize ComfyAPI class

        Args:
            servers: List of ComfyUI servers, format: [{"host": "127.0.0.1", "port": "8188"}, ...]
                    If None, servers will be retrieved from environment variables
        """
        # If no servers provided, get from environment variables
        if servers is None:
            servers = self._parse_env_servers()

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
                    "ws": None,
                    "ws_lock": threading.Lock(),
                    "connected": False,
                    "task_callbacks": {},  # Task callback dictionary, prompt_id -> callback
                    "ws_thread": None,  # WebSocket listener thread
                    "task_results": {},  # Task result dictionary, prompt_id -> result
                    "task_events": {},  # Task event dictionary, prompt_id -> event
                }
            )
        bt.logging.info(f"ComfyUI servers: {self.servers}")
        # Connect WebSocket for all servers
        for server in self.servers:
            self._connect_websocket(server)
            self._start_ws_listener(server)  # Start WebSocket listener thread

    @staticmethod
    def _parse_env_servers() -> List[Dict[str, str]]:
        """
        Parse server list from environment variables

        Environment variable format: COMFYUI_SERVERS=host1:port1,host2:port2,...
        If COMFYUI_SERVERS is not set, default to "127.0.0.1:8188"

        Returns:
            List[Dict[str, str]]: List of server configurations
        """
        servers = []

        # Use COMFYUI_SERVERS environment variable
        comfy_servers = os.environ.get("COMFYUI_SERVERS", "127.0.0.1:8188")

        # Split multiple server addresses by comma
        for server_str in comfy_servers.split(","):
            server_str = server_str.strip()
            if not server_str:
                continue

            # Parse host and port
            if ":" in server_str:
                host, port = server_str.split(":", 1)
            else:
                host = server_str
                port = "8188"  # Default port

            servers.append({"host": host, "port": port})

        return servers

    def _connect_websocket(self, server: Dict[str, Any]) -> bool:
        """
        Connect to WebSocket

        Args:
            server: Server configuration

        Returns:
            bool: Whether connection was successful
        """
        try:
            ws = WebSocket()
            ws.connect(
                f"ws://{server['host']}:{server['port']}/ws?clientId={server['client_id']}"
            )
            # Set a reasonable timeout
            ws.settimeout(60)
            server["ws"] = ws
            server["connected"] = True
            print(f"Connected to ComfyUI server at {server['host']}:{server['port']}")
            return True
        except Exception as e:
            print(
                f"WebSocket connection failed for {server['host']}:{server['port']}: {str(e)}"
            )
            server["connected"] = False
            return False

    def _start_ws_listener(self, server: Dict[str, Any]) -> None:
        """
        Start WebSocket listener thread to handle all messages from the server

        Args:
            server: Server configuration
        """

        def ws_listener():
            while server["connected"]:
                try:
                    if server["ws"] is None:
                        time.sleep(1)
                        continue

                    message = server["ws"].recv()
                    data = json.loads(message)

                    # Handle execution completion messages
                    if data["type"] == "executing":
                        if data["data"]["node"] is None:
                            # Try to get history records
                            prompt_id = data.get("data", {}).get("prompt_id")
                            if prompt_id and prompt_id in server["task_callbacks"]:
                                # Get video filename
                                history_result = self.get_history(server, prompt_id)
                                if history_result:
                                    # Save result and trigger event
                                    server["task_results"][prompt_id] = history_result[
                                        0
                                    ]
                                    if prompt_id in server["task_events"]:
                                        server["task_events"][prompt_id].set()

                except WebSocketTimeoutException:
                    # Timeout, continue listening
                    continue
                except WebSocketConnectionClosedException:
                    # Connection closed, try to reconnect
                    print(
                        f"WebSocket connection closed for {server['host']}:{server['port']}"
                    )
                    server["connected"] = False
                    time.sleep(5)
                    self._connect_websocket(server)
                except Exception as e:
                    print(f"WebSocket listener error: {str(e)}")
                    time.sleep(1)

        # Create and start thread
        server["ws_thread"] = threading.Thread(target=ws_listener, daemon=True)
        server["ws_thread"].start()

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

    def _get_best_server(self) -> Optional[Dict[str, Any]]:
        """
        Get server with lowest load

        Returns:
            Optional[Dict[str, Any]]: Server configuration with lowest load, or None if no servers available
        """
        min_load = float("inf")
        best_server = None

        for server in self.servers:
            # If server is not connected, try to reconnect
            if not server["connected"]:
                self._connect_websocket(server)
                if server["connected"] and server["ws_thread"] is None:
                    self._start_ws_listener(server)

            # If server is connected, check load
            if server["connected"]:
                load = self._get_server_load(server)
                if load < min_load:
                    min_load = load
                    best_server = server

        return best_server

    def get_video(
        self, server: Dict[str, Any], prompt_id: str, timeout: int = 300
    ) -> str:
        """
        Get video generation result using event waiting instead of blocking WebSocket

        Args:
            server: Server configuration
            prompt_id: Prompt ID
            timeout: Timeout in seconds

        Returns:
            str: Video filename, empty string if failed
        """
        try:
            # If result is already in task_results, return it directly
            if prompt_id in server["task_results"]:
                return server["task_results"][prompt_id]

            # Create event and set callback
            if prompt_id not in server["task_events"]:
                server["task_events"][prompt_id] = threading.Event()

            # Wait for event or timeout
            if server["task_events"][prompt_id].wait(timeout):
                # Event triggered, return result
                if prompt_id in server["task_results"]:
                    return server["task_results"][prompt_id]

            # If timeout, try to get from history
            history_result = self.get_history(server, prompt_id)
            if history_result:
                server["task_results"][prompt_id] = history_result[0]
                return history_result[0]

            print(
                f"Timeout waiting for video result from {server['host']}:{server['port']}"
            )
            return ""

        except Exception as e:
            print(f"Error getting video: {str(e)}")
            try:
                history_result = self.get_history(server, prompt_id)
                if history_result:
                    return history_result[0]
            except:
                pass
            return ""

    def get_history(self, server: Dict[str, Any], prompt_id: str) -> List[str]:
        """
        Get video filenames from history

        Args:
            server: Server configuration
            prompt_id: Prompt ID

        Returns:
            List[str]: List of video filenames
        """
        try:
            # Ensure host doesn't include protocol prefix for HTTP requests
            host_for_http = server["host"]
            if not host_for_http.startswith(("http://", "https://")):
                host_for_http = f"http://{host_for_http}"

            # Add timeout setting
            response = requests.get(
                f"{host_for_http}:{server['port']}/history/{prompt_id}", timeout=10
            )
            if response.status_code == 200:
                history = response.json()[prompt_id]

                # Extract all gif filenames
                filenames = []
                for node_id in history["outputs"]:
                    node_output = history["outputs"][node_id]
                    if "gifs" in node_output:
                        for gif in node_output["gifs"]:
                            filenames.append(gif["filename"])

                return filenames
            else:
                print(f"Failed to get history: {response.status_code}")
                return []
        except Exception as e:
            print(f"Error getting history: {str(e)}")
            return []

    def execute_comfy_workflow(
        self, workflow: Dict[str, Any], task_id: str = None
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Execute ComfyUI workflow and return output filename

        Args:
            workflow: Complete workflow configuration
            task_id: Task ID, used for logging

        Returns:
            Tuple[bool, str, Dict[str, Any]]: (whether successful, output filename, server info), returns (False, "", None) if failed
        """
        # Get server with lowest load
        server = self._get_best_server()
        if server is None:
            print("No available ComfyUI servers")
            return False, "", None

        try:
            # Ensure host doesn't include protocol prefix for HTTP requests
            host_for_http = server["host"]
            if not host_for_http.startswith(("http://", "https://")):
                host_for_http = f"http://{host_for_http}"

            # Add timeout setting
            try:
                response = requests.post(
                    f"{host_for_http}:{server['port']}/prompt",
                    json={
                        "prompt": workflow,
                        "client_id": server["client_id"],
                    },
                    timeout=30,
                )

                if response.status_code != 200:
                    print(f"Workflow: {workflow}")
                    print(
                        f"ComfyUI server {host_for_http}:{server['port']} returned error status code: {response.status_code}"
                    )
                    print(f"Response content: {response.text}")
                    return False, "", None

                response_json = response.json()
                if "prompt_id" not in response_json:
                    print(f"Missing prompt_id in ComfyUI response: {response_json}")
                    return False, "", None

                prompt_id = response_json["prompt_id"]
            except requests.exceptions.RequestException as e:
                print(f"Error sending request to ComfyUI server: {str(e)}")
                return False, "", None
            except json.JSONDecodeError as e:
                print(f"Error parsing ComfyUI response: {str(e)}")
                print(f"Response content: {response.text}")
                return False, "", None

            # Get video result
            video_result = self.get_video(server, prompt_id)
            if not video_result:
                print(
                    f"Failed to get video result from ComfyUI, prompt_id: {prompt_id}"
                )
                # Try to get from history again
                history_result = self.get_history(server, prompt_id)
                if history_result:
                    # Return server info
                    server_info = {"host": server["host"], "port": server["port"]}
                    return True, history_result[0], server_info

                return False, "", None

            # Return server info
            server_info = {"host": server["host"], "port": server["port"]}
            return True, video_result, server_info

        except Exception as e:
            print(f"Error executing ComfyUI workflow: {str(e)}")
            print(traceback.format_exc())
            return False, "", None

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
