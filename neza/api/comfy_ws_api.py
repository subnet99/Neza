import time
import json
import uuid
import threading
import requests
import bittensor as bt
from typing import List, Dict, Any, Tuple, Optional
from neza.utils.ws import WebSocketManager
from neza.utils.throttled_handler import ThrottledHandler


class ComfyWSAPI:
    """
    Enhanced ComfyUI WebSocket API wrapper using WebSocketManager
    Simplified design without callbacks, using polling-based approach
    """

    def __init__(self, servers: List[Dict[str, str]] = None, clear_queue=False):
        """
        Initialize ComfyWSAPIEnhanced class

        Args:
            servers: List of ComfyUI servers, format: [{"host": "127.0.0.1", "port": "8188"}, ...]
                    If None, servers will be retrieved from environment variables
        """
        self.servers = []
        self.ws_managers = {}  # WebSocket managers for each server
        self.server_tasks = {}  # Task queue for each server: server_id -> [task_info]
        self.running = False

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

            server_id = f"{host}:{port}"
            self.servers.append(
                {
                    "id": server_id,
                    "host": host,
                    "port": port,
                    "client_id": str(uuid.uuid4()),
                    "available": False,  # Whether server is available
                    "ws_connected": False,  # Whether WebSocket is connected
                }
            )

            # Initialize task queue for this server
            self.server_tasks[server_id] = []

        # Check if ComfyUI servers are available and start WebSocket connections
        for server in self.servers:
            self._check_server_availability(server, clear_queue)
            if server["available"]:
                self._start_websocket_manager(server)

        # Start task processing thread
        self.running = True
        self.task_processor_thread = threading.Thread(
            target=self._process_tasks, daemon=True
        )
        self.task_processor_thread.start()

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
                bt.logging.info(f"ComfyUI server ****:{server['port']} is available.")
                server["available"] = True
                if clear_queue:
                    self.clear_server_queue(server)
                    self.interrupt_server_processing(server)
            else:
                bt.logging.warning(
                    f"ComfyUI server ****:{server['port']} is not available due to HTTP error: {response.status_code}"
                )
                server["available"] = False

        except Exception as e:
            bt.logging.warning(
                f"ComfyUI server ****:{server['port']} is not available due to network error: {str(e)}"
            )
            server["available"] = False

    def _start_websocket_manager(self, server: Dict[str, Any]) -> None:
        """
        Start WebSocket manager for a server
        """
        try:
            host_for_ws = server["host"]
            if not host_for_ws.startswith(("ws://", "wss://")):
                host_for_ws = f"ws://{host_for_ws}"

            ws_url = f"{host_for_ws}:{server['port']}/ws?clientId={server['client_id']}"

            # Create WebSocket manager
            ws_manager = WebSocketManager(
                server_id=server["id"],
                host=server["host"],
                port=server["port"],
                ws_url=ws_url,
                client_id=server["client_id"],
                on_message_callback=self._handle_ws_message,
                on_status_change_callback=self._handle_ws_status_change,
            )

            # Start WebSocket manager
            ws_manager.start()

            # Store WebSocket manager
            self.ws_managers[server["id"]] = ws_manager

            bt.logging.info(
                f"WebSocket manager started for server ****:{server['port']}"
            )

        except Exception as e:
            bt.logging.error(
                f"Error starting WebSocket manager for server ****:{server['port']}: {str(e)}"
            )

    def _handle_ws_message(
        self, message: str, server_id: str, throttler: ThrottledHandler
    ) -> None:
        """Handle WebSocket message from WebSocketManager"""
        try:
            data = json.loads(message)
            message_type = data.get("type")

            # Find server by server_id
            server = next((s for s in self.servers if s["id"] == server_id), None)
            if not server:
                return

            if message_type == "progress_state":
                # Use WebSocket throttled handler for progress_state messages
                prompt_id = data.get("data", {}).get("prompt_id")
                if prompt_id:
                    throttled_key = f"progress_state_{prompt_id}"
                    throttler.handle(
                        throttled_key,
                        (data.get("data", {}), server),
                        self._handle_progress_state_throttled,
                    )
            elif message_type == "execution_cached":
                self._handle_execution_cached(data.get("data", {}), server)
            elif message_type == "execution_start":
                self._handle_execution_start(data.get("data", {}), server)
            elif message_type == "execution_success":
                self._handle_execution_success(data.get("data", {}), server)
            elif message_type == "execution_error":
                self._handle_execution_error(data.get("data", {}), server)
            elif message_type == "progress":
                self._handle_progress(data.get("data", {}), server)

        except Exception as e:
            bt.logging.error(
                f"Error handling WebSocket message: {str(e)} original message: {message}"
            )

    def _handle_ws_status_change(self, server_id: str, connected: bool) -> None:
        """Handle WebSocket status change from WebSocketManager"""
        server = next((s for s in self.servers if s["id"] == server_id), None)
        if server:
            server["ws_connected"] = connected
            if connected:
                bt.logging.info(f"WebSocket connected to server")
            else:
                bt.logging.warning(f"WebSocket disconnected from server")

    def _handle_execution_cached(
        self, data: Dict[str, Any], server: Dict[str, Any]
    ) -> None:
        """
        Handle execution_cached message from ComfyUI
        This message contains cached nodes that will be executed
        """
        try:
            prompt_id = data.get("prompt_id")
            if not prompt_id:
                return

            # Find task by prompt_id
            task = self._find_task_by_prompt_id(prompt_id, server["id"])
            if not task:
                return

            # Get cached nodes from the message
            cached_nodes = data.get("nodes", [])

            # Mark cached nodes as cached
            if "node_progress" not in task:
                task["node_progress"] = {}

            for node_id in cached_nodes:
                if node_id in task["node_progress"]:
                    task["node_progress"][node_id] = True  # Mark as cached
                else:
                    # If node not in progress, add it
                    task["node_progress"][node_id] = True

        except Exception as e:
            bt.logging.error(f"Error handling execution_cached: {str(e)}")

    def _handle_progress_state(
        self, data: Dict[str, Any], server: Dict[str, Any]
    ) -> None:
        """
        Handle progress_state message from ComfyUI
        This message contains the complete state of all nodes
        """
        try:
            prompt_id = data.get("prompt_id")
            if not prompt_id:
                return

            # Find task by prompt_id
            task = self._find_task_by_prompt_id(prompt_id, server["id"])
            if not task:
                return

            # Initialize node tracking if not exists
            if "node_progress" not in task:
                task["node_progress"] = {}

            # Get all nodes from the progress_state message
            nodes = data.get("nodes", {})

            # Update node states based on progress_state
            completed_nodes = 0
            running_nodes = []
            running_progresses = []

            for node_id, node_info in nodes.items():
                state = node_info.get("state", "pending")

                # Mark as completed if state is "finished" or value >= max
                if state == "finished":
                    task["node_progress"][node_id] = True
                    completed_nodes += 1
                else:
                    if node_id not in task["node_progress"]:
                        task["node_progress"][node_id] = False

                if state == "running":
                    value = node_info.get("value", 0)
                    max_val = node_info.get("max", 1)
                    progress_ratio = value / max_val
                    running_nodes.append(node_id)
                    running_progresses.append(progress_ratio)

            task["running_nodes"] = running_nodes
            task["running_progresses"] = running_progresses

            bt.logging.info(
                f"Task {task['task_id']}: running_nodes: {running_nodes} running_progresses: {running_progresses}"
            )

        except Exception as e:
            bt.logging.error(f"Error handling progress_state: {str(e)}")

    def _handle_progress_state_throttled(self, data_tuple: tuple) -> None:
        """
        Throttled version of _handle_progress_state
        This method is called by the WebSocket throttled handler
        """
        try:
            data, server = data_tuple
            self._handle_progress_state(data, server)
        except Exception as e:
            bt.logging.error(f"Error in throttled progress_state handler: {str(e)}")

    def _handle_progress(self, data: Dict[str, Any], server: Dict[str, Any]) -> None:
        """
        Handle progress message from ComfyUI
        This message contains progress for a single node
        """
        try:
            prompt_id = data.get("prompt_id")
            node_id = data.get("node")
            value = data.get("value", 0)
            max_val = data.get("max", 1)

            if not prompt_id or not node_id:
                return

            # Find task by prompt_id
            task = self._find_task_by_prompt_id(prompt_id, server["id"])
            if not task:
                return

            # Initialize node tracking if not exists
            if "node_progress" not in task:
                task["node_progress"] = {}

            # Update node progress - mark as completed if value >= max
            if value >= max_val:
                task["node_progress"][node_id] = True  # Completed

        except Exception as e:
            bt.logging.error(f"Error handling progress: {str(e)}")

    def _handle_execution_start(
        self, data: Dict[str, Any], server: Dict[str, Any]
    ) -> None:
        """Handle execution_start message"""
        try:
            prompt_id = data.get("prompt_id")
            if prompt_id:
                task = self._find_task_by_prompt_id(prompt_id, server["id"])
                if task:
                    task["status"] = "running"
                    bt.logging.info(f"Task {task['task_id']} started execution")

        except Exception as e:
            bt.logging.error(f"Error handling execution_start: {str(e)}")

    def _handle_execution_success(
        self, data: Dict[str, Any], server: Dict[str, Any]
    ) -> None:
        """Handle execution_success message - this indicates task completion"""
        try:
            prompt_id = data.get("prompt_id")
            if prompt_id:
                task = self._find_task_by_prompt_id(prompt_id, server["id"])
                if task:
                    # Task is completed when we receive execution_success
                    bt.logging.info(
                        f"Task {task['task_id']} completed successfully (execution_success received)"
                    )

                    # Set progress to 100% if not already set
                    if task.get("progress", 0) < 100:
                        task["progress"] = 100

                    # Generate output filename based on task_id
                    output_info, execution_time = self.get_task_history(
                        task, server["id"]
                    )

                    # Complete the task with output file info
                    self._complete_task(
                        task["task_id"],
                        server["id"],
                        True,
                        output_info=output_info,
                        execution_time=execution_time,
                    )

        except Exception as e:
            bt.logging.error(f"Error handling execution_success: {str(e)}")

    def _handle_execution_error(
        self, data: Dict[str, Any], server: Dict[str, Any]
    ) -> None:
        """Handle execution_error message"""
        try:
            prompt_id = data.get("prompt_id")
            error_message = data.get("error", "Unknown error")

            if prompt_id:
                task = self._find_task_by_prompt_id(prompt_id, server["id"])
                if task:
                    self._complete_task(
                        task["task_id"], server["id"], False, error_message
                    )

        except Exception as e:
            bt.logging.error(f"Error handling execution_error: {str(e)}")

    def _find_task_by_prompt_id(
        self, prompt_id: str, server_id: str
    ) -> Optional[Dict[str, Any]]:
        """Find task by prompt_id in server tasks"""
        for task in self.server_tasks.get(server_id, []):
            if task.get("prompt_id") == prompt_id:
                return task
        return None

    def _complete_task(
        self,
        task_id: str,
        server_id: str,
        success: bool,
        error: str = None,
        output_info: Dict[str, Any] = None,
        execution_time: float = 0,
    ) -> None:
        """
        Mark task as completed and update its status
        """
        try:
            # Find and update task
            for task in self.server_tasks.get(server_id, []):
                if task.get("task_id") == task_id:
                    task["status"] = "completed" if success else "failed"
                    task["completed_at"] = time.time()
                    if error:
                        task["error_message"] = error
                    if output_info:
                        task["output_info"] = output_info
                    if execution_time:
                        task["execution_time"] = execution_time
                    bt.logging.info(
                        f"Task {task_id} completed on server {server_id} with status: {'success' if success else 'failed'}"
                    )
                    break

        except Exception as e:
            bt.logging.error(f"Error completing task {task_id}: {str(e)}")

    def remove_task(self, task_id: str, server_id: str = None) -> bool:
        """
        Remove task from server_tasks

        Args:
            task_id: Task ID to remove
            server_id: Server ID to remove from (if None, search all servers)

        Returns:
            bool: True if task was removed, False if not found
        """
        try:
            tasks = self.server_tasks.get(server_id, [])
            for i, task in enumerate(tasks):
                if task.get("task_id") == task_id:
                    tasks.pop(i)
                    return True
            return False

        except Exception as e:
            bt.logging.error(f"Error removing task {task_id}: {str(e)}")
            return False

    def stop_task(self, task_info: Dict[str, Any], server: Dict[str, Any]) -> bool:
        """
        Stop task on server
        """
        try:
            if task_info["status"] == "pending":
                self.clear_server_queue(server, task_info["task_id"])
                return True
            elif task_info["status"] == "running":
                self.interrupt_server_processing(server)
                return True
            return False

        except Exception as e:
            bt.logging.error(f"Error stopping task {task_info['task_id']}: {str(e)}")
            return False

    def _get_server_load(self, server: Dict[str, Any]) -> float:
        """
        Get current queue size of the server

        Args:
            server: Server configuration

        Returns:
            float: Current queue size
        """
        try:
            host_for_http = server["host"]
            if not host_for_http.startswith(("http://", "https://")):
                host_for_http = f"http://{host_for_http}"

            response = requests.get(
                f"{host_for_http}:{server['port']}/queue", timeout=10
            )

            if response.status_code == 200:
                queue_data = response.json()
                running = len(queue_data.get("queue_running", []))
                pending = len(queue_data.get("queue_pending", []))
                return running + pending
            else:
                bt.logging.warning(
                    f"Failed to get queue info from server ****:{server['port']}: {response.status_code}"
                )
                return float("inf")  # Return infinity to avoid selecting this server

        except Exception as e:
            bt.logging.warning(
                f"Error getting server load for ****:{server['port']}: {str(e)}"
            )
            return float("inf")

    def _get_best_server(self) -> Optional[Dict[str, Any]]:
        """
        Get server with lowest load

        Returns:
            Optional[Dict[str, Any]]: Server with lowest load, None if no servers available
        """
        available_servers = [s for s in self.servers if s["available"]]

        if not available_servers:
            bt.logging.error("No available ComfyUI servers")
            return None

        # Get load for each server
        server_loads = []
        for server in available_servers:
            load = self._get_server_load(server)
            server_loads.append((server, load))

        # Sort by load (lowest first)
        server_loads.sort(key=lambda x: x[1])

        # Return server with lowest load
        best_server = server_loads[0][0]
        bt.logging.debug(
            f"Selected server ****:{best_server['port']} with load {server_loads[0][1]}"
        )

        return best_server

    def _process_tasks(self):
        """
        Process tasks in the queue
        """
        while self.running:
            try:
                for server_id, tasks in self.server_tasks.items():
                    # Process pending tasks
                    for task in tasks:
                        if task.get("status") == "pending":
                            # Find the server
                            server = next(
                                (s for s in self.servers if s["id"] == server_id), None
                            )
                            if server and server["available"]:
                                self._submit_task_to_server(task, server)

                time.sleep(1)  # Check every second

            except Exception as e:
                bt.logging.error(f"Error in task processing: {str(e)}")
                time.sleep(5)

    def _submit_task_to_server(
        self, task: Dict[str, Any], server: Dict[str, Any]
    ) -> None:
        """
        Submit task to ComfyUI server
        """
        try:
            host_for_http = server["host"]
            if not host_for_http.startswith(("http://", "https://")):
                host_for_http = f"http://{host_for_http}"

            # Submit workflow to ComfyUI
            response = requests.post(
                f"{host_for_http}:{server['port']}/prompt",
                json={
                    "prompt": task["workflow_params"],
                    "client_id": server["client_id"],
                },
                timeout=30,
            )

            if response.status_code == 200:
                data = response.json()
                prompt_id = data.get("prompt_id")

                if prompt_id:
                    task["prompt_id"] = prompt_id
                    task["status"] = "submitted"

                    # Initialize all nodes from workflow
                    self._initialize_nodes_from_workflow(task, task["workflow_params"])

                    bt.logging.info(
                        f"Task {task['task_id']} submitted to server ****:{server['port']} with prompt_id {prompt_id}"
                    )
                else:
                    task["status"] = "failed"
                    task["error_message"] = "No prompt_id in response"
                    bt.logging.error(
                        f"Task {task['task_id']} submission failed: no prompt_id"
                    )
            else:
                task["status"] = "failed"
                task["error_message"] = f"HTTP {response.status_code}"
                bt.logging.error(
                    f"Task {task['task_id']} submission failed: HTTP {response.status_code}"
                )

        except Exception as e:
            task["status"] = "failed"
            task["error_message"] = str(e)
            bt.logging.error(f"Error submitting task {task['task_id']}: {str(e)}")

    def _initialize_nodes_from_workflow(
        self, task: Dict[str, Any], workflow: Dict[str, Any]
    ) -> None:
        """
        Initialize node tracking from workflow definition

        Args:
            task: Task information
            workflow: Workflow definition
        """
        try:
            # Initialize node progress tracking - simple False for all nodes
            task["node_progress"] = {}
            task["running_nodes"] = []
            task["running_progresses"] = []

            # Extract all node IDs from workflow
            if isinstance(workflow, dict):
                for node_id in workflow.keys():
                    if node_id.isdigit():  # Valid node ID
                        task["node_progress"][node_id] = False

            total_nodes = len(task["node_progress"])
            bt.logging.info(
                f"Task {task['task_id']}: initialized {total_nodes} nodes from workflow"
            )

        except Exception as e:
            bt.logging.error(f"Error initializing nodes from workflow: {str(e)}")

    def execute_workflow(
        self, workflow: Dict[str, Any], task_id: str = None
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Execute ComfyUI workflow with WebSocket monitoring

        Args:
            workflow: Complete workflow configuration
            task_id: Task ID, used for logging

        Returns:
            Tuple[bool, str, Dict[str, Any]]
        """

        return self.execute_workflow_on_server(workflow, task_id, None)

    def execute_workflow_on_server(
        self,
        workflow: Dict[str, Any],
        task_id: str = None,
        worker_id: int = None,
    ) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Execute ComfyUI workflow on a specific server

        Args:
            workflow: Complete workflow configuration
            task_id: Task ID, used for logging
            worker_id: Worker ID, used for logging

        Returns:
            Tuple[bool, str, Dict[str, Any]]
        """

        server = self._get_server_by_worker_id(worker_id)

        if server is None:
            bt.logging.error("No available ComfyUI servers")
            return False, "", None

        server_info = {
            "host": server["host"],
            "port": server["port"],
            "id": server["id"],
        }

        # Double-check server availability
        if not server["available"]:
            bt.logging.warning(
                f"Server ****:{server['port']} not available, checking availability"
            )
            self._check_server_availability(server)
            if not server["available"]:
                bt.logging.error(f"Server ****:{server['port']} is still not available")
                return False, "", server_info

        try:
            # Create task info
            task_info = {
                "task_id": task_id or str(uuid.uuid4()),
                "workflow_params": workflow,
                "status": "pending",
                "error_message": "",
                "execution_time": 0,
                "created_at": time.time(),
                "server_id": server["id"],
            }

            # Add task to server queue
            self.server_tasks[server["id"]].append(task_info)

            bt.logging.info(
                f"Task {task_info['task_id']} queued for server ****:{server['port']}"
            )

            # Return server info
            return True, task_info["task_id"], server_info

        except Exception as e:
            bt.logging.error(f"Error executing workflow: {str(e)}")
            return False, "", server_info

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

    def get_task_status(
        self, task_id: str, server_id: str = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get task status by task_id

        Args:
            task_id: Task ID to query
            server_id: Server ID to query
        Returns:
            Optional[Dict[str, Any]]: Task info if found, None if not found
        """
        if server_id:
            tasks = self.server_tasks.get(server_id, [])
            for task in tasks:
                if (
                    task.get("task_id") == task_id
                ):  # Return a copy to avoid external modification
                    return task.copy()
            return None

        for server_id, tasks in self.server_tasks.items():
            for task in tasks:
                if task.get("task_id") == task_id:
                    return task.copy()  # Return a copy to avoid external modification
        return None

    def get_task_detailed_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed task status including all server information

        Args:
            task_id: Task ID to query

        Returns:
            Optional[Dict[str, Any]]: Detailed task info or None if not found
        """
        for server_id, tasks in self.server_tasks.items():
            for task in tasks:
                if task.get("task_id") == task_id:
                    detailed_info = task.copy()
                    detailed_info["server_id"] = server_id
                    detailed_info["ws_connected"] = self.is_server_connected(server_id)

                    # Add server status
                    server_status = self.get_server_status(server_id)
                    if server_status:
                        detailed_info["server_status"] = server_status

                    return detailed_info
        return None

    def clear_server_queue(self, server: Dict[str, Any], task_id: str = None) -> bool:
        """
        Clear the queue of a ComfyUI server

        Args:
            server: Server configuration
            task_id: Task ID to delete

        Returns:
            bool: Whether clearing was successful
        """
        try:
            host_for_http = server["host"]
            if not host_for_http.startswith(("http://", "https://")):
                host_for_http = f"http://{host_for_http}"

            response = requests.post(
                f"{host_for_http}:{server['port']}/queue",
                json={"clear": True} if task_id is None else {"delete": [task_id]},
                timeout=20,
            )

            clear_str = "queue" if task_id is None else f"queue for task {task_id}"

            if response.status_code == 200:
                bt.logging.info(f"Cleared {clear_str} for server ****:{server['port']}")
                return True
            else:
                bt.logging.warning(
                    f"Failed to clear {clear_str} for server ****:{server['port']}: {response.status_code}"
                )
                return False

        except Exception as e:
            bt.logging.error(
                f"Error clearing queue for server ****:{server['port']}: {str(e)}"
            )
            return False

    def interrupt_server_processing(self, server: Dict[str, Any]) -> bool:
        """
        Interrupt processing on a ComfyUI server

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
                f"{host_for_http}:{server['port']}/interrupt",
                timeout=20,
            )

            if response.status_code == 200:
                bt.logging.info(
                    f"Interrupted processing for server ****:{server['port']}"
                )
                return True
            else:
                bt.logging.warning(
                    f"Failed to interrupt processing for server ****:{server['port']}: {response.status_code}"
                )
                return False

        except Exception as e:
            bt.logging.error(
                f"Error interrupting processing for server ****:{server['port']}: {str(e)}"
            )
            return False

    def shutdown(self):
        """
        Shutdown the API and close all connections
        """
        self.running = False

        # Stop WebSocket managers and cleanup throttlers
        for server_id, ws_manager in self.ws_managers.items():
            try:
                ws_manager.cleanup()  # This will stop and cleanup throttler
                bt.logging.info(f"Stopped WebSocket manager for server")
            except Exception as e:
                bt.logging.error(f"Error stopping WebSocket manager for server")

        # Clear task queues
        for server_id in self.server_tasks:
            self.server_tasks[server_id].clear()

        bt.logging.info("ComfyWSAPIEnhanced shutdown completed")

    def is_server_connected(self, server_id: str) -> bool:
        """
        Check if a server is connected via WebSocket

        Args:
            server_id: Server ID to check

        Returns:
            bool: Whether server is connected
        """
        ws_manager = self.ws_managers.get(server_id)
        return ws_manager.is_connected() if ws_manager else False

    def get_server_status(self, server_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed status of a server

        Args:
            server_id: Server ID to query

        Returns:
            Optional[Dict[str, Any]]: Server status if found, None if not found
        """
        server = next((s for s in self.servers if s["id"] == server_id), None)
        if server:
            return {
                "id": server["id"],
                "host": server["host"],
                "port": server["port"],
                "available": server["available"],
                "ws_connected": self.is_server_connected(server_id),
                "task_count": len(self.server_tasks.get(server_id, [])),
                "load": self._get_server_load(server),
            }
        return None

    def get_task_history(
        self, task_info: Dict[str, Any], server_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get task history by task_id

        Args:
            task_info: Task info
            server_id: Server ID to query
        Returns:
            Optional[Dict[str, Any]]: Task history if found, None if not found
        """
        server_info = self.get_server_status(server_id)
        if server_info is None:
            return None

        try:
            # Ensure host doesn't include protocol prefix for HTTP requests
            host_for_http = server_info["host"]
            if not host_for_http.startswith(("http://", "https://")):
                host_for_http = f"http://{host_for_http}"

            # Add timeout setting
            response = requests.get(
                f"{host_for_http}:{server_info['port']}/history/{task_info['prompt_id']}",
                timeout=60,
            )
            if response.status_code == 200:
                response_data = response.json()

                # Skip if history is None
                if task_info["prompt_id"] not in response_data:
                    bt.logging.info(
                        f"History is None for prompt_id: {task_info['prompt_id']}"
                    )
                    return None, None

                history = response_data[task_info["prompt_id"]]

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
                            f"ComfyUI Execution time: {execution_time:.2f}s for prompt_id: {task_info['prompt_id']}"
                        )

                outputs = history["outputs"]
                return outputs, execution_time
            else:
                bt.logging.info(f"Failed to get history: {response.status_code}")
                return None, None
        except Exception as e:
            bt.logging.error(f"Failed to get history: {str(e)}")
            return None, None
