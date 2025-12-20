import asyncio
import websockets
import json
import time
from typing import Dict, Any, Optional
import urllib.parse
import uuid
from functools import partial

import bittensor as bt

from .proxy_auth import ProxyAuthManager
from .task_mapping_manager import TaskMappingManager


class WSProxyService:
    """
    WebSocket proxy service for forwarding connections to ComfyUI
    Simple message forwarding without business logic
    """

    def __init__(
        self,
        task_mapping_manager: TaskMappingManager,
        proxy_auth_manager: ProxyAuthManager,
        proxy_port: int = 8189,
        comfy_api=None,
    ):
        """
        Initialize WebSocket proxy service
        """
        self.task_mapping_manager = task_mapping_manager
        self.proxy_auth_manager = proxy_auth_manager
        self.proxy_port = proxy_port
        self.comfy_api = comfy_api
        self.server = None
        self.active_connections = {}
        self.proxy_connections = {}  # Store proxy connections for message forwarding
        self.message_queue = asyncio.Queue(maxsize=1000)
        self.loop = None

    def register_proxy_connection(
        self, task_id: str, prompt_id: str, websocket, comfy_instance: str
    ):
        """
        Register a proxy connection to receive filtered messages
        """
        self.proxy_connections[task_id] = {
            "prompt_id": prompt_id,
            "websocket": websocket,
            "comfy_instance": comfy_instance,
            "created_at": time.time(),
        }
        bt.logging.info(
            f"Registered proxy connection for task {task_id} (prompt_id: {prompt_id})"
        )

    def unregister_proxy_connection(self, task_id: str):
        """Unregister a proxy connection"""
        if task_id in self.proxy_connections:
            del self.proxy_connections[task_id]
            bt.logging.info(f"Unregistered proxy connection for task {task_id}")

    def forward_message_sync(self, message: str, comfy_instance: str):
        """
        Synchronous method to forward message to registered proxy connections
        """
        if not self.proxy_connections:
            return

        # Parse message to get prompt_id
        message_prompt_id = None
        try:
            data = json.loads(message)
            message_prompt_id = data.get("data", {}).get("prompt_id")
        except (json.JSONDecodeError, KeyError):
            pass

        # Send message to matching proxy connections
        for task_id, proxy_info in list(self.proxy_connections.items()):
            if proxy_info["comfy_instance"] != comfy_instance:
                continue

            if message_prompt_id and proxy_info["prompt_id"] != message_prompt_id:
                continue

            if not self.loop:
                continue

            try:
                # Use call_soon_threadsafe to put in asyncio.Queue from another thread
                self.loop.call_soon_threadsafe(
                    lambda: self.message_queue.put_nowait((task_id, message))
                )
            except Exception as e:
                bt.logging.error(f"Error queuing message for proxy {task_id}: {str(e)}")

    async def _process_message_queue(self):
        """Process messages from the queue and send to WebSocket connections"""
        while True:
            try:
                task_id, message = await self.message_queue.get()
                await self._send_to_specific_websocket(task_id, message)
                self.message_queue.task_done()
            except Exception as e:
                bt.logging.error(f"Error processing message queue: {str(e)}")
                await asyncio.sleep(1)

    async def _send_to_specific_websocket(self, task_id: str, message: str):
        """Send message to a specific WebSocket connection"""
        if task_id in self.proxy_connections:
            websocket = self.proxy_connections[task_id]["websocket"]
            try:
                await websocket.send(message)
            except Exception as e:
                bt.logging.error(f"Error sending message to proxy {task_id}: {str(e)}")
                self.unregister_proxy_connection(task_id)

    async def start(self):
        """Start the WebSocket proxy service"""
        try:
            self.loop = asyncio.get_running_loop()
            bt.logging.info(
                f"Starting WebSocket proxy service on port {self.proxy_port}"
            )

            # Start message queue processor
            asyncio.create_task(self._process_message_queue())

            async def handler(websocket):
                await self.handle_connection(websocket, websocket.request.path)

            self.server = await websockets.serve(handler, "0.0.0.0", self.proxy_port)
            bt.logging.info(
                f"WebSocket proxy service started on port {self.proxy_port}"
            )

            # Start cleanup task
            asyncio.create_task(self.periodic_cleanup())

        except Exception as e:
            bt.logging.error(f"Failed to start WebSocket proxy service: {str(e)}")
            raise

    async def stop(self):
        """Stop the WebSocket proxy service"""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            bt.logging.info("WebSocket proxy service stopped")

    async def handle_connection(self, websocket, path):
        """
        Handle incoming WebSocket connection
        """
        connection_id = id(websocket)
        task_id = None

        try:
            # Parse path: /task_id/credential
            path_parts = path.strip("/").split("/")
            if len(path_parts) != 2:
                await websocket.close(1008, "Invalid path format")
                return

            task_id, credential = path_parts
            credential = urllib.parse.unquote(credential)

            # Get task mapping
            mapping = self.task_mapping_manager.get_mapping(task_id)
            if not mapping:
                await websocket.close(1008, "Task not found or expired")
                return

            status = mapping.get("status")
            if status in ["completed", "failed"]:
                try:
                    final_msg = json.dumps(
                        {
                            "type": (
                                "execution_success"
                                if status == "completed"
                                else "execution_error"
                            ),
                            "data": {
                                "prompt_id": mapping.get("prompt_id"),
                                "status": status,
                            },
                        }
                    )
                    await websocket.send(final_msg)
                except:
                    pass
                await asyncio.sleep(2)
                await websocket.close(1000, f"Task already {status}")
                return

            # Verify credential
            if not self.proxy_auth_manager.verify_credential(task_id, credential):
                await websocket.close(1008, "Invalid credential")
                return

            comfy_instance = mapping["comfy_instance"]
            prompt_id = mapping.get("prompt_id")

            # Check if ComfyUI API has an active connection
            ws_manager = (
                self.comfy_api.ws_managers.get(comfy_instance)
                if self.comfy_api
                else None
            )
            if not ws_manager or not ws_manager.connected:
                reason = "ComfyUI connection not active"
                await websocket.close(1011, reason)
                return

            # Register this proxy connection
            self.register_proxy_connection(
                task_id, prompt_id, websocket, comfy_instance
            )

            # Store connection info
            self.active_connections[connection_id] = {
                "task_id": task_id,
                "prompt_id": prompt_id,
                "owner_ws": websocket,
                "start_time": time.time(),
            }

            # Wait for connection to close
            async for _ in websocket:
                pass

        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            bt.logging.error(f"Error in proxy connection for task {task_id}: {str(e)}")
        finally:
            await self._cleanup_connection(connection_id)

    async def _cleanup_connection(self, connection_id: int):
        """Clean up connection resources"""
        if connection_id not in self.active_connections:
            return

        conn_info = self.active_connections[connection_id]
        task_id = conn_info["task_id"]

        try:
            await conn_info["owner_ws"].close()
        except:
            pass

        self.unregister_proxy_connection(task_id)
        del self.active_connections[connection_id]

    async def periodic_cleanup(self):
        """Periodically cleanup expired connections"""
        while True:
            try:
                await asyncio.sleep(60)
                current_time = time.time()
                stale_connections = [
                    cid
                    for cid, info in self.active_connections.items()
                    if current_time - info["start_time"] > 3600
                ]

                for cid in stale_connections:
                    await self._cleanup_connection(cid)

            except Exception as e:
                bt.logging.error(f"Error in periodic cleanup: {str(e)}")
