#!/usr/bin/env python3

import aiohttp
from aiohttp import web
import json
import time
from typing import Dict, Any, Optional
import bittensor as bt
import urllib.parse

from .task_mapping_manager import TaskMappingManager


class MinerHTTPServer:
    """
    Simple HTTP server integrated with Miner for view requests
    Uses axon port + 2 to avoid conflicts

    SECURITY NOTE: This server exposes ComfyUI view endpoint via public HTTP.
    Access control is based on knowledge of valid task_id.
    """

    def __init__(
        self,
        task_mapping_manager: TaskMappingManager,
        port: int = None,
    ):
        """
        Initialize Miner HTTP server

        Args:
            task_mapping_manager: Task mapping manager instance
            port: Port to listen on (default: axon port + 2)
        """
        self.port = port or 8093
        self.task_mapping_manager = task_mapping_manager
        self.app = None
        self.runner = None

    async def start(self):
        """Start the HTTP server"""
        try:
            bt.logging.info(f"Starting Miner HTTP server on port {self.port}")

            self.app = web.Application()

            self.app.router.add_get("/view/{task_id}", self.handle_view_request)

            self.runner = web.AppRunner(self.app)
            await self.runner.setup()

            site = web.TCPSite(self.runner, "0.0.0.0", self.port)
            await site.start()

            bt.logging.info(f"Miner HTTP server started on port {self.port}")

        except Exception as e:
            bt.logging.error(f"Failed to start Miner HTTP server: {str(e)}")
            raise

    async def stop(self):
        """Stop the HTTP server"""
        if self.runner:
            await self.runner.cleanup()
            bt.logging.info("Miner HTTP server stopped")

    async def handle_view_request(self, request):
        """
        Handle view request and redirect to ComfyUI

        Args:
            request: HTTP request object

        Returns:
            HTTP response (redirect or error)
        """
        try:
            task_id = request.match_info["task_id"]
            bt.logging.info(f"View request for task: {task_id}")

            query_params = dict(request.query)

            mapping = self.task_mapping_manager.get_mapping(task_id)
            if not mapping:
                bt.logging.warning(f"Task {task_id} not found or expired")
                return web.json_response(
                    {"error": "Task not found or expired"}, status=404
                )

            comfy_instance = mapping.get("comfy_instance")
            if not comfy_instance:
                bt.logging.error(f"Task {task_id} has no comfy_instance")
                return web.json_response(
                    {"error": "ComfyUI instance not available"}, status=500
                )

            # Proxy the request to ComfyUI instead of redirecting
            # This hides the real ComfyUI IP from the client
            comfy_host, comfy_port = comfy_instance.split(":")
            comfy_url = f"http://{comfy_host}:{comfy_port}/view"

            if query_params:
                # Use urlencode to safely handle special characters
                query_string = urllib.parse.urlencode(query_params)
                comfy_url += f"?{query_string}"

            bt.logging.info(f"Proxying request to ComfyUI for task {task_id}")

            # Make request to ComfyUI and return the response
            try:
                # Add Authorization header if token is available
                req_headers = {}
                token = mapping.get("token")
                if token:
                    req_headers["Authorization"] = f"Bearer {token}"
                    bt.logging.debug(
                        f"Adding Authorization header for task {task_id} with token..."
                    )
                else:
                    bt.logging.info(
                        f"No token found for task {task_id}, forwarding without auth"
                    )

                async with aiohttp.ClientSession() as session:
                    async with session.get(comfy_url, headers=req_headers) as response:
                        # Get the response content
                        content = await response.read()

                        return web.Response(
                            body=content,
                            status=response.status,
                            content_type=response.content_type,
                        )

            except Exception as e:
                bt.logging.error(f"Failed to proxy request to ComfyUI: {str(e)}")
                return web.json_response(
                    {"error": "Failed to fetch content from ComfyUI"}, status=502
                )

        except Exception as e:
            bt.logging.error(f"Error handling view request: {str(e)}")
            return web.json_response(
                {"error": f"Internal server error: {str(e)}"}, status=500
            )


async def start_miner_http_server(
    task_mapping_manager: TaskMappingManager, port: int = None
) -> MinerHTTPServer:
    """
    Start Miner HTTP server

    Args:
        task_mapping_manager: Task mapping manager instance
        port: Port to listen on (default: axon port + 2)

    Returns:
        MinerHTTPServer instance
    """
    server = MinerHTTPServer(task_mapping_manager, port)
    await server.start()
    return server
