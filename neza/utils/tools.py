import os
import bittensor as bt
from typing import List, Dict, Any, Tuple, Optional


def _parse_env_servers(comfy_servers: str) -> List[Dict[str, str]]:
    """
    Parse server list from environment variables

    Environment variable format: COMFYUI_SERVERS=host1:port1,host2:port2,...
    If COMFYUI_SERVERS is not set, default to "127.0.0.1:8188"

    Returns:
        List[Dict[str, str]]: List of server configurations
    """
    try:
        servers = []
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

    except Exception as e:
        bt.logging.error(f"Error parsing environment servers: {str(e)}")
        return []
