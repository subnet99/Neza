import os
import bittensor as bt
from typing import List, Dict
import mimetypes


def _parse_env_servers(comfy_servers: str) -> List[Dict[str, str]]:
    """
    Parse server list from environment variables

    Environment variable format: COMFYUI_SERVERS=host1:port1,host2:port2,...
    If COMFYUI_SERVERS is not set, default to "127.0.0.1:8188"
    Also parses COMFYUI_TOKENS if available

    Returns:
        List[Dict[str, str]]: List of server configurations
    """
    try:
        servers = []
        # Parse tokens if available
        tokens_str = os.environ.get("COMFYUI_TOKENS", "")
        tokens = [t.strip() for t in tokens_str.split(",")] if tokens_str else []

        # Split multiple server addresses by comma
        for i, server_str in enumerate(comfy_servers.split(",")):
            server_str = server_str.strip()
            if not server_str:
                continue

            # Parse host and port
            if ":" in server_str:
                host, port = server_str.split(":", 1)
            else:
                host = server_str
                port = "8188"  # Default port

            server_config = {"host": host, "port": port}

            # Add token if available for this server
            if i < len(tokens) and tokens[i]:
                server_config["token"] = tokens[i]

            servers.append(server_config)

        return servers

    except Exception as e:
        bt.logging.error(f"Error parsing environment servers: {str(e)}")
        return []


def merge_config(original_config, update_dict):
    """
    Merge config

    Args:
        original_config: Original config
        update_dict: Update dict
    """
    for key, value in update_dict.items():
        current_value = getattr(original_config, key, None)

        if isinstance(current_value, dict) and isinstance(value, dict):
            for sub_key, sub_value in value.items():
                current_value[sub_key] = sub_value
        else:
            setattr(original_config, key, value)


def check_mime(mime_type: str) -> str:
    if not mime_type:
        return "unknown"
    mime_type = mime_type.lower()
    if mime_type.startswith("video/"):
        return "video"
    elif mime_type.startswith("image/"):
        if mime_type == "image/gif":
            return "gif"
        return "image"
    elif mime_type.startswith("audio/"):
        return "audio"
    else:
        return "unknown"


def get_file_type(format_str: str, filename: str) -> str:
    """
    Get file type from format and filename

    Args:
        format: MIME type
        filename: Filename

    Returns:
        str: File type
    """
    if format_str:
        file_type = check_mime(format_str)
        if file_type != "unknown":
            return file_type

    mime_type, _ = mimetypes.guess_type(filename or "")
    return check_mime(mime_type)
