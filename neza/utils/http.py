"""
HTTP Utility Module
Provides HTTP related utility functions
"""

from typing import Any
import aiohttp
import asyncio
import bittensor as bt
import os
import time
from dotenv import load_dotenv
import requests
import traceback
import json
import email.utils
import zipfile
import shutil
import urllib.parse
import uuid

# Load environment variables
load_dotenv()


async def check_url_resource_available(url, get_file_info=False, timeout=30):
    """
    Check if URL resource is available

    Args:
        url: URL to check
        timeout: Timeout in seconds, default is 10 seconds

    Returns:
        bool: Whether the resource is available
    """
    if not url:
        return False, {}

    try:
        # Only send HEAD request to check if resource exists, without downloading actual content
        status_code, file_info = check_get_url_resource_available_sync(
            url, get_file_info, timeout
        )

        # Check if status code indicates success
        if status_code == 0:
            # If status_code is 0, it means there was an error
            return False, file_info
        elif 200 <= status_code < 300:
            return True, file_info
        else:
            bt.logging.warning(
                f"Resource URL returned non-success status code: {status_code}, URL: {url[:142]}..."
            )
            return False, file_info

    except Exception as e:
        bt.logging.error(f"Error checking resource URL: {str(e)}, URL: {url[:142]}...")
        return False, {}


async def http_head_request(url, timeout=10):
    """
    Send HTTP HEAD request

    Args:
        url: Request URL
        timeout: Timeout in seconds

    Returns:
        int: HTTP status code, returns 0 if error occurs
    """
    try:
        # Use aiohttp for asynchronous HTTP request
        async with aiohttp.ClientSession() as session:
            async with session.head(
                url, timeout=timeout, allow_redirects=True
            ) as response:
                return response.status

    except aiohttp.ClientError as e:
        bt.logging.error(f"HTTP HEAD request error: {str(e)}")
        return 0
    except Exception as e:
        bt.logging.error(f"Error during HTTP request: {str(e)}")
        return 0


async def http_get_request(url, timeout=30, json_response=False):
    """
    Send HTTP GET request

    Args:
        url: Request URL
        timeout: Timeout in seconds, default is 30 seconds
        json_response: Whether to parse the response as JSON, default is False

    Returns:
        If json_response is True, returns the parsed JSON object
        Otherwise returns the response text, or None if error occurs
    """
    try:
        # Use aiohttp for asynchronous HTTP request
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout) as response:
                if response.status >= 400:
                    bt.logging.error(
                        f"HTTP GET request failed, status code: {response.status}, URL: {url[:142]}..."
                    )
                    return None

                if json_response:
                    return await response.json()
                else:
                    return await response.text()

    except aiohttp.ClientError as e:
        bt.logging.error(f"HTTP GET request error: {str(e)}")
        return None
    except Exception as e:
        bt.logging.error(f"Error during HTTP request: {str(e)}")
        return None


async def http_post_request(
    url, data=None, json_data=None, timeout=30, json_response=False
):
    """
    Send HTTP POST request

    Args:
        url: Request URL
        data: Form data, default is None
        json_data: JSON data, default is None
        timeout: Timeout in seconds, default is 30 seconds
        json_response: Whether to parse the response as JSON, default is False

    Returns:
        If json_response is True, returns the parsed JSON object
        Otherwise returns the response text, or None if error occurs
    """
    try:
        # Use aiohttp for asynchronous HTTP request
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, data=data, json=json_data, timeout=timeout
            ) as response:
                if response.status >= 400:
                    bt.logging.error(
                        f"HTTP POST request failed, status code: {response.status}, URL: {url[:142]}..."
                    )
                    return None

                if json_response:
                    return await response.json()
                else:
                    return await response.text()

    except aiohttp.ClientError as e:
        bt.logging.error(f"HTTP POST request error: {str(e)}")
        return None
    except Exception as e:
        bt.logging.error(f"Error during HTTP request: {str(e)}")
        return None


def generate_signature_message(body, exclude_fields=None):
    """
    Generate signature message

    Args:
        body: Request body dictionary
        exclude_fields: List of fields to exclude

    Returns:
        str: Signature message
    """
    if exclude_fields is None:
        exclude_fields = []

    # Create new dictionary, excluding specified fields
    filtered_body = {k: v for k, v in body.items() if k not in exclude_fields}
    sorted_items = sorted(filtered_body.items())

    def format_value(value: Any) -> str:
        if isinstance(value, (dict, list, bool)) or value is None:
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        return str(value)

    return "&".join(f"{key}={format_value(value)}" for key, value in sorted_items)


async def request_upload_url(validator_wallet, task_id=None):
    """
    Request upload URL and keys

    Args:
        validator_wallet: Validator wallet
        task_id: Task ID

    Returns:
        dict: Dictionary containing upload URL and keys, or None if failed
    """
    try:
        # Get API URL from environment variables
        owner_host = os.environ.get("OWNER_HOST", "")

        # Try to get server time
        timestamp = int(time.time())

        # Prepare request body
        body = {
            "validator_hotkey": validator_wallet.hotkey.ss58_address,
            "timestamp": timestamp,
            "task_id": task_id,
        }

        # Generate signature message
        exclude_fields = ["validator_signature"]
        message = generate_signature_message(body, exclude_fields)
        # bt.logging.debug(f"Generated signature message: {message}")

        # Sign with wallet
        signature = validator_wallet.hotkey.sign(message.encode()).hex()
        # bt.logging.debug(f"Generated signature: {signature}")

        # Add signature to request body
        body["validator_signature"] = signature

        api_url = f"{owner_host}/v1/validator/upload-url"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        bt.logging.info(f"Requesting upload URL: /upload-url")
        # bt.logging.debug(f"Request body: {body}")

        async with aiohttp.ClientSession() as session:
            async with session.post(
                api_url, json=body, headers=headers, timeout=30
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    bt.logging.info(f"Successfully obtained upload URL")
                    return result
                else:
                    error_text = await response.text()
                    bt.logging.error(
                        f"Failed to request upload URL: status code {response.status}, error: {error_text}"
                    )
                    return None
    except Exception as e:
        bt.logging.error(f"Error requesting upload URL: {str(e)}")
        bt.logging.error(traceback.format_exc())
        return None


async def get_result_upload_urls(validator_wallet, task_id, outputs_info):
    """
    Get result file upload URLs
    """
    try:
        # Get API URL from environment variables
        owner_host = os.environ.get("OWNER_HOST", "")
        if not owner_host:
            bt.logging.error("OWNER_HOST environment variable not set")
            return None

        body = {
            "validator_hotkey": validator_wallet.hotkey.ss58_address,
            "task_id": task_id,
            "timestamp": int(time.time()),
            "outputs": outputs_info,
        }

        message = generate_signature_message(body)
        signature = validator_wallet.hotkey.sign(message.encode()).hex()
        body["validator_signature"] = signature

        api_url = f"{owner_host}/v1/validator/batch-upload-url"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        async with aiohttp.ClientSession() as session:
            async with session.post(
                api_url, json=body, headers=headers, timeout=30
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    return result
                else:
                    error_text = await response.text()
                    bt.logging.error(
                        f"Failed to get result upload URLs: status code {response.status}, error: {error_text}"
                    )
                    return None
    except Exception as e:
        bt.logging.error(f"Error getting result upload URLs: {str(e)}")
        return None


async def upload_task_result(
    task_id,
    validator_wallet,
    miner_hotkey,
    score,
    verification_result,
    processing_time,
    is_error=False,
):
    """
    Upload task result to server

    Args:
        validator_wallet: Validator wallet
        miner_hotkey: Miner hotkey address
        score: Task score
        verification_result: Verification result details
        processing_time: Processing time
        is_error: Whether the task is failed
    """
    try:
        # Get API URL from environment variables
        owner_host = os.environ.get("OWNER_HOST", "")
        if not owner_host:
            bt.logging.error("OWNER_HOST environment variable not set")
            return

        # Get current timestamp
        timestamp = int(time.time())

        # Prepare request body
        body = {
            "task_id": task_id,
            "validator_hotkey": validator_wallet.hotkey.ss58_address,
            "miner_hotkey": miner_hotkey,
            "model": "Wan 2.1",
            "resolution": "512 x 512",
            "duration": 3,
            "processing_time": processing_time,
            "frame_rate": 23,
            "score": score,
            "score_detail": verification_result,
            "task_detail": None,
            "is_failed": is_error,
            "timestamp": timestamp,
        }

        # Generate signature message
        exclude_fields = ["validator_signature"]
        message = generate_signature_message(body, exclude_fields)
        # bt.logging.debug(f"Generated signature message: {message}")

        # Sign with wallet
        signature = validator_wallet.hotkey.sign(message.encode()).hex()
        # bt.logging.debug(f"Generated signature: {signature}")

        # Add signature to request body
        body["validator_signature"] = signature

        api_url = f"{owner_host}/v1/validator/record-task"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        bt.logging.info(f"Uploading task result {task_id}")

        async with aiohttp.ClientSession() as session:
            async with session.post(
                api_url, json=body, headers=headers, timeout=30
            ) as response:
                if response.status == 204:
                    bt.logging.info("Task result uploaded successfully")
                else:
                    error_text = await response.text()
                    bt.logging.error(
                        f"Failed to upload task result: status code {response.status}, error: {error_text}"
                    )
    except Exception as e:
        bt.logging.error(f"Error uploading task result: {str(e)}")


async def upload_miner_completion(
    task_id,
    validator_wallet,
    task_details,
):
    """
    Upload miner completion to server

    Args:
        task_id: Task ID
        validator_wallet: Validator wallet
        task_details: Task details containing miner_hotkey and completion_time
    """
    try:
        # Get API URL from environment variables
        owner_host = os.environ.get("OWNER_HOST", "")
        if not owner_host:
            bt.logging.error("OWNER_HOST environment variable not set")
            return

        # Get current timestamp
        timestamp = int(time.time())

        bt.logging.info(
            f"Uploading miner completion {task_id} completion time {task_details['completion_time']}"
        )

        body = {
            "task_id": task_id,
            "validator_hotkey": validator_wallet.hotkey.ss58_address,
            "miner_hotkey": task_details["miner_hotkey"],
            "model": "Wan 2.1",
            "resolution": "512 x 512",
            "duration": 3,
            "processing_time": task_details["completion_time"],
            "frame_rate": 23,
            "task_detail": None,
            "timestamp": timestamp,
        }

        # Generate signature message
        exclude_fields = ["validator_signature"]
        message = generate_signature_message(body, exclude_fields)
        # bt.logging.debug(f"Generated signature message: {message}")

        # Sign with wallet
        signature = validator_wallet.hotkey.sign(message.encode()).hex()
        # bt.logging.debug(f"Generated signature: {signature}")

        body["validator_signature"] = signature

        api_url = f"{owner_host}/v1/validator/record-miner-completion"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        bt.logging.info(f"Uploading miner completion: /record-miner-completion")

        async with aiohttp.ClientSession() as session:
            async with session.post(
                api_url, json=body, headers=headers, timeout=30
            ) as response:
                if response.status == 204:
                    bt.logging.info("Miner completion uploaded successfully")
                else:
                    error_text = await response.text()
                    bt.logging.error(
                        f"Failed to upload miner completion: status code {response.status}, error: {error_text}"
                    )
    except Exception as e:
        bt.logging.error(f"Error uploading miner completion: {str(e)}")


def upload_cache_file_sync(validator_wallet, cache_file_path=None):
    """
    Upload cache file to server

    Args:
        validator_wallet: Validator wallet
        cache_file_path: Path to the cache file

    Returns:
        dict: Response data, or None if failed
    """
    try:
        # Get API URL from environment variables
        owner_host = os.environ.get("OWNER_HOST", "")
        if not owner_host:
            bt.logging.error("OWNER_HOST environment variable not set")
            return None

        # Use default cache file path if not provided
        if cache_file_path is None:
            return None

        # Read cache file if it exists
        cache_data = {}
        if os.path.exists(cache_file_path):
            try:
                with open(cache_file_path, "r") as f:
                    cache_data = json.load(f)
            except Exception as e:
                bt.logging.error(f"Error reading cache file")
                bt.logging.error(traceback.format_exc())
                return None
        else:
            bt.logging.warning(f"Cache file not found")
            return None

        # Prepare request body
        body = cache_data

        body["timestamp"] = int(time.time())
        body["validator_hotkey"] = validator_wallet.hotkey.ss58_address

        # Generate signature message
        exclude_fields = ["validator_signature"]
        message = generate_signature_message(body, exclude_fields)
        # Sign with wallet
        signature = validator_wallet.hotkey.sign(message.encode()).hex()
        # Add signature to request body
        body["validator_signature"] = signature

        api_url = f"{owner_host}/v1/validator/submit-score"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        response = requests.post(api_url, json=body, headers=headers, timeout=30)

        if response.status_code == 204:
            bt.logging.info("Cache file uploaded successfully")
            return True
        else:
            bt.logging.error(
                f"Failed to upload cache file: status code {response.status_code}"
            )
            return None

    except Exception as e:
        bt.logging.error(f"Error uploading cache file")
        return None


def adjust_config(validator_wallet):
    """
    Adjust config

    Args:
        validator_wallet: Validator wallet

    Returns:
        dict: Response data, or None if failed
    """
    try:
        # Get API URL from environment variables
        owner_host = os.environ.get("OWNER_HOST", "")
        if not owner_host:
            bt.logging.error("OWNER_HOST environment variable not set")
            return None

        # Prepare request body
        body = {}

        body["timestamp"] = int(time.time())
        body["validator_hotkey"] = validator_wallet.hotkey.ss58_address

        # Generate signature message
        message = generate_signature_message(body)
        # Sign with wallet
        signature = validator_wallet.hotkey.sign(message.encode()).hex()
        # Add signature to request body
        body["validator_signature"] = signature

        api_url = f"{owner_host}/v1/validator/config"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        response = requests.post(api_url, json=body, headers=headers, timeout=30)

        if response.status_code == 200:
            return response.json()
        else:
            return None

    except Exception as e:
        bt.logging.error(f"Error loading config")
        return None


async def http_put_request(
    url, data=None, headers=None, timeout=30, json_response=False
):
    """
    Send HTTP PUT request

    Args:
        url: Request URL
        data: Data to upload, can be bytes or string, default is None
        headers: Request header dictionary, default is None
        timeout: Timeout in seconds, default is 30 seconds
        json_response: Whether to parse the response as JSON, default is False

    Returns:
        If json_response is True, returns the parsed JSON object
        Otherwise returns the response text, or None if error occurs
    """
    try:
        # Use aiohttp for asynchronous HTTP request
        async with aiohttp.ClientSession() as session:
            async with session.put(
                url, data=data, headers=headers, timeout=timeout
            ) as response:
                if response.status >= 400:
                    bt.logging.error(
                        f"HTTP PUT request failed, status code: {response.status}, URL: {url[:142]}..."
                    )
                    return None, response.status

                if json_response:
                    return await response.json(), response.status
                else:
                    return await response.text(), response.status

    except aiohttp.ClientError as e:
        bt.logging.error(f"HTTP PUT request error: {str(e)}")
        return None, 0
    except Exception as e:
        bt.logging.error(f"Error during HTTP request: {str(e)}")
        return None, 0


# Synchronous version of HTTP PUT request, for non-async environments
def http_put_request_sync(
    url, data=None, headers=None, timeout=30, json_response=False
):
    """
    Send HTTP PUT request (synchronous version)

    Args:
        url: Request URL
        data: Data to upload, can be bytes or string, default is None
        headers: Request header dictionary, default is None
        timeout: Timeout in seconds, default is 30 seconds
        json_response: Whether to parse the response as JSON, default is False

    Returns:
        If json_response is True, returns the parsed JSON object
        Otherwise returns the response text, or None if error occurs
    """
    try:
        # Send PUT request
        response = requests.put(url, data=data, headers=headers, timeout=timeout)

        if response.status_code >= 400:
            bt.logging.error(
                f"HTTP PUT request failed, status code: {response.status_code}, URL: {url[:142]}..."
            )
            return None, response.status_code

        if json_response:
            return response.json(), response.status_code
        else:
            return response.text, response.status_code

    except requests.RequestException as e:
        bt.logging.error(f"HTTP PUT request error: {str(e)}")
        return None, 0
    except Exception as e:
        bt.logging.error(f"Error during HTTP request: {str(e)}")
        return None, 0


# Synchronous version of HTTP GET request, for non-async environments
def http_get_request_sync(
    url, headers=None, timeout=30, json_response=False, stream=False
):
    """
    Send HTTP GET request (synchronous version)

    Args:
        url: Request URL
        headers: Request header dictionary, default is None
        timeout: Timeout in seconds, default is 30 seconds
        json_response: Whether to parse the response as JSON, default is False
        stream: Whether to return the response as a stream, default is False

    Returns:
        If json_response is True, returns the parsed JSON object
        If stream is True, returns the response object
        Otherwise returns the response content, or None if error occurs
    """
    try:
        # Send GET request
        response = requests.get(url, headers=headers, timeout=timeout, stream=stream)

        if response.status_code >= 400:
            bt.logging.error(
                f"HTTP GET request failed, status code: {response.status_code}, URL: {url[:142]}..."
            )
            return None, response.status_code

        if stream:
            return response, response.status_code
        elif json_response:
            return response.json(), response.status_code
        else:
            return response.content, response.status_code

    except requests.RequestException as e:
        bt.logging.error(f"HTTP GET request error: {str(e)}")
        return None, 0
    except Exception as e:
        bt.logging.error(f"Error during HTTP request: {str(e)}")
        return None, 0


def check_get_url_resource_available_sync(url, get_file_info=False, timeout=10):
    """
    Synchronously check if URL resource is available, using requests library
    Optionally get file size and creation time

    Args:
        url: URL to check
        timeout: Timeout in seconds, default is 10 seconds
        get_file_info: Whether to get file information, default is False

    Returns:
        If get_file_info is False, returns HTTP status code
        If get_file_info is True, returns (status code, file_info object)
    """
    if not url:
        return False if not get_file_info else (False, {})

    try:
        # Send HEAD request to get header information
        head_response = requests.get(url, timeout=timeout)
        status_code = head_response.status_code

        # If file information is not needed, return status code directly
        if not get_file_info:
            bt.logging.info(
                f"HTTP HEAD: status code {status_code}, URL: {url[:142]}..."
            )
            return (status_code, {})

        # Create file_info object
        file_info = {}

        # Get file size
        content_length = head_response.headers.get("Content-Length")
        file_info["file_size"] = (
            int(content_length) if content_length and content_length.isdigit() else None
        )

        # Get file creation/modification time
        last_modified_str = head_response.headers.get("Last-Modified", "")
        file_info["last_modified"] = last_modified_str

        # Convert Last-Modified header to datetime object if it exists
        if last_modified_str:
            try:
                # Parse the RFC 7232 date format
                parsed_date = email.utils.parsedate_to_datetime(last_modified_str)
                if parsed_date:
                    file_info["last_modified"] = parsed_date
            except Exception as e:
                bt.logging.warning(
                    f"Failed to parse Last-Modified date: {last_modified_str}, error: {str(e)}"
                )

        bt.logging.info(
            f"HTTP HEAD: status code {status_code}, URL: {url[:142]}..., file size: {file_info['file_size']}, last modified: {file_info['last_modified']}"
        )
        return (status_code, file_info)

    except requests.exceptions.RequestException as e:
        bt.logging.error(f"Error checking resource URL: {str(e)}, URL: {url[:142]}...")
        return False if not get_file_info else (False, {})
    except Exception as e:
        bt.logging.error(f"Error checking resource URL: {str(e)}, URL: {url[:142]}...")
        return False if not get_file_info else (False, {})


def get_consensus_scores_sync():
    """
    Synchronously get miner consensus scores

    Args:
        miner_hotkey: Optional, specify the hotkey of the miner, if provided, only return the score of the miner

    Returns:
        dict: Dictionary containing miner consensus scores, or None if request fails
    """
    try:
        owner_host = os.environ.get("OWNER_HOST", "")
        if not owner_host:
            bt.logging.error("OWNER_HOST environment variable not set, using default")

        url = f"{owner_host}/v1/public/consensus-scores"

        headers = {
            "accept": "application/json",
            "cache-control": "no-cache",
            "pragma": "no-cache",
        }
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 200:
            result = response.json()
            bt.logging.info(f"Successfully obtained consensus scores")
            return result
        else:
            bt.logging.error(
                f"Failed to get consensus scores: status code {response.status_code}, error: {response.text}"
            )
            return None
    except Exception as e:
        bt.logging.error(f"Error getting consensus scores: {str(e)}")
        bt.logging.error(traceback.format_exc())
        return None


def batch_download_outputs(
    outputs: dict, comfy_url: str, out_dir: str, timeout: int = 120, token: str = None
) -> list[str]:
    """
    Batch download all output files from ComfyUI, rename by node
    """
    try:
        os.makedirs(out_dir, exist_ok=True)
        downloaded_files = []

        # Download all output files and rename by node
        for node, node_output in outputs.items():
            for key in node_output:
                if not isinstance(node_output[key], list):
                    continue
                for fileinfo in node_output[key]:
                    if not isinstance(fileinfo, dict):
                        continue
                    filename = fileinfo.get("filename")
                    file_type = fileinfo.get("type")
                    subfolder = fileinfo.get("subfolder")
                    if not filename:
                        continue

                    file_url = f"{comfy_url}/view?filename={filename}&type={file_type}&subfolder={urllib.parse.quote(subfolder)}"
                    headers = {}
                    if token:
                        headers["Authorization"] = f"Bearer {token}"

                    response, status_code = http_get_request_sync(
                        file_url, headers=headers, timeout=timeout
                    )

                    if status_code == 200 and response:
                        # Rename file by node
                        local_name = f"{out_dir}/{node}_{key}_{filename}"
                        os.makedirs(os.path.dirname(local_name), exist_ok=True)
                        with open(local_name, "wb") as f:
                            f.write(response)
                        downloaded_files.append(
                            {
                                "node": node,
                                "key": key,
                                "filename": filename,
                                "local_name": local_name,
                                "out_dir": out_dir,
                            }
                        )
                        bt.logging.info(
                            f"Downloaded: {node}_{filename} to {local_name}"
                        )
                    else:
                        bt.logging.warning(
                            f"Failed to download {filename}: status {status_code}"
                        )

        if not downloaded_files:
            return None

        # Save outputs.json
        outputs_json_path = f"{out_dir}/outputs.json"
        with open(outputs_json_path, "w") as f:
            json.dump(outputs, f, indent=2)
        downloaded_files.append(
            {
                "node": "outputs",
                "key": "json",
                "filename": "outputs.json",
                "local_name": outputs_json_path,
                "out_dir": out_dir,
            }
        )

        return downloaded_files
    except Exception as e:
        bt.logging.error(f"Error in batch download outputs: {str(e)}")
        bt.logging.error(traceback.format_exc())
        return None


def batch_download_and_package_outputs(
    outputs: dict,
    comfy_url: str,
    task_id: str,
    upload_url: str = None,
    timeout: int = 120,
    token: str = None,
) -> tuple[bool, str, str]:
    """
    Batch download all output files from ComfyUI, rename by node, package with outputs.json and upload

    Args:
        outputs: Outputs dict from ComfyUI history
        comfy_url: ComfyUI server URL (e.g., "http://127.0.0.1:8188")
        task_id: Task ID for naming
        upload_url: Optional upload URL for the final zip
        timeout: Download timeout in seconds
        token: ComfyUI authentication token

    Returns:
        tuple[bool, str, str]: (success, zip_path, error_message)
    """

    try:
        # Create temporary directory for files
        out_dir = f"tmp_tasks/{task_id}"

        downloaded_files = batch_download_outputs(
            outputs, comfy_url, out_dir, timeout, token=token
        )
        if not downloaded_files:
            return False, "", "No files were successfully downloaded"

        # Create zip file
        zip_path = f"{out_dir}.zip"
        with zipfile.ZipFile(zip_path, "w") as zipf:
            for file in downloaded_files:
                zipf.write(
                    file["local_name"], arcname=os.path.basename(file["local_name"])
                )

        bt.logging.info(
            f"Created zip package: {zip_path} with {len(downloaded_files)} files"
        )

        # Upload if URL provided
        if upload_url:
            with open(zip_path, "rb") as f:
                data = f.read()

            for attempt in range(3):
                resp_text, status_code = http_put_request_sync(
                    upload_url, data=data, headers={}, timeout=180
                )
                if status_code in [200, 201, 204]:
                    bt.logging.info(f"Zip uploaded successfully for task {task_id}")
                    break
                else:
                    bt.logging.error(
                        f"Upload attempt {attempt + 1} failed: {status_code}, {resp_text}"
                    )
                    if attempt < 2:
                        time.sleep(5)
            else:
                bt.logging.error(f"Failed to upload zip after 3 attempts")

        return True, zip_path, ""

    except Exception as e:
        error_msg = f"Error in batch download and package: {str(e)}"
        bt.logging.error(error_msg)
        bt.logging.error(traceback.format_exc())
        return False, "", error_msg


def cleanup_task_files(task_id: str):
    """
    Cleanup task files

    Args:
        task_id: Task ID
    """
    # Clean up temporary directory
    task_dir = f"tmp_tasks/{task_id}"
    if os.path.exists(task_dir):
        shutil.rmtree(task_dir)
        bt.logging.info(f"Cleaned up temporary directory for task {task_id}")

    # Clean up zip file
    zip_path = f"tmp_tasks/{task_id}.zip"
    if os.path.exists(zip_path):
        os.remove(zip_path)
        bt.logging.info(f"Cleaned up zip file for task {task_id}")


async def get_online_task_count(validator_wallet, count=1):
    """
    Get task count from online API

    Args:
        validator_wallet: Validator wallet
        count: Number of tasks to request

    Returns:
        int: Number of tasks available from online API
    """
    try:
        # Get API URL from environment variables
        owner_host = os.environ.get("OWNER_HOST", "")
        if not owner_host:
            bt.logging.error("OWNER_HOST environment variable not set")
            return 0, []

        # Get current timestamp
        timestamp = int(time.time())

        # Prepare request body
        body = {
            "validator_hotkey": validator_wallet.hotkey.ss58_address,
            "timestamp": timestamp,
            "count": count,  # Request specified number of tasks
        }

        # Generate signature message
        exclude_fields = ["validator_signature"]
        message = generate_signature_message(body, exclude_fields)

        # Sign with wallet
        signature = validator_wallet.hotkey.sign(message.encode()).hex()
        body["validator_signature"] = signature

        # Make API request
        api_url = f"{owner_host}/v1/validator/get-tasks"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        bt.logging.debug(f"Requesting task count : {count}")

        async with aiohttp.ClientSession() as session:
            async with session.post(
                api_url, json=body, headers=headers, timeout=30
            ) as response:

                if response.status == 200:
                    result = await response.json()
                    return result.get("count", 0), result.get("tasks", [])
                else:
                    error_text = await response.text()
                    bt.logging.error(
                        f"Failed to get task count from online API: status code {response.status}, error: {error_text}"
                    )
                    return 0, []
    except Exception as e:
        bt.logging.error(f"Error getting task count from online API: {str(e)}")
        bt.logging.error(traceback.format_exc())
        return 0, []


async def register_miner_api_keys(miner_wallet, channels=None, models_dict=None):
    """
    Register miner API keys with Owner

    Args:
        miner_wallet: Miner wallet
        channels: List of channel configs [{"model": "sora-2", "base_url": "string", "keys": ["string"]}]
        models_dict: Dictionary of {model_name: [key1, key2]} (legacy format)
    """
    try:
        owner_host = os.environ.get("OWNER_HOST", "")
        if not owner_host:
            bt.logging.error("OWNER_HOST environment variable not set")
            return False

        timestamp = int(time.time())

        body = {
            "miner_hotkey": miner_wallet.hotkey.ss58_address,
            "timestamp": timestamp,
        }
        if channels:
            body["channels"] = channels
        if models_dict:
            body.update(models_dict)

        # Generate signature
        exclude_fields = ["miner_signature"]
        message = generate_signature_message(body, exclude_fields)
        signature = miner_wallet.hotkey.sign(message.encode()).hex()
        body["miner_signature"] = signature

        api_url = f"{owner_host}/v2/miner/register"
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        bt.logging.info(f"Registering API keys: {body}")

        async with aiohttp.ClientSession() as session:
            async with session.post(
                api_url, json=body, headers=headers, timeout=30
            ) as response:
                if response.status >= 200 and response.status < 300:
                    bt.logging.info("Successfully registered API keys")
                    return True
                else:
                    error_text = await response.text()
                    bt.logging.error(
                        f"Failed to register API keys: {response.status}, {error_text}"
                    )
                    return False

    except Exception as e:
        bt.logging.error(f"Error registering API keys: {str(e)}")
        return False


async def sync_miner_api_models(validator_wallet):
    """
    Get miner supported models from Owner
    """
    try:
        owner_host = os.environ.get("OWNER_HOST", "")
        if not owner_host:
            return {}

        api_url = f"{owner_host}/v2/public/miner-models"

        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, timeout=30) as response:
                if response.status == 200:
                    return await response.json()
                return {}
    except Exception as e:
        bt.logging.error(f"Error syncing miner models: {str(e)}")
        return {}


async def get_api_miner_stats(validator_wallet):
    """
    Get API miner statistics for scoring
    """
    try:
        owner_host = os.environ.get("OWNER_HOST", "")
        if not owner_host:
            return {}

        timestamp = int(time.time())
        start_ts = timestamp - 2 * 600

        body = {
            "validator_hotkey": validator_wallet.hotkey.ss58_address,
            "timestamp": timestamp,
            "start_ts": start_ts,
        }

        message = generate_signature_message(body, ["validator_signature"])
        signature = validator_wallet.hotkey.sign(message.encode()).hex()
        body["validator_signature"] = signature

        api_url = f"{owner_host}/v2/validator/miner-stats"
        headers = {"Content-Type": "application/json"}

        async with aiohttp.ClientSession() as session:
            async with session.post(
                api_url, json=body, headers=headers, timeout=30
            ) as response:
                if response.status == 200:
                    return await response.json()
                return {}
    except Exception as e:
        bt.logging.error(f"Error getting API miner stats: {str(e)}")
        return {}


async def listen_for_api_tasks(validator_wallet, task_handler):
    """
    Listen for API tasks via WebSocket and respond with decision

    Args:
        validator_wallet: Validator wallet
        task_handler: Async function that takes request data and returns decision dict
    """
    while True:
        try:
            owner_host = os.environ.get("OWNER_HOST", "")
            if not owner_host:
                bt.logging.error("OWNER_HOST environment variable not set")
                await asyncio.sleep(60)
                continue

            ws_host = owner_host.replace("http://", "ws://").replace(
                "https://", "wss://"
            )

            timestamp = int(time.time())
            auth_body = {
                "validator_hotkey": validator_wallet.hotkey.ss58_address,
                "timestamp": timestamp,
            }
            message = generate_signature_message(auth_body)
            signature = validator_wallet.hotkey.sign(message.encode()).hex()

            query_params = {
                "validator_hotkey": validator_wallet.hotkey.ss58_address,
                "timestamp": str(timestamp),
                "validator_signature": signature,
            }
            ws_url = f"{ws_host}/v2/validator/ws?{urllib.parse.urlencode(query_params)}"

            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_url) as ws:
                    bt.logging.info("Connected to API task stream")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                                match data.get("type"):
                                    case "ping":
                                        await ws.send_json({"type": "pong"})
                                        continue

                                    case "request_miner":
                                        decision = await task_handler(data)

                                        response_data = {
                                            "type": "miner_response",
                                            "miner_hotkey": None,
                                            "request_id": data["request_id"],
                                            **decision,
                                        }

                                        await ws.send_json(response_data)
                                        continue
                                    case _:
                                        bt.logging.info(
                                            f"Unknown message type: {data.get('type')}, full data: {data}"
                                        )
                                        continue
                            except Exception as e:
                                bt.logging.error(
                                    f"Error processing WS message: {str(e)}"
                                )
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            bt.logging.error(
                                "ws connection closed with exception %s", ws.exception()
                            )
                            break

        except Exception as e:
            bt.logging.error(f"Error in API task listener: {str(e)}")
            await asyncio.sleep(30)
