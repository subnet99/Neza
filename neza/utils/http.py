"""
HTTP Utility Module
Provides HTTP related utility functions
"""

from typing import Any
import aiohttp
import bittensor as bt
import os
import time
from dotenv import load_dotenv
import requests
import traceback
import json
import email.utils

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
            return json.dumps(value, separators=(",", ":"))
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
        bt.logging.debug(
            f"Using timestamp: {timestamp}, formatted time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))}"
        )

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

        bt.logging.info(f"Requesting upload URL: {api_url}")
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


async def upload_task_result(
    task_id,
    validator_wallet,
    miner_hotkey,
    score,
    verification_result,
    processing_time,
    task_details,
):
    """
    Upload task result to server

    Args:
        validator_wallet: Validator wallet
        miner_hotkey: Miner hotkey address
        score: Task score
        verification_result: Verification result details
        processing_time: Processing time
        task_details: Task details
    """
    try:
        # Get API URL from environment variables
        owner_host = os.environ.get("OWNER_HOST", "")
        if not owner_host:
            bt.logging.error("OWNER_HOST environment variable not set")
            return

        # Get current timestamp
        timestamp = int(time.time())
        bt.logging.debug(
            f"Using timestamp: {timestamp}, formatted time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))}"
        )

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
            "task_detail": (
                json.loads(json.dumps(task_details, default=str))
                if task_details
                else {}
            ),
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

        bt.logging.info(f"Uploading task result: {api_url}")
        # bt.logging.debug(f"Request body: {body}")

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
        bt.logging.debug(
            f"Using timestamp: {timestamp}, formatted time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))}"
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
            "task_detail": (
                json.loads(json.dumps(task_details, default=str))
                if task_details
                else {}
            ),
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

        bt.logging.info(f"Uploading miner completion: {api_url}")

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
            return response.json()
        else:
            bt.logging.error(
                f"Failed to upload cache file: status code {response.status_code}"
            )
            return None

    except Exception as e:
        bt.logging.error(f"Error uploading cache file")
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
                    bt.logging.debug(
                        f"Converted Last-Modified to datetime: {parsed_date}"
                    )
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
