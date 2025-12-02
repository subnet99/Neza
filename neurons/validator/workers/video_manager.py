import tempfile
import shutil
import os
import threading
import time
import requests
from urllib.parse import urlparse
from datetime import datetime
import bittensor as bt
import asyncio
from datetime import timedelta
import zipfile
import json
import traceback
from neza.utils.tools import get_file_type


class VideoManager:
    def __init__(self):
        self.video_cache_dir = os.path.join(tempfile.gettempdir(), "video_cache")
        # Ensure base cache directory exists
        os.makedirs(self.video_cache_dir, exist_ok=True)

    def process_tasks_on_block(self, block):
        """
        Process tasks on new block

        Args:
            block: Block number (integer)
        """
        # Only clean cache every 14400 blocks (approximately every day with 6s blocks)
        if block % 14400 == 0:
            self.clean_old_video_cache()
            bt.logging.info(f"Cleaned old video cache on block {block}")

    async def download_and_extract_package(self, task_id, package_url, package_type):
        """
        Download and extract package from URL to cache directory

        Args:
            task_id: Task ID
            package_url: URL to package (zip file)
            package_type: Type of package ('miner' or 'validator')

        Returns:
            tuple: (success, extracted_dir_path, outputs_info) where:
                - success is a boolean
                - extracted_dir_path is the path to the extracted directory
                - outputs_info is the parsed outputs.json content
        """
        try:
            # Get target paths
            zip_path, extracted_dir = self.get_package_cache_paths(
                task_id, package_type, False
            )

            bt.logging.info(
                f"task_id: {task_id}, zip_path: {zip_path}, extracted_dir: {extracted_dir}"
            )

            # Ensure directory exists
            os.makedirs(os.path.dirname(zip_path), exist_ok=True)

            # Download package
            bt.logging.info(f"Downloading {package_type} package for task {task_id}")

            download_success = self.download_url_video(package_url, zip_path)
            if (
                not download_success
                or not os.path.exists(zip_path)
                or os.path.getsize(zip_path) == 0
            ):
                bt.logging.warning(
                    f"Download failed for {package_type} package, task {task_id}. "
                    f"Success: {download_success}, exists: {os.path.exists(zip_path)}, "
                    f"size: {os.path.getsize(zip_path) if os.path.exists(zip_path) else 'N/A'}"
                )
                return False, extracted_dir, None

            bt.logging.info(f"Extracting {package_type} package for task {task_id}")

            extract_success = self.extract_package(zip_path, extracted_dir)

            if not extract_success:
                bt.logging.warning(
                    f"Extraction failed for {package_type} package, task {task_id}"
                )
                return False, extracted_dir, None

            # Load outputs.json
            outputs_info = self.load_outputs_json(extracted_dir)
            if outputs_info is None:
                bt.logging.warning(
                    f"No outputs.json found in {package_type} package, task {task_id}"
                )
                return False, extracted_dir, None

            bt.logging.info(
                f"Package download and extraction successful for {package_type}, task {task_id}, extracted to {extracted_dir}"
            )
            return True, extracted_dir, outputs_info
        except Exception as e:
            bt.logging.error(f"Error downloading and extracting package: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False, None, None

    def get_video_cache_paths(
        self, task_id, video_type=None, is_find=False, file_type: str = ""
    ):
        """
        Generate video cache paths

        Args:
            task_id: Task ID
            video_type: Optional, type of video ('miner' or 'validator')
            file_type: Optional, type of file ('.mp4', '.zip', etc.)

        Returns:
            str: Path to the video file
        """

        if video_type == "miner":
            miner_video_path = self.find_video_file(
                task_id, video_type, is_find, 1, file_type
            )
            return miner_video_path
        elif video_type == "validator":
            validator_video_path = self.find_video_file(
                task_id, video_type, is_find, 1, file_type
            )
            return validator_video_path

        return ""

    def get_package_cache_paths(self, task_id, package_type=None, is_find=False):
        """
        Generate package cache paths

        Args:
            task_id: Task ID
            package_type: Optional, type of package ('miner' or 'validator')

        Returns:
            tuple: (zip_path, extracted_dir_path)
        """
        if not package_type:
            return None, None

        extracted_dir = self.get_video_cache_paths(task_id, package_type, is_find)
        # Generate zip and extracted directory paths
        zip_path = os.path.join(extracted_dir, f"miner.zip")

        return zip_path, extracted_dir

    def extract_package(self, zip_path, extract_dir):
        """
        Extract zip package to directory

        Args:
            zip_path: Path to zip file
            extract_dir: Directory to extract to

        Returns:
            bool: Whether extraction was successful
        """
        try:
            # Create extraction directory
            os.makedirs(extract_dir, exist_ok=True)

            # Extract zip file
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)

            return True
        except Exception as e:
            bt.logging.error(f"Error extracting package: {str(e)}")
            return False

    def load_outputs_json(self, extracted_dir):
        """
        Load and parse outputs.json from extracted directory

        Args:
            extracted_dir: Path to extracted directory

        Returns:
            dict: Parsed outputs.json content or None if not found
        """
        try:
            outputs_path = os.path.join(extracted_dir, "outputs.json")
            if not os.path.exists(outputs_path):
                bt.logging.warning(f"outputs.json not found in {extracted_dir}")
                return None

            with open(outputs_path, "r") as f:
                outputs_data = json.load(f)

            return outputs_data
        except Exception as e:
            bt.logging.error(f"Error loading outputs.json: {str(e)}")
            return None

    def get_files_from_outputs(self, outputs_info, extracted_dir, file_types=None):
        """
        Get file paths from outputs.json based on file types

        Args:
            outputs_info: Parsed outputs.json content
            extracted_dir: Path to extracted directory
            file_types: List of file types to filter (e.g., ['images', 'audio', 'video', 'gifs'])

        Returns:
            dict: Mapping of node_id -> {file_type -> [file_paths]}
        """
        try:
            files_mapping = {}

            for node_id, node_output in outputs_info.items():
                if not isinstance(node_output, dict):
                    continue

                for output_type, file_list in node_output.items():
                    if file_types and output_type not in file_types:
                        continue

                    if not isinstance(file_list, list):
                        continue

                    file_paths = []
                    for file_info in file_list:
                        if not isinstance(file_info, dict):
                            continue

                        filename = file_info.get("filename")
                        if filename:
                            save_filename = f"{node_id}_{output_type}_{filename}"
                            file_path = os.path.join(extracted_dir, save_filename)

                            if os.path.exists(file_path):
                                file_paths.append(
                                    {
                                        "base_name": filename,
                                        "file_name": save_filename,
                                        "file_path": file_path,
                                        "file_type": output_type,
                                        "file_type_og": get_file_type(
                                            file_info.get("format", ""), filename
                                        ),
                                    }
                                )

                    if file_paths:
                        files_mapping[f"{node_id}_{output_type}"] = file_paths

            return files_mapping
        except Exception as e:
            bt.logging.error(f"Error getting files from outputs: {str(e)}")
            return {}

    def find_video_file(
        self, task_id, video_type, is_find=False, days_to_back=1, file_type: str = ""
    ):
        """
        Find video file in cache directory
        """
        date_str = datetime.now().strftime("%Y%m%d")
        date_dir = os.path.join(self.video_cache_dir, date_str)
        task_dir = os.path.join(date_dir, task_id)
        today_video_path = os.path.join(task_dir, f"{video_type}{file_type}")

        if not is_find:
            return today_video_path

        if os.path.exists(today_video_path) and os.path.getsize(today_video_path) > 0:
            return today_video_path
        else:
            yesterday_str = (datetime.now() - timedelta(days=days_to_back)).strftime(
                "%Y%m%d"
            )
            yesterday_dir = os.path.join(self.video_cache_dir, yesterday_str)
            yesterday_task_dir = os.path.join(yesterday_dir, task_id)
            yesterday_video_path = os.path.join(
                yesterday_task_dir, f"{video_type}{file_type}"
            )
            return yesterday_video_path

    def clean_old_video_cache(self, days=7):
        """
        Clean video cache older than specified days

        Args:
            days: Number of days to keep, default is 7
        """
        try:
            # Get base cache directory
            if not os.path.exists(self.video_cache_dir):
                return

            # Get current date
            today = datetime.now()

            # Iterate through date folders
            for date_dir in os.listdir(self.video_cache_dir):
                try:
                    # Try to parse folder name as date
                    dir_date = datetime.strptime(date_dir, "%Y%m%d")

                    # If date is older than retention days, delete it
                    if (today - dir_date).days > days:
                        dir_path = os.path.join(self.video_cache_dir, date_dir)
                        bt.logging.info(f"Cleaning old video cache: {dir_path}")
                        shutil.rmtree(dir_path)
                except ValueError:
                    # If folder name is not a date format, skip
                    continue
        except Exception as e:
            bt.logging.error(f"Error cleaning video cache: {str(e)}")

    def download_url_video(self, url, save_path, max_retries=3, timeout=60):
        """
        Download video from URL

        Args:
            url: Video URL
            save_path: Path to save the video
            max_retries: Maximum number of retry attempts
            timeout: Download timeout in seconds

        Returns:
            bool: Whether download was successful
        """
        # Check if URL is S3
        parsed_url = urlparse(url)
        if parsed_url.netloc.endswith("amazonaws.com") or parsed_url.netloc.endswith(
            "digitaloceanspaces.com"
        ):
            return self.download_from_s3(url, save_path)

        # Regular HTTP download
        retry_count = 0
        while retry_count < max_retries:
            try:
                # bt.logging.info(f"Downloading video from {url} to {save_path}")
                response = requests.get(url, stream=True, timeout=timeout)
                response.raise_for_status()

                # Save to file
                with open(save_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                # Check if file exists and has size > 0
                if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
                    bt.logging.info(f"Video download successful: {save_path}")
                    return True
                else:
                    bt.logging.warning(f"Downloaded file is empty: {save_path}")
                    retry_count += 1
                    time.sleep(1)

            except requests.exceptions.RequestException as e:
                bt.logging.error(f"Error downloading video: {str(e)}")
                retry_count += 1
                time.sleep(1)

        bt.logging.error(f"Failed to download video after {max_retries} attempts")
        return False

    def download_from_s3(self, s3_url, save_path):
        """
        Download file from S3 URL

        Args:
            s3_url: S3 URL
            save_path: Path to save the file

        Returns:
            bool: Whether download was successful
        """
        try:
            # bt.logging.info(f"Downloading from S3: {s3_url}")
            response = requests.get(s3_url, stream=True)
            response.raise_for_status()

            with open(save_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            return os.path.exists(save_path) and os.path.getsize(save_path) > 0
        except Exception as e:
            bt.logging.error(f"Error downloading from S3: {str(e)}")
            return False
