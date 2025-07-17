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

    async def download_video(self, task_id, video_url, video_type):
        """
        Download video from URL to cache directory

        Args:
            task_id: Task ID
            video_url: URL to video
            video_type: Type of video ('miner' or 'validator')

        Returns:
            tuple: (success, video_path) where success is a boolean and video_path is the path to the downloaded video
        """
        # Get target path based on video type
        target_path = self.get_video_cache_paths(task_id, video_type)

        # Check if video already exists
        if os.path.exists(target_path) and os.path.getsize(target_path) > 0:
            bt.logging.info(
                f"{video_type.capitalize()} video already exists for task {task_id}: {target_path}"
            )
            return True, target_path

        # Ensure directory exists
        os.makedirs(os.path.dirname(target_path), exist_ok=True)

        # Download video directly
        bt.logging.info(f"Downloading {video_type} video for task {task_id}")

        loop = asyncio.get_event_loop()
        download_success = await loop.run_in_executor(
            None, self.download_url_video, video_url, target_path
        )

        if (
            download_success
            and os.path.exists(target_path)
            and os.path.getsize(target_path) > 0
        ):
            bt.logging.info(
                f"Download successful for {video_type} video, task {task_id}, saved to {target_path}"
            )
            return True, target_path
        else:
            bt.logging.warning(
                f"Download failed for {video_type} video, task {task_id}"
            )
            return False, target_path

    def get_video_cache_paths(self, task_id, video_type=None):
        """
        Generate video cache paths

        Args:
            task_id: Task ID
            video_type: Optional, type of video ('miner' or 'validator')

        Returns:
            If video_type is None:
                tuple: (cache_dir, miner_video_path, validator_video_path)
            If video_type is specified:
                str: Path to the specified video type
        """
        # Get current date as folder name
        date_str = datetime.now().strftime("%Y%m%d")

        # Create cache directory structure: temp/video_cache/date/task_id/
        date_dir = os.path.join(self.video_cache_dir, date_str)
        task_dir = os.path.join(date_dir, task_id)

        # Generate miner and validator video paths
        miner_video_path = os.path.join(task_dir, "miner.mp4")
        validator_video_path = os.path.join(task_dir, "validator.mp4")

        # Return specific path if video_type is specified
        if video_type == "miner":
            return miner_video_path
        elif video_type == "validator":
            return validator_video_path

        # Return all paths by default
        return task_dir, miner_video_path, validator_video_path

    def clean_old_video_cache(self, days=2):
        """
        Clean video cache older than specified days

        Args:
            days: Number of days to keep, default is 2
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
