import json
import time
import os
import shutil
import threading
from typing import Dict, Any, Optional
from datetime import datetime
import uuid
import bittensor as bt


class TaskMappingManager:
    """
    Manages task mapping between task_id and prompt_id
    Uses JSON file storage with automatic cleanup
    """

    def __init__(
        self,
        hotkey: str = None,
        cleanup_interval: int = 300,
        default_expiration: int = 86400,
    ):
        """
        Initialize task mapping manager

        Args:
            hotkey: Miner hotkey address (used as filename)
            cleanup_interval: Cleanup interval in seconds (default: 300 = 5 minutes)
            default_expiration: Default expiration time in seconds (default: 86400 = 24 hours)
        """
        # Build file path: task_mapping/{hotkey}.json
        if hotkey:
            task_mapping_dir = "_task_mapping"
            os.makedirs(task_mapping_dir, exist_ok=True)
            self.file_path = os.path.join(task_mapping_dir, f"{hotkey}.json")
        else:
            self.file_path = "_task_mapping.json"

        self.mapping = {}
        self.lock = threading.RLock()
        self.default_expiration = default_expiration
        self.cleanup_interval = cleanup_interval
        self.last_cleanup = time.time()
        self.running = True

        # Initialize: load existing mappings
        self.mapping = self.load_mapping()
        bt.logging.info(
            f"Initialized TaskMappingManager with {len(self.mapping)} existing mappings from {self.file_path}"
        )

        # Start cleanup thread
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        bt.logging.info(f"Started cleanup thread with interval {cleanup_interval}s")

    def load_mapping(self) -> Dict[str, Any]:
        """Load mapping from file"""
        with self.lock:
            try:
                if os.path.exists(self.file_path):
                    with open(self.file_path, "r") as f:
                        data = json.load(f)
                        # Clean expired entries on load
                        current_time = time.time()
                        cleaned_data = {
                            k: v
                            for k, v in data.items()
                            if current_time <= v.get("expires_at", 0)
                        }
                        if len(cleaned_data) < len(data):
                            bt.logging.info(
                                f"Cleaned {len(data) - len(cleaned_data)} expired mappings on load"
                            )
                        bt.logging.info(
                            f"Loaded {len(cleaned_data)} task mappings from {self.file_path}"
                        )
                        return cleaned_data
                else:
                    bt.logging.info(
                        f"Task mapping file {self.file_path} not found, starting with empty mapping"
                    )
                    return {}
            except Exception as e:
                bt.logging.error(f"Error loading task mapping: {str(e)}")
                return {}

    def save_mapping(self):
        """Save mapping to file (real-time save)"""
        with self.lock:
            try:
                # Atomic write: write to temp file then rename
                temp_file = f"{self.file_path}.tmp"
                with open(temp_file, "w") as f:
                    json.dump(self.mapping, f, indent=2)
                shutil.move(temp_file, self.file_path)
                bt.logging.debug(
                    f"Saved {len(self.mapping)} task mappings to {self.file_path}"
                )
            except Exception as e:
                bt.logging.error(f"Error saving task mapping: {str(e)}")

    def _cleanup_loop(self):
        """Background thread for periodic cleanup"""
        while self.running:
            try:
                time.sleep(self.cleanup_interval)
                if self.running:
                    self.cleanup_expired(force=True)
            except Exception as e:
                bt.logging.error(f"Error in cleanup loop: {str(e)}")

    def stop(self):
        """Stop cleanup thread"""
        self.running = False
        if self.cleanup_thread.is_alive():
            self.cleanup_thread.join(timeout=1)

    def add_mapping(
        self,
        task_id: str,
        prompt_id: str,
        comfy_instance: str,
        expires_in_seconds: int = None,
        client_id: str = None,
        token: str = None,
    ):
        """
        Add new task mapping

        Args:
            task_id: Task ID
            prompt_id: ComfyUI prompt ID
            comfy_instance: ComfyUI instance identifier
            expires_in_seconds: Expiration time in seconds (default: 1 day)
            client_id: Client ID
            token: ComfyUI authentication token
        """
        if expires_in_seconds is None:
            expires_in_seconds = self.default_expiration

        current_time = int(time.time())
        expires_at = current_time + expires_in_seconds

        mapping_data = {
            "prompt_id": prompt_id,
            "comfy_instance": comfy_instance,
            "created_at": current_time,
            "expires_at": expires_at,
            "status": "active",
            "client_id": client_id or str(uuid.uuid4()),
            "token": token,
        }

        bt.logging.info(
            f"Creating mapping for task {task_id} with client_id: {mapping_data['client_id']}"
        )

        with self.lock:
            self.mapping[task_id] = mapping_data
            self.save_mapping()
        bt.logging.info(
            f"Added mapping for task {task_id} -> prompt {prompt_id} (expires in {expires_in_seconds}s)"
        )

    def get_mapping(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get task mapping

        Args:
            task_id: Task ID

        Returns:
            Mapping info or None if not found
        """
        with self.lock:
            if task_id in self.mapping:
                mapping = self.mapping[task_id]
                current_time = time.time()
                if current_time > mapping["expires_at"]:
                    del self.mapping[task_id]
                    self.save_mapping()
                    return None
                return mapping.copy()
            return None

    def update_status(self, task_id: str, status: str):
        """
        Update task status

        Args:
            task_id: Task ID
            status: New status
        """
        with self.lock:
            if task_id in self.mapping:
                self.mapping[task_id]["status"] = status
                self.mapping[task_id]["updated_at"] = int(time.time())
                self.save_mapping()
                bt.logging.debug(f"Updated status for task {task_id} to {status}")

    def remove_mapping(self, task_id: str):
        """
        Remove task mapping

        Args:
            task_id: Task ID
        """
        with self.lock:
            if task_id in self.mapping:
                del self.mapping[task_id]
                self.save_mapping()
                bt.logging.info(f"Removed mapping for task {task_id}")

    def cleanup_expired(self, force: bool = False):
        """
        Clean up expired mappings

        Args:
            force: Force cleanup regardless of last cleanup time
        """
        current_time = time.time()

        if not force and current_time - self.last_cleanup < self.cleanup_interval:
            return

        expired_tasks = []

        with self.lock:
            for task_id, info in self.mapping.items():
                if current_time > info["expires_at"]:
                    expired_tasks.append(task_id)

            if expired_tasks:
                for task_id in expired_tasks:
                    if task_id in self.mapping:
                        del self.mapping[task_id]

                self.save_mapping()
                bt.logging.info(
                    f"Cleaned up {len(expired_tasks)} expired task mappings"
                )

        self.last_cleanup = current_time

    def get_active_mappings(self) -> Dict[str, Any]:
        """
        Get all active mappings

        Returns:
            Dict of active mappings
        """
        self.cleanup_expired()
        with self.lock:
            return self.mapping.copy()

    def get_mapping_count(self) -> int:
        """Get total number of active mappings"""
        self.cleanup_expired()
        with self.lock:
            return len(self.mapping)

    def get_storage_info(self) -> Dict[str, Any]:
        """
        Get storage information

        Returns:
            Dict with storage info
        """
        file_size = 0
        if os.path.exists(self.file_path):
            file_size = os.path.getsize(self.file_path)

        return {
            "type": "file",
            "connected": True,
            "file_path": self.file_path,
            "file_size": file_size,
            "mapping_count": len(self.mapping),
            "cleanup_interval": self.cleanup_interval,
        }
