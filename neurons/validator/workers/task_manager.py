import os
import sys
import time
from urllib.parse import urlparse
import uuid
import json
import random
import asyncio
import threading
import traceback
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor

import bittensor as bt

# Import protocol types
from neza.protocol import VideoGenerationTask, TaskStatusCheck, VideoTask

# Import HTTP utilities
from neza.utils.http import (
    check_url_resource_available,
    get_result_upload_urls,
    http_put_request_sync,
    request_upload_url,
    request_upload_url,
    request_upload_url,
    get_online_task_count,
    listen_for_api_tasks,
)
from neurons.validator.tasks.task import Task
from neurons.validator.tasks.task_database import TaskDatabase


class TaskManager:
    """
    Manages task creation, assignment, monitoring and verification
    """

    def __init__(self, validator):
        """
        Initialize task manager

        Args:
            validator: Parent validator instance
        """
        self.validator = validator

        # Task database
        self.db = TaskDatabase()

        # Task processing lock
        self.task_lock = threading.Lock()

        # Database thread pool
        self.db_executor = ThreadPoolExecutor(
            max_workers=self.validator.validator_config.db_max_workers,
            thread_name_prefix="db_worker",
        )

        # Last task creation time
        self.last_task_creation_time = 0

        # Task density cycle management
        self.sended_miners = (
            {}
        )  # Dict of miners that received tasks in current cycle, mapping hotkey to task count
        self.task_density_cycle_start = (
            time.time()
        )  # Current task density cycle start time

        self._api_candidate_cache = {}

        bt.logging.info("Task manager initialized with block-based processing")

    def start(self):
        """Starts task monitoring"""
        bt.logging.info("Task manager started with block-based processing")
        # No need to start a separate thread as we're using block callbacks

    def stop(self):
        """Stops task monitoring"""
        # Shutdown thread pool
        if hasattr(self, "db_executor"):
            self.db_executor.shutdown(wait=False)

        bt.logging.info("Task manager stopped")

    def process_tasks_on_block(self, block):
        """
        Process tasks on new block

        Args:
            block: Block number (integer)
        """
        # Block is already the block number (integer)
        block_number = block

        # Only process tasks every 5 blocks (approximately every 60 seconds)
        if block_number % 5 == 0:
            bt.logging.info(f"Processing tasks on block {block_number}")
            if self.validator.miner_info_cache is None:
                bt.logging.warning("No miner data available, skipping task processing")
                return
            thread = threading.Thread(target=self._process_tasks_in_thread)
            thread.daemon = True
            thread.start()

        else:
            bt.logging.debug(f"Skipping task processing on block {block_number}")

    def start_api_task_listener(self):
        """Start API task listener"""
        try:
            if (
                not hasattr(self, "api_task_thread")
                or not self.api_task_thread.is_alive()
            ):
                self.api_task_thread = threading.Thread(
                    target=self._start_api_task_listener, daemon=True
                )
                self.api_task_thread.start()
        except Exception as e:
            bt.logging.error(f"Error starting API task listener: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def _process_tasks_in_thread(self):
        """Process tasks in a separate thread"""
        try:
            # Create a new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                # Run task processing
                loop.run_until_complete(self._process_tasks())
            finally:
                # Ensure event loop is properly closed
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.run_until_complete(loop.shutdown_default_executor())
                loop.close()
                # Reset thread event loop
                asyncio.set_event_loop(None)

        except Exception as e:
            bt.logging.error(f"Error in task processing thread: {str(e)}")
            bt.logging.error(traceback.format_exc())

    async def _process_tasks(self):
        """
        Process tasks
        Handles pending, active, completed, and retry tasks
        """
        try:
            # Process pending tasks
            await self._process_pending_tasks()

            # Verify completed tasks
            await self._verify_completed_tasks()

            # Process retry tasks
            await self._process_retry_tasks()

            # Check if task density cycle needs reset
            self._check_task_density_cycle()

        except Exception as e:
            bt.logging.error(f"Error processing tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def _check_task_density_cycle(self):
        """
        Checks if task density cycle needs to be reset
        Uses the same cycle length as verification cycle
        """
        current_time = time.time()
        cycle_length = self.validator.validator_config.miner_selection[
            "send_cycle_length"
        ]

        # Check if cycle has elapsed
        if current_time - self.task_density_cycle_start > cycle_length:
            self.reset_task_density_cycle()

    def reset_task_density_cycle(self):
        """
        Resets task density cycle
        Clears the dict of miners that received tasks in the current cycle
        """
        # Reset cycle start time
        self.task_density_cycle_start = time.time()

        # Reset sended miners
        old_count = len(self.sended_miners)
        self.sended_miners = {}

        bt.logging.info(f"Reset task density cycle, cleared {old_count} sended miners")

    async def _db_op(self, func, *args, **kwargs):
        """
        Execute database operation in thread pool

        Args:
            func: Database function to call
            args: Positional arguments
            kwargs: Keyword arguments

        Returns:
            Result of database operation
        """
        try:
            # Get current event loop
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                # If no event loop is available, create a new one
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            # Choose call method based on function type
            if asyncio.iscoroutinefunction(func):
                # If coroutine function, call directly
                return await func(*args, **kwargs)
            else:
                # If synchronous function, execute in thread pool
                return await loop.run_in_executor(
                    self.db_executor, lambda: func(*args, **kwargs)
                )
        except Exception as e:
            bt.logging.error(f"Error in database operation: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return None

    def _record_score_with_step(self, miner_hotkey, task_id, score):
        """Use score_step to record scores, instead of directly calling record_score

        Args:
            miner_hotkey: The miner's hotkey
            task_id: Task ID
            score: Score value
        """
        # Find miner_uid using score_manager's mapping
        miner_uid = self.validator.score_manager.hotkey_to_uid.get(miner_hotkey)

        # If not found in score_manager, try to find in metagraph
        if miner_uid is None:
            for uid, hotkey in enumerate(self.validator.metagraph.hotkeys):
                if hotkey == miner_hotkey:
                    miner_uid = uid
                    # Update score_manager's mapping
                    self.validator.score_manager.hotkey_to_uid[miner_hotkey] = uid
                    bt.logging.info(
                        f"Updated hotkey_to_uid mapping for {miner_hotkey[:10]}... to UID {uid}"
                    )
                    break

        # Use score_step to record score
        if miner_uid is not None:
            self.validator.score_step(
                responses=[{"task_id": task_id, "score": score}],
                task_name="video_generation",
                task_id=task_id,
                uids=[miner_uid],
            )
            bt.logging.debug(
                f"Recorded score {score} for miner {miner_hotkey[:10]}... (UID: {miner_uid}) on task {task_id}"
            )
        else:
            bt.logging.warning(
                f"Cannot find UID for miner {miner_hotkey[:10]}..., score not recorded"
            )

    async def _process_pending_tasks(self):
        """Process pending tasks and create synthetic tasks"""
        try:
            # 1. Get updated miner load information
            updated_miners_with_capacity = (
                await self.validator.miner_manager.get_miners_with_capacity()
            )
            if not updated_miners_with_capacity:
                return

            # 2. Fetch synthetic tasks
            bt.logging.info(f"Fetching synthetic tasks")
            tasks_created = await self._fetch_synthetic_tasks(
                updated_miners_with_capacity
            )

            # Update last task creation time if tasks were processed or created
            if tasks_created > 0:
                self.last_task_creation_time = time.time()
                bt.logging.info(
                    f"Updated last task creation time to {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.last_task_creation_time))}"
                )

        except Exception as e:
            bt.logging.error(f"Error in _process_pending_tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())

    async def _assign_tasks_to_miners(
        self, tasks, selected_miners, miners_with_capacity
    ):
        """
        Assign tasks to miners

        Args:
            tasks: List of tasks
            selected_miners: List of selected miners
            miners_with_capacity: List of miners with capacity
        """
        try:
            # Pair tasks with miners
            assignments = []
            for i, task in enumerate(tasks):
                if i >= len(selected_miners):
                    break

                miner = selected_miners[i]
                assignments.append((task, miner))

            # Assign tasks
            for task, miner in assignments:
                task_id = task["task_id"]
                miner_uid = miner["uid"]
                miner_hotkey = miner["hotkey"]

                # Get workflow params
                complete_workflow = task["complete_workflow"]

                # Get secret key
                secret_key = task["secret_key"]

                # Get timeout
                timeout_seconds = task.get(
                    "timeout_seconds",
                    self.validator.validator_config.task_timeout_seconds,
                )

                # Send task to miner
                bt.logging.info(
                    f"Assigning task {task_id} to miner {miner_hotkey[:10] if miner_hotkey else 'None'}..."
                )

                success = await self._send_task_to_miner(
                    task_id,
                    complete_workflow,
                    task_id,
                    secret_key,
                    timeout_seconds,
                    miner_uid,
                )

                if success:
                    # Update miner load
                    self._update_miner_load_after_task(miners_with_capacity, miner)

                # Update task count in sended_miners
                if miner_hotkey in self.sended_miners:
                    self.sended_miners[miner_hotkey] += 1
                else:
                    self.sended_miners[miner_hotkey] = 1

                bt.logging.debug(
                    f"Updated task count for miner {miner_hotkey[:10]}... to {self.sended_miners[miner_hotkey]}"
                )

        except Exception as e:
            bt.logging.error(f"Error assigning tasks to miners: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def _update_miner_load_after_task(self, miners_with_capacity, selected_miner):
        """
        Update miner load after task assignment

        Args:
            miners_with_capacity: List of miners with capacity
            selected_miner: Selected miner
        """
        # Find miner in list
        for miner in miners_with_capacity:
            if miner["uid"] == selected_miner["uid"]:
                # Ensure that all necessary keys are present.
                if "current_load" not in miner:
                    miner["current_load"] = 0
                if "active_tasks" not in miner:
                    miner["active_tasks"] = miner.get("current_load", 0)
                if "max_capacity" not in miner:
                    miner["max_capacity"] = 5

                # Update load
                miner["current_load"] += 1
                miner["active_tasks"] = miner["current_load"]
                miner["remaining_capacity"] = max(
                    0, miner["max_capacity"] - miner["current_load"]
                )
                break

    async def _fetch_synthetic_tasks(self, miners_with_capacity):
        """
        fetch synthetic tasks based on available miners with capacity

        Args:
            miners_with_capacity: List of miners with capacity

        Returns:
            int: Number of tasks created
        """
        try:
            # Get configuration values
            generate_max_tasks = self.validator.validator_config.miner_selection.get(
                "generate_max_tasks", 3
            )

            # Get comfy servers count
            comfy_servers = self.validator.validator_config.comfy_servers

            if not comfy_servers:
                bt.logging.info(
                    "No comfy servers configured, skipping synthetic task fetching. "
                )
                return 0

            comfy_server_count = len(comfy_servers)

            # Count miners with capacity
            available_count = len(miners_with_capacity)

            # Calculate request count as minimum of three values
            request_count = min(comfy_server_count, available_count, generate_max_tasks)
            if request_count <= 0:
                bt.logging.debug("No capacity for synthetic tasks")
                return 0

            # Get task count from online API with calculated request count
            online_task_count, online_tasks = await get_online_task_count(
                self.validator.wallet, request_count
            )
            if online_task_count <= 0:
                bt.logging.debug("No tasks available from online API")
                return 0

            # Calculate actual task count as maximum of online tasks and request count
            actual_task_count = max(online_task_count, request_count)
            bt.logging.info(
                f"Task count calculation: comfy_servers={comfy_server_count}, local_capacity={available_count}, generate_max_tasks={generate_max_tasks}, request_count={request_count}, online={online_task_count}, actual={actual_task_count}"
            )

            # Select miners for synthetic tasks
            selected_miners = await self._select_miners_for_synthetic_tasks(
                miners_with_capacity, actual_task_count
            )
            if not selected_miners:
                bt.logging.warning("No miners available for synthetic tasks")
                return 0

            # Log available miners for debugging
            self._log_available_miners(miners_with_capacity)

            # Store online tasks to database and assign to miners
            if online_tasks and len(online_tasks) > 0:
                tasks_assigned = await self._store_and_assign_online_tasks(
                    online_tasks, selected_miners, miners_with_capacity
                )
                bt.logging.info(
                    f"Stored and assigned {tasks_assigned} online tasks to miners"
                )
                return tasks_assigned
            else:
                bt.logging.warning("No online tasks to assign")
                return 0

        except Exception as e:
            bt.logging.error(f"Error fetching synthetic tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return 0

    async def _store_and_assign_online_tasks(
        self, online_tasks, selected_miners, miners_with_capacity
    ):
        """
        Store online tasks to database and assign to miners

        Args:
            online_tasks: List of tasks from online API
            selected_miners: List of selected miners
            miners_with_capacity: List of miners with capacity

        Returns:
            int: Number of tasks successfully assigned
        """
        try:
            # Convert online tasks to the format expected by _assign_tasks_to_miners
            converted_tasks = []

            for task_data in online_tasks:
                task_id = task_data.get("task_id")
                workflow_json = task_data.get("workflow_json", {})

                if not task_id or not workflow_json:
                    bt.logging.warning(f"Invalid task data: {task_data}")
                    continue

                # Create Task object for database storage
                task_obj = Task(
                    task_id=task_id,
                    complete_workflow=workflow_json,
                    timeout_seconds=self.validator.validator_config.task_timeout_seconds,
                )

                # Store task to database
                success = await self._db_op(
                    self.db.add_task,
                    task_obj,
                )

                if not success:
                    bt.logging.error(f"Failed to store task {task_id} to database")
                    continue
                converted_tasks.append(task_obj.to_dict())

            if not converted_tasks:
                bt.logging.warning("No valid tasks to assign")
                return 0

            # Use the existing _assign_tasks_to_miners method
            await self._assign_tasks_to_miners(
                converted_tasks, selected_miners, miners_with_capacity
            )

            return len(converted_tasks)

        except Exception as e:
            bt.logging.error(f"Error storing and assigning online tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return 0

    def _determine_synthetic_task_count(self, miners_with_capacity):
        """
        Determine how many synthetic tasks to create (legacy method, logic moved to _fetch_synthetic_tasks)

        Args:
            miners_with_capacity: List of miners with capacity

        Returns:
            Number of tasks to create
        """
        # This method is kept for backward compatibility
        # The actual logic is now in _fetch_synthetic_tasks
        return 0

    async def _select_miners_for_synthetic_tasks(self, miners_with_capacity, max_tasks):
        """
        Select miners for synthetic tasks

        Args:
            miners_with_capacity: List of miners with capacity
            max_tasks: Maximum number of tasks

        Returns:
            List of selected miners
        """
        # Use task density based selection
        return await self._select_miners_by_task_density(
            miners_with_capacity, max_tasks
        )

    async def _select_miners_by_task_density(self, miners_with_capacity, task_count):
        """
        Selects miners for tasks based on task density
        Similar to verification density but for task assignment

        Args:
            miners_with_capacity: List of miners with capacity info
            task_count: Number of miners to select

        Returns:
            List of selected miners
        """
        if not miners_with_capacity:
            return []

        # Adjust count based on available miners
        count = min(task_count, len(miners_with_capacity))
        if count == 0:
            return []

        # Filter valid miners with capacity
        valid_miners = [m for m in miners_with_capacity if m["remaining_capacity"] > 0]
        if not valid_miners:
            return []

        # Calculate task density for each miner based on sended_miners
        miners_with_density = []
        for miner in valid_miners:
            hotkey = miner["hotkey"]

            # Get task count for this miner in current cycle, default to 0 if not found
            task_count = self.sended_miners.get(hotkey, 0)

            # Calculate density (similar to verification density)
            # Lower task count = lower density = higher priority
            density = task_count  # Simple density based on number of tasks sent in current cycle

            # Get float ratio from config
            float_ratio = self.validator.validator_config.miner_selection[
                "density_float_ratio"
            ]

            # Calculate float range
            float_range = max(
                0.001, density * float_ratio
            )  # Ensure minimum float range

            # Apply random float within range
            random_factor = random.uniform(1.0 - float_range, 1.0 + float_range)
            density = density * random_factor

            # Add to miners with density
            miners_with_density.append({**miner, "density": density})

        # Sort miners by density (ascending) to prioritize miners with lower density
        sorted_miners = sorted(miners_with_density, key=lambda m: m.get("density", 0))

        # Select top miners
        selected_miners = sorted_miners[:count]

        return selected_miners

    def _log_available_miners(self, miners_with_capacity):
        """
        Log available miners

        Args:
            miners_with_capacity: List of miners with capacity
        """
        # Count miners with capacity
        available_count = sum(
            1 for m in miners_with_capacity if m["remaining_capacity"] > 0
        )

        # Log count
        bt.logging.debug(
            f"Available miners with capacity: {available_count}/{len(miners_with_capacity)}"
        )

    async def _send_task_to_miner(
        self,
        task_id,
        complete_workflow,
        file_name,
        secret_key,
        timeout_seconds,
        miner_uid,
    ):
        """
        Send task to miner

        Args:
            task_id: Task ID
            complete_workflow: Complete workflow
            file_name: File name
            secret_key: Secret key
            timeout_seconds: Timeout in seconds
            miner_uid: Miner UID

        Returns:
            bool: True if successful, False otherwise
        """
        start_time = time.time()
        bt.logging.info(f"Starting task {task_id} preparation for miner {miner_uid}")

        try:
            # Get miner axon
            axon = self.validator.metagraph.axons[miner_uid]
            miner_hotkey = self.validator.metagraph.hotkeys[miner_uid]

            # Request upload URL from owner server
            try:
                bt.logging.info(f"Requesting upload URL for task {task_id}")
                upload_info = await request_upload_url(self.validator.wallet, task_id)
                upload_url = upload_info.get("upload_url") if upload_info else ""
                preview_url = upload_info.get("preview_url") if upload_info else ""

                if not upload_url:
                    bt.logging.warning(f"No upload URL received for task {task_id}")
            except Exception as e:
                bt.logging.error(f"Error requesting upload URL: {str(e)}")
                bt.logging.error(traceback.format_exc())
                upload_url = ""
                preview_url = ""

            # Log sending task
            bt.logging.info(
                f"Sending task {task_id} to miner {miner_hotkey[:10] if miner_hotkey else 'None'}... axon {axon}"
            )

            # Record task preparation time
            prep_time = time.time() - start_time
            bt.logging.info(f"Task preparation completed in {prep_time:.2f} seconds")

            task_obj = VideoTask(
                task_id=task_id,
                file_name=file_name,
                secret_key=secret_key,
                workflow_params=complete_workflow,
                upload_url=upload_url,
                preview_url=preview_url,
            )

            # Call _send_task and return result
            return await self._send_task(
                task_id, axon, task_obj, timeout_seconds, miner_hotkey, miner_uid
            )

        except Exception as e:
            bt.logging.error(f"Error sending task to miner: {str(e)}")
            bt.logging.error(traceback.format_exc())

            # Record total time even on failure
            total_time = time.time() - start_time
            bt.logging.error(f"Task preparation failed after {total_time:.2f} seconds")
            return False

    async def _send_task(
        self, task_id, axon, task_obj, timeout_seconds, miner_hotkey, miner_uid
    ):
        """
        Send task to miner
        """

        try:
            # Use async context manager to handle Dendrite session
            async with bt.dendrite(self.validator.wallet) as dendrite:
                try:
                    # Send task with timeout
                    response = await asyncio.wait_for(
                        dendrite.forward(
                            axon,
                            task_obj,  # Send VideoTask directly
                            deserialize=True,
                        ),
                        timeout=min(
                            timeout_seconds, 20
                        ),  # Max 20 second timeout for initial response
                    )

                    # Log response details
                    bt.logging.info(
                        f"Task {task_id} response from miner {miner_hotkey[:10] if miner_hotkey else 'None'}"
                    )
                except asyncio.TimeoutError:
                    bt.logging.warning(
                        f"Timeout sending task {task_id} to miner {miner_hotkey[:10] if miner_hotkey else 'None'}..."
                    )
                except Exception as e:
                    bt.logging.error(f"Error sending task: {str(e)}")
                    bt.logging.error(traceback.format_exc())

            isResponse = (
                hasattr(response, "status_code") and response.status_code == 200
            )

            await self._db_op(
                self.db.update_task_with_upload_info,
                task_id=task_id,
                miner_hotkey=miner_hotkey,
                preview_url=(
                    task_obj.preview_url
                    if task_obj.preview_url
                    else response.video_url if isResponse else ""
                ),
                status=("processing" if isResponse else "assigned"),
                miner_info=self.validator.miner_info_cache,
                upload_url=(
                    task_obj.upload_url if hasattr(task_obj, "upload_url") else ""
                ),
            )

            current_time = time.time()
            await self._db_op(
                self.db.update_task_last_sent_at,
                task_id=task_id,
                last_sent_at=current_time,
            )

            # Update miner UID information
            try:
                # Use lock to prevent WebSocket concurrency issues
                with self.validator._subtensor_lock:
                    current_block = self.validator.subtensor.get_current_block()

                await self._db_op(
                    self.db.update_miner_uid,
                    hotkey=miner_hotkey,
                    uid=miner_uid,
                    current_block=current_block,
                )
            except Exception as e:
                bt.logging.warning(
                    f"Miner UID update error (It may be a duplicate key.): {str(e)}"
                )

            bt.logging.info(
                f"Task {task_id} assigned to miner {miner_hotkey[:10] if miner_hotkey else 'None'}..."
            )
            return isResponse

        except Exception as e:
            bt.logging.error(f"Error in _send_task: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    async def _check_active_tasks_optimized(self):
        """
        Optimized version of active tasks checking with batch processing
        """
        try:
            # Get active tasks
            active_tasks = self.db.get_active_tasks()
            if not active_tasks:
                return

            bt.logging.info(f"Checking {len(active_tasks)} active tasks (optimized)")

            # Separate tasks by timeout status
            timed_out_tasks = []
            valid_tasks = []
            current_time = datetime.now(timezone.utc)

            for task in active_tasks:
                task_id = task["task_id"]
                miner_hotkey = task.get("miner_hotkey")

                # Skip tasks without miner hotkey
                if not miner_hotkey:
                    bt.logging.warning(
                        f"Task {task_id} has no miner hotkey, skipping check"
                    )
                    continue

                # Check if this task is already being processed by another thread

                # Check timeout status
                last_sent_time = task.get("last_sent_at")
                if last_sent_time:
                    timeout_seconds = task.get(
                        "timeout_seconds",
                        self.validator.validator_config.task_timeout_seconds,
                    )

                    # Convert last_sent_time to datetime if needed
                    if isinstance(last_sent_time, (int, float)):
                        last_sent_time = datetime.fromtimestamp(
                            last_sent_time, tz=timezone.utc
                        )
                    elif isinstance(last_sent_time, str):
                        try:
                            last_sent_time = datetime.fromisoformat(
                                last_sent_time.replace("Z", "+00:00")
                            )
                        except:
                            last_sent_time = None

                    # Check if timed out
                    if (
                        last_sent_time
                        and (current_time - last_sent_time).total_seconds()
                        > timeout_seconds
                    ):
                        timed_out_tasks.append((task_id, miner_hotkey))
                        continue

                # Add to valid tasks for URL checking
                video_url = task.get("preview_url") or task.get("s3_video_url")
                if video_url:
                    valid_tasks.append((task, video_url))

            # Batch process timed out tasks
            if timed_out_tasks:
                bt.logging.info(f"Processing {len(timed_out_tasks)} timed out tasks")
                await self._batch_handle_timeout_tasks(timed_out_tasks)

            # Batch check URL availability for valid tasks
            if valid_tasks:
                bt.logging.info(
                    f"Checking {len(valid_tasks)} valid tasks for URL availability"
                )
                await self._batch_check_url_availability(valid_tasks)

        except Exception as e:
            bt.logging.error(f"Error in optimized active tasks check: {str(e)}")
            bt.logging.error(traceback.format_exc())

    async def _batch_handle_timeout_tasks(self, timed_out_tasks):
        """
        Batch handle timeout tasks with optimized database operations
        """
        try:
            if not timed_out_tasks:
                return

            bt.logging.info(
                f"Starting batch processing of {len(timed_out_tasks)} timeout tasks"
            )

            # Extract task IDs and miner hotkeys
            task_ids = [task_id for task_id, _ in timed_out_tasks]
            miner_hotkeys = [hotkey for _, hotkey in timed_out_tasks]

            # Batch get task info for all tasks
            task_infos = await self._db_op(self.db.batch_get_tasks, task_ids)

            # Create a mapping of task_id to task_info for easier lookup
            task_info_map = {
                task_info["task_id"]: task_info for task_info in task_infos
            }

            # Prepare timeout tasks that need score updates
            timeout_tasks_with_scores = []
            penalty_miners = set()

            for task_id in task_ids:
                task_info = task_info_map.get(task_id)
                if task_info and task_info.get("score") is None:
                    miner_hotkey = task_info.get("miner_hotkey")
                    if miner_hotkey:
                        timeout_tasks_with_scores.append((task_id, miner_hotkey, 0.0))
                        penalty_miners.add(miner_hotkey)

            # Batch update task status and scores in one operation
            if timeout_tasks_with_scores:
                await self._db_op(
                    self.db.batch_update_timeout_tasks_with_scores,
                    timeout_tasks=timeout_tasks_with_scores,
                )

            # Record scores in score manager (this is fast, no database operations)
            for task_id, miner_hotkey, score in timeout_tasks_with_scores:
                self.validator.score_manager.record_score(
                    hotkey=miner_hotkey, task_id=task_id, score=score
                )

            # Record failures for miners (this is also fast)
            for miner_hotkey in penalty_miners:
                self.validator.penalty_manager.record_miner_failure(miner_hotkey)

            # Batch upload task results with error information
            upload_tasks = []
            for task_id, miner_hotkey in timed_out_tasks:
                error_info = {"error": "Timeout"}
                upload_tasks.append(
                    self.validator.verification_manager.upload_task(
                        task_id, miner_hotkey, 0, error_info, 0, is_error=True
                    )
                )

            # Execute uploads concurrently
            if upload_tasks:
                await asyncio.gather(*upload_tasks, return_exceptions=True)

            bt.logging.info(
                f"Successfully processed {len(timed_out_tasks)} timeout tasks in batch mode"
            )

        except Exception as e:
            bt.logging.error(f"Error in batch timeout handling: {str(e)}")
            bt.logging.error(traceback.format_exc())

    async def _batch_check_url_availability(self, valid_tasks):
        """
        Batch check URL availability and process completed tasks
        """
        try:
            # Create tasks for concurrent URL checking
            url_check_tasks = []
            for task, video_url in valid_tasks:
                url_check_tasks.append(self._check_single_task_url(task, video_url))

            # Execute all URL checks concurrently
            results = await asyncio.gather(*url_check_tasks, return_exceptions=True)

            # Process results
            completed_tasks = []
            still_processing_tasks = []

            for i, result in enumerate(results):
                task, video_url = valid_tasks[i]
                task_id = task["task_id"]

                if isinstance(result, Exception):
                    bt.logging.error(
                        f"Error checking URL for task {task_id}: {str(result)}"
                    )
                    still_processing_tasks.append(task)
                    continue

                is_available, file_info = result

                if is_available and file_info:
                    # Check file size limit
                    if "file_size" in file_info:
                        file_size_limit = (
                            self.validator.validator_config.file_size_limit
                        )
                        file_size_mb = file_info["file_size"] / (1024 * 1024)

                        if file_size_mb > file_size_limit:
                            bt.logging.warning(
                                f"Task {task_id} failed: File size {file_size_mb:.2f}MB exceeds {file_size_limit}MB limit"
                            )
                            await self._handle_task_failure(
                                task_id,
                                f"File size {file_size_mb:.2f}MB exceeds {file_size_limit}MB limit",
                            )
                            continue

                    completed_tasks.append((task, video_url, file_info))
                else:
                    still_processing_tasks.append(task)

            # Batch process completed tasks
            if completed_tasks:
                bt.logging.info(f"Processing {len(completed_tasks)} completed tasks")
                await self._batch_process_completed_tasks(completed_tasks)

            # Log still processing tasks
            if still_processing_tasks:
                bt.logging.info(f"{len(still_processing_tasks)} tasks still processing")

        except Exception as e:
            bt.logging.error(f"Error in batch URL availability check: {str(e)}")

    async def _check_single_task_url(self, task, video_url):
        """
        Check URL availability for a single task
        """
        try:
            # Use shorter timeout for better performance
            return await check_url_resource_available(
                video_url, get_file_info=True, timeout=10
            )
        except Exception as e:
            bt.logging.error(f"Error checking URL for task {task['task_id']}: {str(e)}")
            return False, {}

    async def _batch_process_completed_tasks(self, completed_tasks):
        """
        Batch process completed tasks with concurrent downloads
        """
        try:
            bt.logging.info(
                f"Starting batch processing of {len(completed_tasks)} completed tasks"
            )

            db_update_tasks = []
            completed_tasks_final = []
            for task, video_url, file_info in completed_tasks:
                task_id = task["task_id"]
                miner_hotkey = task.get("miner_hotkey")

                # Log completion
                file_size = file_info.get("file_size", "unknown")
                last_modified = file_info.get("last_modified", "")
                bt.logging.info(
                    f"Task {task_id} completed by miner {miner_hotkey[:10] if miner_hotkey else 'None'}, "
                    f"file_size: {file_size}, last_modified: {last_modified}"
                )

                # Calculate completion time
                completion_time = 0
                last_sent_at = task.get("last_sent_at")
                if last_modified and last_sent_at:
                    if isinstance(last_sent_at, (int, float)):
                        last_sent_at = datetime.fromtimestamp(
                            last_sent_at, tz=timezone.utc
                        )
                    elif isinstance(last_sent_at, str):
                        try:
                            last_sent_at = datetime.fromisoformat(
                                last_sent_at.replace("Z", "+00:00")
                            )
                        except:
                            last_sent_at = None

                    if last_sent_at:
                        completion_time = (last_modified - last_sent_at).total_seconds()
                        bt.logging.info(
                            f"Task {task_id} completion time: {completion_time:.2f} seconds"
                        )

                completed_tasks_final.append(
                    (
                        {**task, "completion_time": completion_time},
                        video_url,
                        file_info,
                    )
                )

                # Create database update task
                db_update_tasks.append(
                    self._db_op(
                        self.db.update_task_to_completed,
                        task_id=task_id,
                        miner_hotkey=miner_hotkey,
                        file_info=file_info,
                        completion_time=completion_time,
                        completed_at=datetime.now(timezone.utc),
                    )
                )

            # Execute all database updates concurrently
            bt.logging.info(f"Updating database for {len(db_update_tasks)} tasks")
            await asyncio.gather(*db_update_tasks, return_exceptions=True)

            # Batch download videos and upload completions concurrently in background thread
            bt.logging.info(
                f"Starting background downloads and uploads for {len(completed_tasks)} tasks"
            )

            # Start background thread for downloads and uploads
            thread = threading.Thread(
                target=self._run_downloads_and_uploads_in_thread,
                args=(completed_tasks_final,),
                daemon=True,
            )
            thread.start()

            bt.logging.info(
                f"Successfully processed {len(completed_tasks)} completed tasks"
            )

        except Exception as e:
            bt.logging.error(f"Error in batch processing completed tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def _run_downloads_and_uploads_in_thread(self, completed_tasks):
        """
        Run downloads and uploads in a separate thread to avoid blocking main thread
        """
        try:
            # Create a new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                # Run the async download and upload operations
                loop.run_until_complete(
                    self._run_downloads_and_uploads_async(completed_tasks)
                )
            finally:
                # Ensure event loop is properly closed
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.run_until_complete(loop.shutdown_default_executor())
                loop.close()
                # Reset thread event loop
                asyncio.set_event_loop(None)

        except Exception as e:
            bt.logging.error(f"Error in download/upload thread: {str(e)}")
            bt.logging.error(traceback.format_exc())

    async def _run_downloads_and_uploads_async(self, completed_tasks):
        """
        Run downloads and uploads asynchronously
        """
        try:
            # Create tasks for concurrent processing
            download_and_upload_tasks = []
            for task, video_url, file_info in completed_tasks:
                task_id = task["task_id"]
                download_and_upload_tasks.append(
                    self._download_and_upload_single_task(task_id, video_url, task)
                )

            # Execute all downloads and uploads concurrently
            bt.logging.info(
                f"Executing {len(download_and_upload_tasks)} download/upload tasks in background"
            )

            results = await asyncio.gather(
                *download_and_upload_tasks, return_exceptions=True
            )

            bt.logging.info(f"Background downloads and uploads completed: {results}")

        except Exception as e:
            bt.logging.error(f"Error in async download/upload processing: {str(e)}")
            bt.logging.error(traceback.format_exc())

    async def _download_and_upload_single_task(self, task_id, video_url, task):
        """
        Download and upload a single task
        """
        try:
            # Download and extract package
            success, extracted_dir_path, outputs_info = (
                await self.validator.video_manager.download_and_extract_package(
                    task_id, video_url, "miner"
                )
            )

            if success:
                # Get upload URLs
                upload_urls = await get_result_upload_urls(
                    self.validator.wallet, task_id, outputs_info
                )
                if upload_urls:
                    # Upload files to each URL concurrently
                    upload_tasks = []
                    for upload_url in upload_urls:
                        upload_tasks.append(
                            self._upload_single_file(upload_url, extracted_dir_path)
                        )

                    # Execute all uploads concurrently
                    await asyncio.gather(*upload_tasks, return_exceptions=True)

                # Upload completion notification
                await self.validator.verification_manager.upload_miner_completed_task(
                    task_id, task
                )

            return True
        except Exception as e:
            bt.logging.error(f"Error in download/upload for task {task_id}: {str(e)}")
            raise

    async def _upload_single_file(self, upload_url, extracted_dir_path):
        """
        Upload a single file to the given URL
        """
        try:
            parsed_url = urlparse(upload_url)
            filename = os.path.basename(parsed_url.path)
            file_path = os.path.join(extracted_dir_path, filename)
            bt.logging.info(f"Uploading {file_path} to {upload_url}")

            with open(file_path, "rb") as f:
                data = f.read()

            response_text, status_code = http_put_request_sync(
                upload_url,
                data=data,
                headers={},
                timeout=180,
            )

            if 200 <= status_code < 300:
                bt.logging.info(f"Successfully uploaded to {upload_url}")
            else:
                bt.logging.error(
                    f"Upload failed to {upload_url}, status: {status_code}, response: {response_text}"
                )
        except Exception as e:
            bt.logging.error(f"Failed to upload to {upload_url}: {str(e)}")
            raise

    async def _verify_completed_tasks(self):
        """Verify completed tasks"""
        try:
            # Get all completed but unrated tasks
            all_completed_tasks = await self._db_op(
                self.db.db.get_all_completed_tasks_without_score
            )

            if not all_completed_tasks:
                bt.logging.info("No completed tasks found for verification")
                return

            bt.logging.info(
                f"Found {len(all_completed_tasks)} tasks for verification (within last 2 hours)"
            )

            # Get verification manager capacity
            if not self.validator.verification_manager:
                bt.logging.warning("Verification manager not available")
                return

            # Get available capacity
            max_capacity = self.validator.validator_config.verification[
                "max_concurrent_verifications"
            ]
            # Get current verifying tasks count with thread safety
            with self.validator.verification_manager.verifying_tasks_lock:
                current_verifying = len(
                    self.validator.verification_manager.verifying_tasks
                )
            available_capacity = max(0, max_capacity - current_verifying)

            # Get queue size
            queue_size = 0
            if hasattr(self.validator.verification_manager, "verification_queue"):
                queue_size = (
                    self.validator.verification_manager.verification_queue.qsize()
                )

            bt.logging.debug(
                f"Verification capacity: {current_verifying}/{max_capacity}, available: {available_capacity}, queue: {queue_size}"
            )

            # Calculate how many tasks we can safely add to queue
            max_safe_queue_size = max_capacity - current_verifying

            # Calculate target queue size based on improved logic
            idle_workers = max(0, max_capacity - current_verifying)
            buffer_size = max(1, int(max_capacity * 0.5))
            target_queue_size = min(idle_workers + buffer_size, max_safe_queue_size)

            # Check if we need to add tasks to queue
            should_add_tasks = queue_size < target_queue_size

            # Calculate fetch capacity based on target queue size
            fetch_capacity = target_queue_size - queue_size

            bt.logging.debug(
                f"Queue management: current={queue_size}, target={target_queue_size}, "
                f"max_safe={max_safe_queue_size}, idle_workers={idle_workers}, "
                f"buffer_size={buffer_size}, should_add={should_add_tasks}, "
                f"fetch_capacity={fetch_capacity}"
            )

            if should_add_tasks:
                # Use miner selection strategy based on verification density
                sorted_tasks = (
                    self.validator.verification_manager.select_tasks_for_verification(
                        all_completed_tasks
                    )
                )

                task_count = min(fetch_capacity, len(sorted_tasks))
                task_count = max(0, task_count) if sorted_tasks else 0

                if task_count == 0:
                    bt.logging.info(
                        f"No tasks to verify, skipping verification, buffer_size: {buffer_size}, queue_size: {queue_size}"
                    )
                    return

                # Select task
                selected_tasks = sorted_tasks[:task_count]

                bt.logging.info(
                    f"Selected {len(selected_tasks)} tasks for verification (fetch: {fetch_capacity})"
                )

                # Add task to verification queue
                for task in selected_tasks:
                    task_id = task["task_id"]
                    miner_hotkey = task["miner_hotkey"]

                    # Avoid adding task already in verification
                    with self.validator.verification_manager.verifying_tasks_lock:
                        if (
                            task_id
                            not in self.validator.verification_manager.verifying_tasks
                        ):
                            try:
                                # First update task status to "verifying" to prevent duplicate processing
                                await self._db_op(
                                    self.db.update_task_status,
                                    task_id=task_id,
                                    status="verifying",
                                )

                                bt.logging.warning(
                                    f"Update the status of task {task_id} to verifying"
                                )

                                # Try to add task to verification queue
                                success = self.validator.verification_manager.add_verification_task(
                                    {"task_id": task_id, "miner_hotkey": miner_hotkey}
                                )

                                if not success:
                                    bt.logging.warning(
                                        f"Failed to add task {task_id} to verification queue"
                                    )
                                else:
                                    bt.logging.debug(
                                        f"Task {task_id} successfully added to verification queue"
                                    )

                            except Exception as e:
                                bt.logging.error(
                                    f"Error managing verification for task {task_id}: {str(e)}"
                                )
                                bt.logging.error(traceback.format_exc())

        except Exception as e:
            bt.logging.error(f"Error verifying completed tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())

    async def _process_retry_tasks(self):
        """
        Process retry tasks for assigned status tasks
        This method checks tasks in 'assigned' status and retries them if needed
        If retry count exceeds the limit, marks the task as failed
        """
        try:
            # Get assigned tasks using get_tasks_by_status
            assigned_tasks = self.db.db.get_tasks_by_status("assigned", 10)
            if not assigned_tasks:
                return

            bt.logging.info(
                f"Processing {len(assigned_tasks)} assigned tasks for retry"
            )

            # Process each assigned task
            for task in assigned_tasks:
                task_id = task["task_id"]
                retry_count = task.get("retry_count", 0)
                complete_workflow = task.get("complete_workflow")
                miner_hotkey = task.get("miner_hotkey")
                # Get secret key
                secret_key = task.get("secret_key")
                s3_upload_url = task.get("s3_upload_url", "")
                s3_video_url = task.get("s3_video_url", "")

                # Get timeout
                timeout_seconds = task.get(
                    "timeout_seconds",
                    self.validator.validator_config.task_timeout_seconds,
                )

                # Get task failure threshold
                task_failure_threshold = 2

                # If retry count has reached or exceeded threshold, directly mark task as failed and skip
                if retry_count >= task_failure_threshold:
                    bt.logging.warning(
                        f"Task {task_id} already has {retry_count} retries (max: {task_failure_threshold}), marking as failed"
                    )
                    # Handle task failure due to retry limit exceeded
                    await self._handle_task_failure(
                        task_id, "Task failed after retry limit exceeded"
                    )
                    continue

                # Find miner UID
                miner_uid = None
                for uid, hotkey in enumerate(self.validator.metagraph.hotkeys):
                    if hotkey == miner_hotkey:
                        miner_uid = uid
                        break

                if miner_uid is None:
                    bt.logging.warning(
                        f"Miner {miner_hotkey[:10] if miner_hotkey else 'None'}... not found in metagraph for assigned task {task_id}"
                    )
                    continue

                # Send task to miner
                bt.logging.info(
                    f"Retrying assigned task {task_id} (attempt {retry_count if retry_count > 0 else 1}) with same miner {miner_hotkey[:10] if miner_hotkey else 'None'}..."
                )

                # Update task retry count in database - direct synchronous call
                await self._db_op(
                    self.db.update_task_retry,
                    task_id=task_id,
                    miner_hotkey=miner_hotkey,
                    retry_count=retry_count,  # No need to +1, database will handle
                )

                # Check updated retry count again, ensure not exceed threshold
                updated_task = await self._db_op(self.db.get_task, task_id)
                updated_retry_count = (
                    updated_task.get("retry_count", 0) if updated_task else retry_count
                )
                updated_status = (
                    updated_task.get("status", "assigned")
                    if updated_task
                    else "assigned"
                )

                # If status is already failed or retry count has reached threshold, skip sending
                if (
                    updated_status == "failed"
                    or updated_retry_count >= task_failure_threshold
                ):
                    bt.logging.info(
                        f"Task {task_id} status is now {updated_status} with {updated_retry_count} retries, skipping send"
                    )

                    # If status is failed but score not set, set score to 0
                    if updated_status == "failed":
                        # Handle task failure due to failure status
                        await self._handle_task_failure(
                            task_id, "Task failed due to failure status"
                        )
                        continue

                    continue

                # Get miner axon
                axon = self.validator.metagraph.axons[miner_uid]

                # Create task object
                task_obj = VideoTask(
                    task_id=task_id,
                    file_name=task_id,  # Use task_id as file_name
                    secret_key=secret_key,
                    workflow_params=complete_workflow,
                    upload_url=s3_upload_url,
                    preview_url=s3_video_url,
                )

                # Send task to miner using _send_task
                success = await self._send_task(
                    task_id, axon, task_obj, timeout_seconds, miner_hotkey, miner_uid
                )

                if success:
                    bt.logging.info(
                        f"Successfully sent retry task {task_id} to miner {miner_hotkey[:10] if miner_hotkey else 'None'}"
                    )
                else:
                    # Get updated retry count
                    updated_task = await self._db_op(self.db.get_task, task_id)
                    updated_retry_count = (
                        updated_task.get("retry_count", 0)
                        if updated_task
                        else retry_count
                    )

                    if updated_retry_count >= task_failure_threshold:
                        bt.logging.warning(
                            f"Task {task_id} failed after {updated_retry_count} retries (max: {task_failure_threshold})"
                        )
                        # Handle task failure due to send failure after retry limit
                        await self._handle_task_failure(
                            task_id, "Task failed after retry limit exceeded"
                        )
                    else:
                        # Keep in assigned state for next attempt
                        bt.logging.info(
                            f"Task {task_id} will be retried again later (current attempt: {updated_retry_count})"
                        )

        except Exception as e:
            bt.logging.error(f"Error processing assigned tasks for retry: {str(e)}")
            bt.logging.error(traceback.format_exc())

    async def _handle_task_failure(self, task_id, reason, details=None):
        """Handle task failure with score recording and miner notification

        Args:
            task_id: The ID of the failed task
            reason: The reason for failure (e.g. "Timeout", "File size limit exceeded")
            details: Additional details about the failure (optional)
        """
        # Update task status to failed
        await self._db_op(self.db.update_task_status, task_id=task_id, status="failed")

        # Get task info including miner hotkey
        task_info = await self._db_op(self.db.get_task, task_id)
        miner_hotkey = task_info.get("miner_hotkey") if task_info else None

        if not miner_hotkey:
            bt.logging.warning(f"Task {task_id} failed but no miner hotkey found")
            return

        # Check if the task already has a score
        if task_info and task_info.get("score") is None:
            # Update task score to 0 for failed task
            await self._db_op(
                self.db.update_task_with_score,
                task_id=task_id,
                miner_hotkey=miner_hotkey,
                score=0.0,
                completion_time=0.0,
                status="failed",
            )

            # Record score in score manager to save to .score file
            self.validator.score_manager.record_score(
                hotkey=miner_hotkey, task_id=task_id, score=0.0
            )

            bt.logging.info(
                f"Task {task_id} score set to 0 due to {reason}, status set to failed"
            )

            # Record failure for miner
            self.validator.penalty_manager.record_miner_failure(miner_hotkey)

            # Upload task result with error information
            error_info = {"error": reason}
            if details:
                error_info.update(details)

            await self.validator.verification_manager.upload_task(
                task_id, miner_hotkey, 0, error_info, 0, is_error=True
            )

        return True

    def _handle_hotkey_change(self, old_hotkey, new_hotkey):
        """
        Updates sended_miners when a hotkey changes

        Args:
            old_hotkey: Previous hotkey
            new_hotkey: New hotkey
        """
        try:
            if old_hotkey in self.sended_miners:
                min_count = (
                    min(list(self.sended_miners.values())) if self.sended_miners else 0
                )

                self.sended_miners[new_hotkey] = max(0, min_count - 1)
                if old_hotkey in self.sended_miners:
                    del self.sended_miners[old_hotkey]

                bt.logging.info(
                    f"Updated sended_miners for changed hotkey: {old_hotkey[:10]}... -> {new_hotkey[:10]}... "
                    f"Set to {self.sended_miners[new_hotkey]} (min count: {min_count})"
                )
            else:
                self.sended_miners[new_hotkey] = 0
                bt.logging.info(
                    f"Set sended_miners count for new hotkey {new_hotkey[:10]}... to 0"
                )

        except Exception as e:
            bt.logging.error(
                f"Error updating sended_miners for changed hotkey: {str(e)}"
            )
            bt.logging.error(traceback.format_exc())

    def _start_api_task_listener(self):
        """Start API task listener thread"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            loop.run_until_complete(
                listen_for_api_tasks(self.validator.wallet, self._handle_api_task)
            )
        except Exception as e:
            bt.logging.error(f"Error in API task listener thread: {str(e)}")
        finally:
            loop.close()

    def refresh_api_candidate_cache(self):
        """Refresh API candidate scores cache for all models

        This should be called after process_api_scoring completes to pre-compute
        candidate scores, thresholds, and final candidates for each model.
        """
        try:
            api_miners = self.validator.miner_manager.api_miners_cache
            if not api_miners:
                return

            api_scores = self.validator.score_manager.api_scores
            hotkey_to_uid = self.validator.miner_manager.hotkey_to_uid

            new_cache = {}

            for model, candidates in api_miners.items():
                if not candidates:
                    continue

                # 1. Calculate average scores for all candidates
                api_avg_scores = {}
                new_miners = set()

                for hotkey in candidates:
                    uid = hotkey_to_uid.get(hotkey)
                    if uid is not None and uid in api_scores and api_scores[uid]:
                        avg_score = self.validator.score_manager.safe_mean_score(
                            api_scores[uid]
                        )
                        api_avg_scores[uid] = avg_score
                    else:
                        if uid is not None:
                            api_avg_scores[uid] = 0.0
                            new_miners.add(uid)

                # 2. Normalize API scores to 0-1 range (consistent with finalize_epoch)
                normalized_api_scores = (
                    self.validator.score_manager.normalize_api_scores(api_avg_scores)
                )

                # 3. Map normalized scores back to hotkeys
                candidate_scores = {}
                for hotkey in candidates:
                    uid = hotkey_to_uid.get(hotkey)
                    if uid is not None:
                        if uid in new_miners:
                            candidate_scores[hotkey] = 1.0
                        elif uid in normalized_api_scores:
                            normalized_score = normalized_api_scores[uid]
                            final_score = (
                                normalized_score if normalized_score > 0 else 0.01
                            )
                            candidate_scores[hotkey] = final_score
                        else:
                            candidate_scores[hotkey] = 1.0

                # 4. Calculate average score of candidates
                if candidate_scores:
                    avg_threshold = sum(candidate_scores.values()) / len(
                        candidate_scores
                    )
                else:
                    avg_threshold = 0

                # 5. Filter candidates above average
                final_candidates = {
                    k: v for k, v in candidate_scores.items() if v >= avg_threshold
                }

                if not final_candidates:
                    final_candidates = candidate_scores

                new_cache[model] = {
                    "final_candidates": final_candidates,
                    "avg_threshold": avg_threshold,
                    "candidate_scores": candidate_scores,
                }

            self._api_candidate_cache = new_cache

            bt.logging.debug(
                f"Refreshed API candidate cache for {len(new_cache)} models"
            )

        except Exception as e:
            bt.logging.error(f"Error refreshing API candidate cache: {str(e)}")
            bt.logging.error(traceback.format_exc())

    async def _handle_api_task(self, request):
        """Handle incoming API task request"""
        try:
            request_id = request.get("request_id")
            model = request.get("model_type")

            if not request_id or not model:
                return {}

            bt.logging.info(
                f"Received API task request: {request_id} for model {model}"
            )

            # Get cached candidate data
            cached = self._api_candidate_cache.get(model)

            if cached is None:
                return {}

            final_candidates = cached["final_candidates"]
            avg_threshold = cached["avg_threshold"]

            if not final_candidates:
                return {}

            keys = list(final_candidates.keys())
            weights = list(final_candidates.values())
            selected_hotkey = random.choices(keys, weights=weights, k=1)[0]

            bt.logging.info(
                f"Assigned API task {request_id} to {selected_hotkey} (Score: {final_candidates[selected_hotkey]:.4f}, Threshold: {avg_threshold:.4f})"
            )

            return {"request_id": request_id, "miner_hotkey": selected_hotkey}

        except Exception as e:
            bt.logging.error(f"Error handling API task: {str(e)}")
            return {}
