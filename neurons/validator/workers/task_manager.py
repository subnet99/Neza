import os
import sys
import time
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
from neza.utils.http import check_url_resource_available, request_upload_url
from neurons.validator.tasks.task import Task
from neurons.validator.tasks.task_database import TaskDatabase
from neurons.validator.tasks.task_factory import TaskFactory


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

        # Task factory
        self.task_factory = TaskFactory(self.validator.material_manager)

        # Task processing lock
        self.task_lock = threading.Lock()

        # Database thread pool
        self.db_executor = ThreadPoolExecutor(
            max_workers=self.validator.validator_config.db_max_workers,
            thread_name_prefix="db_worker",
        )

        # Last task creation time
        self.last_task_creation_time = 0

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

        # Only process tasks every 10 blocks (approximately every 120 seconds)
        if block_number % 10 == 0:
            bt.logging.info(f"Processing tasks on block {block_number}")
            if self.validator.miner_info_cache is None:
                bt.logging.warning("No miner data available, skipping task processing")
                return
            # Create a new thread to process tasks
            thread = threading.Thread(target=self._process_tasks_in_thread)
            thread.daemon = True
            thread.start()
        else:
            bt.logging.debug(
                f"Skipping task processing on block {block_number} (not divisible by 10)"
            )

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

            # Check active tasks
            await self._check_active_tasks()

            # Verify completed tasks
            await self._verify_completed_tasks()

            # Process retry tasks
            await self._process_retry_tasks()

        except Exception as e:
            bt.logging.error(f"Error processing tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())

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
            # 1. Get available miners and their load information
            miners_with_capacity = (
                await self.validator.miner_manager.get_miners_with_capacity()
            )
            if not miners_with_capacity:
                return

            # 2. Process new pending tasks from database (excluding retry tasks)
            tasks_processed = await self._process_db_pending_tasks(miners_with_capacity)

            # 3. Get updated miner load information (may have changed)
            updated_miners_with_capacity = (
                await self.validator.miner_manager.get_miners_with_capacity()
            )
            if not updated_miners_with_capacity:
                return

            # 4. Create synthetic tasks
            bt.logging.info(f"Creating synthetic tasks")
            tasks_created = await self._create_synthetic_tasks(
                updated_miners_with_capacity
            )

            # Update last task creation time if tasks were processed or created
            if tasks_processed or tasks_created > 0:
                self.last_task_creation_time = time.time()
                bt.logging.info(
                    f"Updated last task creation time to {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.last_task_creation_time))}"
                )

        except Exception as e:
            bt.logging.error(f"Error in _process_pending_tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())

    async def _process_db_pending_tasks(self, miners_with_capacity):
        """
        Process pending tasks from database

        Args:
            miners_with_capacity: List of miners with capacity

        Returns:
            Number of tasks processed
        """
        try:
            # Get pending tasks from database
            pending_tasks = await self._db_op(self.db.get_pending_tasks, 10)
            if not pending_tasks:
                return 0

            bt.logging.info(f"Processing {len(pending_tasks)} pending tasks")

            # Select miners for tasks
            selected_miners = (
                await self.validator.miner_manager.select_miners_for_tasks(
                    miners_with_capacity, len(pending_tasks)
                )
            )

            if not selected_miners:
                bt.logging.warning("No miners available for pending tasks")
                return 0

            # Assign tasks to miners
            await self._assign_tasks_to_miners(
                pending_tasks, selected_miners, miners_with_capacity
            )

            return len(pending_tasks)

        except Exception as e:
            bt.logging.error(f"Error processing DB pending tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return 0

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
                workflow_params = task["workflow_params"]
                if isinstance(workflow_params, str):
                    try:
                        workflow_params = json.loads(workflow_params)
                    except:
                        bt.logging.warning(
                            f"Failed to parse workflow params for task {task_id}"
                        )
                        continue

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
                    workflow_params,
                    task_id,
                    secret_key,
                    timeout_seconds,
                    miner_uid,
                )

                if success:
                    # Update miner load
                    self._update_miner_load_after_task(miners_with_capacity, miner)

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

    async def _create_synthetic_tasks(self, miners_with_capacity):
        """
        Create synthetic tasks based on available miners with capacity

        Args:
            miners_with_capacity: List of miners with capacity

        Returns:
            int: Number of tasks created
        """
        try:
            # Determine how many synthetic tasks to create based on miners with capacity
            max_tasks = self._determine_synthetic_task_count(miners_with_capacity)
            if max_tasks <= 0:
                bt.logging.debug("No capacity for synthetic tasks")
                return 0

            bt.logging.info(f"Creating up to {max_tasks} synthetic tasks")

            # Select miners for synthetic tasks based on various factors like:
            selected_miners = await self._select_miners_for_synthetic_tasks(
                miners_with_capacity, max_tasks
            )
            if not selected_miners:
                bt.logging.warning("No miners available for synthetic tasks")
                return 0

            # Log available miners for debugging
            self._log_available_miners(miners_with_capacity)

            # Create and assign synthetic tasks to the selected miners
            await self._create_and_assign_synthetic_tasks(
                selected_miners, miners_with_capacity
            )

            bt.logging.info(
                f"Created and assigned {len(selected_miners)} synthetic tasks"
            )
            return len(selected_miners)

        except Exception as e:
            bt.logging.error(f"Error creating synthetic tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return 0

    def _determine_synthetic_task_count(self, miners_with_capacity):
        """
        Determine how many synthetic tasks to create

        Args:
            miners_with_capacity: List of miners with capacity

        Returns:
            Number of tasks to create
        """
        # Get max tasks from config
        max_tasks = self.validator.validator_config.miner_selection.get(
            "generate_max_tasks", 2
        )

        # Count miners with capacity
        available_count = sum(
            1
            for m in miners_with_capacity
            if m["remaining_capacity"] > 0 and not m["is_penalized"]
        )

        # Limit to a reasonable number
        return min(available_count, max_tasks)

    async def _select_miners_for_synthetic_tasks(self, miners_with_capacity, max_tasks):
        """
        Select miners for synthetic tasks

        Args:
            miners_with_capacity: List of miners with capacity
            max_tasks: Maximum number of tasks

        Returns:
            List of selected miners
        """
        # Use miner manager to select miners
        return await self.validator.miner_manager.select_miners_for_tasks(
            miners_with_capacity, max_tasks
        )

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

    async def _create_and_assign_synthetic_tasks(
        self, selected_miners, available_miners
    ):
        """
        Create and assign synthetic tasks to selected miners

        Args:
            selected_miners: List of selected miners to receive tasks
            available_miners: Complete list of available miners with capacity info
        """
        try:
            # Log the number of miners selected for task assignment
            bt.logging.info(
                f"Creating tasks for {len(selected_miners)} selected miners"
            )

            # Get materials for all miners at once
            materials_list = self.validator.material_manager.get_multiple_materials(
                len(selected_miners)
            )
            if not materials_list:
                bt.logging.warning(
                    "Failed to get materials for tasks, using fallback method"
                )

            # Create and assign tasks for each selected miner
            for i, miner in enumerate(selected_miners):
                # Extract miner information for logging
                miner_hotkey = miner.get("hotkey", "unknown")[:10]
                miner_uid = miner.get("uid", -1)

                task = None

                # Try to use pre-fetched materials if available
                if materials_list and i < len(materials_list):
                    material_info = materials_list[i]
                    if "task_params" in material_info and material_info["task_params"]:
                        # Generate a task ID
                        task_id = f"{uuid.uuid4().hex}"

                        # Use validator config for timeout
                        timeout_seconds = (
                            self.validator.validator_config.task_timeout_seconds
                        )

                        # Create task with the material parameters
                        task = Task(
                            task_id=task_id,
                            workflow_params=material_info["task_params"],
                            timeout_seconds=timeout_seconds,
                            is_synthetic=True,
                        )
                        bt.logging.info(
                            f"Created task from pre-fetched materials for miner {miner_hotkey}"
                        )

                # Fallback to task factory methods if material approach failed
                if not task:
                    # Create synthetic task using material manager
                    task = self.task_factory.create_task_from_materials()
                    if not task:
                        # Fallback to simple synthetic task if materials-based task creation fails
                        bt.logging.warning(
                            f"Failed to create materials-based task for miner {miner_hotkey}, trying simple task"
                        )
                        task = self.task_factory.create_synthetic_task()

                if not task:
                    bt.logging.warning(
                        f"Failed to create any synthetic task for miner {miner_hotkey}"
                    )
                    continue

                # Add task to database with proper tracking information
                success = await self._db_op(self.db.add_task, task)
                if not success:
                    bt.logging.error(
                        f"Failed to add synthetic task {task.task_id} to database for miner {miner_hotkey}"
                    )
                    continue

                bt.logging.info(
                    f"Added synthetic task {task.task_id} to database for miner {miner_hotkey}"
                )

                # Send task to miner with proper parameters
                await self._send_synthetic_task(
                    task.task_id,
                    task.workflow_params,
                    task.secret_key,
                    task.timeout_seconds,
                    miner,
                    available_miners,
                )

        except Exception as e:
            bt.logging.error(f"Error creating and assigning synthetic tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())

    async def _send_synthetic_task(
        self,
        task_id,
        task_params,
        secret_key,
        timeout_seconds,
        selected_miner,
        available_miners,
    ):
        """
        Send synthetic task to miner

        Args:
            task_id: Task ID
            task_params: Task parameters
            secret_key: Secret key
            timeout_seconds: Timeout in seconds
            selected_miner: Selected miner
            available_miners: List of available miners
        """
        try:
            miner_uid = selected_miner["uid"]
            miner_hotkey = selected_miner["hotkey"]

            # Send task to miner
            bt.logging.info(
                f"Sending synthetic task {task_id} to miner {miner_hotkey[:10] if miner_hotkey else 'None'}..."
            )

            # For synthetic tasks, task_id is used as file_name
            success = await self._send_task_to_miner(
                task_id, task_params, task_id, secret_key, timeout_seconds, miner_uid
            )

            if success:
                # Update miner load
                self._update_miner_load_after_task(available_miners, selected_miner)

        except Exception as e:
            bt.logging.error(f"Error sending synthetic task: {str(e)}")
            bt.logging.error(traceback.format_exc())

    async def _send_task_to_miner(
        self,
        task_id,
        workflow_params,
        file_name,
        secret_key,
        timeout_seconds,
        miner_uid,
    ):
        """
        Send task to miner

        Args:
            task_id: Task ID
            workflow_params: Workflow parameters
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

                if upload_url:
                    bt.logging.info(f"Got upload URL: {upload_url[:30]}...")
                else:
                    bt.logging.warning(f"No upload URL received for task {task_id}")
            except Exception as e:
                bt.logging.error(f"Error requesting upload URL: {str(e)}")
                bt.logging.error(traceback.format_exc())
                upload_url = ""
                preview_url = ""

            # Get the task with complete workflow from database
            task_details = await self._db_op(self.db.get_task, task_id)
            if not task_details:
                bt.logging.error(f"Task {task_id} not found in database after saving")
                return False

            # Get Comfy config
            comfy_config = self.validator.material_manager.get_comfy_config()
            if not comfy_config:
                bt.logging.error("Unable to get Comfy config")
                return False

            # Log workflow preparation
            bt.logging.info(f"Preparing workflow configuration for task {task_id}")

            # Merge config and workflow parameters to generate complete workflow configuration
            workflow_mapping = self.validator.material_manager.get_workflow_mapping()

            # Measure workflow preparation time
            workflow_start_time = time.time()

            if "mapping" not in workflow_params:
                # If no mapping is provided, use static mapping
                workflow_params_with_mapping = {
                    "params": workflow_params.get("params", workflow_params),
                    "mapping": workflow_mapping,
                }
                final_workflow_params = self.validator.material_manager.merge_configs(
                    comfy_config, workflow_params_with_mapping
                )
            else:
                # If mapping is already provided, use it directly
                final_workflow_params = self.validator.material_manager.merge_configs(
                    comfy_config, workflow_params
                )

            workflow_prep_time = time.time() - workflow_start_time
            bt.logging.info(
                f"Workflow preparation completed in {workflow_prep_time:.2f} seconds"
            )

            # Log workflow complexity
            if isinstance(final_workflow_params, dict):
                node_count = len(final_workflow_params)
                bt.logging.info(f"Final workflow contains {node_count} nodes")

            # Save the generated complete workflow back to database
            await self._db_op(
                self.db.update_task_complete_workflow,
                task_id=task_id,
                complete_workflow=final_workflow_params,
                upload_url=upload_url,
                preview_url=preview_url,
                miner_hotkey=miner_hotkey,
                miner_info=getattr(self.validator, "miner_info_cache", {}),
            )

            # Create task object - use VideoTask directly
            task_obj = VideoTask(
                task_id=task_id,
                file_name=file_name,
                secret_key=secret_key,
                workflow_params=final_workflow_params,
                upload_url=upload_url,
                preview_url=preview_url,
            )

            # Log sending task
            bt.logging.info(
                f"Sending task {task_id} to miner {miner_hotkey[:10] if miner_hotkey else 'None'}... axon {axon}"
            )

            # Record task preparation time
            prep_time = time.time() - start_time
            bt.logging.info(f"Task preparation completed in {prep_time:.2f} seconds")

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

            # Get current task status
            current_task = await self._db_op(self.db.get_task, task_id)
            current_status = current_task.get("status") if current_task else None

            # If current status is failed, do not update status
            if current_status == "failed":
                bt.logging.info(
                    f"Task {task_id} is already in failed status, not updating status"
                )
                # Only update other information, do not update status
                await self._db_op(
                    self.db.update_task_with_upload_info,
                    task_id=task_id,
                    miner_hotkey=miner_hotkey,
                    preview_url=(
                        task_obj.preview_url
                        if task_obj.preview_url
                        else response.video_url if isResponse else ""
                    ),
                    status=current_status,  # Keep current status
                    miner_info=getattr(self.validator, "miner_info_cache", {}),
                    upload_url=(
                        task_obj.upload_url if hasattr(task_obj, "upload_url") else ""
                    ),
                )
            else:
                # Normal update status
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
                    miner_info=getattr(self.validator, "miner_info_cache", {}),
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
                await self._db_op(
                    self.db.update_miner_uid,
                    hotkey=miner_hotkey,
                    uid=miner_uid,
                    current_block=self.validator.subtensor.get_current_block(),
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

    async def _check_active_tasks(self):
        """
        Check active tasks (processing status) and update status if needed
        """
        try:
            # Get active tasks - now a synchronous call
            active_tasks = self.db.get_active_tasks()
            if not active_tasks:
                return

            bt.logging.info(f"Checking {len(active_tasks)} active tasks")

            # Track task status counts
            completed_count = 0
            failed_count = 0
            still_running_count = 0
            timeout_count = 0

            # Process each active task
            for task in active_tasks:
                task_id = task["task_id"]
                miner_hotkey = task.get("miner_hotkey")

                # Skip tasks without miner hotkey
                if not miner_hotkey:
                    bt.logging.warning(
                        f"Task {task_id} has no miner hotkey, skipping check"
                    )
                    continue

                # Get last sent time
                last_sent_time = task.get("last_sent_at")

                if last_sent_time:
                    # Get timeout time
                    timeout_seconds = task.get(
                        "timeout_seconds",
                        self.validator.validator_config.task_timeout_seconds,
                    )

                    # Calculate task runtime
                    if isinstance(last_sent_time, str):
                        try:
                            last_sent_time = datetime.fromisoformat(
                                last_sent_time.replace("Z", "+00:00")
                            )
                        except:
                            last_sent_time = None

                    # Check if timed out
                    if (
                        last_sent_time
                        and (
                            datetime.now(timezone.utc) - last_sent_time
                        ).total_seconds()
                        > timeout_seconds
                    ):
                        # Update task status to timed out
                        bt.logging.warning(
                            f"Task {task_id} timed out after {timeout_seconds} seconds"
                        )

                        # Handle task failure due to timeout
                        await self._handle_task_failure(task_id, "Timeout")
                        timeout_count += 1
                        continue

                # Get miner UID
                miner_uid = task.get("miner_uid")
                if not miner_uid:
                    # Try to get miner UID from hotkey
                    try:
                        miner_uid = self.validator.metagraph.hotkeys.index(miner_hotkey)
                    except ValueError:
                        bt.logging.warning(
                            f"Miner hotkey {miner_hotkey[:10]} not found in metagraph"
                        )
                        continue

                # Check if preview URL or video URL is available
                video_url = task.get("preview_url") or task.get("s3_video_url")
                if video_url:
                    is_available, file_info = await check_url_resource_available(
                        video_url, get_file_info=True
                    )

                    # Check if file size exceeds 10MB limit
                    if is_available and file_info and "file_size" in file_info:
                        file_size_limit = (
                            self.validator.validator_config.file_size_limit
                        )
                        file_size_mb = file_info["file_size"] / (
                            1024 * 1024
                        )  # Convert bytes to MB
                        if file_size_mb > file_size_limit:
                            bt.logging.warning(
                                f"Task {task_id} failed: File size {file_size_mb:.2f}MB exceeds {file_size_limit}MB limit"
                            )
                            # Handle task failure due to file size limit
                            await self._handle_task_failure(
                                task_id,
                                f"File size {file_size_mb:.2f}MB exceeds {file_size_limit}MB limit",
                            )
                            continue

                    if is_available:
                        # File is available, mark as completed
                        file_size = file_info.get("file_size", "unknown")
                        last_modified = file_info.get("last_modified", "")
                        bt.logging.info(
                            f"Task {task_id} completed by miner {miner_hotkey[:10] if miner_hotkey else 'None'}, file_size: {file_size}, last_modified: {last_modified}"
                        )
                        # Pre-download video to cache directory
                        await self.validator.video_manager.download_video(
                            task_id, video_url, "miner"
                        )

                        # Update task in database
                        await self._db_op(
                            self.db.update_task_to_completed,
                            task_id=task_id,
                            miner_hotkey=miner_hotkey,
                            file_info=file_info,
                        )

                        completed_count += 1
                    else:
                        # File not available, still processing
                        bt.logging.debug(
                            f"Task {task_id} still processing by miner {miner_hotkey[:10] if miner_hotkey else 'None'}"
                        )
                        still_running_count += 1
                else:
                    # No URL yet, still processing
                    bt.logging.debug(
                        f"Task {task_id} still processing by miner {miner_hotkey[:10] if miner_hotkey else 'None'} (no URL yet)"
                    )
                    still_running_count += 1

            # Record task status summary
            if completed_count > 0 or failed_count > 0 or timeout_count > 0:
                bt.logging.info(
                    f"Task status update: completed={completed_count}, failed={failed_count}, timeout={timeout_count}, running={still_running_count}"
                )

        except Exception as e:
            bt.logging.error(f"Error checking active tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())

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
            current_verifying = len(self.validator.verification_manager.verifying_tasks)
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

            # Calculate fetch capacity: max_capacity * 1.5 but not exceeding max_capacity
            # If no available capacity but queue is empty, get at least 1 task
            if available_capacity > 0 or queue_size == 0:
                # Use miner selection strategy based on verification density
                sorted_tasks = (
                    self.validator.verification_manager.select_tasks_for_verification(
                        all_completed_tasks
                    )
                )

                # Select task based on available capacity
                fetch_capacity = min(max_capacity, max(1, int(max_capacity * 1.5)))
                task_count = min(fetch_capacity, len(sorted_tasks))
                task_count = max(1, task_count) if sorted_tasks else 0

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
                task_id, miner_hotkey, 0, error_info, 0
            )

        return True

    def add_task(self, workflow_params, timeout_seconds=600):
        """
        Add a new task

        Args:
            workflow_params: Workflow parameters
            timeout_seconds: Timeout in seconds

        Returns:
            Task ID or None if failed
        """
        try:
            # Create task
            task = self.task_factory.create_task_from_params(
                workflow_params, timeout_seconds
            )
            if not task:
                bt.logging.error("Failed to create task")
                return None

            # Add to database
            loop = asyncio.new_event_loop()
            try:
                success = loop.run_until_complete(self._db_op(self.db.add_task, task))
            finally:
                loop.close()

            if not success:
                bt.logging.error("Failed to add task to database")
                return None

            bt.logging.info(f"Added task {task.task_id}")
            return task.task_id

        except Exception as e:
            bt.logging.error(f"Error adding task: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return None
