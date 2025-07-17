import time
import threading
import asyncio
import traceback
import json
from neza.utils.http import upload_task_result

import bittensor as bt


class VerificationManager:
    """
    Manages verification workers and verification tasks
    """

    def __init__(self, validator):
        """
        Initialize verification manager

        Args:
            validator: Parent validator instance
        """
        self.validator = validator

        # Verification queue and task tracking
        self.verification_queue = None
        self.verifying_tasks = set()  # Set of task IDs currently being verified
        self.verification_workers = []  # List of verification worker tasks

        # Verification cycle management
        self.verified_miners = set()  # Set of miners verified in current cycle
        self.verified_task_counts = (
            {}
        )  # Dict mapping miner hotkey to number of verified tasks
        self.verification_cycle_start = (
            time.time()
        )  # Current verification cycle start time

        # Start verification workers
        self._start_verification_workers()

    def _start_verification_workers(self):
        """
        Starts verification worker threads
        Creates specified number of async verification worker tasks
        """
        bt.logging.info(
            f"Starting {self.validator.validator_config.verification['max_concurrent_verifications']} verification workers"
        )

        # Clear old workers if any
        for worker in self.verification_workers:
            if not worker.done():
                worker.cancel()
        self.verification_workers = []

        # Create event loop and worker thread
        self.verification_loop = asyncio.new_event_loop()

        # Recreate the queue
        self.verification_queue = asyncio.Queue()

        # Run workers in new thread
        def run_verification_workers():
            asyncio.set_event_loop(self.verification_loop)

            # Create worker tasks
            for i in range(
                self.validator.validator_config.verification[
                    "max_concurrent_verifications"
                ]
            ):
                worker = self.verification_loop.create_task(
                    self._verification_worker(worker_id=i)
                )
                self.verification_workers.append(worker)

            # Run event loop
            try:
                self.verification_loop.run_forever()
            except Exception as e:
                bt.logging.error(f"Verification worker event loop error: {str(e)}")
                bt.logging.error(traceback.format_exc())

        # Start worker thread
        self.verification_thread = threading.Thread(
            target=run_verification_workers, daemon=True
        )
        self.verification_thread.start()
        bt.logging.info("Verification worker thread started")

    def stop_verification_workers(self):
        """
        Stops all verification workers
        """
        bt.logging.info("Stopping all verification workers")

        # Cancel all tasks
        if hasattr(self, "verification_workers"):
            for worker in self.verification_workers:
                if not worker.done():
                    worker.cancel()
            self.verification_workers = []

        # Stop event loop
        if hasattr(self, "verification_loop") and self.verification_loop.is_running():
            self.verification_loop.call_soon_threadsafe(self.verification_loop.stop)

        # Wait for thread to end
        if hasattr(self, "verification_thread") and self.verification_thread.is_alive():
            self.verification_thread.join(timeout=2.0)
            if self.verification_thread.is_alive():
                bt.logging.warning(
                    "Verification worker thread did not stop within timeout"
                )

    async def _verification_worker(self, worker_id):
        """
        Verification worker coroutine
        Processes verification tasks from the queue

        Args:
            worker_id: Worker identifier
        """
        bt.logging.info(f"Verification worker {worker_id} started")

        while True:
            try:
                # Get next verification task
                task = await self._get_next_verification_task()
                if not task:
                    # No task available, wait and try again
                    await asyncio.sleep(5)
                    continue

                # Extract task details
                task_id = task["task_id"]
                miner_hotkey = task["miner_hotkey"]
                miner_uid = self._get_miner_uid(miner_hotkey)

                # Skip if already being verified
                if task_id in self.verifying_tasks:
                    bt.logging.debug(f"Task {task_id} already being verified, skipping")
                    continue

                # Mark as being verified
                self.verifying_tasks.add(task_id)

                # Execute verification
                bt.logging.info(
                    f"Worker {worker_id} verifying task {task_id} from miner {miner_hotkey[:10]}..."
                )

                try:
                    await self._execute_verification(
                        task_id, miner_hotkey, miner_uid, worker_id
                    )

                except Exception as e:
                    bt.logging.error(f"Error verifying task {task_id}: {str(e)}")
                    bt.logging.error(traceback.format_exc())

                    # Update as verification failed
                    await self._update_verification_failed(
                        task_id, miner_hotkey, miner_uid
                    )

                finally:
                    # Add miner to verified miners set
                    self.verified_miners.add(miner_hotkey)

                    # Update verified task count for this miner
                    if miner_hotkey in self.verified_task_counts:
                        self.verified_task_counts[miner_hotkey] += 1
                    else:
                        self.verified_task_counts[miner_hotkey] = 1

                    # Remove from verifying tasks
                    if task_id in self.verifying_tasks:
                        self.verifying_tasks.remove(task_id)

            except asyncio.CancelledError:
                bt.logging.info(f"Verification worker {worker_id} cancelled")
                break

            except Exception as e:
                bt.logging.error(f"Error in verification worker {worker_id}: {str(e)}")
                bt.logging.error(traceback.format_exc())
                await asyncio.sleep(5)  # Wait before retrying

        bt.logging.info(f"Verification worker {worker_id} stopped")

    async def _get_next_verification_task(self):
        """Gets next task from verification queue"""
        try:
            if asyncio.get_running_loop() is not self.verification_loop:
                bt.logging.warning("Attempting to get task from wrong event loop")
                return None

            return await asyncio.wait_for(self.verification_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            return None
        except RuntimeError as e:
            bt.logging.error(f"Runtime error in _get_next_verification_task: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return None
        except Exception as e:
            bt.logging.error(f"Error getting next verification task: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return None

    def _get_miner_uid(self, miner_hotkey):
        """Gets miner UID from hotkey"""
        for uid, hotkey in enumerate(self.validator.metagraph.hotkeys):
            if hotkey == miner_hotkey:
                return uid
        return None

    async def _execute_verification(self, task_id, miner_hotkey, miner_uid, worker_id):
        """
        Executes verification for a task

        Args:
            task_id: Task ID to verify
            miner_hotkey: Miner hotkey
            miner_uid: Miner UID
            worker_id: Worker ID for server selection
        """
        try:
            # Add verification start time
            verification_start_time = time.time()

            # Get task details
            task_details = await self._get_task_details(task_id)
            if not task_details:
                bt.logging.warning(f"Task {task_id} not found in database")
                return

            score, result = await self._verify_task(task_details, worker_id)

            # Get completion_time from database
            # This value should be calculated as last_modified - last_sent_at in the database
            completion_time = task_details.get("completion_time")

            # If completion_time is not available in the database, use 0
            if completion_time is None:
                bt.logging.warning(
                    f"Task {task_id} has no completion_time in database, using 0"
                )
                completion_time = 0

            # Update verification result
            await self._update_verification_result(
                task_id,
                miner_hotkey,
                miner_uid,
                score,
                completion_time,
                result,  # Pass the entire result as metrics
            )

            # Record verification time
            verification_time = time.time() - verification_start_time

            # Add None check for score and verification_time
            score_str = f"{score:.4f}" if score is not None else "None"
            time_str = (
                f"{verification_time:.2f}" if verification_time is not None else "None"
            )
            bt.logging.info(
                f"Task {task_id} verified in {time_str}s with score {score_str}"
            )

        except Exception as e:
            bt.logging.error(
                f"Error executing verification for task {task_id}: {str(e)}"
            )
            bt.logging.error(traceback.format_exc())
            # Update task status to verification failed when error occurs
            await self._update_verification_failed(task_id, miner_hotkey, miner_uid)
            raise

    async def _get_task_details(self, task_id):
        """
        Gets task details from database

        Args:
            task_id: Task ID to get details for

        Returns:
            Dict with task details or None if not found
        """
        try:
            # Get task from database
            task = await self.validator.task_manager.db.get_task(task_id)

            if not task:
                bt.logging.warning(f"Task {task_id} not found in database")
                return None

            # Check if task has result URL
            if not task.get("s3_video_url"):
                bt.logging.warning(f"Task {task_id} has no video URL")
                return None

            return task

        except Exception as e:
            bt.logging.error(f"Error getting task details for {task_id}: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return None

    async def _verify_task(self, task_details, worker_id):
        """
        Verifies a task

        Args:
            task_details: Task details from database
            worker_id: Worker ID for server selection

        Returns:
            tuple: (score, result) where score is a float and result is a dict
        """
        try:
            # Get task parameters
            task_id = task_details.get("task_id")

            # Get completion time if available
            completion_time = None
            if "completion_time" in task_details:
                completion_time = task_details.get("completion_time")
                # Convert to seconds if it's a datetime object
                if hasattr(completion_time, "timestamp"):
                    completion_time = completion_time.timestamp()

            # Get complete workflow
            complete_workflow = self._get_complete_workflow(task_details)

            # Perform verification
            score, result = await self._perform_verification(
                task_id, complete_workflow, completion_time, worker_id
            )

            return score, result

        except Exception as e:
            bt.logging.error(
                f"Error verifying task {task_details.get('task_id')}: {str(e)}"
            )
            bt.logging.error(traceback.format_exc())
            # Return tuple (score, result) instead of dict
            return 0, {"error": str(e)}

    def _get_complete_workflow(self, task_details):
        """
        Gets complete workflow from task details

        Args:
            task_details: Task details from database

        Returns:
            Dict with complete workflow
        """
        try:
            # Extract complete workflow
            complete_workflow = task_details.get("complete_workflow", {})

            # If complete_workflow is not available, fall back to workflow_params
            if not complete_workflow:
                bt.logging.warning(
                    f"Complete workflow not found, falling back to workflow_params"
                )
                workflow_params = task_details.get("workflow_params", {})

                # If empty, try to parse from task info
                if not workflow_params and task_details.get("task_info"):
                    task_info = task_details.get("task_info")
                    if isinstance(task_info, str):

                        try:
                            task_info = json.loads(task_info)
                            workflow_params = task_info.get("workflow_params", {})
                        except:
                            pass

                return workflow_params

            return complete_workflow

        except Exception as e:
            bt.logging.error(f"Error getting complete workflow: {str(e)}")
            return {}

    async def _perform_verification(
        self, task_id, complete_workflow, completion_time=None, worker_id=None
    ):
        """
        Performs verification using video verifier

        Args:
            task_id: Task ID
            complete_workflow: Complete workflow
            completion_time: Task completion time in seconds
            worker_id: Worker ID for server selection

        Returns:
            tuple: (score, result) where score is a float and result is a dict
        """
        try:
            # Log verification
            bt.logging.info(f"Verifying task {task_id}")

            # Use video verifier
            score, result = await self.validator.verifier.verify_task(
                task_id=task_id,
                complete_workflow=complete_workflow,
                completion_time=completion_time,
                worker_id=worker_id,
            )

            return score, result

        except Exception as e:
            bt.logging.error(f"Error in verification: {str(e)}")
            bt.logging.error(traceback.format_exc())
            # Return tuple (score, result) instead of dict
            return 0, {"error": str(e)}

    async def _update_verification_result(
        self,
        task_id,
        miner_hotkey,
        miner_uid,
        score,
        completion_time=0,
        metrics=None,
    ):
        """
        Updates verification result

        Args:
            task_id: Task ID
            miner_hotkey: Miner hotkey
            miner_uid: Miner UID
            score: Verification score
            completion_time: Task completion time
            metrics: Detailed metrics from verification
        """
        try:
            # Update task in database
            await self.validator.task_manager._db_op(
                self.validator.task_manager.db.update_task_with_score,
                task_id=task_id,
                miner_hotkey=miner_hotkey,
                score=score,
                completion_time=completion_time,
                metrics=metrics,
            )

            # Record success if score is good
            if score is not None and score >= 0.5:
                self.validator.penalty_manager.record_miner_success(miner_hotkey)
            else:
                self.validator.penalty_manager.record_miner_failure(miner_hotkey)

            # Handle None score for logging
            score_str = f"{score:.4f}" if score is not None else "None"
            bt.logging.info(
                f"Updated verification result for task {task_id}, score: {score_str}"
            )

            # Score step for weights
            if miner_uid is not None:
                self.validator.score_step(
                    responses=[{"task_id": task_id, "score": score}],
                    task_name="video_generation",
                    task_id=task_id,
                    uids=[miner_uid],
                )

            await self.upload_task(
                task_id, miner_hotkey, score, metrics, completion_time
            )
        except Exception as e:
            bt.logging.error(f"Error updating verification result: {str(e)}")
            bt.logging.error(traceback.format_exc())

    async def _update_verification_failed(self, task_id, miner_hotkey, miner_uid):
        """
        Updates task as verification failed

        Args:
            task_id: Task ID
            miner_hotkey: Miner hotkey
            miner_uid: Miner UID
        """
        try:
            # Check if the task already has a score.
            task_with_score = await self.validator.task_manager._db_op(
                self.validator.task_manager.db.get_task, task_id
            )

            # Update task in database
            await self.validator.task_manager._db_op(
                self.validator.task_manager.db.update_task_verification_failed,
                task_id=task_id,
                miner_hotkey=miner_hotkey,
            )

            # Record failure
            self.validator.penalty_manager.record_miner_failure(miner_hotkey)

            # Only record the score as 0 when the task has no score.
            if task_with_score and task_with_score.get("score") is None:
                # Record zero score
                await self.validator.task_manager._db_op(
                    self.validator.task_manager.db.update_task_with_score,
                    task_id=task_id,
                    miner_hotkey=miner_hotkey,
                    score=0,
                    completion_time=0,
                    status="failed",
                )

                bt.logging.warning(
                    f"Verification failed for task {task_id}, score set to 0, status set to failed"
                )

                # Score step for weights with zero score
                if miner_uid is not None:
                    self.validator.score_step(
                        responses=[{"task_id": task_id, "score": 0}],
                        task_name="video_generation",
                        task_id=task_id,
                        uids=[miner_uid],
                    )

                await self.upload_task(
                    task_id, miner_hotkey, 0, {"error": "Verification failed"}, 0
                )
            else:
                bt.logging.info(
                    f"Task {task_id} score already set, skipping score update"
                )

        except Exception as e:
            bt.logging.error(f"Error updating verification failed: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def reset_verification_cycle(self):
        """Resets verification cycle"""
        # Reset cycle start time
        self.verification_cycle_start = time.time()

        # Reset verified miners
        old_count = len(self.verified_miners)
        self.verified_miners = set()

        # Reset verified task counts
        self.verified_task_counts = {}

        bt.logging.info(
            f"Reset verification cycle, cleared {old_count} verified miners and task counts"
        )

    def get_worker_count(self):
        """Gets current verification worker count"""
        return len([w for w in self.verification_workers if not w.done()])

    def adjust_verification_workers(self, new_count):
        """
        Adjusts number of verification workers

        Args:
            new_count: New worker count
        """
        current_count = self.get_worker_count()

        if new_count == current_count:
            return

        bt.logging.info(
            f"Adjusting verification workers from {current_count} to {new_count}"
        )

        # Restart workers with new count
        self.validator.validator_config.verification["max_concurrent_verifications"] = (
            new_count
        )

        # Stop current workers
        self.stop_verification_workers()

        # Start new workers
        self._start_verification_workers()

    def add_verification_task(self, task):
        """
        Adds a task to verification queue

        Args:
            task: Task to verify

        Returns:
            bool: True if task was successfully added to queue, False otherwise
        """
        # Add to queue in thread-safe way
        if hasattr(self, "verification_loop") and self.verification_loop.is_running():
            try:
                # Extract task details for potential rollback
                task_id = task["task_id"]

                # Add task to queue with timeout
                future = asyncio.run_coroutine_threadsafe(
                    self.verification_queue.put(task), self.verification_loop
                )
                future.result(timeout=180.0)
                bt.logging.debug(f"Added task {task_id} to verification queue")
                return True
            except Exception as e:
                bt.logging.error(f"Error adding task to verification queue: {str(e)}")
                bt.logging.error(traceback.format_exc())

                # Task failed to be added to queue, return False to indicate failure
                # Status rollback will be handled by the caller
                return False
        else:
            bt.logging.warning(
                f"Cannot add task {task['task_id']} to verification queue: loop not running"
            )
            return False  # Failed to add task to queue

    def get_verification_status(self):
        """
        Gets verification status information

        Returns:
            Dict with verification status
        """
        # Calculate cycle progress
        current_time = time.time()
        cycle_length = self.validator.validator_config.verification[
            "verification_cycle_length"
        ]
        elapsed = current_time - self.verification_cycle_start
        remaining = max(0, cycle_length - elapsed)

        # Get queue size
        queue_size = (
            self.verification_queue.qsize()
            if hasattr(self, "verification_queue")
            else 0
        )

        # Get active verifications
        active_verifications = len(self.verifying_tasks)

        # Get verified miners count
        verified_miners = len(self.verified_miners)

        # Get total verified tasks count
        total_verified_tasks = sum(self.verified_task_counts.values())
        # Get worker count
        worker_count = self.get_worker_count()

        return {
            "cycle_start": self.verification_cycle_start,
            "cycle_length": cycle_length,
            "elapsed_seconds": elapsed,
            "elapsed_minutes": elapsed / 60,
            "remaining_seconds": remaining,
            "remaining_minutes": remaining / 60,
            "progress_percent": min(100, (elapsed / cycle_length) * 100),
            "queue_size": queue_size,
            "active_verifications": active_verifications,
            "verified_miners": verified_miners,
            "total_verified_tasks": total_verified_tasks,
            "worker_count": worker_count,
        }

    def select_tasks_for_verification(self, tasks):
        """
        Select tasks for verification based on miner verification density

        Args:
            tasks: List of candidate tasks

        Returns:
            list: Sorted task list, prioritizing miners with low verification density
        """
        if not tasks:
            return []

        # Get miner verification density
        miner_verification_density = {}
        miner_task_count = {}

        # Calculate task count for each miner
        for task in tasks:
            miner_hotkey = task.get("miner_hotkey")
            if not miner_hotkey:
                continue

            if miner_hotkey not in miner_task_count:
                miner_task_count[miner_hotkey] = 0
            miner_task_count[miner_hotkey] += 1

        # Calculate verification density (verified tasks count / total task count)
        for miner_hotkey, task_count in miner_task_count.items():
            # Get number of verified tasks for this miner in current cycle
            verification_count = self.verified_task_counts.get(miner_hotkey, 0)

            # Calculate density
            density = verification_count / max(1, task_count)
            miner_verification_density[miner_hotkey] = density

            bt.logging.debug(
                f"Miner {miner_hotkey[:10]}... verification density: {density:.4f} ({verification_count}/{task_count})"
            )

        # Sort tasks by verification density
        sorted_tasks = sorted(
            tasks,
            key=lambda task: (
                miner_verification_density.get(
                    task.get("miner_hotkey"), 0
                ),  # First sort by verification density
                task.get("completed_at", ""),  # Then by completion time
            ),
        )

        return sorted_tasks

    def _handle_hotkey_change(self, old_hotkey, new_hotkey):
        """
        Updates verified task counts when a hotkey changes

        Args:
            old_hotkey: Previous hotkey
            new_hotkey: New hotkey
        """
        try:
            # Get minimum count from all miners and subtract 1, but not less than 0
            min_count = (
                min(list(self.verified_task_counts.values()))
                if self.verified_task_counts
                else 0
            )
            self.verified_task_counts[new_hotkey] = max(0, min_count - 1)
            if old_hotkey in self.verified_task_counts:
                del self.verified_task_counts[old_hotkey]

        except Exception as e:
            bt.logging.error(
                f"Error updating verified task counts for changed hotkey: {str(e)}"
            )
            bt.logging.error(traceback.format_exc())

    async def upload_task(self, task_id, miner_hotkey, score, metrics, completion_time):
        # Get task details for upload
        task_details = await self.validator.task_manager._db_op(
            self.validator.task_manager.db.get_task, task_id=task_id
        )

        # Upload task result to owner
        if task_details:
            try:
                # Upload task result
                await upload_task_result(
                    task_id=task_id,
                    validator_wallet=self.validator.wallet,
                    miner_hotkey=miner_hotkey,
                    score=score,
                    verification_result=metrics,
                    processing_time=completion_time,
                    task_details=task_details,
                )
                bt.logging.info(
                    f"Task result for {task_id} uploaded to dashboard successfully"
                )
            except Exception as e:
                bt.logging.error(f"Failed to upload task result to dashboard: {str(e)}")
                bt.logging.error(traceback.format_exc())
