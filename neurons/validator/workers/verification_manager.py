import time
import threading
import asyncio
import traceback
import json
import queue
from neza.utils.http import upload_task_result, upload_miner_completion

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
        self.verification_queue = queue.Queue()
        self.verifying_tasks = set()  # Set of task IDs currently being verified
        self.verifying_tasks_lock = threading.Lock()  # Thread lock for verifying_tasks
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
        Starts verification worker threads with independent event loops for true parallelism.
        Each worker runs in its own thread with its own event loop, allowing workers to operate
        independently even if one worker is blocked.
        """
        worker_count = self.validator.validator_config.verification[
            "max_concurrent_verifications"
        ]
        bt.logging.info(f"Starting {worker_count} independent verification workers")

        # Clear any existing workers
        self.stop_verification_workers()

        # Track worker threads and their status
        self.worker_threads = []
        self.worker_status = {}

        # Create and start independent worker threads
        for i in range(worker_count):
            thread = threading.Thread(
                target=self._run_worker_in_thread,
                args=(i,),
                daemon=True,
                name=f"verification_worker_{i}",
            )
            thread.start()
            self.worker_threads.append(thread)
            self.worker_status[i] = {
                "thread": thread,
                "active": True,
                "last_activity": time.time(),
                "current_task": None,
            }

        # Start health monitor thread
        self.monitor_thread = threading.Thread(
            target=self._monitor_worker_health, daemon=True, name="verification_monitor"
        )
        self.monitor_thread.start()

        bt.logging.info(
            f"Started {len(self.worker_threads)} independent verification worker threads"
        )

    def _run_worker_in_thread(self, worker_id):
        """
        Runs a verification worker in its own thread with a dedicated event loop.

        Args:
            worker_id: Unique identifier for this worker
        """
        # Create independent event loop for this worker
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        bt.logging.info(
            f"Verification worker {worker_id} started with independent event loop"
        )

        try:
            # Run the worker coroutine
            loop.run_until_complete(self._verification_worker(worker_id))
        except Exception as e:
            bt.logging.error(f"Error in verification worker {worker_id}: {str(e)}")
            bt.logging.error(traceback.format_exc())
        finally:
            # Clean up
            loop.close()
            bt.logging.info(f"Verification worker {worker_id} stopped")

    def _monitor_worker_health(self):
        """
        Monitors the health of verification workers and restarts any that appear to be stuck.
        Runs in a separate thread to provide independent monitoring.
        """
        while True:
            try:
                # Check each worker's status
                current_time = time.time()
                for worker_id, status in self.worker_status.items():
                    if not status["active"]:
                        continue

                    # Check if worker thread is alive
                    if not status["thread"].is_alive():
                        bt.logging.warning(
                            f"Worker {worker_id} thread is not alive, restarting"
                        )
                        self._restart_worker(worker_id)
                        continue

                    # Check for inactivity
                    last_activity = status["last_activity"]
                    if current_time - last_activity > 3600:  # 1 hour without activity
                        task_info = (
                            f" on task {status['current_task']}"
                            if status["current_task"]
                            else ""
                        )
                        bt.logging.warning(
                            f"Worker {worker_id} appears stuck{task_info}, restarting"
                        )
                        self._restart_worker(worker_id)

                # Sleep before next check
                time.sleep(300)  # Check every 5 minutes

            except Exception as e:
                bt.logging.error(f"Error in worker health monitor: {str(e)}")
                bt.logging.error(traceback.format_exc())
                time.sleep(60)  # Wait before retrying

    def _restart_worker(self, worker_id):
        """
        Restarts a verification worker thread.

        Args:
            worker_id: ID of the worker to restart
        """
        try:
            # Mark old worker as inactive
            if worker_id in self.worker_status:
                self.worker_status[worker_id]["active"] = False

            # Start a new worker thread
            thread = threading.Thread(
                target=self._run_worker_in_thread,
                args=(worker_id,),
                daemon=True,
                name=f"verification_worker_{worker_id}_restarted",
            )
            thread.start()

            # Update worker status
            self.worker_status[worker_id] = {
                "thread": thread,
                "active": True,
                "last_activity": time.time(),
                "current_task": None,
            }

            bt.logging.info(f"Restarted verification worker {worker_id}")

        except Exception as e:
            bt.logging.error(f"Failed to restart worker {worker_id}: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def stop_verification_workers(self):
        """
        Stops all verification workers and cleans up resources.
        """
        bt.logging.info("Stopping all verification workers")

        # Mark all workers as inactive
        if hasattr(self, "worker_status"):
            for worker_id in self.worker_status:
                self.worker_status[worker_id]["active"] = False

        # Clear the verification queue if it exists
        if hasattr(self, "verification_queue") and self.verification_queue is not None:
            while not self.verification_queue.empty():
                try:
                    self.verification_queue.get_nowait()
                    self.verification_queue.task_done()
                except queue.Empty:
                    break

        # Wait for threads to end (with timeout)
        if hasattr(self, "worker_threads"):
            for thread in self.worker_threads:
                if thread.is_alive():
                    thread.join(timeout=2.0)

        # Reset tracking variables
        self.worker_threads = []
        self.worker_status = {}
        self.verification_queue = queue.Queue()

        bt.logging.info("All verification workers stopped")

    async def _verification_worker(self, worker_id):
        """
        Verification worker coroutine that processes tasks from the queue.

        Args:
            worker_id: Unique identifier for this worker
        """
        bt.logging.info(f"Verification worker {worker_id} coroutine started")

        while True:
            try:
                # Update worker status
                if worker_id in self.worker_status:
                    self.worker_status[worker_id]["last_activity"] = time.time()

                if not self.verification_queue.empty():
                    try:
                        comfy_api = self.validator.verifier.comfy_api
                        server = comfy_api.servers[worker_id]
                        comfy_api._check_server_availability(server, clear_queue=False)
                        comfy_available = server["available"]
                        if not comfy_available:
                            bt.logging.warning(
                                f"Worker {worker_id}: ComfyUI server is unavailable, retry after waiting for 30 seconds"
                            )
                            await asyncio.sleep(30)
                            continue
                    except Exception as e:
                        bt.logging.error(
                            f"Error checking ComfyUI server availability: {str(e)}"
                        )
                        bt.logging.error(traceback.format_exc())
                        await asyncio.sleep(30)
                        continue

                # Get next task from the thread-safe queue with timeout
                # Using blocking get with timeout instead of async to work with thread-safe queue
                try:
                    task = self.verification_queue.get(timeout=5.0)
                    bt.logging.info(
                        f"Worker {worker_id} received task {task['task_id']}"
                    )

                    # Update worker status with current task
                    if worker_id in self.worker_status:
                        self.worker_status[worker_id]["current_task"] = task["task_id"]
                except queue.Empty:
                    # No task available, wait and try again
                    await asyncio.sleep(5)
                    continue

                # Extract task details
                task_id = task["task_id"]
                miner_hotkey = task["miner_hotkey"]
                miner_uid = self._get_miner_uid(miner_hotkey)

                # Skip if already being verified
                with self.verifying_tasks_lock:
                    if task_id in self.verifying_tasks:
                        bt.logging.debug(
                            f"Task {task_id} already being verified, skipping"
                        )
                        self.verification_queue.task_done()
                        continue

                    # Mark as being verified
                    self.verifying_tasks.add(task_id)

                # Execute verification
                bt.logging.info(
                    f"Worker {worker_id} verifying task {task_id} from miner {miner_hotkey[:10]}..."
                )

                try:
                    # Set a timeout for the entire verification process
                    start_time = time.time()
                    verification_timeout = 1800  # 30 minutes

                    # Create a task with timeout
                    verification_task = asyncio.create_task(
                        self._execute_verification(
                            task_id, miner_hotkey, miner_uid, worker_id
                        )
                    )

                    # Wait for verification to complete with timeout
                    await asyncio.wait_for(
                        verification_task, timeout=verification_timeout
                    )

                    # Update worker status - task completed
                    elapsed_time = time.time() - start_time
                    bt.logging.info(
                        f"Worker {worker_id} completed task {task_id} in {elapsed_time:.2f}s"
                    )

                except asyncio.TimeoutError:
                    bt.logging.error(
                        f"Verification timeout for task {task_id} after {verification_timeout}s"
                    )
                    # Update as verification failed
                    await self._update_verification_failed(
                        task_id, miner_hotkey, miner_uid
                    )

                except Exception as e:
                    bt.logging.error(f"Error verifying task {task_id}: {str(e)}")
                    bt.logging.error(traceback.format_exc())
                    # Update as verification failed
                    await self._update_verification_failed(
                        task_id, miner_hotkey, miner_uid
                    )

                finally:
                    # Clean up
                    # Add miner to verified miners set
                    self.verified_miners.add(miner_hotkey)

                    # Update verified task count for this miner
                    if miner_hotkey in self.verified_task_counts:
                        self.verified_task_counts[miner_hotkey] += 1
                    else:
                        self.verified_task_counts[miner_hotkey] = 1

                    # Remove from verifying tasks
                    with self.verifying_tasks_lock:
                        if task_id in self.verifying_tasks:
                            self.verifying_tasks.remove(task_id)

                    # Mark task as done in queue
                    self.verification_queue.task_done()

                    # Update worker status - no current task
                    if worker_id in self.worker_status:
                        self.worker_status[worker_id]["current_task"] = None
                        self.worker_status[worker_id]["last_activity"] = time.time()

            except asyncio.CancelledError:
                bt.logging.info(f"Verification worker {worker_id} cancelled")
                break

            except Exception as e:
                bt.logging.error(f"Error in verification worker {worker_id}: {str(e)}")
                bt.logging.error(traceback.format_exc())
                # Wait before retrying to avoid tight error loops
                await asyncio.sleep(5)

        bt.logging.info(f"Verification worker {worker_id} stopped")

    async def _get_next_verification_task(self):
        """Gets next task from verification queue"""
        try:
            if asyncio.get_running_loop() is not self.verification_loop:
                bt.logging.warning("Attempting to get task from wrong event loop")
                return None

            return await asyncio.wait_for(self.verification_queue.get(), timeout=6.0)
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
        """Get task details from database"""
        try:
            task = await self.validator.task_manager.db.get_task(task_id)
            return task
        except Exception as e:
            bt.logging.error(f"Error getting task details: {str(e)}")
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

            # Use verifier
            score, result = await self.validator.verifier.verify_task_with_package(
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
            miner_hotkey: Miner original hotkey
            miner_uid: Miner UID
            score: Verification score
            completion_time: Task completion time
            metrics: Detailed metrics from verification
        """
        try:
            # Check if the original hotkey matches the current hotkey for this UID
            current_hotkey = None
            try:
                if miner_uid is not None:
                    # Use cached hotkey from miner manager
                    current_hotkey = self.validator.miner_manager.all_miner_hotkeys.get(
                        miner_uid
                    )
                    if current_hotkey is None:
                        # Fallback to metagraph if not in cache
                        if miner_uid < len(self.validator.metagraph.hotkeys):
                            current_hotkey = self.validator.metagraph.hotkeys[miner_uid]
            except Exception as e:
                bt.logging.warning(
                    f"Error getting current hotkey for UID {miner_uid}: {str(e)}"
                )

            # If hotkey has changed, skip score recording
            if current_hotkey is not None and current_hotkey != miner_hotkey:
                bt.logging.warning(
                    f"Hotkey changed for UID {miner_uid}: original={miner_hotkey[:10]}..., current={current_hotkey[:10]}..., skipping score recording for task {task_id}"
                )
                # Update task status to indicate hotkey change
                await self.validator.task_manager._db_op(
                    self.validator.task_manager.db.update_task_status,
                    task_id=task_id,
                    status="hotkey_changed",
                )
                return

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
                    task_id,
                    miner_hotkey,
                    0,
                    {"error": "Verification failed"},
                    0,
                    is_error=True,
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
        Adds a task to verification queue.
        Uses thread-safe queue to distribute tasks to workers.

        Args:
            task: Task to verify

        Returns:
            bool: True if task was successfully added to queue, False otherwise
        """
        try:
            # Add task to thread-safe queue with timeout
            self.verification_queue.put(task, timeout=30.0)
            bt.logging.debug(f"Added task {task['task_id']} to verification queue")
            return True
        except queue.Full:
            bt.logging.error(
                f"Verification queue is full, could not add task {task['task_id']}"
            )
            return False
        except Exception as e:
            bt.logging.error(f"Error adding task to verification queue: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

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

    async def upload_task(
        self,
        task_id,
        miner_hotkey,
        score,
        metrics,
        completion_time,
        is_error=False,
    ):
        try:
            await upload_task_result(
                task_id=task_id,
                validator_wallet=self.validator.wallet,
                miner_hotkey=miner_hotkey,
                score=score,
                verification_result=metrics,
                processing_time=completion_time,
                is_error=is_error,
            )
            bt.logging.info(
                f"Task result for {task_id} uploaded to dashboard successfully"
            )
        except Exception as e:
            bt.logging.error(f"Failed to upload task result to dashboard: {str(e)}")
            bt.logging.error(traceback.format_exc())

    async def upload_miner_completed_task(self, task_id, task_detail):

        # Upload miner completion to owner
        if task_detail:
            await upload_miner_completion(task_id, self.validator.wallet, task_detail)
