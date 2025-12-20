import time
import json
import asyncio
import traceback
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timezone

import bittensor as bt

from neza.database.validator import validator_db
from neurons.validator.tasks.task import Task


class TaskDatabase:
    """
    Interface to the task database
    Provides methods for storing and retrieving tasks
    """

    def __init__(self):
        """Initialize task database"""
        self.db = validator_db  # ValidatorDatabase instance

    async def add_task(self, task: Task) -> bool:
        """
        Add task to database

        Args:
            task: Task to add

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert task to dictionary
            task_dict = task.to_dict()

            # Directly call the original add_task method, without using await
            success = self.db.add_task(
                task_dict["task_id"],
                task_dict["complete_workflow"],
                task_dict["secret_key"],
                task_dict["timeout_seconds"],
            )

            return success

        except Exception as e:
            bt.logging.error(f"Error adding task to database: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    async def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get task from database

        Args:
            task_id: Task ID

        Returns:
            Dict with task data or None if not found
        """
        try:
            return self.db.get_task_by_id(task_id)
        except Exception as e:
            bt.logging.error(f"Error getting task {task_id}: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return None

    async def batch_get_tasks(self, task_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Get multiple tasks from database

        Args:
            task_ids: List of task IDs

        Returns:
            List of task dictionaries
        """
        try:
            return self.db.batch_get_tasks(task_ids)
        except Exception as e:
            bt.logging.error(f"Error batch getting tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return []

    async def batch_update_timeout_tasks_with_scores(
        self, timeout_tasks: List[tuple]
    ) -> bool:
        """
        Batch update timeout tasks with status and scores

        Args:
            timeout_tasks: List of tuples (task_id, miner_hotkey, score)

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return self.db.batch_update_timeout_tasks_with_scores(timeout_tasks)
        except Exception as e:
            bt.logging.error(f"Error batch updating timeout tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    async def update_task_status(self, task_id: str, status: str) -> bool:
        """
        Update task status

        Args:
            task_id: Task ID
            status: New status

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return self.db.update_task_status(task_id, status)
        except Exception as e:
            bt.logging.error(f"Error updating task status: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    async def update_task_result(self, task_id: str, result_url: str) -> bool:
        """
        Update task result

        Args:
            task_id: Task ID
            result_url: Result URL

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return self.db.update_task_result(
                task_id=task_id, result_url=result_url, completion_time=time.time()
            )
        except Exception as e:
            bt.logging.error(f"Error updating task result: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    async def update_task_verification(
        self,
        task_id: str,
        verification_score: float,
        verification_result: Dict[str, Any],
        verification_time: float = None,
    ) -> bool:
        """
        Update task verification

        Args:
            task_id: Task ID
            verification_score: Verification score
            verification_result: Verification result
            verification_time: Verification time

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert verification result to JSON string if needed
            if isinstance(verification_result, dict):
                verification_result = json.dumps(verification_result)

            return self.db.update_task_verification(
                task_id=task_id,
                verification_score=verification_score,
                verification_result=verification_result,
                verification_time=verification_time or time.time(),
            )
        except Exception as e:
            bt.logging.error(f"Error updating task verification: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    async def update_task_verification_failed(
        self, task_id: str, miner_hotkey: str
    ) -> bool:
        """
        Update task as verification failed

        Args:
            task_id: Task ID
            miner_hotkey: Miner hotkey

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return self.db.update_task_verification_failed(task_id, miner_hotkey)
        except Exception as e:
            bt.logging.error(f"Error updating task verification failed: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    async def update_task_with_score(
        self,
        task_id: str,
        miner_hotkey: str,
        score: float,
        completion_time: float,
        metrics: Dict[str, Any] = None,
        status: str = "scored",
    ) -> bool:
        """
        Update task with score

        Args:
            task_id: Task ID
            miner_hotkey: Miner hotkey
            score: Verification score
            completion_time: Task completion time
            metrics: Detailed metrics from verification
            status: Task status after scoring, default is 'scored'

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return self.db.update_task_with_score(
                task_id=task_id,
                miner_hotkey=miner_hotkey,
                score=score,
                completion_time=completion_time,
                metrics=metrics,
                status=status,
            )
        except Exception as e:
            bt.logging.error(f"Error updating task with score: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    def get_pending_tasks(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get pending tasks

        Args:
            limit: Maximum number of tasks to return

        Returns:
            List of pending tasks
        """
        try:
            # get_pending_tasks is a synchronous method
            return self.db.get_pending_tasks(limit)
        except Exception as e:
            bt.logging.error(f"Error getting pending tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return []

    def get_active_tasks(self) -> List[Dict[str, Any]]:
        """
        Get active tasks

        Returns:
            List of active tasks
        """
        try:
            return self.db.get_active_tasks()
        except Exception as e:
            bt.logging.error(f"Error getting active tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return []

    def get_completed_tasks(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get completed tasks

        Args:
            limit: Maximum number of tasks to return

        Returns:
            List of completed tasks
        """
        try:
            # get_completed_tasks_without_score is a synchronous method
            return self.db.get_completed_tasks_without_score(limit)
        except Exception as e:
            bt.logging.error(f"Error getting completed tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return []

    async def get_tasks_for_miner(self, miner_hotkey: str) -> List[Dict[str, Any]]:
        """
        Get tasks for miner

        Args:
            miner_hotkey: Miner hotkey

        Returns:
            List of tasks for miner
        """
        try:
            return self.db.get_tasks_for_miner(miner_hotkey)
        except Exception as e:
            bt.logging.error(f"Error getting tasks for miner: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return []

    def get_miners_task_creation_info(self) -> List[Dict[str, Any]]:
        """
        Get task creation info for miners

        Returns:
            List of miner task creation info
        """
        try:
            # Use get_miners_with_creation_time instead of get_miners_task_creation_info
            miners_info = self.db.get_miners_with_creation_time()

            # Format the result to match expected structure
            result = []
            for miner in miners_info:
                result.append(
                    {
                        "hotkey": miner["hotkey"],
                        "task_count": miner["total_tasks"],
                        "last_creation_time": miner["creation_time"],
                    }
                )

            return result
        except Exception as e:
            bt.logging.error(f"Error getting miners task creation info: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return []

    async def get_failed_tasks(
        self, limit: int = 50, status: str = None
    ) -> List[Dict[str, Any]]:
        """
        Get failed tasks

        Args:
            limit: Maximum number of tasks to return
            status: Filter by status

        Returns:
            List of failed tasks
        """
        try:
            return self.db.get_failed_tasks(limit, status)
        except Exception as e:
            bt.logging.error(f"Error getting failed tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return []

    async def get_expired_tasks(self) -> List[Dict[str, Any]]:
        """
        Get expired tasks

        Returns:
            List of expired tasks
        """
        try:
            return self.db.get_expired_tasks()
        except Exception as e:
            bt.logging.error(f"Error getting expired tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return []

    def get_retry_tasks(self) -> List[Dict[str, Any]]:
        """
        Get tasks for retry

        Returns:
            List of tasks for retry
        """
        try:
            # Check if there is a get_retry_tasks method
            if hasattr(self.db, "get_retry_tasks"):
                # get_retry_tasks is a synchronous method
                return self.db.get_retry_tasks()
            else:
                # If there is no specific method, try using an alternative method
                bt.logging.warning(
                    "get_retry_tasks not available, using alternative method"
                )
                # Try to get tasks with status 'retry'
                if hasattr(self.db, "get_tasks_by_status"):
                    return self.db.get_tasks_by_status("retry", 10)
                else:
                    bt.logging.error("No suitable method found to get retry tasks")
                    return []
        except Exception as e:
            bt.logging.error(f"Error getting retry tasks: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return []

    async def update_task_preview_url(self, task_id: str, preview_url: str) -> bool:
        """
        Update task preview URL

        Args:
            task_id: Task ID
            preview_url: Preview URL

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Check if db has this method
            if hasattr(self.db, "update_task_preview_url"):
                return self.db.update_task_preview_url(
                    task_id=task_id, preview_url=preview_url
                )
            else:
                # Fall back to a more generic update method if available
                bt.logging.warning(
                    f"update_task_preview_url not available in database, using alternative method"
                )
                if hasattr(self.db, "update_task"):
                    return self.db.update_task(task_id=task_id, preview_url=preview_url)
                else:
                    bt.logging.error("No suitable method found to update preview URL")
                    return False
        except Exception as e:
            bt.logging.error(f"Error updating task preview URL: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    async def update_task_complete_workflow(
        self,
        task_id: str,
        complete_workflow: Dict[str, Any],
        upload_url: str = "",
        preview_url: str = "",
        miner_hotkey: str = None,
        miner_info: Dict[str, Any] = None,
    ) -> bool:
        """
        Update task complete workflow

        Args:
            task_id: Task ID
            complete_workflow: Complete workflow configuration
            upload_url: Upload URL (optional)
            preview_url: Preview URL (optional)
            miner_hotkey: Miner hotkey (optional)
            miner_info: Miner information cache (optional)

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Check if db has this method
            if hasattr(self.db, "update_task_complete_workflow"):
                return self.db.update_task_complete_workflow(
                    task_id=task_id,
                    complete_workflow=complete_workflow,
                    upload_url=upload_url,
                    preview_url=preview_url,
                    miner_hotkey=miner_hotkey,
                    miner_info=miner_info,
                )
            else:
                # Fall back to a more generic update method if available
                bt.logging.warning(
                    f"update_task_complete_workflow not available in database, using alternative method"
                )
                if hasattr(self.db, "update_task"):
                    update_data = {"complete_workflow": complete_workflow}
                    if upload_url:
                        update_data["upload_url"] = upload_url
                    if preview_url:
                        update_data["preview_url"] = preview_url
                    if miner_hotkey:
                        update_data["miner_hotkey"] = miner_hotkey

                    return self.db.update_task(task_id=task_id, **update_data)
                else:
                    bt.logging.error(
                        "No suitable method found to update complete workflow"
                    )
                    return False
        except Exception as e:
            bt.logging.error(f"Error updating task complete workflow: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    async def update_task_result_urls(
        self, task_id: str, video_url: str, metadata_url: str = ""
    ) -> bool:
        """
        Update task result URLs

        Args:
            task_id: Task ID
            video_url: Video URL
            metadata_url: Metadata URL

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return self.db.update_task_result_urls(
                task_id=task_id, video_url=video_url, metadata_url=metadata_url
            )
        except Exception as e:
            bt.logging.error(f"Error updating task result URLs: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    async def update_task_last_sent_at(self, task_id: str, last_sent_at: float) -> bool:
        """
        Update task last sent time

        Args:
            task_id: Task ID
            last_sent_at: Last sent timestamp

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            last_sent_datetime = datetime.fromtimestamp(last_sent_at, tz=timezone.utc)

            # Call the database update method
            return self.db.update_task_last_sent_at(
                task_id=task_id, last_sent_at=last_sent_datetime
            )
        except Exception as e:
            bt.logging.error(f"Error updating task last sent time: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    async def update_task_with_upload_info(
        self,
        task_id: str,
        miner_hotkey: str,
        preview_url: str,
        status: str = "pending",
        miner_info: Dict[str, Any] = None,
        upload_url: str = "",
    ) -> bool:
        """
        Update task with upload information

        Args:
            task_id: Task ID
            miner_hotkey: Miner hotkey
            preview_url: Preview URL
            status: Task status, default is "pending"
            miner_info: Miner information cache (optional)
            upload_url: Upload URL for the task

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return self.db.update_task_with_upload_info(
                task_id=task_id,
                miner_hotkey=miner_hotkey,
                preview_url=preview_url,
                status=status,
                miner_info=miner_info,
                upload_url=upload_url,
            )
        except Exception as e:
            bt.logging.error(f"Error updating task with upload info: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    async def update_miner_uid(
        self, hotkey: str, uid: int, current_block: int = None
    ) -> bool:
        """
        Update miner UID information

        Args:
            hotkey: Miner hotkey
            uid: Miner UID
            current_block: Current block number

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return self.db.update_miner_uid(
                hotkey=hotkey, uid=uid, current_block=current_block
            )
        except Exception as e:
            bt.logging.error(f"Error updating miner UID: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    def update_task_retry(
        self, task_id: str, miner_hotkey: str, retry_count: int
    ) -> bool:
        """Update task retry count

        Args:
            task_id: Task ID
            miner_hotkey: Miner hotkey
            retry_count: Current retry count

        Returns:
            bool: Whether the update was successful
        """
        try:
            now = datetime.now(timezone.utc)

            # Get database connection
            conn = self.db.get_connection()
            cursor = conn.cursor()

            try:
                # Get current retry count
                cursor.execute(
                    """
                    SELECT retry_count FROM tasks 
                    WHERE task_id = %s
                    """,
                    (task_id,),
                )

                result = cursor.fetchone()
                current_retry_count = result[0] if result else 0

                # First failure set to 1, subsequent retries add 1
                new_retry_count = (
                    1 if current_retry_count == 0 else current_retry_count + 1
                )

                # Update retry count but keep status as 'assigned'
                cursor.execute(
                    """
                    UPDATE tasks 
                    SET retry_count = %s,
                        updated_at = %s
                    WHERE task_id = %s
                    RETURNING retry_count
                    """,
                    (new_retry_count, now, task_id),
                )

                result = cursor.fetchone()
                updated_retry_count = result[0] if result else new_retry_count

                # Add task history record
                cursor.execute(
                    """
                    INSERT INTO task_history (task_id, status, timestamp, details)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        task_id,
                        "retry",
                        now,
                        f"Retry {updated_retry_count} with miner {miner_hotkey}: maintaining assigned status",
                    ),
                )

                # Update miner statistics if needed
                if miner_hotkey:
                    cursor.execute(
                        """
                        UPDATE miners 
                        SET last_active = %s
                        WHERE hotkey = %s
                        """,
                        (now, miner_hotkey),
                    )

                # Get task failure threshold from validator config
                task_failure_threshold = 2

                # If retry count exceeds limit, mark as failed
                if updated_retry_count >= task_failure_threshold:
                    bt.logging.warning(
                        f"Task {task_id} reached failure threshold ({updated_retry_count} >= {task_failure_threshold}), marking as failed"
                    )

                    cursor.execute(
                        """
                        UPDATE tasks 
                        SET status = 'failed',
                            updated_at = %s,
                            completed_at = %s
                        WHERE task_id = %s
                        """,
                        (now, now, task_id),
                    )

                    # Add task history record for failure
                    cursor.execute(
                        """
                        INSERT INTO task_history (task_id, status, timestamp, details)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (
                            task_id,
                            "failed",
                            now,
                            f"Failed after {updated_retry_count} retries with miner {miner_hotkey}",
                        ),
                    )

                conn.commit()
                return True
            except Exception as e:
                if conn:
                    conn.rollback()
                bt.logging.error(f"Error updating task retry count: {str(e)}")
                bt.logging.error(traceback.format_exc())
                return False
            finally:
                if conn:
                    self.db.put_connection(conn)
        except Exception as e:
            bt.logging.error(f"Error in update_task_retry: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    async def update_task_to_timeout(
        self,
        task_id: str,
        miner_hotkey: str,
        timeout_seconds: int,
        miner_info: Dict[str, Any] = None,
    ) -> bool:
        """
        Update task status to timeout

        Args:
            task_id: Task ID
            miner_hotkey: Miner hotkey
            timeout_seconds: Timeout in seconds
            miner_info: Miner information cache

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Check if update_task_to_timeout method exists
            return self.db.update_task_to_timeout(
                task_id, miner_hotkey, timeout_seconds, miner_info=miner_info
            )
        except Exception as e:
            bt.logging.error(f"Error updating task to timeout: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    async def update_task_to_completed(
        self,
        task_id: str,
        miner_hotkey: str,
        file_info: Dict[str, Any] = None,
        completion_time: float = None,
        completed_at: datetime = None,
    ) -> bool:
        """
        Update task status to completed

        Args:
            task_id: Task ID
            miner_hotkey: Miner hotkey
            file_info: Optional file information

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            return self.db.update_task_to_completed(
                task_id=task_id,
                miner_hotkey=miner_hotkey,
                file_info=file_info,
                completion_time=completion_time,
                completed_at=completed_at,
            )
        except Exception as e:
            bt.logging.error(f"Error updating task to completed: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False
