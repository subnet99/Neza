"""
Validator database module
Provides database operations required by validator nodes
"""

from datetime import datetime, timedelta, timezone
import json
import bittensor as bt
import psycopg2.extras
import time

from neza.database_base import DatabaseConnection


class ValidatorDatabase(DatabaseConnection):
    """
    Validator database operations class
    Encapsulates database operations required by validators
    """

    def __init__(self):
        """Initialize validator database"""
        super().__init__()

        # Ensure database is initialized
        self.init_database()

        # Cache
        self.task_cache = {}

        bt.logging.info("Validator database initialization completed")

    def init_database(self):
        """Initialize database tables"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Create tasks table
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS tasks (
                id SERIAL PRIMARY KEY,
                task_id TEXT UNIQUE,
                workflow_params JSONB,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                miner_hotkey TEXT,
                s3_video_url TEXT,
                s3_upload_url TEXT,
                s3_metadata_url TEXT,
                score REAL,
                retry_count INTEGER DEFAULT 0,
                timeout_seconds INTEGER DEFAULT 600,
                secret_key TEXT,
                last_sent_at TIMESTAMP WITH TIME ZONE,
                completed_at TIMESTAMP WITH TIME ZONE,
                completion_time DOUBLE PRECISION,
                complete_workflow JSONB,
                file_size INTEGER DEFAULT 0,
                last_modified TIMESTAMP WITH TIME ZONE,
                metrics JSONB
            )
            """
            )

            # Create task history table
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS task_history (
                id SERIAL PRIMARY KEY,
                task_id TEXT,
                status TEXT,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                details TEXT
            )
            """
            )

            # Create miners table
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS miners (
                hotkey TEXT PRIMARY KEY,
                uid INTEGER,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                creation_block INTEGER DEFAULT 0,
                completed_tasks INTEGER DEFAULT 0,
                failed_tasks INTEGER DEFAULT 0,
                avg_completion_time REAL DEFAULT 0,
                avg_score REAL DEFAULT 0,
                last_active TIMESTAMP WITH TIME ZONE,
                is_active BOOLEAN DEFAULT TRUE,
                last_task_time REAL DEFAULT 0,
                active_tasks INTEGER DEFAULT 0,
                tasks_last_hour INTEGER DEFAULT 0
            )
            """
            )

            # creation_block column is now part of the miners table definition
            # No need to check if it exists

            # Create indexes
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks (status)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_miner ON tasks (miner_hotkey)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_task_history_task_id ON task_history (task_id)"
            )

            conn.commit()
            bt.logging.info("Database tables initialization completed")
            return True
        except Exception as e:
            bt.logging.error(f"Error initializing database tables: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def get_task_count(self):
        """Get current total task count"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM tasks")
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            bt.logging.error(f"Error getting task count: {str(e)}")
            return 0
        finally:
            if conn:
                conn.close()

    def get_pending_tasks(self, limit=5):
        """Get pending tasks"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(
                    """
                SELECT task_id, workflow_params, secret_key, timeout_seconds 
                FROM tasks 
                WHERE status = 'pending' 
                ORDER BY created_at ASC 
                LIMIT %s
                """,
                    (limit,),
                )
                tasks = cursor.fetchall()

                # Convert JSON strings to dictionaries
                for task in tasks:
                    if isinstance(task["workflow_params"], str):
                        task["workflow_params"] = json.loads(task["workflow_params"])

                return tasks
        except Exception as e:
            bt.logging.error(f"Error getting pending tasks: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def get_active_tasks(self):
        """Get tasks in progress"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(
                    """
                SELECT * FROM tasks 
                WHERE status = 'processing' 
                ORDER BY last_sent_at ASC
                """
                )
                tasks = cursor.fetchall()
                return tasks
        except Exception as e:
            bt.logging.error(f"Error getting active tasks: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def get_completed_tasks_without_score(self, limit=10):
        """Get completed tasks without scores"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)

                cursor.execute(
                    """
                SELECT * FROM tasks 
                WHERE status = 'completed' AND score IS NULL AND completed_at >= %s
                ORDER BY completed_at DESC 
                LIMIT %s
                """,
                    (two_hours_ago, limit),
                )
                tasks = cursor.fetchall()
                return tasks
        except Exception as e:
            bt.logging.error(f"Error getting completed tasks without score: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def get_all_completed_tasks_without_score(self):
        """Get all completed tasks without scores, no limit"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                two_hours_ago = datetime.now(timezone.utc) - timedelta(hours=2)

                cursor.execute(
                    """
                SELECT * FROM tasks 
                WHERE status = 'completed' AND score IS NULL AND completed_at >= %s
                ORDER BY completed_at ASC
                """,
                    (two_hours_ago,),
                )
                tasks = cursor.fetchall()
                return tasks
        except Exception as e:
            bt.logging.error(
                f"Error getting all completed tasks without score: {str(e)}"
            )
            return []
        finally:
            if conn:
                conn.close()

    def get_miner_stats(self):
        """Get statistics for all miners"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(
                    """
                SELECT 
                    hotkey, 
                    uid,
                    completed_tasks, 
                    failed_tasks, 
                    avg_completion_time, 
                    avg_score, 
                    last_active, 
                    is_active
                FROM miners
                ORDER BY avg_score DESC
                """
                )
                miners = cursor.fetchall()

                # Convert to dictionary list
                result = []
                for miner in miners:
                    result.append(dict(miner))

                return result
        except Exception as e:
            bt.logging.error(f"Error getting miner stats: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def get_miner_tasks_last_hour(self, hotkey):
        """Get number of tasks completed by a miner in the last hour"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
            SELECT tasks_last_hour FROM miners WHERE hotkey = %s
            """,
                (hotkey,),
            )
            result = cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            bt.logging.error(f"Error getting miner tasks last hour: {str(e)}")
            return 0
        finally:
            if conn:
                conn.close()

    def get_task_by_id(self, task_id):
        """Get task by ID"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(
                    """
                SELECT * FROM tasks WHERE task_id = %s
                """,
                    (task_id,),
                )
                task = cursor.fetchone()

                # Convert row to dictionary
                if task:
                    task_dict = dict(task)
                    # Convert workflow_params to dictionary if it's a string
                    if isinstance(task_dict.get("workflow_params"), str):
                        task_dict["workflow_params"] = json.loads(
                            task_dict["workflow_params"]
                        )
                    return task_dict
                return None
        except Exception as e:
            bt.logging.error(f"Error getting task by ID: {str(e)}")
            return None
        finally:
            if conn:
                conn.close()

    def update_task_to_processing(
        self, task_id, miner_hotkey, s3_video_url, s3_metadata_url
    ):
        """Update task status to processing"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            now = datetime.now(timezone.utc)
            # Update task status
            cursor.execute(
                """
            UPDATE tasks 
            SET status = 'processing', 
                updated_at = %s,
                miner_hotkey = %s,
                s3_video_url = %s,
                s3_metadata_url = %s,
                last_sent_at = %s
            WHERE task_id = %s
            """,
                (
                    now,
                    miner_hotkey,
                    s3_video_url,
                    s3_metadata_url,
                    now,
                    task_id,
                ),
            )

            # Add task history record
            cursor.execute(
                """
            INSERT INTO task_history (task_id, status, timestamp, details)
            VALUES (%s, %s, %s, %s)
            """,
                (task_id, "processing", now, f"Task sent to miner {miner_hotkey}"),
            )

            # Update miner active status and task count
            current_utc_time = datetime.now(timezone.utc)
            cursor.execute(
                """
            UPDATE miners
            SET last_task_time = %s,
                active_tasks = active_tasks + 1,
                tasks_last_hour = tasks_last_hour + 1,
                last_active = %s
            WHERE hotkey = %s
            """,
                (current_utc_time, current_utc_time, miner_hotkey),
            )

            # If miner doesn't exist, create a new record
            if cursor.rowcount == 0:
                cursor.execute(
                    """
                INSERT INTO miners (hotkey, last_task_time, active_tasks, is_active, created_at, last_active)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                    (miner_hotkey, current_utc_time, 1, True, now, now),
                )

            conn.commit()
            return True
        except Exception as e:
            bt.logging.error(f"Error updating task to processing: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def update_task_retry_count(
        self, task_id, miner_hotkey=None, error_message="Unknown error"
    ):
        """Update task retry count"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get current time
            now = datetime.now(timezone.utc)

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

            new_retry_count = 1 if current_retry_count == 0 else current_retry_count + 1

            # Update retry count
            cursor.execute(
                """
            UPDATE tasks 
            SET retry_count = %s,
                updated_at = %s,
                status = 'pending'
            WHERE task_id = %s
            RETURNING retry_count
            """,
                (new_retry_count, now, task_id),
            )

            result = cursor.fetchone()
            retry_count = result[0] if result else new_retry_count

            miner_info = f" with miner {miner_hotkey}" if miner_hotkey else ""
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
                    f"Retry {retry_count}{miner_info}: {error_message}",
                ),
            )

            # If miner information is provided, update miner statistics
            if miner_hotkey:
                cursor.execute(
                    """
                UPDATE miners 
                SET active_tasks = GREATEST(active_tasks - 1, 0),
                    last_active = %s
                WHERE hotkey = %s
                """,
                    (now, miner_hotkey),
                )

                # If miner doesn't exist, create a new record
                if cursor.rowcount == 0 and miner_hotkey != "unknown":
                    cursor.execute(
                        """
                    INSERT INTO miners (hotkey, failed_tasks, last_active, is_active)
                    VALUES (%s, %s, %s, %s)
                    """,
                        (miner_hotkey, 1, now, True),
                    )

            # If retry count is too high, mark as failed
            if retry_count >= 3:
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

                # Add task history record
                cursor.execute(
                    """
                INSERT INTO task_history (task_id, status, timestamp, details)
                VALUES (%s, %s, %s, %s)
                """,
                    (
                        task_id,
                        "failed",
                        now,
                        f"Failed after {retry_count} retries{miner_info}",
                    ),
                )

            conn.commit()
            return retry_count
        except Exception as e:
            bt.logging.error(f"Error updating task retry count: {str(e)}")
            if conn:
                conn.rollback()
            return 0
        finally:
            if conn:
                conn.close()

    def update_task_to_completed(
        self,
        task_id,
        miner_hotkey,
        file_info=None,
        completion_time=None,
        completed_at=None,
    ):
        """Update task status to completed"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get file size and last modified time from file_info
            file_size = None
            last_modified = None
            if file_info:
                file_size = file_info.get("file_size")
                last_modified = file_info.get("last_modified")
                bt.logging.info(
                    f"Task {task_id} File Size: {file_size}, Last Modified: {last_modified}"
                )

            # Update task status
            cursor.execute(
                """
            UPDATE tasks 
            SET status = 'completed',
                completed_at = %s,
                updated_at = %s,
                completion_time = %s,
                file_size = %s,
                last_modified = %s
            WHERE task_id = %s
            """,
                (
                    completed_at,
                    completed_at,
                    completion_time,
                    file_size,
                    last_modified,
                    task_id,
                ),
            )

            # Add task history record
            completion_info = (
                f", completion time: {completion_time:.2f} seconds"
                if completion_time
                else ""
            )
            file_info_str = ""
            if file_size:
                file_info_str += f"File size: {file_size} bytes"
            if last_modified:
                file_info_str += f", Modification time: {last_modified}"

            cursor.execute(
                """
            INSERT INTO task_history (task_id, status, timestamp, details)
            VALUES (%s, %s, %s, %s)
            """,
                (
                    task_id,
                    "completed",
                    completed_at,
                    f"Completed by miner {miner_hotkey}{completion_info}{file_info_str}",
                ),
            )

            # Update miner statistics - Note: only decrease active_tasks here, completed_tasks will be incremented when scoring
            cursor.execute(
                """
            UPDATE miners 
            SET active_tasks = GREATEST(active_tasks - 1, 0),
                last_active = %s
            WHERE hotkey = %s
            """,
                (completed_at, miner_hotkey),
            )

            # If miner doesn't exist, create a new record
            if cursor.rowcount == 0 and miner_hotkey and miner_hotkey != "unknown":
                cursor.execute(
                    """
                INSERT INTO miners (hotkey, last_active, is_active)
                VALUES (%s, %s, %s)
                """,
                    (miner_hotkey, completed_at, True),
                )

            conn.commit()
            return True
        except Exception as e:
            bt.logging.error(f"Error updating task to completed: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def update_task_to_failed(self, task_id, miner_hotkey, error_message):
        """Update task status to failed"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get current time
            failed_at = datetime.now(timezone.utc)

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

            # Set to 1 for first failure, increment for subsequent retries
            new_retry_count = 1

            # Update retry count
            cursor.execute(
                """
            UPDATE tasks 
            SET status = 'failed',
                updated_at = %s,
                miner_hotkey = %s,
                completed_at = %s,
                retry_count = %s
            WHERE task_id = %s
            """,
                (
                    failed_at,
                    miner_hotkey,
                    failed_at,
                    new_retry_count,
                    task_id,
                ),
            )

            # Add task history record
            cursor.execute(
                """
            INSERT INTO task_history (task_id, status, timestamp, details)
            VALUES (%s, %s, %s, %s)
            """,
                (
                    task_id,
                    "failed",
                    failed_at,
                    f"Failed with error: {error_message}, Miner: {miner_hotkey}, Retry count: {new_retry_count}",
                ),
            )

            # Update miner statistics
            cursor.execute(
                """
            UPDATE miners 
            SET failed_tasks = failed_tasks + 1,
                active_tasks = GREATEST(active_tasks - 1, 0),
                last_active = %s
            WHERE hotkey = %s
            """,
                (failed_at, miner_hotkey),
            )

            # If miner doesn't exist, create a new record
            if cursor.rowcount == 0 and miner_hotkey and miner_hotkey != "unknown":
                cursor.execute(
                    """
                INSERT INTO miners (hotkey, failed_tasks, last_active, is_active)
                VALUES (%s, %s, %s, %s)
                """,
                    (miner_hotkey, 1, failed_at, True),
                )

            conn.commit()
            bt.logging.info(f"Task {task_id} marked as failed: {error_message}")
            return True
        except Exception as e:
            bt.logging.error(f"Error updating task to failed: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def update_task_to_timeout(
        self, task_id, miner_hotkey, timeout_seconds, miner_info=None
    ):
        """Update task status to timeout"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get current time
            timeout_at = datetime.now(timezone.utc)

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

            new_retry_count = 1 if current_retry_count == 0 else current_retry_count + 1

            # Set to 1 for first failure, increment for subsequent retries
            cursor.execute(
                """
            UPDATE tasks 
            SET status = 'timeout',
                updated_at = %s,
                miner_hotkey = %s,
                completed_at = %s,
                retry_count = %s
            WHERE task_id = %s
            """,
                (
                    timeout_at,
                    miner_hotkey,
                    timeout_at,
                    new_retry_count,
                    task_id,
                ),  # Use timeout time as completion time
            )

            # Add task history record
            cursor.execute(
                """
            INSERT INTO task_history (task_id, status, timestamp, details)
            VALUES (%s, %s, %s, %s)
            """,
                (
                    task_id,
                    "timeout",
                    timeout_at,
                    f"Task timed out after {timeout_seconds} seconds, Miner: {miner_hotkey}, Retry count: {new_retry_count}",
                ),
            )

            # Try to get UID from miner info cache
            uid = None
            if miner_info and "uid" in miner_info:
                uid = miner_info.get("uid")
                bt.logging.debug(f"Found UID {uid} in miner_info for {miner_hotkey}")

            # Update miner statistics
            cursor.execute(
                """
            UPDATE miners 
            SET failed_tasks = failed_tasks + 1,
                active_tasks = GREATEST(active_tasks - 1, 0),
                last_active = %s
            WHERE hotkey = %s
            """,
                (timeout_at, miner_hotkey),
            )

            # If miner doesn't exist, create a new record
            if cursor.rowcount == 0 and miner_hotkey and miner_hotkey != "unknown":
                # If UID information is available, include it in the insert
                if uid is not None:
                    cursor.execute(
                        """
                    INSERT INTO miners (hotkey, uid, failed_tasks, last_active, is_active, creation_block)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                        (miner_hotkey, uid, 1, timeout_at, True, 0),
                    )
                else:
                    cursor.execute(
                        """
                    INSERT INTO miners (hotkey, failed_tasks, last_active, is_active)
                    VALUES (%s, %s, %s, %s)
                    """,
                        (miner_hotkey, 1, timeout_at, True),
                    )

            conn.commit()
            return True
        except Exception as e:
            bt.logging.error(f"Error updating task to timeout: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def update_task_with_score(
        self,
        task_id,
        miner_hotkey,
        score,
        completion_time,
        metrics=None,
        status="scored",
    ):
        """Update task score"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get task's actual completion time (if available)
            cursor.execute(
                """
            SELECT completion_time FROM tasks WHERE task_id = %s
            """,
                (task_id,),
            )
            result = cursor.fetchone()
            if result and result[0] is not None:
                # Use recorded completion time
                actual_completion_time = result[0]
                bt.logging.info(
                    f"Using recorded completion time: {actual_completion_time:.2f} seconds"
                )
            else:
                # Use provided parameter
                actual_completion_time = completion_time
                bt.logging.info(
                    f"No completion time recorded in database, using provided parameter: {actual_completion_time} seconds"
                )

            # Prepare metrics JSON if provided
            metrics_json = None
            if metrics:
                if isinstance(metrics, dict):
                    metrics_json = json.dumps(metrics)
                elif isinstance(metrics, str):
                    # Assume it's already a JSON string
                    metrics_json = metrics

            # Update task score and status
            if metrics_json:
                cursor.execute(
                    """
                UPDATE tasks 
                SET score = %s, 
                    updated_at = %s,
                    status = %s,
                    metrics = %s
                WHERE task_id = %s
                """,
                    (score, datetime.now(timezone.utc), status, metrics_json, task_id),
                )
            else:
                cursor.execute(
                    """
                UPDATE tasks 
                SET score = %s, 
                    updated_at = %s,
                    status = %s
                WHERE task_id = %s
                """,
                    (score, datetime.now(timezone.utc), status, task_id),
                )

            # Add task history record
            metrics_summary = ""
            if metrics:
                # Add a brief summary of metrics to history
                if isinstance(metrics, dict):
                    if "video_cosine_similarity" in metrics:
                        metrics_summary = f", video_sim: {metrics.get('video_cosine_similarity', 0):.4f}"
                    if "audio_cosine_similarity" in metrics:
                        metrics_summary += f", audio_sim: {metrics.get('audio_cosine_similarity', 0):.4f}"

            cursor.execute(
                """
            INSERT INTO task_history (task_id, status, timestamp, details)
            VALUES (%s, %s, %s, %s)
            """,
                (
                    task_id,
                    status,
                    datetime.now(timezone.utc),
                    f"Task scored: {score}, completion time: {actual_completion_time:.2f}s {metrics_summary}",
                ),
            )

            # Update miner statistics
            cursor.execute(
                """
            UPDATE miners 
            SET completed_tasks = completed_tasks + 1,
                active_tasks = GREATEST(active_tasks - 1, 0),
                avg_score = (avg_score * completed_tasks + %s) / (completed_tasks + 1),
                avg_completion_time = (avg_completion_time * completed_tasks + %s) / (completed_tasks + 1)
            WHERE hotkey = %s
            """,
                (score, actual_completion_time, miner_hotkey),
            )

            conn.commit()
            return True
        except Exception as e:
            bt.logging.error(f"Error updating task score: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def update_task_verification_failed(self, task_id, miner_hotkey):
        """Update task verification failed"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get current time
            failed_at = datetime.now(timezone.utc)

            # Update task status
            cursor.execute(
                """
            UPDATE tasks 
            SET status = 'verification_failed',
                updated_at = %s,
                completed_at = %s
            WHERE task_id = %s
            """,
                (failed_at, failed_at, task_id),
            )

            # Add task history record
            cursor.execute(
                """
            INSERT INTO task_history (task_id, status, timestamp, details)
            VALUES (%s, %s, %s, %s)
            """,
                (
                    task_id,
                    "verification_failed",
                    failed_at,
                    f"Verification failed for miner {miner_hotkey} at {failed_at.strftime('%Y-%m-%d %H:%M:%S')}",
                ),
            )

            # Update miner statistics
            cursor.execute(
                """
            UPDATE miners 
            SET failed_tasks = failed_tasks + 1,
                active_tasks = GREATEST(active_tasks - 1, 0),
                last_active = %s
            WHERE hotkey = %s
            """,
                (failed_at, miner_hotkey),
            )

            # If miner doesn't exist, create a new record
            if cursor.rowcount == 0 and miner_hotkey and miner_hotkey != "unknown":
                cursor.execute(
                    """
                INSERT INTO miners (hotkey, failed_tasks, last_active, is_active)
                VALUES (%s, %s, %s, %s)
                """,
                    (miner_hotkey, 1, failed_at, True),
                )

            conn.commit()
            return True
        except Exception as e:
            bt.logging.error(f"Error updating task verification failed: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def add_task(self, task_id, complete_workflow, secret_key, timeout_seconds=600):
        """Add new task"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Insert new task
            cursor.execute(
                """
            INSERT INTO tasks (
                task_id, complete_workflow, status, created_at, updated_at, 
                timeout_seconds, secret_key
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    task_id,
                    json.dumps(complete_workflow),
                    "pending",
                    datetime.now(timezone.utc),
                    datetime.now(timezone.utc),
                    timeout_seconds,
                    secret_key,
                ),
            )

            # Add task history record
            cursor.execute(
                """
            INSERT INTO task_history (task_id, status, timestamp, details)
            VALUES (%s, %s, %s, %s)
            """,
                (task_id, "pending", datetime.now(timezone.utc), "Task created"),
            )

            conn.commit()
            return True
        except Exception as e:
            bt.logging.error(f"Error adding task: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def clean_old_task_history(self, days=7):
        """Clean up old task history records"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Clean up old task history records
            seven_days_ago = datetime.now(timezone.utc) - timedelta(days=days)
            cursor.execute(
                """
            DELETE FROM task_history 
            WHERE timestamp < %s
            """,
                (seven_days_ago,),
            )
            conn.commit()
            return True
        except Exception as e:
            bt.logging.error(f"Error cleaning old task history: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def update_miners_active_status(self, hours=1):
        """Update miners active status"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Update miners active status
            one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=hours)
            cursor.execute(
                """
            UPDATE miners 
            SET is_active = FALSE
            WHERE last_active < %s
            """,
                (one_hour_ago,),
            )

            conn.commit()
            return True
        except Exception as e:
            bt.logging.error(f"Error updating miners active status: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def reset_miners_hourly_tasks(self):
        """Reset miners hourly task count"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute("UPDATE miners SET tasks_last_hour = 0")

            conn.commit()
            return True
        except Exception as e:
            bt.logging.error(f"Error resetting miners hourly tasks: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def get_tasks_by_miner(self, miner_hotkey, limit=50, status=None):
        """Query tasks processed by a specific miner"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                if status:
                    # Query tasks with specific status
                    cursor.execute(
                        """
                    SELECT * FROM tasks 
                    WHERE miner_hotkey = %s AND status = %s
                    ORDER BY created_at DESC 
                    LIMIT %s
                    """,
                        (miner_hotkey, status, limit),
                    )
                else:
                    # Query all status tasks
                    cursor.execute(
                        """
                    SELECT * FROM tasks 
                    WHERE miner_hotkey = %s
                    ORDER BY created_at DESC 
                    LIMIT %s
                    """,
                        (miner_hotkey, limit),
                    )

                tasks = cursor.fetchall()

                # Convert to dictionary list
                result = []
                for task in tasks:
                    task_dict = dict(task)
                    # If workflow_params is a string, convert to dictionary
                    if isinstance(task_dict["workflow_params"], str):
                        task_dict["workflow_params"] = json.loads(
                            task_dict["workflow_params"]
                        )
                    result.append(task_dict)

                return result

        except Exception as e:
            bt.logging.error(f"Error getting tasks by miner: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def get_tasks_by_status(self, status, limit=50):
        """Query tasks with specific status"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(
                    """
                SELECT * FROM tasks 
                WHERE status = %s
                ORDER BY updated_at DESC 
                LIMIT %s
                """,
                    (status, limit),
                )

                tasks = cursor.fetchall()

                # Convert to dictionary list
                result = []
                for task in tasks:
                    task_dict = dict(task)
                    # If workflow_params is a string, convert to dictionary
                    if isinstance(task_dict["workflow_params"], str):
                        task_dict["workflow_params"] = json.loads(
                            task_dict["workflow_params"]
                        )
                    result.append(task_dict)

                return result

        except Exception as e:
            bt.logging.error(f"Error getting tasks by status: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def update_task_attempt(self, task_id, miner_hotkey):
        """Update task attempt miner information"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get current time
            attempt_time = datetime.now(timezone.utc)

            # Update task record, record attempt to send to which miner
            cursor.execute(
                """
            UPDATE tasks 
            SET miner_hotkey = %s,
                updated_at = %s,
                last_sent_at = %s
            WHERE task_id = %s
            """,
                (miner_hotkey, attempt_time, attempt_time, task_id),
            )

            # Add task history record
            cursor.execute(
                """
            INSERT INTO task_history (task_id, status, timestamp, details)
            VALUES (%s, %s, %s, %s)
            """,
                (
                    task_id,
                    "attempt",
                    attempt_time,
                    f"Attempt to send task to miner {miner_hotkey}",
                ),
            )

            conn.commit()
            return True
        except Exception as e:
            bt.logging.error(f"Error updating task attempt: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def get_task_history(self, task_id):
        """Get task history records"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(
                    """
                SELECT * FROM task_history 
                WHERE task_id = %s
                ORDER BY timestamp DESC
                """,
                    (task_id,),
                )

                history = cursor.fetchall()
                return history

        except Exception as e:
            bt.logging.error(f"Error getting task history: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def update_task_complete_workflow(
        self,
        task_id,
        complete_workflow,
        upload_url="",
        preview_url="",
        miner_hotkey=None,
        miner_info=None,
    ):
        """Update task complete workflow configuration"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Update task complete workflow configuration
            if miner_hotkey:
                cursor.execute(
                    """
                UPDATE tasks SET 
                    complete_workflow = %s,
                    s3_upload_url = %s,
                    s3_video_url = %s,
                    miner_hotkey = %s,
                    updated_at = %s
                WHERE task_id = %s
                """,
                    (
                        json.dumps(complete_workflow),
                        upload_url,
                        preview_url,
                        miner_hotkey,
                        datetime.now(timezone.utc),
                        task_id,
                    ),
                )
                bt.logging.info(
                    f"Task {task_id} miner public key updated to {miner_hotkey[:10] if miner_hotkey else 'None'}..."
                )
            else:
                cursor.execute(
                    """
                UPDATE tasks SET 
                    complete_workflow = %s,
                    s3_upload_url = %s,
                    s3_video_url = %s,
                    updated_at = %s
                WHERE task_id = %s
                """,
                    (
                        json.dumps(complete_workflow),
                        upload_url,
                        preview_url,
                        datetime.now(timezone.utc),
                        task_id,
                    ),
                )

            # If miner public key is provided, update miner record
            if miner_hotkey:
                # Try to get UID from miner info cache
                uid = None
                if miner_info and miner_hotkey in miner_info:
                    miner_data = miner_info[miner_hotkey]
                    if isinstance(miner_data, dict) and "uid" in miner_data:
                        uid = miner_data["uid"]
                        bt.logging.info(
                            f"Found miner {miner_hotkey[:10] if miner_hotkey else 'None'}... UID: {uid}"
                        )

                # If UID is not found, try to get from metagraph
                if uid is None:
                    try:
                        # Get current subtensor instance
                        subtensor = bt.subtensor()

                        # Get current metagraph
                        metagraph = subtensor.metagraph(
                            netuid=subtensor.network.neuron.netuid
                        )

                        # Find miner hotkey corresponding UID
                        for i, hotkey in enumerate(metagraph.hotkeys):
                            if hotkey == miner_hotkey:
                                uid = i
                                break

                        bt.logging.info(
                            f"Found miner {miner_hotkey[:10] if miner_hotkey else 'None'}... UID: {uid}"
                        )
                    except Exception as e:
                        bt.logging.warning(f"Error getting miner UID: {str(e)}")

                # Insert or update miner record
                if uid is not None:
                    # If UID is found, include it in the insert
                    cursor.execute(
                        """
                    INSERT INTO miners (hotkey, uid, last_active, tasks_last_hour, creation_block) 
                    VALUES (%s, %s, %s, 1, %s)
                    ON CONFLICT (hotkey) 
                    DO UPDATE SET 
                        last_active = %s,
                        tasks_last_hour = miners.tasks_last_hour + 1
                    """,
                        (
                            miner_hotkey,
                            uid,
                            datetime.now(timezone.utc),
                            0,
                            datetime.now(timezone.utc),
                        ),
                    )
                else:
                    # If UID is not found, do not include UID field
                    cursor.execute(
                        """
                    INSERT INTO miners (hotkey, last_active, tasks_last_hour) 
                    VALUES (%s, %s, 1)
                    ON CONFLICT (hotkey) 
                    DO UPDATE SET 
                        last_active = %s,
                        tasks_last_hour = miners.tasks_last_hour + 1
                    """,
                        (
                            miner_hotkey,
                            datetime.now(timezone.utc),
                            datetime.now(timezone.utc),
                        ),
                    )

            conn.commit()
            bt.logging.info(f"Task {task_id} complete workflow configuration saved")
            return True
        except Exception as e:
            bt.logging.error(
                f"Error updating task complete workflow configuration: {str(e)}"
            )
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def get_verified_task_count(self):
        """Get verified task total"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
            SELECT COUNT(*) FROM tasks
            WHERE status = 'completed' AND score IS NOT NULL
            """
            )
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            bt.logging.error(f"Error getting verified task total: {str(e)}")
            return 0
        finally:
            if conn:
                conn.close()

    def get_failed_verification_count(self):
        """Get failed verification task total"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
            SELECT COUNT(*) FROM tasks
            WHERE status = 'verification_failed'
            """
            )
            count = cursor.fetchone()[0]
            return count
        except Exception as e:
            bt.logging.error(f"Error getting failed verification task total: {str(e)}")
            return 0
        finally:
            if conn:
                conn.close()

    def get_retry_tasks(self):
        """Get retry tasks (tasks in pending state but assigned to miner)"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(
                    """
                SELECT task_id, miner_hotkey, retry_count 
                FROM tasks 
                WHERE status = 'pending' AND miner_hotkey IS NOT NULL AND retry_count > 0
                """
                )

                tasks = cursor.fetchall()
                return tasks
        except Exception as e:
            bt.logging.error(f"Error getting retry tasks: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def get_tasks_for_retry(self):
        """Get tasks for retry (tasks in pending state and assigned to miner)"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(
                    """
                SELECT * FROM tasks 
                WHERE status = 'pending' AND miner_hotkey IS NOT NULL
                ORDER BY retry_count ASC, created_at ASC
                """
                )

                tasks = cursor.fetchall()
                return tasks
        except Exception as e:
            bt.logging.error(f"Error getting tasks for retry: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def get_new_pending_tasks(self, limit=10):
        """Get new pending tasks (tasks not assigned to miner)"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(
                    """
                SELECT * FROM tasks 
                WHERE status = 'pending' AND miner_hotkey IS NULL
                ORDER BY created_at ASC
                LIMIT %s
                """,
                    (limit,),
                )
                tasks = cursor.fetchall()
                return [dict(task) for task in tasks]
        except Exception as e:
            bt.logging.error(f"Error getting new pending tasks: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()

    def update_task_with_upload_info(
        self,
        task_id,
        miner_hotkey,
        preview_url,
        status="pending",
        miner_info=None,
        upload_url="",
    ):
        """Update task preview URL and status"""

        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Update task preview URL, upload URL, status and miner_hotkey
            cursor.execute(
                """
            UPDATE tasks SET 
                s3_video_url = %s,
                s3_upload_url = %s,
                status = %s,
                miner_hotkey = %s,
                updated_at = %s
            WHERE task_id = %s
            """,
                (
                    preview_url,
                    upload_url,
                    status,
                    miner_hotkey,
                    datetime.now(timezone.utc),
                    task_id,
                ),
            )

            # If miner public key is provided, update miner record
            if miner_hotkey:
                # Try to get UID from miner info cache
                uid = None
                if miner_info and miner_hotkey in miner_info:
                    miner_data = miner_info[miner_hotkey]
                    if isinstance(miner_data, dict) and "uid" in miner_data:
                        uid = miner_data["uid"]
                        bt.logging.info(
                            f"Found miner {miner_hotkey[:10] if miner_hotkey else 'None'}... UID: {uid}"
                        )

                # If UID is not found, try to get from metagraph
                if uid is None:
                    try:
                        # Get current subtensor instance
                        subtensor = bt.subtensor()

                        # Get current metagraph
                        metagraph = subtensor.metagraph(
                            netuid=subtensor.network.neuron.netuid
                        )

                        # Find miner hotkey corresponding UID
                        for i, hotkey in enumerate(metagraph.hotkeys):
                            if hotkey == miner_hotkey:
                                uid = i
                                break

                        bt.logging.info(
                            f"Found miner {miner_hotkey[:10] if miner_hotkey else 'None'}... UID: {uid}"
                        )
                    except Exception as e:
                        bt.logging.warning(f"Error getting miner UID: {str(e)}")

                # Insert or update miner record
                if uid is not None:
                    # If UID is found, include it in the insert
                    cursor.execute(
                        """
                    INSERT INTO miners (hotkey, uid, last_active, tasks_last_hour, creation_block) 
                    VALUES (%s, %s, %s, 1, %s)
                    ON CONFLICT (hotkey) 
                    DO UPDATE SET 
                        last_active = %s,
                        tasks_last_hour = miners.tasks_last_hour + 1
                    """,
                        (
                            miner_hotkey,
                            uid,
                            datetime.now(timezone.utc),
                            0,
                            datetime.now(timezone.utc),
                        ),
                    )
                else:
                    # If UID is not found, do not include UID field
                    cursor.execute(
                        """
                    INSERT INTO miners (hotkey, last_active, tasks_last_hour) 
                    VALUES (%s, %s, 1)
                    ON CONFLICT (hotkey) 
                    DO UPDATE SET 
                        last_active = %s,
                        tasks_last_hour = miners.tasks_last_hour + 1
                    """,
                        (
                            miner_hotkey,
                            datetime.now(timezone.utc),
                            datetime.now(timezone.utc),
                        ),
                    )

            conn.commit()
            bt.logging.info(f"Task {task_id} preview URL and status updated")
            return True

        except Exception as e:
            bt.logging.error(f"Error updating task preview URL: {str(e)}")
            if conn:
                conn.rollback()
            return False

        finally:
            if conn:
                conn.close()

    def get_miner_by_uid(self, uid):
        """Get miner by UID"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(
                    """
                SELECT * FROM miners 
                WHERE uid = %s
                """,
                    (uid,),
                )
                miner = cursor.fetchone()
                return dict(miner) if miner else None
        except Exception as e:
            bt.logging.error(f"Error getting miner by uid: {str(e)}")
            return None
        finally:
            if conn:
                conn.close()

    def update_miner_uid(self, hotkey, uid, current_block=None):
        """Update miner UID, reset miner data if UID changes"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            now = datetime.now(timezone.utc)

            # Attempt to update existing records
            cursor.execute(
                "UPDATE miners SET last_active = %s WHERE hotkey = %s AND uid = %s",
                (now, hotkey, uid),
            )

            # Check if any records have been updated
            if cursor.rowcount > 0:
                # Successfully updated the record, indicating that the hotkey and uid match.
                conn.commit()
                return True

            # If no records have been updated, check if the hotkey exists.
            cursor.execute("SELECT uid FROM miners WHERE hotkey = %s", (hotkey,))
            hotkey_record = cursor.fetchone()

            if hotkey_record:
                # The hotkey exists but the uid is different, update the uid and reset the data.
                bt.logging.info(
                    f"Miner {hotkey[:10]} UID changed from {hotkey_record[0]} to {uid}, resetting data"
                )
                cursor.execute(
                    """
                    UPDATE miners SET 
                        uid = %s,
                        created_at = %s,
                        creation_block = %s,
                        completed_tasks = 0,
                        failed_tasks = 0,
                        avg_completion_time = 0,
                        avg_score = 0,
                        last_active = %s,
                        active_tasks = 0,
                        tasks_last_hour = 0
                    WHERE hotkey = %s
                    """,
                    (uid, now, current_block, now, hotkey),
                )
                conn.commit()
                return True

            # The hotkey does not exist, check if there are other records using the same uid.
            cursor.execute("SELECT hotkey FROM miners WHERE uid = %s", (uid,))
            uid_record = cursor.fetchone()

            if uid_record:
                # There are other hotkeys using the same uid, please delete the old record first.
                old_hotkey = uid_record[0]
                bt.logging.warning(
                    f"UID {uid} reassigned from hotkey {old_hotkey[:10]} to {hotkey[:10]}, deleting old record"
                )
                cursor.execute("DELETE FROM miners WHERE uid = %s", (uid,))

            # Create new record
            bt.logging.info(
                f"Creating new miner record for hotkey {hotkey[:10]} with UID {uid}"
            )
            cursor.execute(
                """
                INSERT INTO miners (hotkey, uid, created_at, last_active, creation_block)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (hotkey, uid, now, now, current_block),
            )

            conn.commit()
            return True

        except Exception as e:
            bt.logging.error(f"Error updating miner uid: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def get_miners_with_creation_time(self):
        """
        Get miner creation time, creation block and total tasks

        Returns:
            list: List containing miner creation time, creation block and total tasks for each miner
        """
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                # Query miner creation time and total tasks
                cursor.execute(
                    """
                SELECT 
                    m.hotkey,
                    EXTRACT(EPOCH FROM m.created_at)::float as creation_time,
                    m.creation_block,
                    COUNT(t.task_id) as total_tasks
                FROM miners m
                LEFT JOIN tasks t ON m.hotkey = t.miner_hotkey
                GROUP BY m.hotkey, m.created_at, m.creation_block
                """
                )

                return cursor.fetchall()
        except Exception as e:
            bt.logging.error(
                f"Error getting miner creation time and total tasks: {str(e)}"
            )
            return []
        finally:
            if conn:
                conn.close()

    def update_task_result_urls(self, task_id, video_url, metadata_url=""):
        """
        Update task video and metadata URL

        Args:
            task_id: Task ID
            video_url: Video URL
            metadata_url: Metadata URL

        Returns:
            bool: Whether the operation was successful
        """
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Update task video and metadata URL
            cursor.execute(
                """
            UPDATE tasks 
            SET s3_video_url = %s, 
                s3_metadata_url = %s,
                updated_at = %s
            WHERE task_id = %s
            """,
                (video_url, metadata_url, datetime.now(timezone.utc), task_id),
            )

            # Add task history record
            cursor.execute(
                """
            INSERT INTO task_history (task_id, status, timestamp, details)
            VALUES (%s, %s, %s, %s)
            """,
                (
                    task_id,
                    "result_urls_updated",
                    datetime.now(timezone.utc),
                    f"Updated video URL and metadata URL",
                ),
            )

            conn.commit()
            bt.logging.info(f"Task {task_id} URL updated")
            return True

        except Exception as e:
            bt.logging.error(f"Error updating task URL: {str(e)}")
            if conn:
                conn.rollback()
            return False

        finally:
            if conn:
                conn.close()

    def update_task_last_sent_at(self, task_id, last_sent_at):
        """Update task last_sent_at timestamp"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get current time
            now = datetime.now(timezone.utc)

            # Update task
            cursor.execute(
                """
            UPDATE tasks 
            SET last_sent_at = %s,
                updated_at = %s
            WHERE task_id = %s
            """,
                (last_sent_at, now, task_id),
            )

            conn.commit()
            return True
        except Exception as e:
            bt.logging.error(f"Error updating task last_sent_at: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def update_task_status(self, task_id, status):
        """Update task status"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get current time
            now = datetime.now(timezone.utc)

            # Update task status
            cursor.execute(
                """
            UPDATE tasks 
            SET status = %s,
                updated_at = %s
            WHERE task_id = %s
            """,
                (status, now, task_id),
            )

            # Add task history record
            cursor.execute(
                """
            INSERT INTO task_history (task_id, status, timestamp, details)
            VALUES (%s, %s, %s, %s)
            """,
                (
                    task_id,
                    status,
                    now,
                    f"Status updated to {status}",
                ),
            )

            conn.commit()
            bt.logging.info(f"Task {task_id} status updated to {status}")
            return True
        except Exception as e:
            bt.logging.error(f"Error updating task status: {str(e)}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def batch_update_timeout_tasks_with_scores(self, timeout_tasks):
        """Batch update timeout tasks with status and scores in one operation

        Args:
            timeout_tasks: List of tuples (task_id, miner_hotkey, score)
        """
        if not timeout_tasks:
            return True

        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Get current time
            now = datetime.now(timezone.utc)

            # Create placeholders for the IN clause
            task_ids = [task_id for task_id, _, _ in timeout_tasks]
            placeholders = ",".join(["%s"] * len(task_ids))

            # Batch update task status and score for all timeout tasks
            cursor.execute(
                f"""
            UPDATE tasks 
            SET status = 'failed',
                score = 0.0,
                updated_at = %s
            WHERE task_id IN ({placeholders})
            """,
                (now,) + tuple(task_ids),
            )

            # Add task history records for all tasks
            history_records = []
            for task_id, miner_hotkey, score in timeout_tasks:
                history_records.append(
                    (
                        task_id,
                        "failed",
                        now,
                        f"Batch timeout update: Task scored {score}, status set to failed",
                    )
                )

            # Use executemany for batch insert
            cursor.executemany(
                """
            INSERT INTO task_history (task_id, status, timestamp, details)
            VALUES (%s, %s, %s, %s)
            """,
                history_records,
            )

            conn.commit()
            bt.logging.info(
                f"Batch updated {len(timeout_tasks)} timeout tasks with scores"
            )
            return True
        except Exception as e:
            bt.logging.error(
                f"Error in batch updating timeout tasks with scores: {str(e)}"
            )
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                conn.close()

    def batch_get_tasks(self, task_ids):
        """Get multiple tasks by IDs

        Args:
            task_ids: List of task IDs to retrieve

        Returns:
            List of task dictionaries
        """
        if not task_ids:
            return []

        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Create placeholders for the IN clause
            placeholders = ",".join(["%s"] * len(task_ids))

            cursor.execute(
                f"""
            SELECT * FROM tasks WHERE task_id IN ({placeholders})
            """,
                tuple(task_ids),
            )

            results = cursor.fetchall()
            if results:
                # Convert to list of dicts
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in results]
            return []

        except Exception as e:
            bt.logging.error(f"Error batch getting tasks: {str(e)}")
            return []
        finally:
            if conn:
                conn.close()


# Global validator database instance
validator_db = ValidatorDatabase()
