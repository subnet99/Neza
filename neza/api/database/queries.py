"""
API database query module
Provides database query functions needed for API services
"""

from typing import List, Dict, Any, Optional
from datetime import datetime

import bittensor as bt
from neza.database.validator.db import validator_db


async def get_task_by_id(task_id: str) -> Optional[Dict[str, Any]]:
    """
    Get task details by ID

    Args:
        task_id: Task ID

    Returns:
        Task details dictionary, or None if not found
    """
    return validator_db.get_task_by_id(task_id)


async def get_tasks(
    status: Optional[str] = None, limit: int = 10, offset: int = 0
) -> List[Dict[str, Any]]:
    """
    Get task list

    Args:
        status: Optional, filter by status
        limit: Result count limit
        offset: Result offset

    Returns:
        Task list
    """
    conn = None
    try:
        conn = validator_db.get_connection()
        with conn.cursor(
            cursor_factory=validator_db.psycopg2.extras.DictCursor
        ) as cursor:
            # Build query
            query = "SELECT task_id, status, created_at FROM tasks"
            params = []

            if status:
                query += " WHERE status = %s"
                params.append(status)

            query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])

            cursor.execute(query, params)
            tasks = cursor.fetchall()

            # Convert to dictionary list
            result = []
            for task in tasks:
                task_dict = dict(task)
                result.append(task_dict)

            return result
    except Exception as e:
        bt.logging.error(f"Error getting tasks: {str(e)}")
        return []
    finally:
        if conn:
            conn.close()


async def get_system_stats() -> Dict[str, int]:
    """
    Get system statistics

    Returns:
        Dictionary containing various statistics
    """
    conn = None
    try:
        conn = validator_db.get_connection()
        with conn.cursor() as cursor:
            # Get total task count
            cursor.execute("SELECT COUNT(*) FROM tasks")
            total_tasks = cursor.fetchone()[0]

            # Get task counts by status
            cursor.execute("SELECT COUNT(*) FROM tasks WHERE status = 'pending'")
            pending_tasks = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM tasks WHERE status = 'processing'")
            active_tasks = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM tasks WHERE status = 'completed'")
            completed_tasks = cursor.fetchone()[0]

            cursor.execute(
                "SELECT COUNT(*) FROM tasks WHERE status = 'failed' OR status = 'timeout'"
            )
            failed_tasks = cursor.fetchone()[0]

            # Get active miner count
            cursor.execute("SELECT COUNT(*) FROM miners WHERE is_active = TRUE")
            active_miners = cursor.fetchone()[0]

            return {
                "total_tasks": total_tasks,
                "pending_tasks": pending_tasks,
                "active_tasks": active_tasks,
                "completed_tasks": completed_tasks,
                "failed_tasks": failed_tasks,
                "active_miners": active_miners,
            }
    except Exception as e:
        bt.logging.error(f"Error getting system stats: {str(e)}")
        return {
            "total_tasks": 0,
            "pending_tasks": 0,
            "active_tasks": 0,
            "completed_tasks": 0,
            "failed_tasks": 0,
            "active_miners": 0,
        }
    finally:
        if conn:
            conn.close()


async def get_miners_stats() -> List[Dict[str, Any]]:
    """
    Get miner statistics

    Returns:
        List of miner statistics
    """
    return validator_db.get_miner_stats()


async def verify_task_secret(task_id: str, secret_key: str) -> bool:
    """
    Verify task secret key

    Args:
        task_id: Task ID
        secret_key: Secret key

    Returns:
        Whether the secret key is correct
    """
    conn = None
    try:
        conn = validator_db.get_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
        SELECT secret_key FROM tasks WHERE task_id = %s
        """,
            (task_id,),
        )

        result = cursor.fetchone()
        if result and result[0] == secret_key:
            return True
        return False
    except Exception as e:
        bt.logging.error(f"Error verifying task secret: {str(e)}")
        return False
    finally:
        if conn:
            conn.close()


async def get_task_history(task_id: str) -> List[Dict[str, Any]]:
    """
    Get task history

    Args:
        task_id: Task ID

    Returns:
        List of task history records
    """
    return validator_db.get_task_history(task_id)


async def get_miner_tasks(
    hotkey: str, limit: int = 10, offset: int = 0
) -> List[Dict[str, Any]]:
    """
    Get tasks processed by a specific miner

    Args:
        hotkey: Miner's hotkey
        limit: Result count limit
        offset: Result offset

    Returns:
        Task list
    """
    tasks = validator_db.get_tasks_by_miner(hotkey, limit)
    # Handle offset
    if offset < len(tasks):
        return tasks[offset : offset + limit]
    return []
