import time
import uuid
from typing import Dict, Any, Optional


class Task:
    """
    Represents a video generation task
    """

    def __init__(
        self,
        task_id: Optional[str] = None,
        workflow_params: Dict[str, Any] = None,
        miner_uid: Optional[int] = None,
        miner_hotkey: Optional[str] = None,
        timeout_seconds: int = 600,
        is_synthetic: bool = False,
    ):
        """
        Initialize a task

        Args:
            task_id: Unique task ID (generated if not provided)
            workflow_params: Workflow parameters for the task
            miner_uid: UID of the miner assigned to the task
            miner_hotkey: Hotkey of the miner assigned to the task
            timeout_seconds: Task timeout in seconds
            is_synthetic: Whether this is a synthetic task
        """
        # Generate task ID if not provided
        self.task_id = task_id or str(uuid.uuid4())

        # Task parameters
        self.workflow_params = workflow_params or {}
        self.miner_uid = miner_uid
        self.miner_hotkey = miner_hotkey
        self.timeout_seconds = timeout_seconds
        self.is_synthetic = is_synthetic

        # Task status
        self.status = "created"
        self.creation_time = time.time()
        self.assignment_time = None
        self.completion_time = None
        self.verification_time = None
        self.expiration_time = self.creation_time + timeout_seconds

        # Task result
        self.result_url = None
        self.verification_score = None
        self.verification_result = None

        # Task secret (for verification)
        self.secret_key = str(uuid.uuid4())

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert task to dictionary

        Returns:
            Dict representation of task
        """
        return {
            "task_id": self.task_id,
            "workflow_params": self.workflow_params,
            "miner_uid": self.miner_uid,
            "miner_hotkey": self.miner_hotkey,
            "timeout_seconds": self.timeout_seconds,
            "is_synthetic": self.is_synthetic,
            "status": self.status,
            "creation_time": self.creation_time,
            "assignment_time": self.assignment_time,
            "completion_time": self.completion_time,
            "verification_time": self.verification_time,
            "expiration_time": self.expiration_time,
            "result_url": self.result_url,
            "verification_score": self.verification_score,
            "secret_key": self.secret_key,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        """
        Create task from dictionary

        Args:
            data: Dictionary with task data

        Returns:
            Task instance
        """
        task = cls(
            task_id=data.get("task_id"),
            workflow_params=data.get("workflow_params"),
            miner_uid=data.get("miner_uid"),
            miner_hotkey=data.get("miner_hotkey"),
            timeout_seconds=data.get("timeout_seconds", 600),
            is_synthetic=data.get("is_synthetic", False),
        )

        # Set status fields
        task.status = data.get("status", "created")
        task.creation_time = data.get("creation_time", task.creation_time)
        task.assignment_time = data.get("assignment_time")
        task.completion_time = data.get("completion_time")
        task.verification_time = data.get("verification_time")
        task.expiration_time = data.get("expiration_time", task.expiration_time)

        # Set result fields
        task.result_url = data.get("result_url")
        task.verification_score = data.get("verification_score")
        task.verification_result = data.get("verification_result")

        # Set secret key
        task.secret_key = data.get("secret_key", task.secret_key)

        return task

    def is_expired(self) -> bool:
        """
        Check if task is expired

        Returns:
            True if task is expired, False otherwise
        """
        return time.time() > self.expiration_time

    def is_completed(self) -> bool:
        """
        Check if task is completed

        Returns:
            True if task is completed, False otherwise
        """
        return self.status == "completed" and self.result_url is not None

    def is_verified(self) -> bool:
        """
        Check if task is verified

        Returns:
            True if task is verified, False otherwise
        """
        return self.verification_score is not None

    def assign_to_miner(self, miner_uid: int, miner_hotkey: str):
        """
        Assign task to miner

        Args:
            miner_uid: UID of miner
            miner_hotkey: Hotkey of miner
        """
        self.miner_uid = miner_uid
        self.miner_hotkey = miner_hotkey
        self.status = "assigned"
        self.assignment_time = time.time()

    def complete(self, result_url: str):
        """
        Mark task as completed

        Args:
            result_url: URL to task result
        """
        self.result_url = result_url
        self.status = "completed"
        self.completion_time = time.time()

    def verify(self, score: float, verification_result: Dict[str, Any] = None):
        """
        Mark task as verified

        Args:
            score: Verification score
            verification_result: Full verification result
        """
        self.verification_score = score
        self.verification_result = verification_result
        self.status = "verified"
        self.verification_time = time.time()

    def fail(self, reason: str = None):
        """
        Mark task as failed

        Args:
            reason: Failure reason
        """
        self.status = "failed"
        self.verification_result = (
            {"error": reason} if reason else {"error": "Unknown failure"}
        )
        self.verification_time = time.time()
        self.verification_score = 0.0
