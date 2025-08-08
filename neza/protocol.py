import bittensor as bt


class VideoGenerationTask(bt.Synapse):
    """
    Protocol definition for text-to-video generation task

    Attributes:
        task_id (str): Unique task identifier
        prompt (str): Video generation prompt
        secret_key (str): Key used to verify the task
        timeout_seconds (int): Task timeout in seconds
        status (str): Task status, can be "pending", "processing", "completed", "failed", "timeout"
        s3_video_url (str): S3 URL for video storage
        s3_metadata_url (str): S3 URL for metadata storage
    """

    # Request fields
    task_id: str = None
    prompt: str = None
    secret_key: str = None
    timeout_seconds: int = 3600  # Default 1 hour

    # Response fields
    status: str = "pending"
    s3_video_url: str = None
    s3_metadata_url: str = None

    def deserialize(self) -> "VideoGenerationTask":
        """Deserialization handler"""
        return self


class VideoTask(bt.Synapse):
    """
    Protocol definition for video generation task

    Attributes:
        task_id (str): Unique task identifier
        file_name (str): Name of the generated file
        secret_key (str): Key used to verify the task
        workflow_params (dict): Workflow parameters containing all parameters needed for video generation
        upload_url (str): URL for miners to upload videos
        preview_url (str): URL for video preview
        status_code (int): Response status code
        video_url (str): S3 URL for video storage
        metadata_url (str): S3 URL for metadata storage
        error (str): Error message
    """

    # Request fields
    task_id: str = ""
    file_name: str = ""  # New field for specifying the generated file name
    secret_key: str = ""
    workflow_params: dict = {}
    upload_url: str = ""  # Upload URL
    preview_url: str = ""  # Preview URL

    # Response fields
    status_code: int = 0
    video_url: str = ""
    metadata_url: str = ""
    error: str = ""

    def deserialize(self) -> "VideoTask":
        """Deserialize response"""
        return self


class TaskStatusCheck(bt.Synapse):
    """
    Protocol definition for task status check

    Attributes:
        task_id (str): ID of the task to check
        secret_key (str): Key used to verify the task
        status (str): Current task status
        progress (float): Task progress percentage (0-100)
        estimated_time (int): Estimated time remaining in seconds
        error_message (str): Error message if task failed
    """

    # Request fields
    task_id: str = None
    secret_key: str = None

    # Response fields
    status: str = None
    progress: float = 0.0
    estimated_time: int = None
    error_message: str = None

    def deserialize(self) -> "TaskStatusCheck":
        """Deserialization handler"""
        return self


class TaskCancellation(bt.Synapse):
    """
    Protocol definition for task cancellation

    Attributes:
        task_id (str): ID of the task to cancel
        secret_key (str): Key used to verify the task
        success (bool): Whether cancellation was successful
        message (str): Detailed information about cancellation result
    """

    # Request fields
    task_id: str = None
    secret_key: str = None

    # Response fields
    success: bool = False
    message: str = None

    def deserialize(self) -> "TaskCancellation":
        """Deserialization handler"""
        return self


class MinerCapacity(bt.Synapse):
    """
    Protocol definition for miner capacity query

    Attributes:
        available (bool): Whether the miner is available to accept new tasks
        queue_size (int): Number of tasks currently in queue
        estimated_wait_time (int): Estimated wait time in seconds
        max_concurrent_tasks (int): Maximum number of concurrent tasks
    """

    # Response fields
    available: bool = True
    queue_size: int = 0
    estimated_wait_time: int = 0
    max_concurrent_tasks: int = 1

    def deserialize(self) -> "MinerCapacity":
        """Deserialization handler"""
        return self


class Dummy(bt.Synapse):
    """
    Example protocol class for testing network connectivity

    Attributes:
        dummy_input (int): Input value
        dummy_output (int): Output value
    """

    # Request fields
    dummy_input: int = 0

    # Response fields
    dummy_output: int = 0

    def deserialize(self) -> "Dummy":
        """Deserialization handler"""
        return self
