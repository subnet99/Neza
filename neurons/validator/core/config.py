import bittensor as bt
import os
from neza.utils.tools import _parse_env_servers


class ValidatorConfig:
    """
    Centralized configuration for validator parameters
    """

    def __init__(self, config=None):
        """
        Initialize validator configuration

        Args:
            config: Bittensor config object
        """
        self.config = config

        # Task processing lock
        self.task_lock_enabled = True

        # Database thread pool configuration
        self.db_max_workers = 16

        # Miner cache configuration
        self.miners_cache_ttl = 60  # Cache TTL in seconds

        # File size limit configuration
        self.file_size_limit = 10

        # Task timeout configuration
        self.task_timeout_seconds = 1800  # Default task timeout (30 minutes)

        # Miner selection configuration
        self.miner_selection = {
            "send_cycle_length": 21600,  # send cycle length in seconds (6 h)
            "generate_max_tasks": 3,  # Maximum synthetic tasks
            "density_float_ratio": 0.003,  # Task density float ratio (0.3%)
        }

        self.comfy_servers = _parse_env_servers(os.environ.get("COMFYUI_SERVERS", ""))

        # Verification configuration
        self.verification = {
            "max_concurrent_verifications": max(
                1, len(self.comfy_servers)
            ),  # Maximum concurrent verification workers
            "verification_sample_rate": 1.0,
            "new_miner_priority": True,  # Prioritize new miners
            "low_verification_threshold": 5,  # Low verification count threshold
            "low_verification_ratio": 0.3,  # Low verification miner allocation ratio
            "random_verification_ratio": 0.7,  # Random verification allocation ratio
            "verification_batch_size": 50,  # Maximum tasks per batch
            "gpu_verification_timeout": 1000,  # GPU verification timeout in seconds
            "verification_cycle_length": 21600,  # Verification cycle length in seconds (6 h)
            "min_verification_per_cycle": 1,  # Minimum verifications per miner per cycle
            "verification_time_estimate": 300,  # Estimated verification time (5 minutes)
        }

        # Miner penalty configuration
        self.miner_penalty = {
            "failure_threshold": 2,  # Consecutive failure threshold
            "penalty_duration": 5 * 60,  # Penalty duration in seconds (5 minutes)
            "max_penalty_duration": 30 * 60,  # Maximum penalty duration (30 minutes)
            "penalty_multiplier": 2,  # Penalty duration multiplier
            "task_failure_threshold": 2,  # Task retry failure threshold
        }

        # Score management configuration
        self.score_management = {
            "history_move_blocks": 6000,  # Move scores to history every 6000 blocks (approx 20 hours)
        }

        # Score manager configuration
        self.score_manager = {
            "cache_version": 1,  # Cache version
            "min_cache_version": 1,  # Minimum compatible cache version
            "history_weight": 0.4,  # Weight for historical scores
            "current_weight": 0.6,  # Weight for current scores
            "max_history": 6,  # Maximum number of historical scores to keep
            "sliding_window": 30,  # Sliding window size for task scores
            "always_save_cache": True,  # Whether to always save cache
        }

        # Emission control configuration
        self.emission_control = {
            "enabled": False,  # Enable emission control
            "uid": 0,  # Target UID for emission control
            "percentage": 0,  # Percentage of total weight for target UID (0.0-1.0)
        }

        bt.logging.info(
            f"Validator configuration initialized with {self.verification['max_concurrent_verifications']} verification workers"
        )
