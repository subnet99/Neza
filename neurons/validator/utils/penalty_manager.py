import time
import threading
import traceback
from typing import Dict, Any, Optional

import bittensor as bt


class PenaltyManager:
    """
    Manages miner penalties for failed tasks and other violations
    """

    def __init__(self, validator):
        """
        Initialize penalty manager

        Args:
            validator: Parent validator instance
        """
        self.validator = validator

        # Lock for thread safety
        self.penalty_lock = threading.Lock()

        # Miner penalties dictionary
        self.miner_penalties = {}

    def record_miner_failure(self, miner_hotkey):
        """
        Records a miner failure and applies penalties if needed

        Args:
            miner_hotkey: Miner hotkey that failed

        Returns:
            Dict with penalty information
        """
        with self.penalty_lock:
            # Get current time
            current_time = time.time()

            # Initialize miner penalty record if not exists
            if miner_hotkey not in self.miner_penalties:
                self.miner_penalties[miner_hotkey] = {
                    "failures": 0,
                    "penalty_until": 0,
                    "penalty_count": 0,
                    "task_failures": 0,
                }

            # Get miner penalty record
            penalty_record = self.miner_penalties[miner_hotkey]

            # Increment task failure count
            penalty_record["task_failures"] += 1

            # Check if task failure threshold reached
            if (
                penalty_record["task_failures"]
                >= self.validator.validator_config.miner_penalty[
                    "task_failure_threshold"
                ]
            ):
                # Reset task failures
                penalty_record["task_failures"] = 0

                # Increment failures
                penalty_record["failures"] += 1

                # Check if failure threshold reached
                if (
                    penalty_record["failures"]
                    >= self.validator.validator_config.miner_penalty[
                        "failure_threshold"
                    ]
                ):
                    # Reset failures
                    penalty_record["failures"] = 0

                    # Increment penalty count
                    penalty_record["penalty_count"] += 1

                    # Calculate penalty duration
                    base_duration = self.validator.validator_config.miner_penalty[
                        "penalty_duration"
                    ]
                    multiplier = self.validator.validator_config.miner_penalty[
                        "penalty_multiplier"
                    ] ** (penalty_record["penalty_count"] - 1)
                    duration = min(
                        base_duration * multiplier,
                        self.validator.validator_config.miner_penalty[
                            "max_penalty_duration"
                        ],
                    )

                    # Set penalty until timestamp
                    penalty_record["penalty_until"] = current_time + duration

                    bt.logging.warning(
                        f"Miner {miner_hotkey[:10]}... penalized for {duration/60:.1f} minutes "
                        f"(penalty count: {penalty_record['penalty_count']})"
                    )
                else:
                    bt.logging.info(
                        f"Miner {miner_hotkey[:10]}... failure count increased to {penalty_record['failures']}"
                    )
            else:
                bt.logging.debug(
                    f"Miner {miner_hotkey[:10]}... task failure count increased to {penalty_record['task_failures']}"
                )

            return {
                "hotkey": miner_hotkey,
                "failures": penalty_record["failures"],
                "task_failures": penalty_record["task_failures"],
                "penalty_count": penalty_record["penalty_count"],
                "penalty_until": penalty_record["penalty_until"],
                "is_penalized": current_time < penalty_record["penalty_until"],
            }

    def record_miner_success(self, miner_hotkey):
        """
        Records a miner success and reduces failure counts

        Args:
            miner_hotkey: Miner hotkey that succeeded
        """
        with self.penalty_lock:
            # Skip if miner not in penalties
            if miner_hotkey not in self.miner_penalties:
                return

            # Get miner penalty record
            penalty_record = self.miner_penalties[miner_hotkey]

            # Reset task failures on success
            if penalty_record["task_failures"] > 0:
                penalty_record["task_failures"] = 0

            # Reduce failures on success
            if penalty_record["failures"] > 0:
                penalty_record["failures"] -= 1
                bt.logging.debug(
                    f"Miner {miner_hotkey[:10]}... failure count reduced to {penalty_record['failures']}"
                )

    def is_miner_penalized(self, miner_hotkey):
        """
        Checks if a miner is currently penalized

        Args:
            miner_hotkey: Miner hotkey to check

        Returns:
            bool: True if miner is penalized, False otherwise
        """
        with self.penalty_lock:
            # Get current time
            current_time = time.time()

            # Check if miner in penalties
            if miner_hotkey not in self.miner_penalties:
                return False

            # Get miner penalty record
            penalty_record = self.miner_penalties[miner_hotkey]

            # Check if penalty is active
            is_penalized = current_time < penalty_record["penalty_until"]

            # If penalty expired but was active, log it
            if not is_penalized and penalty_record["penalty_until"] > 0:
                # Only log if penalty was recently active
                if current_time - penalty_record["penalty_until"] < 60:
                    bt.logging.info(f"Miner {miner_hotkey[:10]}... penalty expired")

            return is_penalized

    def get_penalized_miners(self):
        """
        Gets list of currently penalized miners

        Returns:
            List of penalized miner information
        """
        with self.penalty_lock:
            # Get current time
            current_time = time.time()

            # Filter penalized miners
            penalized_miners = []

            for hotkey, penalty in self.miner_penalties.items():
                if current_time < penalty["penalty_until"]:
                    # Calculate remaining time
                    remaining_time = penalty["penalty_until"] - current_time

                    # Add to penalized list
                    penalized_miners.append(
                        {
                            "hotkey": hotkey,
                            "penalty_count": penalty["penalty_count"],
                            "penalty_until": penalty["penalty_until"],
                            "remaining_time": remaining_time,
                            "remaining_minutes": remaining_time / 60,
                        }
                    )

            # Sort by remaining time
            penalized_miners.sort(key=lambda x: x["remaining_time"], reverse=True)

            return penalized_miners

    def clear_miner_penalty(self, miner_hotkey=None):
        """
        Clears penalties for a miner or all miners

        Args:
            miner_hotkey: Specific miner hotkey to clear, or None to clear all

        Returns:
            int: Number of miners cleared
        """
        with self.penalty_lock:
            cleared_count = 0

            if miner_hotkey is not None:
                # Clear specific miner
                if miner_hotkey in self.miner_penalties:
                    self.miner_penalties[miner_hotkey] = {
                        "failures": 0,
                        "penalty_until": 0,
                        "penalty_count": 0,
                        "task_failures": 0,
                    }
                    cleared_count = 1
                    bt.logging.info(
                        f"Cleared penalties for miner {miner_hotkey[:10]}..."
                    )
            else:
                # Clear all miners
                cleared_count = len(self.miner_penalties)
                self.miner_penalties = {}
                bt.logging.info(
                    f"Cleared penalties for all miners ({cleared_count} miners)"
                )

            return cleared_count
