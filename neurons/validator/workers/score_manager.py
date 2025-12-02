"""
Miner Score Management Module
Provides functionality for miner score recording, hotkey change detection, and weight calculation
"""

import os
import json
import time
import traceback
import bittensor as bt
import torch
import numpy as np
from typing import Dict, List, Optional, Any, Tuple
from neza.utils.http import upload_cache_file_sync


class MinerScoreManager:
    """Miner Score Manager

    Manages miner score records, detects hotkey changes, and calculates weights
    """

    # Constants
    DEFAULT_CACHE_VERSION = 1
    DEFAULT_MIN_CACHE_VERSION = 1
    DEFAULT_HISTORY_WEIGHT = 0.3
    DEFAULT_CURRENT_WEIGHT = 0.7
    DEFAULT_MAX_HISTORY = 10
    DEFAULT_SLIDING_WINDOW = 30
    DEFAULT_CACHE_FILE = ".score/score_cache.json"

    # Sigmoid function parameters
    SIGMOID_STEEPNESS = 10
    SIGMOID_MIDPOINT = 0.5

    # Normalization parameters
    MIN_SCORE_FACTOR = 0.9
    LOW_SCORE_THRESHOLD = 1.1
    BASE_SCORE = 0.05

    def __init__(self, config, validator):
        """Initialize score manager

        Args:
            config: Configuration object containing score_manager related settings
        """
        # Configuration
        self.config = config if config else {}
        # Use project directory for cache file
        project_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..")
        )
        self.cache_file = os.path.join(project_root, self.DEFAULT_CACHE_FILE)
        bt.logging.info(f"Cache file path: {self.cache_file}")

        # Configuration parameters
        # Fix configuration retrieval method, ensure cache_version is set correctly
        if hasattr(config, "score_manager") and config.score_manager is not None:
            self.cache_version = getattr(
                config.score_manager, "cache_version", self.DEFAULT_CACHE_VERSION
            )
            self.min_cache_version = getattr(
                config.score_manager,
                "min_cache_version",
                self.DEFAULT_MIN_CACHE_VERSION,
            )
            self.history_weight = getattr(
                config.score_manager, "history_weight", self.DEFAULT_HISTORY_WEIGHT
            )
            self.current_weight = getattr(
                config.score_manager, "current_weight", self.DEFAULT_CURRENT_WEIGHT
            )
            self.max_history = getattr(
                config.score_manager, "max_history", self.DEFAULT_MAX_HISTORY
            )
            self.sliding_window = getattr(
                config.score_manager, "sliding_window", self.DEFAULT_SLIDING_WINDOW
            )

            # Get configuration for whether to save cache every time
            self.always_save_cache = getattr(
                config.score_manager, "always_save_cache", True
            )

            bt.logging.info(
                f"Using custom score_manager configuration: cache_version={self.cache_version}"
            )
        else:
            # Use default values
            self.cache_version = self.DEFAULT_CACHE_VERSION
            self.min_cache_version = self.DEFAULT_MIN_CACHE_VERSION
            self.history_weight = self.DEFAULT_HISTORY_WEIGHT
            self.current_weight = self.DEFAULT_CURRENT_WEIGHT
            self.max_history = self.DEFAULT_MAX_HISTORY
            self.sliding_window = self.DEFAULT_SLIDING_WINDOW
            self.always_save_cache = True

            bt.logging.info(
                f"Using default score_manager configuration: cache_version={self.cache_version}"
            )

        self.validator = validator
        # Current cycle score records
        self.current_scores = {}  # {uid: [score1, score2, ...]}

        # Historical score records
        self.historical_scores = (
            {}
        )  # {uid: [avg_score1, avg_score2, ...], maximum 30 historical records}

        # Miner task scores
        self.task_scores = {}  # {uid: [score1, score2, ...]}

        self.global_verification_errors = 0
        self.FAILURE_THRESHOLD = 3

        self.upldate_miner_state()

        bt.logging.info("Miner score manager initialization complete")

    def initialize(self):
        """Initialize score manager and load cache

        Args:
            metagraph: Metagraph object, used to get miner hotkeys

        Returns:
            bool: Whether initialization was successful
        """
        try:
            # Load cached data
            self.load_cache()

            return True
        except Exception as e:
            bt.logging.error(f"Failed to initialize score manager: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return False

    def load_cache(self):
        """Load historical data from cache file"""
        try:
            # Check if file exists
            if not os.path.exists(self.cache_file):
                bt.logging.info(
                    f"Cache file {self.cache_file} does not exist, using empty records"
                )
                return

            with open(self.cache_file, "r") as file:
                data = json.load(file)

                # Verify version compatibility
                if data.get("version", 0) < self.min_cache_version:
                    bt.logging.warning(
                        f"Cache file version too low ({data.get('version', 0)} < {self.min_cache_version}), not loading"
                    )
                    return

                # Load historical scores
                if "historical_scores" in data:
                    self.historical_scores = {
                        int(k): v for k, v in data["historical_scores"].items()
                    }

                # Load current cycle scores
                if "current_scores" in data:
                    self.current_scores = {
                        int(k): v for k, v in data["current_scores"].items()
                    }

                # Load task scores
                if "task_scores" in data:
                    self.task_scores = {
                        int(k): v for k, v in data["task_scores"].items()
                    }
                # Compatible with older version's synthetic_scores
                elif "synthetic_scores" in data:
                    self.task_scores = {
                        int(k): v for k, v in data["synthetic_scores"].items()
                    }

                # Load hotkey mapping
                if "miner_hotkeys" in data:
                    # Ensure uid is of integer type
                    cached_hotkeys = {
                        int(k): v for k, v in data["miner_hotkeys"].items()
                    }

                    # Check if we already have hotkeys
                    if self.miner_hotkeys:
                        # Check if hotkeys have changed, if changed clear scores for that UID
                        need_clear_uids = []
                        for uid, hotkey in self.miner_hotkeys.items():
                            if uid in cached_hotkeys and cached_hotkeys[uid] != hotkey:
                                bt.logging.warning(
                                    f"Detected hotkey change for UID {uid}: {cached_hotkeys[uid]} -> {hotkey}, clearing score records"
                                )
                                need_clear_uids.append(uid)
                        self._clear_miners_scores(need_clear_uids)
                    else:
                        # If we don't have hotkeys yet, use the cached ones
                        self.miner_hotkeys = cached_hotkeys

                bt.logging.info(
                    f"Loaded historical scores for {len(self.historical_scores)} miners from cache"
                )

        except json.JSONDecodeError:
            bt.logging.error(f"Cache file {self.cache_file} format error")
        except Exception as e:
            bt.logging.error(f"Failed to load cache: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def upldate_miner_state(self):
        """Initialize state"""
        self.miner_hotkeys = self.validator.miner_manager.all_miner_hotkeys
        self.hotkey_to_uid = self.validator.miner_manager.hotkey_to_uid
        self.miner_online = self.validator.miner_manager.miner_online

    def load_state(self):
        """Alias for load_cache to maintain compatibility with validator.py"""
        return self.load_cache()

    def _clear_miners_scores(self, uids):
        """Clear all score records for the specified miners

        Args:
            uids: List of UIDs of the miners to clear scores for
        """
        if not uids:
            return

        for uid in uids:
            self._clear_miner_scores(uid)

        self.save_cache()

    def _clear_miner_scores(self, uid):
        """Clear all score records for the specified miner

        Args:
            uid: UID of the miner to clear scores for
        """
        if uid in self.historical_scores:
            del self.historical_scores[uid]
        if uid in self.current_scores:
            del self.current_scores[uid]
        if uid in self.task_scores:
            del self.task_scores[uid]

    def record_score(self, hotkey, task_id, score):
        """Record the score, convert the hotkey to UID, and call add_score.

        Args:
            hotkey: Miner hotkey
            task_id: Task ID
            score: Score value
        """
        # Find UID through hotkey
        uid = self.hotkey_to_uid.get(hotkey)
        if uid is not None:
            score_str = f"{score:.4f}" if score is not None else "None"
            bt.logging.debug(
                f"Recording score {score_str} for hotkey: {hotkey[:10]}... Task {task_id} UID: {uid}"
            )

            self.add_score(uid, score)
            # Cache is automatically saved by add_score when always_save_cache is True
        else:
            bt.logging.warning(
                f"Unable to record score for hotkey {hotkey[:10]}... corresponding UID not found."
            )
            # bt.logging.warning(f"Current hotkey_to_uid mapping: {self.hotkey_to_uid}")
            # bt.logging.warning(f"Current miner_hotkeys: {self.miner_hotkeys}")

            for uid, known_hotkey in self.miner_hotkeys.items():
                if known_hotkey == hotkey:
                    bt.logging.info(
                        f"Found matching hotkey in miner_hotkeys for UID {uid}"
                    )
                    self.hotkey_to_uid[hotkey] = uid
                    score_str = f"{score:.4f}" if score is not None else "None"
                    bt.logging.info(
                        f"Adding score {score_str} for UID {uid} after fixing mapping"
                    )
                    self.add_score(uid, score)
                    # Cache is automatically saved by add_score when always_save_cache is True
                    return

            bt.logging.error(
                f"Could not find matching UID for hotkey {hotkey} in any mapping"
            )

    def add_score(self, uid, score=0.0):
        """Add score to current cycle score records

        Args:
            uid: Miner UID
            score: Score value
        """
        # Add to current cycle total scores
        if uid not in self.current_scores:
            self.current_scores[uid] = []

        self.current_scores[uid].append(score)

        # Add to task scores
        if uid not in self.task_scores:
            self.task_scores[uid] = []

        self.task_scores[uid].append(score)

        # If configured to save cache every time, save
        if hasattr(self, "always_save_cache") and self.always_save_cache:
            self.save_cache()

    def safe_mean_score(self, data):
        """Safe calculate average score, handle None values and exceptions

        Args:
            data: List of scores, may contain None values

        Returns:
            float: Average score, returns 0.0 if no valid data
        """
        # Filter out None values
        clean_data = [x for x in data if x is not None]

        # If no valid data, return 0
        if len(clean_data) == 0:
            return 0.0

        # Calculate mean value
        mean_value = np.mean(clean_data)

        # Handle exceptions
        if np.isnan(mean_value) or np.isinf(mean_value):
            return 0.0

        # Adjust score based on coverage of valid data
        coverage = len(clean_data) / len(data) if data else 0

        # Use sigmoid function to adjust score, reflect data coverage
        return mean_value * self.sigmoid(coverage)

    def sigmoid(self, x):
        """Sigmoid function, maps input to 0-1 range

        Args:
            x: Input value

        Returns:
            float: sigmoid result
        """
        return 1 / (1 + np.exp(-self.SIGMOID_STEEPNESS * (x - self.SIGMOID_MIDPOINT)))

    def finalize_epoch(self):
        """Complete current cycle, calculate average score and add to historical records"""
        for uid, scores in self.current_scores.items():
            if not scores:
                continue

            # Calculate average of all current scores
            avg_score = self.safe_mean_score(scores)

            # Add average score to historical records
            if uid not in self.historical_scores:
                self.historical_scores[uid] = []

            self.historical_scores[uid].append(avg_score)

            # Limit historical records
            if len(self.historical_scores[uid]) > self.max_history:
                self.historical_scores[uid] = self.historical_scores[uid][
                    -self.max_history :
                ]

            # Use the average score as the initial score for the next cycle
            self.current_scores[uid] = [avg_score]

            bt.logging.debug(
                f"UID {uid}: Calculated average score {avg_score:.4f} added to history and set as initial score for next cycle"
            )

        # Limit task scores
        for uid in self.task_scores:
            if len(self.task_scores[uid]) > self.sliding_window:
                self.task_scores[uid] = self.task_scores[uid][-self.sliding_window :]

        bt.logging.info(
            f"Cycle complete, scores processed, {len(self.historical_scores)} miners in historical records"
        )

        # Save cache
        self.save_cache()

    def calculate_weights(self, active_uids=None):
        """Calculate final weight scores

        Args:
            active_uids: Optional, list of active miner UIDs, if provided calculate weights for these miners only

        Returns:
            Dict[int, float]: UID to weight score mapping
        """
        weights = {}

        # Determine which UIDs to calculate
        uids_to_calculate = (
            active_uids if active_uids is not None else list(self.miner_hotkeys.keys())
        )
        for uid in uids_to_calculate:
            history_avg = 0
            if uid in self.historical_scores and self.historical_scores[uid]:
                history_avg = self.safe_mean_score(self.historical_scores[uid])

            # Calculate task average score
            task_avg = 0
            task_count = 0
            if uid in self.task_scores and self.task_scores[uid]:
                task_avg = self.safe_mean_score(self.task_scores[uid])
                task_count = len(self.task_scores[uid])

            # Calculate final score
            if history_avg == 0 and task_avg == 0:
                weights[uid] = 0
            else:
                bt.logging.debug(
                    f"UID {uid} - History: {history_avg:.4f}, Task: {task_avg:.4f}, Task Count: {task_count}"
                )

            # Calculate weighted score - simplified condition check
            history_component = history_avg * self.history_weight
            current_component = task_avg * self.current_weight
            weights[uid] = history_component + current_component

            if task_count < 2:
                weights[uid] *= 0.5

        return weights

    def normalize_weights(self, weights):
        """Normalize weight scores, use more complex normalization method

        Args:
            weights: UID to weight score mapping

        Returns:
            Dict[int, float]: Normalized weight scores
        """
        if not weights:
            return {}

        # Extract score lists
        scores = list(weights.values())

        # If all scores are 0, return all 0
        if all(score == 0 for score in scores):
            return {uid: 0.0 for uid in weights}

        # Find maximum
        max_score = max(scores)

        # Find minimum non-zero value
        non_zero_scores = [s for s in scores if s > 0]
        min_non_zero = (
            min(non_zero_scores) * self.MIN_SCORE_FACTOR if non_zero_scores else 0
        )

        # Calculate difference
        diff = max_score - min_non_zero

        # Normalize
        normalized = {}
        for uid, score in weights.items():
            if score <= 0:
                normalized[uid] = 0
            else:
                # If score is close to minimum non-zero value, give a small base score
                if score < min_non_zero * self.LOW_SCORE_THRESHOLD:
                    normalized[uid] = self.BASE_SCORE
                else:
                    # Otherwise perform normalization
                    normalized[uid] = ((score - min_non_zero) / diff) if diff > 0 else 0

        return normalized

    def save_cache(self):
        """Save current state to cache file"""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)

            # Ensure version is not null
            cache_version = (
                self.cache_version
                if hasattr(self, "cache_version") and self.cache_version is not None
                else self.DEFAULT_CACHE_VERSION
            )

            # Convert uid keys to strings for JSON serialization
            # Important: We need to preserve the real UIDs, not use indices
            updated_historical_scores = {
                str(k): v for k, v in self.historical_scores.items()
            }
            updated_current_scores = {str(k): v for k, v in self.current_scores.items()}
            updated_task_scores = {str(k): v for k, v in self.task_scores.items()}
            updated_miner_hotkeys = {str(k): v for k, v in self.miner_hotkeys.items()}

            data = {
                "version": cache_version,
                "timestamp": time.time(),
                "historical_scores": updated_historical_scores,
                "current_scores": updated_current_scores,
                "task_scores": updated_task_scores,
                "miner_hotkeys": updated_miner_hotkeys,
            }

            with open(self.cache_file, "w") as file:
                json.dump(data, file)

            bt.logging.info(f"Successfully saved scores file")

        except Exception as e:
            bt.logging.error(f"Failed to save cache: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def upload_cache_file(self):
        """Upload cache file"""
        try:
            upload_cache_file_sync(self.validator.wallet, self.cache_file)
        except Exception as e:
            bt.logging.error(f"Failed to upload cache file")

    def _format_uid_message(self, uid, single_format, all_format):
        """Format UID message

        Args:
            uid: Miner UID, if None represents all miners
            single_format: Single miner message format
            all_format: All miners message format

        Returns:
            str: Formatted message
        """
        if uid is not None:
            return single_format.format(uid=uid)
        else:
            return all_format

    def clear_historical_scores(self, uid=None):
        """Clear historical score records

        Args:
            uid: Optional, specify miner UID to clear, if not provided clear all miners' historical scores

        Returns:
            int: Number of cleared records
        """
        count = 0

        if uid is not None:
            # Clear historical scores for specified miner
            if uid in self.historical_scores:
                del self.historical_scores[uid]
                count = 1
                bt.logging.info(
                    self._format_uid_message(
                        uid,
                        "Cleared historical score records for miner {uid}",
                        "Cleared all miners' historical score records",
                    )
                )
        else:
            # Clear all miners' historical scores
            count = len(self.historical_scores)
            self.historical_scores = {}
            bt.logging.info(
                f"Cleared all miners' historical score records, {count} records"
            )

        # Save updated cache
        self.save_cache()

        return count

    def clear_current_scores(self, uid=None):
        """Clear current cycle score records

        Args:
            uid: Optional, specify miner UID to clear, if not provided clear all miners' current scores

        Returns:
            int: Number of cleared records
        """
        count = 0

        if uid is not None:
            # Clear current scores for specified miner
            if uid in self.current_scores:
                count = 1
                bt.logging.info(
                    self._format_uid_message(
                        uid,
                        "Cleared current cycle score records for miner {uid}",
                        "Cleared all miners' current cycle score records",
                    )
                )
                # Use helper method to clear scores
                self._clear_miner_scores(uid)
        else:
            # Clear all miners' current scores
            count = len(self.current_scores)
            self.current_scores = {}
            self.task_scores = {}
            bt.logging.info(
                f"Cleared all miners' current cycle score records, {count} records"
            )

        # Save updated cache
        self.save_cache()

        return count

    def clear_all_scores(self, uid=None):
        """Clear all score records (historical and current)

        Args:
            uid: Optional, specify miner UID to clear, if not provided clear all miners' scores

        Returns:
            tuple: (Number of cleared historical records, Number of cleared current records)
        """
        history_count = self.clear_historical_scores(uid)
        current_count = self.clear_current_scores(uid)

        bt.logging.info(
            self._format_uid_message(
                uid, "Cleared all scores for miner {uid}", "Cleared all miners' scores"
            )
        )

        return (history_count, current_count)

    def record_verification_error(self):
        """Record a global verification error (not per miner)"""
        self.global_verification_errors += 1

    def record_verification_success(self):
        """Reset global verification error count on success"""
        if self.global_verification_errors > 0:
            self.global_verification_errors = 0

    def should_use_consensus_only(self):
        """Check if should use consensus score only for all miners

        Returns:
            bool: True if should use consensus only (ComfyUI unavailable or too many errors)
        """
        return self.global_verification_errors >= self.FAILURE_THRESHOLD
