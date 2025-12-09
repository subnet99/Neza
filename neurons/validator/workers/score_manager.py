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
import math
from typing import Dict, List, Optional, Any, Tuple
from neza.utils.http import upload_cache_file_sync, get_api_miner_stats


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
    DEFAULT_ALWAYS_SAVE_CACHE = True
    DEFAULT_CACHE_FILE = ".score/score_cache.json"

    # Sigmoid function parameters
    SIGMOID_STEEPNESS = 10
    SIGMOID_MIDPOINT = 0.5

    # Normalization parameters
    MIN_SCORE_FACTOR = 0.9
    LOW_SCORE_THRESHOLD = 1.1
    BASE_SCORE = 0.05

    # Default weights
    DEFAULT_COMFY_TASK_WEIGHT = 0.2
    DEFAULT_API_TASK_WEIGHT = 0.8

    DEFAULT_EMISSION_MIN = 0.2
    DEFAULT_EMISSION_MAX = 1.0
    DEFAULT_EMISSION_MAX_PRO = 1.0

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

        self.validator = validator
        # Current cycle score records
        self.current_scores = {}  # {uid: [score1, score2, ...]}

        # Historical score records
        self.historical_scores = (
            {}
        )  # {uid: [avg_score1, avg_score2, ...], maximum 30 historical records}

        # Miner task scores
        self.task_scores = {}  # {uid: [score1, score2, ...]}
        self.api_scores = {}  # {uid: [score1, score2, ...]}

        self.global_verification_errors = 0
        self.FAILURE_THRESHOLD = 3

        self.upldate_miner_state()

        bt.logging.info("Miner score manager initialization complete")

    @property
    def score_config(self):
        if (
            hasattr(self.config, "score_manager")
            and self.config.score_manager is not None
        ):
            return self.config.score_manager
        return {
            "emission_min": 0.3,
            "emission_max": 1.0,
            "emission_max_pro": 1.0,
            "emission_k1": 3.0,
            "emission_k2": 0.5,
            "emission_transition": 0.5,
            "miner_factor_min": 0.2,
            "miner_factor_max": 2.5,
            "miner_factor_k1": 4.0,
            "miner_factor_k2": 1.0,
            "miner_factor_transition": 0.5,
            "comfy_task_weight": self.DEFAULT_COMFY_TASK_WEIGHT,
            "api_task_weight": self.DEFAULT_API_TASK_WEIGHT,
            "history_weight": self.DEFAULT_HISTORY_WEIGHT,
            "current_weight": self.DEFAULT_CURRENT_WEIGHT,
            "max_history": self.DEFAULT_MAX_HISTORY,
            "sliding_window": self.DEFAULT_SLIDING_WINDOW,
            "always_save_cache": self.DEFAULT_ALWAYS_SAVE_CACHE,
            "min_cache_version": self.DEFAULT_MIN_CACHE_VERSION,
            "cache_version": self.DEFAULT_CACHE_VERSION,
        }

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
                if data.get("version", 0) < self.score_config["min_cache_version"]:
                    bt.logging.warning(
                        f"Cache file version too low ({data.get('version', 0)} < {self.score_config['min_cache_version']}), not loading"
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

                # Load API scores
                if "api_scores" in data:
                    self.api_scores = {int(k): v for k, v in data["api_scores"].items()}

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
        if uid in self.api_scores:
            del self.api_scores[uid]

    def record_score(self, hotkey, task_id, score):
        """Record the score, convert the hotkey to UID, and call add_score.

        Args:
            hotkey: Miner hotkey
            task_id: Task ID
            score: Score value
        """
        # Find UID through hotkey
        uid = self.hotkey_to_uid.get(hotkey)

        is_api = task_id == "api_task"

        if uid is not None:
            score_str = f"{score:.4f}" if score is not None else "None"
            bt.logging.debug(
                f"Recording score {score_str} for hotkey: {hotkey[:10]}... Task {task_id} UID: {uid}"
            )

            self.add_score(uid, score, is_api)
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
                    self.add_score(uid, score, is_api)
                    # Cache is automatically saved by add_score when always_save_cache is True
                    return

            bt.logging.error(
                f"Could not find matching UID for hotkey {hotkey} in any mapping"
            )

    def add_score(self, uid, score=0.0, is_api=False):
        """Add score to current cycle score records

        Args:
            uid: Miner UID
            score: Score value
            is_api: Whether this is an API task score
        """
        if is_api:
            if uid not in self.api_scores:
                self.api_scores[uid] = []
            self.api_scores[uid].append(score)
        else:
            if uid not in self.task_scores:
                self.task_scores[uid] = []
            self.task_scores[uid].append(score)

        # If configured to save cache every time, save
        if self.score_config["always_save_cache"]:
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

    def normalize_api_scores(self, api_avg_scores):
        """Normalize API scores to 0-1 range using percentile-based normalization
        This is more robust to outliers than min-max normalization

        Args:
            api_avg_scores: Dict of {uid: api_avg_score}

        Returns:
            Dict of {uid: normalized_score} in 0-1 range
        """
        if not api_avg_scores:
            return {}

        valid_scores = [
            score
            for score in api_avg_scores.values()
            if score is not None and score > 0
        ]

        if not valid_scores:
            return {uid: 0.0 for uid in api_avg_scores.keys()}

        if len(valid_scores) == 1:
            return {
                uid: 0.5 if score is not None and score > 0 else 0.0
                for uid, score in api_avg_scores.items()
            }

        sorted_scores = sorted(valid_scores)
        n = len(sorted_scores)

        p5_idx = max(0, int(n * 0.05))  # 5th percentile index
        p95_idx = min(n - 1, int(n * 0.95))  # 95th percentile index

        min_score = sorted_scores[p5_idx]  # Use 5th percentile as min
        max_score = sorted_scores[p95_idx]  # Use 95th percentile as max

        if max_score == min_score:
            return {
                uid: 0.5 if score is not None and score > 0 else 0.0
                for uid, score in api_avg_scores.items()
            }

        normalized = {}
        for uid, score in api_avg_scores.items():
            if score is None or score <= 0:
                normalized[uid] = 0.0
            else:
                clamped_score = max(min_score, min(max_score, score))
                normalized[uid] = (clamped_score - min_score) / (max_score - min_score)
                normalized[uid] = max(0.0, min(1.0, normalized[uid]))

        return normalized

    def finalize_epoch(self):
        """Complete current cycle, calculate average score from current_scores and add to historical records"""
        all_uids = set(self.current_scores.keys())

        for uid in all_uids:
            # Calculate average from current_scores (which contains combined_score values)
            epoch_score = 0
            if uid in self.current_scores and self.current_scores[uid]:
                epoch_score = self.safe_mean_score(self.current_scores[uid])

            # Add average score to historical records
            if uid not in self.historical_scores:
                self.historical_scores[uid] = []

            self.historical_scores[uid].append(epoch_score)

            # Limit historical records
            if len(self.historical_scores[uid]) > self.score_config["max_history"]:
                self.historical_scores[uid] = self.historical_scores[uid][
                    -self.score_config["max_history"] :
                ]

            # Use the average score as the initial value for the next cycle
            self.current_scores[uid] = [epoch_score]

            bt.logging.debug(
                f"UID {uid}: Calculated epoch score {epoch_score:.4f} from {len(self.current_scores[uid])} current_scores, added to history"
            )

        # Limit task scores
        for uid in self.task_scores:
            if len(self.task_scores[uid]) > self.score_config["sliding_window"]:
                self.task_scores[uid] = self.task_scores[uid][
                    -self.score_config["sliding_window"] :
                ]

        for uid in self.api_scores:
            if len(self.api_scores[uid]) > self.score_config["sliding_window"]:
                self.api_scores[uid] = self.api_scores[uid][
                    -self.score_config["sliding_window"] :
                ]

        bt.logging.info(
            f"Cycle complete, scores processed, {len(self.historical_scores)} miners in historical records"
        )

        # Save cache
        self.save_cache()

    def calculate_emission_percentage(self, total_miner_stake, avg_validator_stake):
        """Calculate emission percentage allocated to miners (segmented exponential model)"""
        if avg_validator_stake == 0:
            return self.score_config["emission_min"]

        emission_min = self.score_config["emission_min"]
        emission_max = self.score_config["emission_max"]
        k1 = self.score_config["emission_k1"]
        k2 = self.score_config["emission_k2"]
        transition = self.score_config["emission_transition"]

        ratio = total_miner_stake / avg_validator_stake

        if ratio >= 1.0:
            return emission_max

        if ratio < transition:
            normalized_ratio = ratio / transition
            emission = emission_min + (emission_max - emission_min) * (
                1 - math.exp(-k1 * normalized_ratio)
            )
        else:
            transition_emission = emission_min + (emission_max - emission_min) * (
                1 - math.exp(-k1)
            )
            normalized_ratio = (ratio - transition) / (1.0 - transition)
            remaining_range = emission_max - transition_emission
            emission = transition_emission + remaining_range * (
                1 - math.exp(-k2 * normalized_ratio)
            )

        return min(emission_max, max(emission_min, emission))

    def calculate_miner_factor(self, miner_stake, avg_miner_stake):
        """Calculate score adjustment factor based on miner stake (segmented exponential model)"""
        if avg_miner_stake == 0:
            return 1.0

        min_factor = self.score_config["miner_factor_min"]
        max_factor = self.score_config["miner_factor_max"]
        k1 = self.score_config["miner_factor_k1"]
        k2 = self.score_config["miner_factor_k2"]
        transition = self.score_config["miner_factor_transition"]

        ratio = miner_stake / avg_miner_stake

        if ratio < transition:
            normalized_ratio = ratio / transition
            factor = min_factor + (1.0 - min_factor) * (
                1 - math.exp(-k1 * normalized_ratio)
            )
        else:
            transition_factor = min_factor + (1.0 - min_factor) * (1 - math.exp(-k1))
            max_ratio = 3.0
            normalized_ratio = (ratio - transition) / (max_ratio - transition)
            normalized_ratio = min(1.0, normalized_ratio)
            remaining_range = max_factor - transition_factor
            factor = transition_factor + remaining_range * (
                1 - math.exp(-k2 * normalized_ratio)
            )

        return max(min_factor, min(max_factor, factor))

    def calculate_weights(self, active_uids=None):
        """Calculate final weight scores

        Args:
            active_uids: Optional, list of active miner UIDs, if provided calculate weights for these miners only

        Returns:
            tuple: (Dict[int, float], float) - (UID to weight score mapping, emission percentage)
        """
        weights = {}

        stake_metrics = self.validator.miner_manager.get_stake_metrics()
        avg_validator_stake = stake_metrics["avg_validator_stake"]
        total_miner_stake = stake_metrics["total_miner_stake"]
        avg_miner_stake = stake_metrics["avg_miner_stake"]
        miner_stakes = stake_metrics["miner_stakes"]

        emission_percentage = self.calculate_emission_percentage(
            total_miner_stake, avg_validator_stake
        )

        bt.logging.info(
            f"Stake Metrics: Avg Val Stake: {avg_validator_stake:.2f}, Total Miner Stake: {total_miner_stake:.2f}, Avg Miner Stake: {avg_miner_stake:.2f}"
        )
        bt.logging.info(f"Emission Percentage: {emission_percentage:.2f}")

        # Determine which UIDs to calculate
        uids_to_calculate = (
            active_uids if active_uids is not None else list(self.miner_hotkeys.keys())
        )

        api_avg_scores = {}
        for uid in uids_to_calculate:
            api_avg = 0
            if uid in self.api_scores and self.api_scores[uid]:
                api_avg = self.safe_mean_score(self.api_scores[uid])
            api_avg_scores[uid] = api_avg

        normalized_api_scores = self.normalize_api_scores(api_avg_scores)

        for uid in uids_to_calculate:
            history_avg = 0
            if uid in self.historical_scores and self.historical_scores[uid]:
                history_avg = self.safe_mean_score(self.historical_scores[uid])

            task_avg = 0
            if uid in self.task_scores and self.task_scores[uid]:
                task_avg = self.safe_mean_score(self.task_scores[uid])

            api_avg = normalized_api_scores.get(uid, 0.0)

            combined_score = (task_avg * self.score_config["comfy_task_weight"]) + (
                api_avg * self.score_config["api_task_weight"]
            )

            # Push combined_score to current_scores for finalize_epoch
            if uid not in self.current_scores:
                self.current_scores[uid] = []
            self.current_scores[uid].append(combined_score)

            history_component = history_avg * self.score_config["history_weight"]
            current_component = combined_score * self.score_config["current_weight"]
            base_weight = history_component + current_component

            total_tasks = len(self.task_scores.get(uid, [])) + len(
                self.api_scores.get(uid, [])
            )
            if total_tasks < 2:
                base_weight *= 0.5

            miner_stake = miner_stakes.get(uid, 0.0)
            miner_factor = self.calculate_miner_factor(miner_stake, avg_miner_stake)

            final_weight = base_weight * miner_factor
            weights[uid] = final_weight

            if base_weight > 0:
                bt.logging.debug(
                    f"UID {uid} - Base: {base_weight:.4f}, Stake: {miner_stake:.2f}, Factor: {miner_factor:.2f}, Final: {final_weight:.4f}"
                )

        return weights, emission_percentage

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
            cache_version = self.score_config["cache_version"]

            # Convert uid keys to strings for JSON serialization
            # Important: We need to preserve the real UIDs, not use indices
            updated_historical_scores = {
                str(k): v for k, v in self.historical_scores.items()
            }
            updated_current_scores = {str(k): v for k, v in self.current_scores.items()}
            updated_task_scores = {str(k): v for k, v in self.task_scores.items()}
            updated_api_scores = {str(k): v for k, v in self.api_scores.items()}
            updated_miner_hotkeys = {str(k): v for k, v in self.miner_hotkeys.items()}

            data = {
                "version": cache_version,
                "timestamp": time.time(),
                "historical_scores": updated_historical_scores,
                "current_scores": updated_current_scores,
                "task_scores": updated_task_scores,
                "api_scores": updated_api_scores,
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
            self.api_scores = {}
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

    async def process_api_scoring(self):
        """Process API miner scoring"""
        try:
            bt.logging.info("Processing API scoring")
            stats = await get_api_miner_stats(self.validator.wallet)
            if not stats:
                return

            model_ratios = stats.get("model_call_ratio", {})
            miners_info = stats.get("miner_stats", {})

            active_hotkeys = set()
            api_miners_cache = self.validator.miner_manager.api_miners_cache
            if api_miners_cache:
                for model_name, hotkeys in api_miners_cache.items():
                    active_hotkeys.update(hotkeys)

            for uid, hotkey in self.miner_hotkeys.items():
                if hotkey not in active_hotkeys and uid in self.api_scores:
                    self.api_scores[uid] = [0.0]

            for hotkey, models_data in miners_info.items():
                if models_data is None:
                    continue
                miner_total_score = 0.0

                for model_name, stat in models_data.items():
                    model_weight = model_ratios.get(model_name, 0.0)
                    call_count = stat.get("call_count", 0)
                    success_count = stat.get("success_count", 0)
                    total_cost = stat.get("total_cost", 0.0)

                    if call_count > 0:
                        success_rate = success_count / call_count
                        model_score = total_cost * success_rate
                        miner_total_score += model_score * model_weight

                self.record_score(hotkey, "api_task", miner_total_score)

        except Exception as e:
            bt.logging.error(f"Error processing API scoring: {str(e)}")
