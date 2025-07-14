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

    def __init__(self, config):
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

        # Current cycle score records
        self.current_scores = {}  # {uid: [score1, score2, ...]}

        # Historical score records
        self.historical_scores = (
            {}
        )  # {uid: [avg_score1, avg_score2, ...], maximum 30 historical records}

        # Miner hotkey mapping
        self.miner_hotkeys = {}

        # Reverse mapping for easy lookup of UID by hotkey
        self.hotkey_to_uid = {}

        # Miner online status
        self.miner_online = {}

        # Miner task scores
        self.task_scores = {}  # {uid: [score1, score2, ...]}

        bt.logging.info("Miner score manager initialization complete")

    def initialize(self, metagraph):
        """Initialize score manager and load cache

        Args:
            metagraph: Metagraph object, used to get miner hotkeys

        Returns:
            bool: Whether initialization was successful
        """
        try:
            # Sync current hotkey mapping
            self.sync_hotkeys(metagraph)

            # Load cached data
            self.load_cache()

            self.hotkey_to_uid = {
                hotkey: uid for uid, hotkey in self.miner_hotkeys.items()
            }
            bt.logging.info(
                f"After initialization, hotkey_to_uid has {len(self.hotkey_to_uid)} entries"
            )

            # Update miner online status
            self.update_miner_status(metagraph)

            bt.logging.info(
                f"Score manager initialization complete, currently tracking {len(self.miner_hotkeys)} miners"
            )
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

                    # Check if we already have hotkeys (from metagraph)
                    if self.miner_hotkeys:
                        # Check if hotkeys have changed, if changed clear scores for that UID
                        for uid, hotkey in self.miner_hotkeys.items():
                            if uid in cached_hotkeys and cached_hotkeys[uid] != hotkey:
                                bt.logging.warning(
                                    f"Detected hotkey change for UID {uid}: {cached_hotkeys[uid]} -> {hotkey}, clearing score records"
                                )
                                self._clear_miner_scores(uid)
                    else:
                        # If we don't have hotkeys yet, use the cached ones
                        self.miner_hotkeys = cached_hotkeys

                    # Update Reverse Mapping
                    self.hotkey_to_uid = {
                        hotkey: uid for uid, hotkey in self.miner_hotkeys.items()
                    }

                bt.logging.info(
                    f"Loaded historical scores for {len(self.historical_scores)} miners from cache"
                )

        except json.JSONDecodeError:
            bt.logging.error(f"Cache file {self.cache_file} format error")
        except Exception as e:
            bt.logging.error(f"Failed to load cache: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def load_state(self):
        """Alias for load_cache to maintain compatibility with validator.py"""
        return self.load_cache()

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

    def sync_hotkeys(self, hotkeys_or_metagraph, miner_info_cache=None):
        """Sync miner hotkeys, detect changes and clear scores for changed miners

        Args:
            hotkeys_or_metagraph: List of miner hotkeys or metagraph object
            miner_info_cache: Optional, miner info cache for getting real UIDs when hotkeys list is provided

        Returns:
            List[int]: List of UIDs that have changed
        """
        new_hotkeys = {}
        changed_uids = []

        # Check input type
        if hasattr(hotkeys_or_metagraph, "hotkeys"):
            # If it's a metagraph object
            if hasattr(hotkeys_or_metagraph, "uids"):
                # Use real UIDs from metagraph
                for i, uid in enumerate(hotkeys_or_metagraph.uids.tolist()):
                    new_hotkeys[uid] = hotkeys_or_metagraph.hotkeys[i]
            else:
                # Fallback to enumeration if uids not available
                for uid, hotkey in enumerate(hotkeys_or_metagraph.hotkeys):
                    new_hotkeys[uid] = hotkey
        else:
            # If it's a hotkeys list
            if miner_info_cache:
                # Use miner_info_cache to get real UIDs
                for hotkey in hotkeys_or_metagraph:
                    if hotkey in miner_info_cache and "uid" in miner_info_cache[hotkey]:
                        uid = miner_info_cache[hotkey]["uid"]
                        new_hotkeys[uid] = hotkey
                    else:
                        bt.logging.warning(
                            f"No UID found for hotkey {hotkey[:10]}... in miner_info_cache"
                        )
            else:
                # Fallback to enumeration if miner_info_cache not available
                for uid, hotkey in enumerate(hotkeys_or_metagraph):
                    new_hotkeys[uid] = hotkey
                bt.logging.warning(
                    "Using enumeration for UIDs as miner_info_cache not provided"
                )

        # Check for changes
        for uid, hotkey in new_hotkeys.items():
            if uid in self.miner_hotkeys and self.miner_hotkeys[uid] != hotkey:
                bt.logging.warning(
                    f"Detected hotkey change for UID {uid}: {self.miner_hotkeys[uid]} -> {hotkey}"
                )
                changed_uids.append(uid)

        # Update hotkeys
        self.miner_hotkeys = new_hotkeys

        # Update reverse mapping
        self.hotkey_to_uid = {hotkey: uid for uid, hotkey in self.miner_hotkeys.items()}

        bt.logging.info(
            f"Updated hotkey_to_uid mapping with {len(self.hotkey_to_uid)} entries"
        )
        bt.logging.debug(f"Current miner_hotkeys: {self.miner_hotkeys}")
        bt.logging.debug(f"Current hotkey_to_uid mapping: {self.hotkey_to_uid}")

        return changed_uids

    def update_miner_status(self, metagraph):
        """Update miner online status

        Args:
            metagraph: Metagraph object, used to get miner online status
        """
        online_count = 0
        for uid, axon in enumerate(metagraph.axons):
            is_online = axon.is_serving
            self.miner_online[uid] = is_online
            if is_online:
                online_count += 1

        bt.logging.info(
            f"Update miner online status complete, {online_count}/{len(metagraph.axons)} miners online"
        )

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
            # Always save cache after recording a score
            self.save_cache()
        else:
            bt.logging.warning(
                f"Unable to record score for hotkey {hotkey[:10]}... corresponding UID not found."
            )
            bt.logging.warning(f"Current hotkey_to_uid mapping: {self.hotkey_to_uid}")
            bt.logging.warning(f"Current miner_hotkeys: {self.miner_hotkeys}")

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

                    # Always save cache after recording a score
                    self.save_cache()
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

            # If we have more than 3 scores, keep only the 3 most recent ones
            if len(scores) > 3:
                # Calculate average score for scores to move
                scores_to_move = scores[:-3]  # All except the last 3
                avg_score = self.safe_mean_score(scores_to_move)

                # Add to historical records
                if uid not in self.historical_scores:
                    self.historical_scores[uid] = []

                self.historical_scores[uid].append(avg_score)

                # Limit historical records
                if len(self.historical_scores[uid]) > self.max_history:
                    self.historical_scores[uid] = self.historical_scores[uid][
                        -self.max_history :
                    ]

                # Keep only the 3 most recent scores
                self.current_scores[uid] = scores[-3:]

                bt.logging.debug(
                    f"UID {uid}: Moved {len(scores_to_move)} scores to history, keeping {len(self.current_scores[uid])} recent scores"
                )
            # If we have 3 or fewer scores, do nothing (keep them all)

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
            # If miner not online, score set to 0
            if not self.miner_online.get(uid, False):
                weights[uid] = 0
                continue

            # Calculate historical average score
            history_avg = 0
            if uid in self.historical_scores and self.historical_scores[uid]:
                history_avg = self.safe_mean_score(self.historical_scores[uid])

            # Calculate task average score
            task_avg = 0
            if uid in self.task_scores and self.task_scores[uid]:
                task_avg = self.safe_mean_score(self.task_scores[uid])

            # Calculate final score
            if history_avg == 0 and task_avg == 0:
                weights[uid] = 0
            else:
                bt.logging.debug(
                    f"UID {uid} - History: {history_avg:.4f}, Task: {task_avg:.4f}"
                )

            # Calculate weighted score - simplified condition check
            history_component = history_avg * self.history_weight
            current_component = task_avg * self.current_weight
            weights[uid] = history_component + current_component

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

            bt.logging.info(f"Successfully saved scores to {self.cache_file}")

        except Exception as e:
            bt.logging.error(f"Failed to save cache: {str(e)}")
            bt.logging.error(traceback.format_exc())

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

    def get_available_miners(self, metagraph, self_uid, vpermit_tao_limit=None):
        """Get available miner UID list

        Args:
            metagraph: Metagraph object
            self_uid: Own UID
            vpermit_tao_limit: Optional, validator weight limit

        Returns:
            List[int]: Available miner UID list
        """
        available_uids = []

        # Update miner online status
        self.update_miner_status(metagraph)

        for uid in range(len(metagraph.hotkeys)):
            # Skip self
            if uid == self_uid:
                continue

            # Filter out offline miners
            if not self.miner_online.get(uid, False):
                continue

            # Filter out miners with too high validator weight
            if hasattr(metagraph, "validator_permit") and vpermit_tao_limit is not None:
                if metagraph.validator_permit[uid]:
                    if metagraph.S[uid] > vpermit_tao_limit:
                        continue

            available_uids.append(uid)

        bt.logging.info(f"Retrieved {len(available_uids)} available miners")
        return available_uids
