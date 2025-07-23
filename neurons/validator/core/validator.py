import os
import sys
import time
import threading
import traceback
from typing import List, Dict, Any, Optional, Set, Tuple
from datetime import datetime

import bittensor as bt
import torch
import asyncio
import numpy as np
import wandb

from neza.base.validator import BaseValidatorNeuron
from neza.api.utils import set_validator
from neza.utils.material_manager import MaterialManager
from neza.validator.verify import VideoVerifier

# Import validator modules
from neurons.validator.core.config import ValidatorConfig
from neurons.validator.workers.verification_manager import VerificationManager
from neurons.validator.workers.task_manager import TaskManager
from neurons.validator.workers.miner_manager import MinerManager
from neurons.validator.utils.penalty_manager import PenaltyManager
from neurons.validator.workers.score_manager import MinerScoreManager
from neurons.validator.workers.video_manager import VideoManager
from neza.utils.http import get_consensus_scores_sync

# Load environment variables
from dotenv import load_dotenv

load_dotenv()


class VideoValidator(BaseValidatorNeuron):
    """
    Video Validator Neuron - Modularized and multi-threaded implementation
    Responsible for sending video generation tasks to miners and verifying results
    """

    def __init__(self, config=None):
        # Call parent constructor
        super(VideoValidator, self).__init__(config=config)

        # Initialize validator configuration
        self.validator_config = ValidatorConfig(self.config)
        self.task_counter = 0

        # Initialize material manager
        self.material_manager = MaterialManager(self)
        bt.logging.info("Initializing material manager...")
        success = self.material_manager.initialize()
        if success:
            bt.logging.info("Material initialization successful")
        else:
            bt.logging.warning(
                "Material initialization failed, will retry on block callback"
            )

        # Get workflow mapping configuration
        self.workflow_mapping = self.material_manager.get_workflow_mapping()

        # Initialize video verifier
        self.verifier = VideoVerifier(self)
        self.video_manager = VideoManager()

        # Initialize managers
        self.miner_manager = MinerManager(self)
        self.penalty_manager = PenaltyManager(self)
        self.task_manager = TaskManager(self)
        self.verification_manager = VerificationManager(self)

        # Initialize score manager
        self.score_manager = MinerScoreManager(self.validator_config, self)
        self.miner_manager.init_miners_cache()
        self.score_manager.initialize()

        # Initialize miner info cache
        self.miner_info_cache = None

        # Initialize wandb run
        self.wandb_run_start_time = None
        if not self.config.wandb.off:
            if os.getenv("WANDB_API_KEY"):
                self.new_wandb_run()
            else:
                bt.logging.exception(
                    "WANDB_API_KEY not found in environment variables, skipping wandb run"
                )
                self.config.wandb.off = True
        else:
            bt.logging.warning("WANDB is disabled, skipping wandb run")

        # Register block callbacks
        self.register_block_callback(self.miner_manager.update_miners_on_block)
        self.register_block_callback(self.manage_verification_cycle)
        self.register_block_callback(self.update_materials_on_block)
        self.register_block_callback(self.adjust_config_on_block)
        self.register_block_callback(self.move_scores_on_interval)
        self.register_block_callback(self.task_manager.process_tasks_on_block)
        self.register_block_callback(self.video_manager.process_tasks_on_block)

        # Set API validator instance
        try:
            set_validator(self)
        except Exception as e:
            bt.logging.error(f"Error setting API validator instance: {str(e)}")
        try:
            self._update_config_async()
        except Exception as e:
            bt.logging.error(f"Error updating config: {str(e)}")

        # Log initialization
        worker_count = self.validator_config.verification[
            "max_concurrent_verifications"
        ]
        bt.logging.info(
            f"Video Validator initialized with {worker_count} verification workers"
        )

    async def forward(self):
        """
        Validator forward pass, handles task scheduling and monitoring
        """
        await asyncio.sleep(1800)

    def manage_verification_cycle(self, block):
        """
        Manages verification cycle on block updates

        Args:
            block: Current block number
        """
        # Check if cycle needs to be reset
        current_time = time.time()
        cycle_length = self.validator_config.verification["verification_cycle_length"]

        if (
            current_time - self.verification_manager.verification_cycle_start
            >= cycle_length
        ):
            # Reset cycle
            bt.logging.info(
                f"Resetting verification cycle after {cycle_length/60:.1f} minutes"
            )
            self.verification_manager.reset_verification_cycle()
            self._adjust_workers_for_cycle()

    def _adjust_workers_for_cycle(self):
        """Adjusts number of verification workers based on current cycle requirements"""
        # Calculate optimal worker count based on miners and cycle length
        miner_count = len(self.miner_manager.get_available_miners_cache())
        if miner_count == 0:
            return

        cycle_minutes = (
            self.validator_config.verification["verification_cycle_length"] / 60
        )
        verification_time_minutes = (
            self.validator_config.verification["verification_time_estimate"] / 60
        )

        # Calculate how many workers needed to verify all miners in one cycle
        min_verifications = self.validator_config.verification[
            "min_verification_per_cycle"
        ]
        total_verifications_needed = miner_count * min_verifications

        # How many verifications can one worker do in a cycle
        verifications_per_worker = cycle_minutes / verification_time_minutes

        # How many workers needed
        workers_needed = max(
            1, int(total_verifications_needed / verifications_per_worker)
        )

        # Limit by max concurrent verifications
        new_worker_count = min(
            workers_needed,
            self.validator_config.verification["max_concurrent_verifications"],
        )

        # Adjust if different from current
        if new_worker_count != self.verification_manager.get_worker_count():
            bt.logging.info(
                f"Adjusting verification workers from {self.verification_manager.get_worker_count()} to {new_worker_count}"
            )
            self.verification_manager.adjust_verification_workers(new_worker_count)

    def update_materials_on_block(self, block):
        """
        Updates materials on block callback

        Args:
            block: Current block number
        """
        # Update materials every 100 blocks
        if block % 100 == 0:
            bt.logging.info(f"Updating materials on block {block}")

            # Run update in background thread
            def run_update():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(self._update_materials_async(block))
                except Exception as e:
                    bt.logging.error(f"Error updating materials: {str(e)}")
                    bt.logging.error(traceback.format_exc())
                finally:
                    loop.close()

            update_thread = threading.Thread(target=run_update, daemon=True)
            update_thread.start()

    async def _update_materials_async(self, block):
        """Asynchronously updates materials"""
        bt.logging.info("Refreshing materials...")
        success = await self.material_manager._update_materials_info(force=False)
        if success:
            bt.logging.info("Materials refreshed successfully")
        else:
            bt.logging.warning("Failed to refresh materials")

    def update_base_scores(self):
        """
        Update BaseValidatorNeuron's scores attribute to use base class set_weights method
        """
        try:
            # Get all miner UIDs
            all_uids = self.get_all_miner_uids()
            if not all_uids:
                bt.logging.warning("No miners found, skipping score update")
                return

            # Get current scores from score_manager (raw scores, not normalized)
            weights = self.score_manager.calculate_weights(all_uids)

            # Create a new scores array
            new_scores = np.zeros(self.metagraph.n, dtype=np.float32)

            # Fill in current scores (using raw scores, not normalized)
            for uid in all_uids:
                new_scores[uid] = weights.get(uid, 0.0)

            # Update BaseValidatorNeuron's scores attribute
            self.scores = new_scores

            bt.logging.info(
                f"Successfully updated base class scores with {len(all_uids)} miners (including offline)"
            )

        except Exception as e:
            bt.logging.error(f"Error updating base class scores: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def combine_consensus_scores(self):
        """
        Obtain the consensus score from the API and combine it with the original score of the current validator.
        The consensus score and the original score each account for 50%.
        """
        try:
            bt.logging.info(f"original scores:{self.scores}")
            consensus_scores_array = get_consensus_scores_sync()
            adjusted_scores = np.zeros(self.metagraph.n, dtype=np.float32)
            len_score = len(self.scores)
            for uid, value in enumerate(consensus_scores_array):
                try:
                    if uid < len_score:
                        adjusted_scores[uid] = value * 0.5 + self.scores[uid] * 0.5
                    else:
                        adjusted_scores[uid] = value * 0.5
                except Exception as e:
                    bt.logging.error(f"Error processing UID {uid}: {str(e)}")
                    continue
            self.scores = adjusted_scores
            bt.logging.info(f"consensus scores:{consensus_scores_array}")
            bt.logging.info(f"adjusted scores:{self.scores}")
        except Exception as e:
            bt.logging.error(f"Error combining consensus scores: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def get_all_miner_uids(self):
        """
        Gets all miner UIDs (except self), including offline miners

        Returns:
            List[int]: List of all miner UIDs except self
        """
        all_uids = []
        for uid in range(len(self.metagraph.hotkeys)):
            if uid != self.uid:  # Skip self
                all_uids.append(uid)

        bt.logging.info(f"Found {len(all_uids)} total miners (including offline)")
        return all_uids

    def deal_with_changed_uids(self, changed_uids):
        """
        Processing changes in UIDs, including newly added and replaced miners.

        Args:
            changed_uids: List of changed UIDs, each item is a tuple of (uid, current_hotkey, old_hotkey).
        """
        if not changed_uids:
            return

        bt.logging.info(f"Processing {len(changed_uids)} changed miner UIDs")

        try:
            for uid, current_hotkey, old_hotkey in changed_uids:
                bt.logging.info(
                    f"Handled UID change for {uid}: {old_hotkey} -> {current_hotkey}"
                )

                self.verification_manager._handle_hotkey_change(
                    old_hotkey, current_hotkey
                )

                self.task_manager._handle_hotkey_change(old_hotkey, current_hotkey)

            # 3. Clear scores for all changed UIDs
            self.score_manager._clear_miners_scores([uid for uid, _, _ in changed_uids])

        except Exception as e:
            bt.logging.error(f"Error processing changed UIDs: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def set_weights(self):
        """Sets weights for miners"""
        self.update_base_scores()
        self.combine_consensus_scores()
        self.score_manager.upload_cache_file()
        bt.logging.info(f"miner_score:{self.scores}")

        # Apply emission control if enabled
        if self.validator_config.emission_control["enabled"]:
            target_uid = self.validator_config.emission_control["uid"]
            percentage = self.validator_config.emission_control["percentage"]
            bt.logging.info(
                f"Setting weights to {round(percentage*100)}% for emission controlling UID {target_uid} and {round((1-percentage)*100)}% for the rest."
            )
            self.emission_control_scores(target_uid)

        bt.logging.info("==========start Setting weights==========")
        super().set_weights()
        bt.logging.info("==========end Setting weights==========")
        return

    def emission_control_scores(self, target_uid):
        """
        Adjusts scores to give a specific percentage of total weight to a target UID
        and distribute the remaining weight proportionally among other UIDs

        Args:
            target_uid: The UID to give controlled percentage of total weight
        """
        scores = self.scores
        total_score = np.sum(scores)

        if (
            not isinstance(target_uid, int)
            or target_uid < 0
            or target_uid >= len(scores)
        ):
            bt.logging.info(
                f"target_uid {target_uid} is out of bounds for scores array"
            )
            return

        # Get percentage from config
        percentage = self.validator_config.emission_control["percentage"]

        # Calculate new target score based on percentage of total
        new_target_score = percentage * total_score

        # Calculate remaining weight for other UIDs
        remaining_weight = (1 - percentage) * total_score

        # Calculate current total of non-target scores
        total_other_scores = total_score - scores[target_uid]

        if total_other_scores == 0:
            bt.logging.warning("All scores are zero except target UID, cannot scale.")
            return

        # Scale other scores proportionally
        new_scores = np.zeros_like(scores, dtype=float)
        for uid in range(len(scores)):
            if uid == target_uid:
                new_scores[uid] = new_target_score
            else:
                new_scores[uid] = (scores[uid] / total_other_scores) * remaining_weight

        self.scores = new_scores

    def _calculate_and_set_weights(self, available_uids):
        """Calculates and sets weights"""
        # Calculate weight scores
        weights = self.score_manager.calculate_weights(available_uids)

        # Normalize weights
        normalized_weights = self.score_manager.normalize_weights(weights)

        # Create weight vector
        weight_vector = torch.zeros(len(self.metagraph.hotkeys))
        for uid, weight in normalized_weights.items():
            weight_vector[uid] = weight

        # Log weight distribution
        self._log_weight_distribution(normalized_weights, available_uids)

        # Submit weights
        self._submit_weights(available_uids, weight_vector)

    def _log_weight_distribution(self, normalized_weights, available_uids):
        """Logs weight distribution"""
        non_zero_weights = {
            uid: weight for uid, weight in normalized_weights.items() if weight > 0
        }
        bt.logging.info(
            f"Setting weights, {len(non_zero_weights)}/{len(available_uids)} miners have non-zero weights"
        )

        # Print detailed weight info for each miner
        bt.logging.info("Miner weight details:")
        for uid in sorted(available_uids):
            weight = normalized_weights.get(uid, 0.0)
            hotkey = (
                self.metagraph.hotkeys[uid][:10] + "..."
                if uid < len(self.metagraph.hotkeys)
                else "Unknown"
            )
            bt.logging.info(f"UID: {uid}, Hotkey: {hotkey}, Weight: {weight:.6f}")

    def _submit_weights(self, available_uids, weight_vector):
        """Submits weights to blockchain"""
        try:
            self.subtensor.set_weights(
                netuid=self.config.netuid,
                wallet=self.wallet,
                uids=available_uids,
                weights=weight_vector[available_uids],
                wait_for_inclusion=False,
            )
            bt.logging.info(
                f"Successfully set weights for {len(available_uids)} miners"
            )
        except Exception as e:
            bt.logging.error(f"Failed to set weights: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def _set_default_weights(self):
        """Sets default weights when normal weight setting fails"""
        bt.logging.info("Setting default weights")

        try:
            # Get available UIDs (excluding self)
            available_uids = []
            for uid in range(len(self.metagraph.hotkeys)):
                if uid != self.uid:
                    available_uids.append(uid)

            if not available_uids:
                bt.logging.warning("No available UIDs for default weights")
                return

            # Create default weight vector (equal weights)
            default_weight = 1.0 / len(available_uids)
            weight_vector = torch.zeros(len(self.metagraph.hotkeys))
            for uid in available_uids:
                weight_vector[uid] = default_weight

            # Log default weights
            self._log_default_weights(available_uids, default_weight)

            # Submit weights
            self.subtensor.set_weights(
                netuid=self.config.netuid,
                wallet=self.wallet,
                uids=available_uids,
                weights=weight_vector[available_uids],
                wait_for_inclusion=False,
            )
            bt.logging.info(
                f"Successfully set default weights for {len(available_uids)} miners"
            )

        except Exception as e:
            bt.logging.error(f"Failed to set default weights: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def _log_default_weights(self, uids, default_weight):
        """Logs default weight distribution"""
        bt.logging.info(
            f"Setting default weights: {default_weight:.6f} for {len(uids)} miners"
        )

        # Print sample of miners
        sample_size = min(5, len(uids))
        if sample_size > 0:
            sample_uids = sorted(uids)[:sample_size]
            bt.logging.info("Sample of miners receiving default weights:")
            for uid in sample_uids:
                hotkey = (
                    self.metagraph.hotkeys[uid][:10] + "..."
                    if uid < len(self.metagraph.hotkeys)
                    else "Unknown"
                )
                bt.logging.info(
                    f"UID: {uid}, Hotkey: {hotkey}, Weight: {default_weight:.6f}"
                )

    def __exit__(self, exc_type, exc_value, traceback):
        """Cleanup on exit"""
        bt.logging.info("Shutting down validator...")

        # Stop verification workers
        self.verification_manager.stop_verification_workers()

        # Stop task manager
        self.task_manager.stop()

        # Save state
        self.save_state()

        bt.logging.info("Validator shutdown complete")

    def save_state(self):
        """Saves validator state"""
        bt.logging.info("Saving validator state")

        # Check if score_manager has been initialized
        if hasattr(self, "score_manager") and self.score_manager is not None:
            self.score_manager.save_cache()
        else:
            bt.logging.warning("Score manager not initialized yet, skipping save_state")

        # Call the parent class's save_state method.
        super(BaseValidatorNeuron, self).save_state()

    def load_state(self):
        """Loads validator state"""
        bt.logging.info("Loading validator state")
        # Load score manager state
        self.score_manager.load_cache()

    def adjust_config_on_block(self, block):
        """
        Updates config on block callback

        Args:
            block: Current block number
        """
        # Update every 50 blocks
        if block % 50 == 0:
            self._update_config_async()

    def _update_config_async(self):
        """Asynchronously updates config"""

        # Run update in background thread
        def run_update():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self.material_manager.update_config())
            except Exception as e:
                bt.logging.error(f"Error updating config")
            finally:
                loop.close()

        update_thread = threading.Thread(target=run_update, daemon=True)
        update_thread.start()

    def score_step(self, responses, task_name, task_id, uids):
        """
        Records scores for a step

        Args:
            responses: List of responses
            task_name: Task name
            task_id: Task ID
            uids: List of UIDs
        """
        try:
            # Process each response
            for i, response in enumerate(responses):
                if i >= len(uids):
                    break

                uid = uids[i]
                score = response.get("score", 0)

                # Record score in score manager
                hotkey = self.metagraph.hotkeys[uid]
                self.score_manager.record_score(
                    hotkey=hotkey, task_id=task_id, score=score
                )

                if score is not None:
                    bt.logging.debug(
                        f"Recorded score {score:.4f} for UID {uid} on task {task_id}"
                    )
                else:
                    bt.logging.debug(
                        f"Recorded None score for UID {uid} on task {task_id}"
                    )

            # If using base class set_weights method, update base class scores periodically
            self.task_counter += 1
            if self.task_counter % 10 == 0:
                bt.logging.info(
                    f"Periodically updating base class scores, task counter: {self.task_counter}"
                )
                self.update_base_scores()

        except Exception as e:
            bt.logging.error(f"Error in score_step: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def move_scores_on_interval(self, block):
        """
        Move scores from current_scores to historical_scores on interval
        Executes based on configured block interval (approximately 2 hours by default)

        Args:
            block: Current block number
        """
        # Get the configured block interval
        history_move_blocks = self.validator_config.score_management[
            "history_move_blocks"
        ]

        # Execute every configured number of blocks
        if block % history_move_blocks == 0:
            bt.logging.info(
                f"Moving scores to history at block {block} (interval: {history_move_blocks} blocks)"
            )

            # Use the existing finalize_epoch method to move scores to history
            self.score_manager.finalize_epoch()

            bt.logging.info("Score movement complete, cache saved")

    def new_wandb_run(self):
        """
        Create a new wandb run
        """
        now = datetime.now()
        run_id = now.strftime("%y%m%d%H%M%S")

        self.wandb_run_start_time = now
        self.wandb_run = wandb.init(
            name=f"validator-{self.uid}-{run_id}",
            entity=self.config.wandb.entity,
            project=self.config.wandb.project_name,
            config={
                "uid": self.uid,
                "hotkey": self.wallet.hotkey.ss58_address,
                "run_id": run_id,
                "type": "validator",
            },
            allow_val_change=True,
            anonymous="allow",
        )
