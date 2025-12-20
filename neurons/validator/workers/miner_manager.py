import time
import threading
import asyncio
import traceback
from typing import List, Dict, Any, Optional, Set
import random
import bittensor as bt
from neza.utils.http import sync_miner_api_models
from neza.protocol import ComfySupport


class MinerManager:
    """
    Manages miner-related operations including tracking available miners,
    miner capacity, and miner selection
    """

    def __init__(self, validator):
        """
        Initialize miner manager

        Args:
            validator: Parent validator instance
        """
        self.validator = validator

        # Miner cache
        self.available_miners_cache = None
        self.miners_cache_time = 0
        self.all_miner_hotkeys = {}
        self.hotkey_to_uid = {}
        self.miner_online = []
        self.miner_info_cache = {}
        self.miners_cache_ttl = self.validator.validator_config.miners_cache_ttl
        self.api_miners_cache = {}
        self.comfy_support_miners = set()
        self.blacklisted_hotkeys = set()
        self.blacklisted_uids = set()

    def init_miners_cache(self):
        """
        Initializes miner cache
        """
        # Use lock to prevent WebSocket concurrency issues
        with self.validator._subtensor_lock:
            current_block = self.validator.subtensor.get_current_block()
        self._update_miners_in_thread(current_block)
        self._update_api_miners_in_thread(current_block)

    def update_miners_on_block(self, block):
        """
        Updates miner information on block callback

        Args:
            block: Current block number
        """
        try:
            # Only update every 10 blocks
            if block % 10 != 0:
                return

            # Start a new thread to handle the update to avoid WebSocket concurrency issues
            thread = threading.Thread(
                target=self.update_miners_info_on_block, args=(block,)
            )
            thread.daemon = True
            thread.start()

        except Exception as e:
            bt.logging.error(f"Error updating miners on block: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def update_miners_info_on_block(self, block):
        self._update_miners_in_thread(block)
        self._update_api_miners_in_thread(block)

    def _update_miners_in_thread(self, block):
        """
        Updates miner information in a separate thread to avoid WebSocket concurrency issues

        Args:
            block: Current block number
        """
        try:
            # Get metagraph
            metagraph = self.validator.metagraph

            # Refresh metagraph
            if hasattr(metagraph, "sync") and callable(metagraph.sync):
                bt.logging.info(f"Syncing metagraph on block {block}")
                with self.validator._subtensor_lock:
                    metagraph.sync(subtensor=self.validator.subtensor)

            # Update miner information
            bt.logging.debug(f"Updating miner information on block {block}")
            db_updates = []  # Store database update operations for batch processing
            new_hotkeys = {}
            hotkey_to_uid = {}
            changed_uids = []

            # Update miner UIDs in database
            for uid in range(len(metagraph.hotkeys)):
                # Skip self
                if uid == self.validator.uid:
                    continue

                # Get hotkey
                hotkey = metagraph.hotkeys[uid]
                # Add to database updates (will be processed later)
                db_updates.append((hotkey, uid))

                new_hotkeys[uid] = hotkey
                hotkey_to_uid[hotkey] = uid
                old_hotkey = self.all_miner_hotkeys.get(uid, None)

                if old_hotkey and (old_hotkey != hotkey):
                    changed_uids.append((uid, hotkey, old_hotkey))

            self.all_miner_hotkeys = new_hotkeys
            self.hotkey_to_uid = hotkey_to_uid
            self.validator.deal_with_changed_uids(changed_uids)

            # Update miner info cache
            miner_info_cache = {}
            for uid in range(len(metagraph.hotkeys)):
                if uid == self.validator.uid:
                    continue

                hotkey = metagraph.hotkeys[uid]
                stake = float(metagraph.stake[uid])
                miner_info_cache[hotkey] = {
                    "uid": uid,
                    "hotkey": hotkey,
                    "stake": 0,
                    "original_stake": stake,
                    "trust": float(metagraph.trust[uid]),
                    "incentive": float(metagraph.incentive[uid]),
                    "consensus": float(metagraph.consensus[uid]),
                    "dividends": float(metagraph.dividends[uid]),
                    "last_update": block,
                    "axon": str(metagraph.axons[uid]),
                }

            # Save miner info cache to validator instance for other components to use
            self.validator.miner_info_cache = miner_info_cache
            self.miner_info_cache = miner_info_cache

            # Update available miners
            self._update_available_miners_sync()
            # Now process database updates in a new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                # Process database updates
                for hotkey, uid in db_updates:
                    loop.run_until_complete(
                        self.validator.task_manager._db_op(
                            self.validator.task_manager.db.update_miner_uid,
                            hotkey=hotkey,
                            uid=uid,
                            current_block=block,
                        )
                    )
            finally:
                # Clean up the event loop
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()

        except Exception as e:
            bt.logging.error(traceback.format_exc())

    def _update_api_miners_in_thread(self, block):
        """
        Updates API miners information in a separate thread to avoid WebSocket concurrency issues

        Args:
            block: Current block number
        """
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                api_models = loop.run_until_complete(
                    sync_miner_api_models(self.validator.wallet)
                )
                if api_models:
                    bt.logging.info(f"API miners: {api_models}")
                    self.api_miners_cache = api_models
                    bt.logging.info(f"Synced {len(api_models)} API miners")
            finally:
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()

        except Exception as e:
            bt.logging.error(f"Error in _update_api_miners_in_thread: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def _update_available_miners_sync(self):
        """Synchronously updates available miners list"""
        try:
            available_miners = []
            miner_info_cache = {}
            metagraph = self.validator.metagraph

            coldkey_to_hotkeys = {}
            coldkey_to_uids = {}
            for uid in range(len(metagraph.hotkeys)):
                if uid == self.validator.uid:
                    continue

                stake = metagraph.S[uid].item()
                axon = metagraph.axons[uid]
                hotkey = metagraph.hotkeys[uid]
                coldkey = metagraph.coldkeys[uid]

                is_online = axon.is_serving and axon.ip != "0.0.0.0" and axon.port != 0
                if not is_online:
                    continue

                is_validator = metagraph.validator_permit[uid]
                if is_validator:
                    continue

                miner_info_cache[hotkey] = {
                    "uid": uid,
                    "stake": 0,
                    "original_stake": stake,
                    "axon": {
                        "ip": axon.ip,
                        "port": axon.port,
                        "is_serving": axon.is_serving,
                    },
                    "hotkey": hotkey,
                }

                if coldkey not in coldkey_to_hotkeys:
                    coldkey_to_hotkeys[coldkey] = []
                    coldkey_to_uids[coldkey] = []
                coldkey_to_hotkeys[coldkey].append(hotkey)
                coldkey_to_uids[coldkey].append(uid)

                # Add to available miners list
                available_miners.append(uid)

            # Calculate coldkey stake info and update miner stake
            coldkey_stakes = {}
            netuid = self.validator.config.netuid

            with self.validator._subtensor_lock:
                for coldkey, hotkeys in coldkey_to_hotkeys.items():
                    try:
                        all_stake_info = [
                            info
                            for info in self.validator.subtensor.get_stake_info_for_coldkey(
                                coldkey
                            )
                            if info.netuid == netuid
                        ]

                        if not all_stake_info:
                            continue

                        total_stake = sum(info.stake.tao for info in all_stake_info)
                        miner_count = len(hotkeys)

                        if miner_count == 0:
                            continue

                        stake_per_hotkey = total_stake / miner_count

                        coldkey_stakes[coldkey] = {
                            "total_stake": total_stake,
                            "miner_count": miner_count,
                            "stake_per_hotkey": stake_per_hotkey,
                        }

                        caches_to_update = [miner_info_cache]
                        if self.validator.miner_info_cache:
                            caches_to_update.append(self.validator.miner_info_cache)
                        if self.miner_info_cache:
                            caches_to_update.append(self.miner_info_cache)

                        for hotkey in hotkeys:
                            for cache in caches_to_update:
                                if hotkey in cache:
                                    cache[hotkey]["stake"] = stake_per_hotkey

                    except Exception as e:
                        bt.logging.warning(
                            f"Error getting stake info for coldkey {coldkey}: {str(e)}"
                        )
                        continue

            # Update cache
            random.shuffle(available_miners)
            self.available_miners_cache = available_miners
            self.miner_online = available_miners
            self.miners_cache_time = time.time()
            self.validator.score_manager.update_miner_state()
            self._update_blacklisted_hotkeys(coldkey_to_hotkeys, coldkey_to_uids)

            bt.logging.info(
                f"Updated available miners list, found {len(available_miners)} available miners Available miners UIDs: {available_miners}"
            )
            bt.logging.info(f"Cached information for {len(miner_info_cache)} miners")
            bt.logging.info(f"Updated stake info for {len(coldkey_stakes)} coldkeys")

        except Exception as e:
            bt.logging.error(f"Error updating miners list: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def _update_blacklisted_hotkeys(self, coldkey_to_hotkeys, coldkey_to_uids):
        """
        Update blacklisted hotkeys and UIDs cache based on coldkey blacklist

        Args:
            coldkey_to_hotkeys: Dict mapping coldkey to list of hotkeys
            coldkey_to_uids: Dict mapping coldkey to list of UIDs
        """
        try:
            blacklisted_coldkeys = set(
                self.validator.validator_config.coldkey_blacklist_array
            )
            if not blacklisted_coldkeys:
                self.blacklisted_hotkeys = set()
                self.blacklisted_uids = set()
                return

            blacklisted_hotkeys = set()
            blacklisted_uids = set()
            for coldkey in blacklisted_coldkeys:
                if coldkey in coldkey_to_hotkeys:
                    blacklisted_hotkeys.update(coldkey_to_hotkeys[coldkey])
                if coldkey in coldkey_to_uids:
                    blacklisted_uids.update(coldkey_to_uids[coldkey])

            self.blacklisted_hotkeys = blacklisted_hotkeys
            self.blacklisted_uids = blacklisted_uids

            if blacklisted_hotkeys:
                bt.logging.warning(
                    f"Blacklisted {len(blacklisted_hotkeys)} hotkeys ({len(blacklisted_uids)} UIDs) from {len(blacklisted_coldkeys)} blacklisted coldkeys"
                )
        except Exception as e:
            bt.logging.error(f"Error updating blacklisted hotkeys: {str(e)}")
            bt.logging.error(traceback.format_exc())

    def get_stake_metrics(self):
        """
        Calculates stake metrics for validators and miners

        Returns:
            dict: {
                "avg_validator_stake": float,
                "total_miner_stake": float,
                "avg_miner_stake": float,
                "miner_stakes": dict {uid: stake}
            }
        """
        try:
            metagraph = self.validator.metagraph

            validator_stakes = []
            for uid in range(len(metagraph.hotkeys)):
                stake = float(metagraph.S[uid])

                is_validator = metagraph.validator_permit[uid]

                if is_validator:
                    validator_stakes.append(stake)

            avg_validator_stake = (
                sum(validator_stakes) / len(validator_stakes)
                if validator_stakes
                else 0.0
            )

            available_miners = self.get_available_miners_cache()
            miner_stakes_dict = {}
            all_miner_stakes = []

            for uid in available_miners:
                hotkey = metagraph.hotkeys[uid]
                if hotkey in self.miner_info_cache:
                    stake = self.miner_info_cache[hotkey].get("stake", 0.0)
                else:
                    stake = float(metagraph.S[uid])
                miner_stakes_dict[uid] = stake
                all_miner_stakes.append(stake)

            total_miner_stake = sum(all_miner_stakes)

            avg_miner_stake = 0.0
            if all_miner_stakes:
                sorted_stakes = sorted(all_miner_stakes)

                cutoff_index = int(len(sorted_stakes) * 0.1)
                valid_stakes = sorted_stakes[cutoff_index:]

                if valid_stakes:
                    avg_miner_stake = max(
                        sum(valid_stakes), avg_validator_stake * 0.5
                    ) / len(valid_stakes)

            return {
                "avg_validator_stake": avg_validator_stake,
                "total_miner_stake": total_miner_stake,
                "avg_miner_stake": avg_miner_stake,
                "miner_stakes": miner_stakes_dict,
            }

        except Exception as e:
            bt.logging.error(f"Error calculating stake metrics: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return {
                "avg_validator_stake": 0.0,
                "total_miner_stake": 0.0,
                "avg_miner_stake": 0.0,
                "miner_stakes": {},
            }

    def get_available_miners_cache(self):
        """
        Gets available miners from cache or updates if expired

        Returns:
            List of available miner UIDs
        """

        return self.available_miners_cache or []

    async def get_miners_with_capacity(self):
        """
        Gets all available miners without considering load limits
        Consistent with original implementation, does not filter out miners without capacity

        Returns:
            List of miners with capacity information
        """
        try:
            # Get available miners
            available_miners = self.get_available_miners_cache()
            if not available_miners:
                bt.logging.warning("No available miners found")
                return []

            # Get current miner load
            miners_load = await self._get_miners_load(available_miners)

            # Create miners with capacity list (including all miners regardless of capacity)
            miners_with_capacity = []

            for uid in available_miners:
                try:
                    # Get miner info
                    hotkey = self.validator.metagraph.hotkeys[uid]
                    current_load = miners_load.get(uid, 0)

                    is_penalized = False
                    remaining_capacity = 1000

                    # Add miner with capacity info
                    miners_with_capacity.append(
                        {
                            "uid": uid,
                            "hotkey": hotkey,
                            "active_tasks": current_load,
                            "retry_tasks": 0,  # Consistent with original implementation
                            "total_load": current_load,
                            "remaining_capacity": remaining_capacity,
                            "is_penalized": is_penalized,
                        }
                    )
                except Exception as e:
                    bt.logging.error(
                        f"Error processing miner {uid} load info: {str(e)}"
                    )

            if not miners_with_capacity:
                bt.logging.warning("No miners with capacity found")
                return []

            # Log miners info
            bt.logging.info(f"Found {len(miners_with_capacity)} available miners")

            return miners_with_capacity

        except Exception as e:
            bt.logging.error(f"Error getting miners with capacity: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return []

    async def _get_miners_load(self, miner_uids):
        """
        Gets current load for miners

        Args:
            miner_uids: List of miner UIDs to check

        Returns:
            Dict mapping UIDs to current load
        """
        try:
            # Initialize with zero load
            miners_load = {uid: 0 for uid in miner_uids}

            # Get active tasks from database - now a synchronous call
            active_tasks = self.validator.task_manager.db.get_active_tasks()

            # Count tasks per miner
            for task in active_tasks:
                miner_uid = task.get("miner_uid")
                if miner_uid in miners_load:
                    miners_load[miner_uid] += 1

            return miners_load

        except Exception as e:
            bt.logging.error(f"Error getting miners load: {str(e)}")
            bt.logging.error(traceback.format_exc())
            return {uid: 0 for uid in miner_uids}  # Return zero load on error

    async def select_miners_for_tasks(self, miners_with_capacity, task_count):
        """
        Selects miners for tasks based on configuration

        Args:
            miners_with_capacity: List of miners with capacity info
            task_count: Number of miners to select

        Returns:
            List of selected miners
        """
        if not miners_with_capacity:
            return []

        # Adjust count based on available miners
        count = min(task_count, len(miners_with_capacity))
        if count == 0:
            return []

        # Use density-based selection
        miners_with_density = await self._calculate_miners_density(miners_with_capacity)

        # Sort miners by density (descending) to prioritize miners with higher density
        sorted_miners = sorted(
            miners_with_density, key=lambda m: m.get("density", 0), reverse=True
        )

        # Select top miners
        selected_miners = sorted_miners[:count]

        # Format selected miners for logging
        formatted_miners = self._format_selected_miners(selected_miners, count)

        bt.logging.info(
            f"Selected {len(selected_miners)} miners for tasks (density-based): {formatted_miners}"
        )

        return selected_miners

    async def _select_miners_randomly(self, miners_with_capacity, count):
        """
        Selects miners randomly to ensure fair task distribution

        Args:
            miners_with_capacity: List of miners with capacity info
            count: Number of miners to select

        Returns:
            List of selected miners
        """
        # Filter valid miners with capacity
        valid_miners = self._filter_valid_miners(miners_with_capacity)
        if not valid_miners:
            return []

        # Randomly shuffle the miners
        shuffled_miners = random.sample(valid_miners, len(valid_miners))

        # Select miners up to the requested count
        selected_miners = shuffled_miners[:count]

        return selected_miners

    def _filter_valid_miners(self, miners_with_capacity):
        """Filters valid miners with capacity"""
        return [m for m in miners_with_capacity if m["remaining_capacity"] > 0]

    async def _calculate_miners_density(self, valid_miners):
        """Calculates task density for miners"""
        # Get creation info for miners
        creation_info = await self._get_miners_creation_info()

        # Calculate base density for each miner
        miners_with_density = []
        total_density = 0

        for miner in valid_miners:
            uid = miner["uid"]
            hotkey = miner["hotkey"]

            # Get creation count and last creation time
            creation_count = creation_info.get(hotkey, {}).get("count", 0)
            last_creation = creation_info.get(hotkey, {}).get("last_creation", 0)

            # Calculate time factor (more time since last task = higher priority)
            current_time = time.time()
            time_factor = 1.0
            if last_creation > 0:
                time_since_last = current_time - last_creation
                # Normalize time factor: 1.0 at 0 seconds, 2.0 at 288 minutes or more
                time_factor = min(2.0, 1.0 + time_since_last / 17280)

            # Calculate count factor
            count_factor = 1.0 / (creation_count + 1)

            # Calculate base density
            base_density = time_factor * count_factor

            # Apply random float to density
            density = self._apply_density_float(base_density)

            # Add to miners with density
            miner_with_density = {**miner, "density": density}
            miners_with_density.append(miner_with_density)
            total_density += density

        # Normalize densities
        if total_density > 0:
            for miner in miners_with_density:
                miner["normalized_density"] = miner["density"] / total_density
                self._log_miner_density(miner)

        return miners_with_density

    async def _get_miners_creation_info(self):
        """Gets task creation info for miners"""
        try:
            # Get from database
            creation_info = (
                self.validator.task_manager.db.get_miners_task_creation_info()
            )

            # Format as dict keyed by hotkey
            result = {}
            for info in creation_info:
                result[info["hotkey"]] = {
                    "count": info["task_count"],
                    "last_creation": info["last_creation_time"],
                }

            return result

        except Exception as e:
            bt.logging.error(f"Error getting miners creation info: {str(e)}")
            return {}

    def _apply_density_float(self, base_density):
        """Applies random float to density value"""
        # Get float ratio from config
        float_ratio = self.validator.validator_config.miner_selection[
            "density_float_ratio"
        ]

        # Calculate float range
        float_range = base_density * float_ratio

        # Apply random float within range
        density = base_density + random.uniform(-float_range, float_range)

        # Ensure density is positive
        return max(0.0001, density)

    def _log_miner_density(self, miner):
        """Logs miner density information"""
        uid = miner["uid"]
        hotkey = miner["hotkey"][:10] + "..."
        density = miner["density"]
        normalized = miner.get("normalized_density", 0)

        bt.logging.debug(
            f"Miner {hotkey} (UID {uid}) - Density: {density:.6f}, Normalized: {normalized:.6f}"
        )

    def _format_selected_miners(self, selected_miners, count):
        """Formats selected miners for logging"""
        if not selected_miners:
            return "None"

        # Format miner info
        miner_info = []
        for miner in selected_miners:
            uid = miner["uid"]
            hotkey = miner["hotkey"][:10] + "..."
            density = miner.get("normalized_density", 0)

            miner_info.append(f"{hotkey}({uid}):{density:.4f}")

        return ", ".join(miner_info)

    def _log_selected_miner(self, miner):
        """Logs selected miner details"""
        uid = miner["uid"]
        hotkey = miner["hotkey"][:10] + "..."
        density = miner.get("normalized_density", 0)

        bt.logging.debug(
            f"Selected miner {hotkey} (UID {uid}) with density {density:.6f}"
        )

    def _format_selected_miners_random(self, selected_miners):
        """Formats randomly selected miners for logging"""
        if not selected_miners:
            return "None"

        # Format miner info
        miner_info = []
        for miner in selected_miners:
            uid = miner["uid"]
            hotkey = miner["hotkey"][:10] + "..."

            miner_info.append(f"{hotkey}({uid})")

        return ", ".join(miner_info)

    async def _select_miners_round_robin(self, miners_with_capacity, count=1):
        """
        Selects miners using round-robin algorithm to ensure equal task distribution

        Args:
            miners_with_capacity: List of miners with capacity
            count: Number of miners to select

        Returns:
            List of selected miners
        """
        # Filter valid miners
        valid_miners = miners_with_capacity
        if not valid_miners:
            return []

        # Get task counts for all miners
        task_counts = {}
        try:
            # Get from database
            creation_info = (
                self.validator.task_manager.db.get_miners_task_creation_info()
            )

            # Format as dict keyed by hotkey
            for info in creation_info:
                task_counts[info["hotkey"]] = info["task_count"]
        except Exception as e:
            bt.logging.error(f"Error getting miners task counts: {str(e)}")
            # Continue with empty task counts if there's an error

        # Add task count to miners and sort by task count (ascending)
        for miner in valid_miners:
            miner["task_count"] = task_counts.get(miner["hotkey"], 0)

        # Sort miners by task count (ascending) to prioritize miners with fewer tasks
        sorted_miners = sorted(valid_miners, key=lambda m: m["task_count"])

        # Select top miners
        selected_miners = sorted_miners[:count]

        # Log selection
        for miner in selected_miners:
            bt.logging.debug(
                f"Selected miner {miner['hotkey'][:10]}... (UID {miner['uid']}) with {miner['task_count']} previous tasks"
            )

        return selected_miners

    async def query_comfy_support(self, miner_uids: List[int]) -> Set[int]:
        """
        Query miners for ComfyUI support

        Args:
            miner_uids: List of miner UIDs to query

        Returns:
            Set of UIDs that support ComfyUI
        """
        if not miner_uids:
            return set()

        comfy_supporters = set()
        axons = [self.validator.metagraph.axons[uid] for uid in miner_uids]

        try:
            async with bt.Dendrite(self.validator.wallet) as dendrite:
                responses = await asyncio.wait_for(
                    dendrite.forward(
                        axons=axons,
                        synapse=ComfySupport(),
                        deserialize=True,
                        timeout=5,
                    ),
                    timeout=10,
                )

                for i, response in enumerate(responses):
                    if i >= len(miner_uids):
                        break
                    uid = miner_uids[i]
                    if response and getattr(response, "supports_comfy", False):
                        comfy_supporters.add(uid)

                bt.logging.debug(f"Miners {sorted(comfy_supporters)} support ComfyUI")

        except Exception as e:
            bt.logging.error(f"Error querying comfy support: {str(e)}")

        return comfy_supporters

    def get_comfy_support_miners(self) -> List[int]:
        """
        Get list of miners that support ComfyUI

        Returns:
            List of miner UIDs that support ComfyUI
        """
        available = self.get_available_miners_cache()
        return [uid for uid in available if uid in self.comfy_support_miners]
