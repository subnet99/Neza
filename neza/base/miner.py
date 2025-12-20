import time
import asyncio
import threading
import argparse
import traceback

import bittensor as bt

from neza.base.neuron import BaseNeuron
from neza.utils.config import add_miner_args

from typing import Union


class BaseMinerNeuron(BaseNeuron):
    """
    Base class for Bittensor miners.
    """

    neuron_type: str = "MinerNeuron"

    @classmethod
    def add_args(cls, parser: argparse.ArgumentParser):
        super().add_args(parser)
        add_miner_args(cls, parser)

    def __init__(self, config=None):
        super().__init__(config=config)

        # Warn if allowing incoming requests from anyone.
        if not self.config.blacklist.force_validator_permit:
            bt.logging.warning(
                "You are allowing non-validators to send requests to your miner. This is a security risk."
            )
        if self.config.blacklist.allow_non_registered:
            bt.logging.warning(
                "You are allowing non-registered entities to send requests to your miner. This is a security risk."
            )
        # The axon handles request processing, allowing validators to send this miner requests.
        self.axon = bt.Axon(
            wallet=self.wallet,
            config=self.config() if callable(self.config) else self.config,
        )

        # Attach determiners which functions are called when servicing a request.
        bt.logging.info(f"Attaching forward function to miner axon.")
        self.axon.attach(
            forward_fn=self.forward,
            blacklist_fn=self.blacklist,
            priority_fn=self.priority,
        )
        bt.logging.info(f"Axon created: {self.axon}")

        # Instantiate runners
        self.should_exit: bool = False
        self.is_running: bool = False
        self.thread: Union[threading.Thread, None] = None
        self.lock = asyncio.Lock()

        # Add local cache to track last synced block
        self._last_local_update_block = 0
        self._min_sync_interval = 10  # Minimum sync interval (seconds)

    def run(self):
        """
        Initiates and manages the main loop for the miner on the Bittensor network. The main loop handles graceful shutdown on keyboard interrupts and logs unforeseen errors.

        This function performs the following primary tasks:
        1. Check for registration on the Bittensor network.
        2. Starts the miner's axon, making it active on the network.
        3. Periodically resynchronizes with the chain; updating the metagraph with the latest network state and setting weights.

        The miner continues its operations until `should_exit` is set to True or an external interruption occurs.
        During each epoch of its operation, the miner waits for new blocks on the Bittensor network, updates its
        knowledge of the network (metagraph), and sets its weights. This process ensures the miner remains active
        and up-to-date with the network's latest state.

        Note:
            - The function leverages the global configurations set during the initialization of the miner.
            - The miner's axon serves as its interface to the Bittensor network, handling incoming and outgoing requests.

        Raises:
            KeyboardInterrupt: If the miner is stopped by a manual interruption.
            Exception: For unforeseen errors during the miner's operation, which are logged for diagnosis.
        """

        # Check that miner is registered on the network.
        self.sync()
        # Initialize local update block to current block
        self._last_local_update_block = self.block

        # Serve passes the axon information to the network + netuid we are hosting on.
        # This will auto-update if the axon port of external ip have changed.
        bt.logging.info(
            f"Serving miner axon {self.axon} on network: {self.config.subtensor.chain_endpoint} with netuid: {self.config.netuid}"
        )
        self.axon.serve(netuid=self.config.netuid, subtensor=self.subtensor)

        # Start  starts the miner's axon, making it active on the network.
        self.axon.start()

        bt.logging.info(f"Miner starting at block: {self.block}")

        # Record last synced block to prevent multiple syncs in the same block
        last_sync_block = self.block
        # Record last sync timestamp
        last_sync_time = time.time()

        # This loop maintains the miner's operations until intentionally stopped.
        try:
            bt.logging.info(f"Initial state - self.should_exit:{self.should_exit}")
            bt.logging.info(
                f"Initial state - self.block:{self.block} self.uid:{self.uid} self.metagraph.last_update[self.uid]:{self.metagraph.last_update[self.uid]}"
            )
            bt.logging.info(
                f"Initial state - self._last_local_update_block:{self._last_local_update_block}"
            )

            while not self.should_exit:
                try:
                    current_block = self.block
                except Exception as e:
                    bt.logging.error("Failed to get current block")
                    time.sleep(10)
                    continue

                current_time = time.time()

                # Use locally cached last update block instead of relying on metagraph.last_update
                block_diff = current_block - self._last_local_update_block
                time_diff = current_time - last_sync_time

                # Log sync status
                if self.step % 10 == 0:
                    bt.logging.debug(
                        f"Sync status: block_diff={block_diff}, time_diff={time_diff:.1f}s, epoch_length={self.config.neuron.epoch_length}"
                    )
                    bt.logging.debug(
                        f"Current block: {current_block}, Last local update: {self._last_local_update_block}, Last metagraph update: {self.metagraph.last_update[self.uid]}"
                    )

                # Check if sync is needed (any of the following conditions):
                # 1. Block difference greater than epoch_length
                # 2. Time difference greater than 60 seconds (but at least exceeding minimum sync interval)
                needs_sync = block_diff > self.config.neuron.epoch_length or (
                    time_diff > 60 and time_diff > self._min_sync_interval
                )

                # Prevent multiple syncs in the same block
                if current_block == last_sync_block:
                    needs_sync = False

                if needs_sync:
                    # Perform sync
                    bt.logging.info(
                        f"Syncing at block {current_block}, last local update was at block {self._last_local_update_block}"
                    )
                    self.sync()
                    self.step += 1

                    # Update our local cache regardless of whether metagraph.last_update is updated
                    self._last_local_update_block = current_block
                    bt.logging.info(f"Updated local sync block to {current_block}")

                    # Update sync timestamp and block
                    last_sync_time = time.time()
                    last_sync_block = current_block

                    # Prevent too frequent sync attempts
                    time.sleep(self._min_sync_interval / 2)
                else:
                    # Wait for a while before checking again
                    wait_time = min(5, max(1, self.config.neuron.epoch_length / 10))
                    time.sleep(wait_time)

        # If someone intentionally stops the miner, it'll safely terminate operations.
        except KeyboardInterrupt:
            self.axon.stop()
            bt.logging.success("Miner killed by keyboard interrupt.")
            exit()

        # In case of unforeseen errors, the miner will log the error and continue operations.
        except Exception as e:
            bt.logging.error(traceback.format_exc())

    def resync_metagraph(self):
        """Resyncs the metagraph and updates the hotkeys and moving averages based on the new metagraph."""
        if bt.logging.get_level() <= 10:  # DEBUG level is usually 10
            bt.logging.info("resync_metagraph()")
        else:
            bt.logging.trace("resync_metagraph()")

        # Record last_update value before sync
        pre_sync = None
        if hasattr(self.metagraph, "last_update") and self.uid < len(
            self.metagraph.last_update
        ):
            pre_sync = self.metagraph.last_update[self.uid]

        # Sync the metagraph.
        self.metagraph.sync(subtensor=self.subtensor)

        # Record last_update value after sync to confirm if it was updated
        if (
            pre_sync is not None
            and hasattr(self.metagraph, "last_update")
            and self.uid < len(self.metagraph.last_update)
        ):
            post_sync = self.metagraph.last_update[self.uid]
            if post_sync <= pre_sync:
                # If metagraph.last_update wasn't updated, log a warning but don't try to force update
                # We'll rely on the locally cached _last_local_update_block
                bt.logging.warning(
                    f"Metagraph last_update not updated during sync ({pre_sync}->{post_sync}). Using local tracking instead."
                )

    def run_in_background_thread(self):
        """
        Starts the miner's operations in a separate background thread.
        This is useful for non-blocking operations.
        """
        if not self.is_running:
            bt.logging.debug("Starting miner in background thread.")
            self.should_exit = False
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
            self.is_running = True
            bt.logging.debug("Started")

    def stop_run_thread(self):
        """
        Stops the miner's operations that are running in the background thread.
        """
        if self.is_running:
            bt.logging.debug("Stopping miner in background thread.")
            self.should_exit = True
            if self.thread is not None:
                self.thread.join(5)
            self.is_running = False
            bt.logging.debug("Stopped")

    def __enter__(self):
        """
        Starts the miner's operations in a background thread upon entering the context.
        This method facilitates the use of the miner in a 'with' statement.
        """
        self.run_in_background_thread()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Stops the miner's background operations upon exiting the context.
        This method facilitates the use of the miner in a 'with' statement.

        Args:
            exc_type: The type of the exception that caused the context to be exited.
                      None if the context was exited without an exception.
            exc_value: The instance of the exception that caused the context to be exited.
                       None if the context was exited without an exception.
            traceback: A traceback object encoding the stack trace.
                       None if the context was exited without an exception.
        """
        self.stop_run_thread()
