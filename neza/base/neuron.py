import copy
import typing
import threading
import asyncio
import inspect
import time
from typing import Callable, List, Dict, Any, Optional

import bittensor as bt
import traceback

from abc import ABC, abstractmethod

# Sync calls set weights and also resyncs the metagraph.
from neza.utils.config import check_config, add_args, config
from neza.utils.misc import ttl_get_block
from neza import __spec_version__ as spec_version
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException


class BaseNeuron(ABC):
    """
    Base class for Bittensor miners. This class is abstract and should be inherited by a subclass. It contains the core logic for all neurons; validators and miners.

    In addition to creating a wallet, subtensor, and metagraph, this class also handles the synchronization of the network state via a basic checkpointing mechanism based on epoch length.
    """

    neuron_type: str = "BaseNeuron"

    # Bittensor related properties
    subtensor: "bt.subtensor"
    wallet: "bt.wallet"
    metagraph: "bt.metagraph"
    spec_version: int = spec_version

    # Block callback related properties
    block_callbacks: List[Callable] = []
    substrate_thread: Optional[threading.Thread] = None
    substrate: Optional[SubstrateInterface] = None

    # Substrate configuration constants
    SUBSTRATE_SS58_FORMAT = 42  # Bittensor SS58 format
    SUBSTRATE_TYPE_REGISTRY = None  # Use default type registry

    @classmethod
    def check_config(cls, config: "bt.Config"):
        check_config(cls, config)

    @classmethod
    def add_args(cls, parser):
        add_args(cls, parser)

    @classmethod
    def config(cls):
        return config(cls)

    @property
    def block(self):
        return ttl_get_block(self)

    def __init__(self, config=None):
        base_config = copy.deepcopy(config or BaseNeuron.config())
        self.config = self.config()
        self.config.merge(base_config)
        self.check_config(self.config)

        # Initialize block callback list
        self.block_callbacks = []

        # Set up logging with the provided configuration.
        bt.logging.set_config(config=self.config.logging)

        # If a gpu is required, set the device to cuda:N (e.g. cuda:0)
        self.device = self.config.neuron.device

        # Log the configuration for reference.
        bt.logging.info(self.config)

        # Build Bittensor objects
        # These are core Bittensor classes to interact with the network.
        bt.logging.info("Setting up bittensor objects.")

        # The wallet holds the cryptographic key pairs for the miner.

        self.wallet = bt.wallet(config=self.config)
        self.subtensor = bt.subtensor(config=self.config)
        self.metagraph = self.subtensor.metagraph(self.config.netuid)

        bt.logging.info(f"Wallet: {self.wallet}")
        bt.logging.info(f"Subtensor: {self.subtensor}")
        bt.logging.info(f"Metagraph: {self.metagraph}")

        # Check if the miner is registered on the Bittensor network before proceeding further.
        self.check_registered()

        # Each miner gets a unique identity (UID) in the network for differentiation.
        self.uid = self.metagraph.hotkeys.index(self.wallet.hotkey.ss58_address)
        bt.logging.info(
            f"Running neuron on subnet: {self.config.netuid} with uid {self.uid} using network: {self.subtensor.chain_endpoint}"
        )
        self.step = 0

        # Add basic block callback functions
        self.block_callbacks.append(self.maybe_sync_metagraph)

        # Initialize block subscription
        self._initialize_block_subscription()

    def _initialize_block_subscription(self):
        """Initialize block subscription, create SubstrateInterface and start subscription thread"""
        try:
            self.substrate = SubstrateInterface(
                ss58_format=self.SUBSTRATE_SS58_FORMAT,
                use_remote_preset=True,
                url=self.config.subtensor.chain_endpoint,
                type_registry=self.SUBSTRATE_TYPE_REGISTRY,
            )
            self.substrate_thread = self._start_block_subscription_thread()
            bt.logging.info("Block subscription started in background thread")
        except Exception as e:
            bt.logging.error(f"Failed to create SubstrateInterface: {str(e)}")
            self.substrate = None
            self.substrate_thread = None

    def register_block_callback(self, callback: Callable):
        """
        Register a block callback function that will be called when each new block is produced

        Args:
            callback: Callback function that receives block number as parameter
        """
        if callback not in self.block_callbacks:
            self.block_callbacks.append(callback)
            bt.logging.debug(f"Registered block callback: {callback.__name__}")

    def unregister_block_callback(self, callback: Callable):
        """
        Unregister a block callback function

        Args:
            callback: Callback function to unregister
        """
        if callback in self.block_callbacks:
            self.block_callbacks.remove(callback)
            bt.logging.debug(f"Unregistered block callback: {callback.__name__}")

    def _create_block_handler(self):
        """Create block subscription handler"""

        def handler(obj, update_nr, _):
            try:
                block_number = obj["header"]["number"]

                if update_nr >= 1:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    return loop.run_until_complete(self._run_callbacks(block_number))
            except Exception as e:
                bt.logging.error(f"Block subscription handler error: {str(e)}")

        return handler

    def _start_block_subscription(self):
        """Start block subscription"""
        try:
            if self.substrate:
                return self.substrate.subscribe_block_headers(
                    self._create_block_handler()
                )
        except Exception as e:
            bt.logging.error(f"Failed to start block subscription: {str(e)}")
            return None

    def _start_block_subscription_thread(self):
        """Create and start block subscription thread"""
        subscription_thread = threading.Thread(
            target=self._start_block_subscription,
            daemon=True,
            name="BlockSubscriptionThread",
        )
        subscription_thread.start()
        return subscription_thread

    async def _run_callbacks(self, block):
        """Run all registered callback functions"""
        for callback in self.block_callbacks:
            try:
                res = callback(block)
                if inspect.isawaitable(res):
                    await res
            except Exception as e:
                bt.logging.error(
                    f"Failed to run callback {callback.__name__}: {str(e)}"
                )
                bt.logging.error(traceback.format_exc())

    def maybe_sync_metagraph(self, block):
        """
        Default block callback - sync metagraph

        Args:
            block: Current block number

        Returns:
            bool: Whether sync was performed
        """
        # Defensive check to ensure epoch_length has a value
        if (
            not hasattr(self.config.neuron, "epoch_length")
            or self.config.neuron.epoch_length is None
        ):
            self.config.neuron.epoch_length = 100  # Set default value
            bt.logging.warning(
                "neuron.epoch_length was None, setting default value to 100"
            )

        # Only execute at the end of each epoch
        if block % self.config.neuron.epoch_length != 0:
            return False

        # Ensure miner or validator hotkey is still registered on the network
        self.check_registered()
        bt.logging.info("Resyncing metagraph")
        self.metagraph.sync(subtensor=self.subtensor)
        return True

    def register_all_block_callbacks(self):
        """
        This method is kept for backward compatibility but is no longer needed to be called explicitly
        """
        bt.logging.info(
            "Block callbacks are now handled automatically by internal mechanism"
        )

    @abstractmethod
    async def forward(self, synapse: bt.Synapse) -> bt.Synapse: ...

    @abstractmethod
    def run(self): ...

    def sync(self):
        """
        Wrapper for synchronizing the state of the network for the given miner or validator.
        """
        # Ensure miner or validator hotkey is still registered on the network.
        self.check_registered()

        if self.should_sync_metagraph():
            self.resync_metagraph()

        if self.should_set_weights():
            self.set_weights()

        # Always save state.
        self.save_state()

    def check_registered(self):
        # --- Check for registration.
        if not self.subtensor.is_hotkey_registered(
            netuid=self.config.netuid,
            hotkey_ss58=self.wallet.hotkey.ss58_address,
        ):
            bt.logging.error(
                f"Wallet: {self.wallet} is not registered on netuid {self.config.netuid}."
                f" Please register the hotkey using `btcli subnets register` before trying again"
            )
            exit()

    def should_sync_metagraph(self):
        """
        Check if enough epoch blocks have elapsed since the last checkpoint to sync.
        """
        # Defensive check to ensure epoch_length has a value
        if (
            not hasattr(self.config.neuron, "epoch_length")
            or self.config.neuron.epoch_length is None
        ):
            self.config.neuron.epoch_length = 100  # Set default value
            bt.logging.warning(
                "neuron.epoch_length was None, setting default value to 100"
            )

        return (
            self.block - self.metagraph.last_update[self.uid]
        ) > self.config.neuron.epoch_length

    def should_set_weights(self) -> bool:
        # Don't set weights on initialization.
        if self.step == 0:
            return False

        # Check if enough epoch blocks have elapsed since the last epoch.
        if self.config.neuron.disable_set_weights:
            return False

        # Defensive check to ensure epoch_length has a value
        if (
            not hasattr(self.config.neuron, "epoch_length")
            or self.config.neuron.epoch_length is None
        ):
            self.config.neuron.epoch_length = 100  # Set default value
            bt.logging.warning(
                "neuron.epoch_length was None, setting default value to 100"
            )

        # Define appropriate logic for when set weights.
        return (
            self.block - self.metagraph.last_update[self.uid]
        ) > self.config.neuron.epoch_length and self.neuron_type != "MinerNeuron"  # don't set weights if you're a miner

    def save_state(self):
        bt.logging.trace(
            "save_state() not implemented for this neuron. You can implement this function to save model checkpoints or other useful data."
        )

    def load_state(self):
        bt.logging.trace(
            "load_state() not implemented for this neuron. You can implement this function to load model checkpoints or other useful data."
        )
