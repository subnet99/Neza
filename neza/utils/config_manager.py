import bittensor as bt

from neza.utils.http import adjust_config
from neza.utils.tools import merge_config


class ConfigManager:
    """Config Manager Class"""

    def __init__(self, validator):
        """
        Initialize config manager
        """
        # Configuration file cache
        self.validator = validator
        self.config_cache = {}

    def update_config(self):
        """
        Update config
        """
        config = adjust_config(self.validator.wallet)

        if config:
            merge_config(self.validator.validator_config, config)
