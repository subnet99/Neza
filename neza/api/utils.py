"""
API utility functions
"""

import bittensor as bt

# Global validator instance
_validator = None


def set_validator(validator):
    """
    Set the global validator instance

    Args:
        validator: Validator instance
    """
    global _validator
    _validator = validator
    bt.logging.info("API: Global validator instance has been set")


def get_validator():
    """
    Get the global validator instance

    Returns:
        Validator instance, or None if not set
    """
    global _validator
    return _validator
