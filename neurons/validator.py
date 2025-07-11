#!/usr/bin/env python3

import os
import sys
import time
import argparse
import traceback
from typing import List, Dict, Any, Optional

import bittensor as bt

from neurons.validator.core.validator import VideoValidator


if __name__ == "__main__":
    with VideoValidator() as validator:
        while True:
            bt.logging.info(f"Validator running... {time.time()}")
            time.sleep(60)
