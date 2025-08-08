import threading
import time
import bittensor as bt
from typing import Callable, Any


class ThrottledHandler:
    """Throttled handler for limiting message processing frequency"""

    def __init__(self, throttle_seconds: float = 1.0):
        self.throttle_seconds = throttle_seconds
        self.pending_data = {}  # {key: (data, timestamp)}
        self.lock = threading.Lock()
        self.timers = {}  # {key: timer}

    def handle(self, key: str, data: Any, handler_func: Callable) -> None:
        """
        Throttling processing: If requests with the same key occur within the throttling time, execution is delayed.
        """
        with self.lock:
            current_time = time.time()

            # Store the latest data.
            self.pending_data[key] = (data, current_time)

            # If there is an existing timer, cancel it.
            if key in self.timers:
                self.timers[key].cancel()

            # Create a new timer
            timer = threading.Timer(
                self.throttle_seconds, self._execute_delayed, args=[key, handler_func]
            )
            self.timers[key] = timer
            timer.start()

    def _execute_delayed(self, key: str, handler_func: Callable) -> None:
        """Delay execution processing function"""
        with self.lock:
            if key in self.pending_data:
                data, timestamp = self.pending_data[key]
                current_time = time.time()

                # Execute the processing function (data is always valid because we execute after the throttle time)
                try:
                    handler_func(data)
                except Exception as e:
                    bt.logging.error(
                        f"Error in throttled handler for key {key}: {str(e)}"
                    )

                # Clean up
                del self.pending_data[key]
                if key in self.timers:
                    del self.timers[key]

    def cleanup(self):
        """Clean up all timers and data"""
        with self.lock:
            for timer in self.timers.values():
                timer.cancel()
            self.timers.clear()
            self.pending_data.clear()
