import threading
import websocket
import bittensor as bt
from typing import Callable, Any, Dict
from neza.utils.throttled_handler import ThrottledHandler


class WebSocketManager:
    """
    WebSocket connection manager with automatic reconnection
    """

    def __init__(
        self,
        server_id: str,
        host: str,
        port: str,
        ws_url: str,
        client_id: str,
        on_message_callback: Callable,
        on_status_change_callback: Callable = None,
    ):
        """
        Initialize WebSocket manager

        Args:
            server_id: Server identifier
            host: Server host
            port: Server port
            ws_url: WebSocket URL
            client_id: Client identifier
            on_message_callback: Callback for handling messages
            on_status_change_callback: Callback for connection status changes
        """
        self.server_id = server_id
        self.host = host
        self.port = port
        self.client_id = client_id
        self.on_message_callback = on_message_callback
        self.on_status_change_callback = on_status_change_callback

        self.ws = None
        self.ws_url = ws_url
        self.ws_thread = None
        self.connected = False
        self.running = False
        self.reconnect_delay = 5.0
        self.max_reconnect_attempts = 10
        self.reconnect_attempts = 0
        self.throttler = ThrottledHandler(throttle_seconds=1.0)

    def start(self):
        """Start WebSocket connection"""
        self.running = True
        self._connect()

    def stop(self):
        """Stop WebSocket connection"""
        self.running = False
        self._disconnect()

    def _connect(self):
        """Establish WebSocket connection"""
        if not self.running:
            return

        try:

            self.ws = websocket.WebSocketApp(
                self.ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )

            self.ws_thread = threading.Thread(target=self._run_forever, daemon=True)
            self.ws_thread.start()

            bt.logging.info(f"Started WebSocket connection for server {self.port}")

        except Exception as e:
            bt.logging.error(
                f"Error starting WebSocket connection for server {self.port}: {str(e)}"
            )
            self._schedule_reconnect()

    def _run_forever(self):
        """Run WebSocket in a separate thread"""
        try:
            self.ws.run_forever()
        except Exception as e:
            bt.logging.error(
                f"WebSocket run_forever error for server {self.port}: {str(e)}"
            )

    def _disconnect(self):
        """Disconnect WebSocket"""
        if self.ws:
            try:
                self.ws.close()
            except:
                pass
            self.ws = None
        self.connected = False

    def _on_open(self, ws):
        """WebSocket connection opened"""
        self.connected = True
        self.reconnect_attempts = 0
        bt.logging.info(f"WebSocket connected to server {self.port}")

        if self.on_status_change_callback:
            self.on_status_change_callback(self.server_id, True)

    def _on_message(self, ws, message):
        """Handle WebSocket message"""
        try:
            # Skip binary messages
            if isinstance(message, bytes):
                bt.logging.debug(f"Skipping binary message from server {self.port}")
                return
            elif not isinstance(message, str):
                bt.logging.debug(f"Skipping non-string message from server {self.port}")
                return

            self.on_message_callback(message, self.server_id, self.throttler)
        except Exception as e:
            bt.logging.error(
                f"Error in message callback for server {self.port}: {str(e)}"
            )

    def _on_error(self, ws, error):
        """WebSocket error"""
        self.connected = False
        bt.logging.error(f"WebSocket error for server {self.port}: {str(error)}")

        if self.on_status_change_callback:
            self.on_status_change_callback(self.server_id, False)

    def _on_close(self, ws, close_status_code, close_msg):
        """WebSocket connection closed"""
        self.connected = False
        bt.logging.warning(f"WebSocket connection closed for server {self.port}")

        if self.on_status_change_callback:
            self.on_status_change_callback(self.server_id, False)

        if self.running:
            self._schedule_reconnect()

    def _schedule_reconnect(self):
        """Schedule reconnection attempt"""
        if not self.running or self.reconnect_attempts >= self.max_reconnect_attempts:
            return

        self.reconnect_attempts += 1
        delay = self.reconnect_delay * (
            2 ** (self.reconnect_attempts - 1)
        )  # Exponential backoff

        bt.logging.info(
            f"Scheduling reconnection for server {self.port} in {delay:.1f}s (attempt {self.reconnect_attempts})"
        )
        threading.Timer(delay, self._connect).start()

    def is_connected(self) -> bool:
        """Check if WebSocket is connected"""
        return self.connected and self.ws is not None

    def send_message(self, message: str) -> bool:
        """Send message through WebSocket"""
        if not self.is_connected():
            return False

        try:
            self.ws.send(message)
            return True
        except Exception as e:
            bt.logging.error(f"Error sending message to server {self.port}: {str(e)}")
            return False

    def handle_throttled(self, key: str, data: Any, handler_func: Callable) -> None:
        """
        Handle message with throttling

        Args:
            key: Unique key for throttling
            data: Data to process
            handler_func: Function to call after throttling
        """
        self.throttler.handle(key, data, handler_func)

    def cleanup(self):
        """Cleanup resources"""
        self.stop()
        if hasattr(self, "throttler"):
            self.throttler.cleanup()
