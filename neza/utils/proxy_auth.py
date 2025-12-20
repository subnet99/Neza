import hashlib
import hmac
import time
import base64
import json
from typing import Dict, Any
import secrets

import bittensor as bt


class ProxyAuthManager:
    """
    Simple proxy authentication manager for ComfyUI proxy connections
    Uses base64 encoded credentials stored in a JSON file
    """

    def __init__(self, secret_key: str = None):
        """
        Initialize proxy auth manager

        Args:
            secret_key: Secret key for credential generation
        """
        if not secret_key:
            self.secret_key = secrets.token_hex(32)
            bt.logging.warning(
                "⚠️ SECURITY WARNING: No secret key provided, generated random key. "
                "This is insecure for production! Please set a strong random secret key."
            )
        elif secret_key == "default_proxy_secret":
            self.secret_key = secret_key
            bt.logging.warning(
                "⚠️ SECURITY WARNING: Using default secret key. "
                "This is insecure! Please set a strong random secret key."
            )
        else:
            self.secret_key = secret_key

    def generate_simple_credential(self, task_id: str) -> str:
        """
        Generate simple base64 encoded credential

        Args:
            task_id: Task ID

        Returns:
            str: Base64 encoded credential
        """
        # Credential: task_id + timestamp + hmac_sha256(task_id + timestamp, secret_key)
        timestamp = int(time.time())
        message = f"{task_id}:{timestamp}"
        signature = hmac.new(
            self.secret_key.encode(), message.encode(), hashlib.sha256
        ).hexdigest()[:16]

        credential_data = f"{task_id}:{timestamp}:{signature}"
        return base64.b64encode(credential_data.encode()).decode()

    def verify_credential(
        self, task_id: str, credential: str, max_age_seconds: int = 3600
    ) -> bool:
        """
        Verify credential

        Args:
            task_id: Task ID
            credential: Base64 encoded credential
            max_age_seconds: Maximum age of credential in seconds
        Returns:
            bool: True if credential is valid
        """
        try:
            decoded = base64.b64decode(credential.encode()).decode()
            parts = decoded.split(":")

            if len(parts) != 3:
                return False

            cred_task_id, timestamp, signature = parts

            if cred_task_id != task_id:
                return False

            cred_timestamp = int(timestamp)
            if int(time.time()) - cred_timestamp > max_age_seconds:
                return False

            message = f"{task_id}:{timestamp}"
            expected_signature = hmac.new(
                self.secret_key.encode(), message.encode(), hashlib.sha256
            ).hexdigest()[:16]

            return hmac.compare_digest(signature, expected_signature)

        except Exception as e:
            bt.logging.error(f"Error verifying credential: {str(e)}")
            return False

    def create_simple_proxy_connection_info(
        self, task_id: str, client_id: str = None
    ) -> Dict[str, Any]:
        """
        Create simple proxy connection information

        Args:
            task_id: Task ID
            client_id: WebSocket client ID for ComfyUI connection

        Returns:
            Dict containing proxy connection info
        """
        credential = self.generate_simple_credential(task_id)
        timestamp = int(time.time())

        return {
            "task_id": task_id,
            "credential": credential,
            "timestamp": timestamp,
            "expires_at": timestamp + 3600,
            "client_id": client_id,
        }
