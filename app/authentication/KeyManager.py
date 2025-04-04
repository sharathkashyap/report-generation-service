import os
import base64
import logging
from cryptography.hazmat.primitives.serialization import load_der_public_key
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

logger = logging.getLogger(__name__)

class KeyManager:
    key_map = {}

    @staticmethod
    def init(base_path):
        try:
            for root, _, files in os.walk(base_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            content = f.read()
                            public_key = KeyManager.load_public_key(content)
                            KeyManager.key_map[file] = public_key
                    except Exception as e:
                        logger.error(f"KeyManager:init: exception in reading public key from {file_path}", exc_info=e)
        except Exception as e:
            logger.error("KeyManager:init: exception in loading public keys", exc_info=e)

    @staticmethod
    def get_public_key(key_id):
        return KeyManager.key_map.get(key_id)

    @staticmethod
    def load_public_key(key):
        try:
            logger.debug(f"The public key is {key}")
            public_key = key.replace("-----BEGIN PUBLIC KEY-----", "").replace("-----END PUBLIC KEY-----", "").strip()
            key_bytes = base64.b64decode(public_key)
            return load_der_public_key(key_bytes)
        except Exception as e:
            logger.error("KeyManager:load_public_key: exception in loading public key", exc_info=e)
            raise
