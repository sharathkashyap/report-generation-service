import base64
import json
import logging
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import load_der_public_key
from datetime import datetime
from app.authentication.KeyManager import KeyManager
from constants import SUNBIRD_SSO_URL, SUNBIRD_SSO_REALM
from app.authentication.CryptoUtil import CryptoUtil
logger = logging.getLogger(__name__)

class AccessTokenValidator:
    @staticmethod
    def validate_token(token, check_active):
        try:
            header, payload, signature = token.split(".")
            decoded_header = json.loads(base64.urlsafe_b64decode(header + "==").decode("utf-8"))
            decoded_payload = json.loads(base64.urlsafe_b64decode(payload + "==").decode("utf-8"))
            key_id = decoded_header.get("kid")
            public_key_pem = KeyManager.get_public_key(key_id)  # Fetch public key from KeyManager

            # Debug log for the signature before decoding
            logger.debug(f"Raw signature before decoding: {signature}")

            # Add padding to the signature if necessary
            signature += '=' * (-len(signature) % 4)

            # Debug log for the signature after padding
            logger.debug(f"Padded signature: {signature}")

            signature_bytes = base64.urlsafe_b64decode(signature)
            
            # Verify the signature directly without using CryptoUtil
            try:
                public_key_pem.verify(
                    signature_bytes,
                    (header + "." + payload).encode('utf-8'),
                    padding.PKCS1v15(),
                    hashes.SHA256()
                )
            except Exception as e:
                logger.error("Invalid RSA signature", exc_info=e)
                return {}

            if check_active and AccessTokenValidator.is_expired(decoded_payload.get("exp")):
                return {}

            return decoded_payload
        except Exception as ex:
            logger.error("Exception in AccessTokenValidator: validate_token", exc_info=ex)
            return {}

    @staticmethod
    def verify_user_token(token, check_active):
        logger.debug("Inside the verify_user_token method")
        user_id = "UNAUTHORIZED"
        try:
            payload = AccessTokenValidator.validate_token(token, check_active)
            logger.debug(f"The token body is {json.dumps(payload)}")
            if payload and AccessTokenValidator.check_iss(payload.get("iss")):
                user_id = payload.get("sub", "UNAUTHORIZED")
                if user_id:
                    user_id = user_id.split(":")[-1]
        except Exception as ex:
            logger.error("Exception in AccessTokenValidator: verify_user_token", exc_info=ex)
        return user_id

    @staticmethod
    def verify_user_token_get_org(token, check_active):
        logger.debug("Inside the verify_user_token method")
        org_id = ""
        try:
            payload = AccessTokenValidator.validate_token(token, check_active)
            logger.debug(f"The token body is {json.dumps(payload)}")
            if payload and AccessTokenValidator.check_iss(payload.get("iss")):
                org_id = payload.get("org", "")
        except Exception as ex:
            logger.error("Exception in AccessTokenValidator: verify_user_token", exc_info=ex)
        return org_id

    @staticmethod
    def check_iss(iss):
        realm_url = f"{SUNBIRD_SSO_URL}realms/{SUNBIRD_SSO_REALM}"
        logger.info(f"The realm URL is {realm_url}")
        return realm_url.lower() == iss.lower()

    @staticmethod
    def is_expired(expiration):
        return datetime.now().timestamp() > expiration
