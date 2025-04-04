from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

class CryptoUtil:
    @staticmethod
    def verify_rsa_sign(payload: str, signature: bytes, public_key: RSAPublicKey, algorithm: str) -> bool:
        try:
            # Choose the hash algorithm based on the input
            if algorithm.upper() == "SHA256withRSA":
                hash_algorithm = hashes.SHA256()
            elif algorithm.upper() == "SHA1withRSA":
                hash_algorithm = hashes.SHA1()
            else:
                raise ValueError("Unsupported algorithm")

            # Verify the signature
            public_key.verify(
                signature,
                payload.encode('utf-8'),
                padding.PKCS1v15(),
                hash_algorithm
            )
            return True
        except Exception as e:
            return False