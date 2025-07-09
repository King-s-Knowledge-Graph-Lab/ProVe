from base64 import b64encode
import os
from typing import Union

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.primitives.serialization import load_pem_public_key

from .local_secrets import API_KEY, PRIVATE_KEY


class AsyncAuth:
    """
    A class to handle RSA encryption and decryption for API key authentication.
    It provides methods to generate, save, load, encrypt, decrypt, and validate API keys.
    """

    @classmethod
    def get_private_key(cls) -> rsa.RSAPrivateKeyWithSerialization:
        if not os.path.exists(PRIVATE_KEY):
            os.makedirs("/".join(PRIVATE_KEY.split("/")[:-1]), exist_ok=True)
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
                backend=default_backend()
            )
            cls.save_key(private_key)
            return private_key
        with open(PRIVATE_KEY, "rb") as key_file:
            pemlines = key_file.read()
        return cls.load_key(pemlines, True)

    @classmethod
    def get_public_key(cls, serialize: bool = True) -> rsa.RSAPublicKeyWithSerialization:
        key = cls.get_private_key()
        if not serialize:
            return key.public_key()
        return key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode()

    @classmethod
    def encrypt(cls, key: rsa.RSAPublicKeyWithSerialization, message: bytes) -> bytes:
        if isinstance(message, str):
            message = message.encode("utf-8")

        return key.encrypt(
            message,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=rsa.hashes.SHA256()),
                algorithm=rsa.hashes.SHA256(),
                label=None
            )
        )

    @classmethod
    def decrypt(cls, message: bytes) -> str:
        return cls.get_private_key().decrypt(
            message,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=rsa.hashes.SHA256()),
                algorithm=rsa.hashes.SHA256(),
                label=None
            )
        ).decode()

    @classmethod
    def serialize(cls, message: bytes) -> str:
        return b64encode(message).decode()

    @classmethod
    def save_key(cls, pk) -> None:
        pem = pk.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        )
        with open(PRIVATE_KEY, 'wb') as pem_out:
            pem_out.write(pem)

    @classmethod
    def load_key(cls, key: bytes, private: bool = True) -> Union[
        rsa.RSAPublicKeyWithSerialization,
        rsa.RSAPrivateKeyWithSerialization,
    ]:
        if private:
            return load_pem_private_key(key, None, default_backend())
        return load_pem_public_key(key, default_backend())

    @classmethod
    def is_valid(cls, api_key: bytes) -> bool:
        if not isinstance(api_key, bytes):
            api_key = api_key.encode("utf-8")

        try:
            api_key = cls.decrypt(api_key)
        except ValueError:
            return False

        return api_key == API_KEY
