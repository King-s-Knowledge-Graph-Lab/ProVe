import importlib


def _auth_module():
    return importlib.import_module("prove_shared.auth")


def test_serialize_returns_base64_string():
    AsyncAuth = _auth_module().AsyncAuth
    value = AsyncAuth.serialize(b"abc")
    assert value == "YWJj"


def test_encrypt_then_decrypt_returns_original_message(monkeypatch):
    auth_module = _auth_module()
    AsyncAuth = auth_module.AsyncAuth

    rsa = auth_module.rsa
    default_backend = auth_module.default_backend

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())
    public_key = private_key.public_key()

    monkeypatch.setattr(AsyncAuth, "get_private_key", classmethod(lambda cls: private_key))

    encrypted = AsyncAuth.encrypt(public_key, "hello")
    decrypted = AsyncAuth.decrypt(encrypted)

    assert isinstance(encrypted, bytes)
    assert decrypted == "hello"


def test_is_valid_returns_true_when_decrypted_key_matches(monkeypatch):
    AsyncAuth = _auth_module().AsyncAuth
    monkeypatch.setattr(AsyncAuth, "decrypt", classmethod(lambda cls, payload: "test-api-key"))

    assert AsyncAuth.is_valid(b"payload") is True


def test_is_valid_returns_false_on_decrypt_value_error(monkeypatch):
    AsyncAuth = _auth_module().AsyncAuth

    def _raise(_cls, _payload):
        raise ValueError("bad payload")

    monkeypatch.setattr(AsyncAuth, "decrypt", classmethod(_raise))

    assert AsyncAuth.is_valid(b"payload") is False
