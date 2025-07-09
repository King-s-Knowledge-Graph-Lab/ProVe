import requests
import uuid
import hashlib

from local_secrets import ENDPOINT, API_KEY
from auth import AsyncAuth

class QueueManager:
    def __init__(self, queue_name: str):
        self.public_key = self.get_key()
        raw_uuid = str(uuid.uuid4()).encode('utf-8')
        self.uuid = hashlib.sha256(raw_uuid).hexdigest()
        self.queue = 'random' if 'random_collection' else 'user'

    def get_key(self, retry_count: int = 0) -> str:
        try:
            return self.public_key
        except AttributeError:
            response = requests.get(f"{ENDPOINT}getKey", timeout=120)
            if response.status_code == 200:
                public_key = response.json().get('public key')
                return AsyncAuth.load_key(
                    public_key.encode('utf-8'),
                    private=False
                )
            else:
                retry_count += 1
                if retry_count < 5:
                    return self.get_key(retry_count)
                raise ConnectionError("Failed to retrieve public key from the server.")

    def get_next(self):
        payload = {
            "api_key": AsyncAuth.serialize(AsyncAuth.encrypt(self.public_key, API_KEY)),
            "uuid": self.uuid,
            'queue': self.queue,
            "Content-Type": "application/json"
        }
        response = requests.post(f"{ENDPOINT}getNextQueue", json=payload, timeout=120)
        if response.status_code == 200:
            return response.json()
        else:
            raise ConnectionError("Failed to retrieve the next item from the queue.")


if __name__ == "__main__":
    queue_manager = QueueManager("random_collection")
    try:
        next_item = queue_manager.get_next()
        print("Next item in the queue:", next_item)
    except ConnectionError as e:
        print("Error:", e)
    except Exception as e:
        print("An unexpected error occurred:", e)