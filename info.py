from collections import defaultdict
from api.custom_decorators import StatsDBHandler
import sys
from tqdm import tqdm
import time
import numpy as np

from api.local_secrets import CODE_PATH
from api.utils_api import get_ip_location

sys.path.append(CODE_PATH)
from pymongo import MongoClient
from ProVe_main_service import MongoDBHandler


class TMPStatsDBHandler(MongoDBHandler):
    def __init__(self, connection_string="mongodb://localhost:27017/", max_retries=3):
        super().__init__(connection_string, max_retries)

    def connect(self, max_retries, connection_string):
        for attempt in range(self.max_retries):
            try:
                self.client = MongoClient(self.connection_string)
                self.client.server_info()
                self.db = self.client['tmp_service_usage']
                self.usage_collection = self.db['usage']
                print("Successfully connected to StatsDB")
                return True
            except Exception as e:
                print(f"StatsDB connection attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries - 1:
                    raise ConnectionError("Failed to connect to StatsDB") from e
                time.sleep(5)  # Wait before retry

    def close(self):
        """Closes the MongoDB connection."""
        if self.client:
            self.client.close()
            print("MongoDB connection closed")

    def __enter__(self):
        """Enables use with 'with' statement."""
        self.connect()
        return self  # Allows access to the instance in 'with' block

    def __exit__(self, exc_type, exc_value, traceback):
        """Ensures the connection is closed when exiting 'with' block."""
        self.close()


if __name__ == "__main__":
    storage = StatsDBHandler()
    storage.connect(storage.max_retries, storage.connection_string)
    documents = storage.usage_collection.find()

    tmp_storage = TMPStatsDBHandler()
    tmp_storage.connect(tmp_storage.max_retries, tmp_storage.connection_string)

    def get_ip_location(ip: str) -> None:
        from urllib.request import urlopen
        import json
        url = 'https://geolocation-db.com/json/e2bfd850-e6d9-11ef-bc40-012fd2b64c41/' + ip
        # if res==None, check your internet connection
        res = urlopen(url)
        data = json.load(res)
        
        if "country_name" not in data.keys():
            raise KeyError()

        return {
            "country_code": data.get("country_code", None),
            "country_name": data.get("country_name", None),
            "city": data.get("city", None),
            "state": data.get("state", None),
            "latitude": data.get("latitude", None),
            "longitude": data.get("longitude", None),
        }

    def get_entry_by_info(entry: dict, dictionary: dict[int, dict[str, any]]) -> dict[str, any]:
        for key, value in dictionary.items():
            value.pop("hash", None)
            if value == entry:
                return key
        return None

    def get_entry_by_hash(entry: int, dictionary: dict[int, dict[str, any]]) -> dict[str, any]:
        for key, value in dictionary.items():
            if key == entry:
                return value
        return None

    count = [1 for _ in documents]
    count = sum(count)

    locations = defaultdict(lambda: defaultdict(int))
    documents = storage.usage_collection.find()
    for i, doc in enumerate(tqdm(documents, total=count)):
        try:
            request_type = doc['url'].split("api")[-1].split("?")[0]
            request_type = request_type.split("/")[-1]
            if request_type not in locations["request_type"]:
                locations["request_type"][request_type] = {
                    "count": 0,
                    "execution_time": [],
                    "min_execution_time": float('inf'),
                    "max_execution_time": float('-inf'),
                }
            locations["request_type"][request_type]["count"] += 1
            locations["request_type"][request_type]["execution_time"].append(doc.get("execution_time"))
            if doc.get("execution_time") < locations["request_type"][request_type]["min_execution_time"]:
                locations["request_type"][request_type]["min_execution_time"] = doc.get("execution_time")
            if doc.get("execution_time") > locations["request_type"][request_type]["max_execution_time"]:
                locations["request_type"][request_type]["max_execution_time"] = doc.get("execution_time")

            headers = doc["headers"]
            headers['location'].pop('latitude', None)
            headers['location'].pop('longitude', None)
            headers['location'].pop('contry_code', None)
            try:
                locations['Referer'][headers['Referer']] += 1
            except KeyError:
                try:
                    locations['Referer'][headers['From']] += 1
                except KeyError:
                    locations['Referer'][headers['User-Agent']] += 1

            for key, value in headers['location'].items():
                locations[key][value] += 1
            
            locations['timestamp'][doc['timestamp'].split('T')[0]] += 1
            timestamp = doc['timestamp'].split('T')[1].split('.')[0]
            month_year = doc['timestamp'].split('T')[0].split('-')
            if f"{month_year[1]}-{month_year[0]}" not in locations["month_year"]:
                locations["month_year"][f"{month_year[1]}-{month_year[0]}"] = 0
            locations["month_year"][f"{month_year[1]}-{month_year[0]}"] += 1

            try:
                item = doc['url'].split("qid=")[-1]
                locations["qid"][item] += 1
            except KeyError:
                pass
        except AttributeError:
            pass
        except KeyError:
            pass

    for key, value in locations["request_type"].items():
        value["average_execution_time"] = np.mean(value["execution_time"])
        del value["execution_time"]

    import json
    json_data = json.dumps(locations, indent=4)
    with open("info.json", "w") as f:
        json.dump(locations, f, indent=4)

