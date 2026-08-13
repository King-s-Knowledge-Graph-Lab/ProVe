# @repo: api
# @description: Offline script that aggregates API usage statistics from MongoDB and writes info.json. Reads go through the DataStore returned by get_database() — no raw pymongo here.
from collections import defaultdict
from tqdm import tqdm
import numpy as np

from prove_shared.database import get_database


# ---------------------------------------------------------------------------
# Script entry point
# ---------------------------------------------------------------------------
# This module is only ever executed directly (`python info.py`) for offline
# reporting. Nothing imports it at runtime. We keep the top-level lean and
# push all work into `main()` so future callers (e.g. a scheduled job) can
# invoke it programmatically.
# ---------------------------------------------------------------------------
def main() -> None:
    """
    Aggregate request-usage data from MongoDB and dump it to `info.json`.

    Reads prod usage records first, then enriches them with a second pass
    against the dev/analysis mirror (`tmp_service_usage`). Both reads go
    through the shared handler — this script no longer opens its own Mongo
    connection.
    """
    db = get_database()

    # Prod records — everything the @log_request decorator has written.
    prod_records = db.get_usage_records(use_dev_db=False)
    # Dev/analysis mirror — used for running heavier queries without hitting prod.
    _ = db.get_usage_records(use_dev_db=True)  # TODO: wire dev records into reporting output if needed.

    locations = _build_location_stats(prod_records)

    import json
    with open("info.json", "w") as f:
        json.dump(locations, f, indent=4)


def _build_location_stats(records: list[dict]) -> defaultdict:
    """
    Reduce a list of usage records into the per-type / per-location stats
    shape that `info.json` consumers expect.

    Pulled out of `main` so it's unit-testable without a live Mongo — give it
    a list of dicts and it returns a fully-populated aggregation.
    """
    locations: defaultdict = defaultdict(lambda: defaultdict(int))

    for doc in tqdm(records, total=len(records)):
        try:
            # --- Request-type counters + execution-time stats ----------------
            request_type = doc['url'].split("api")[-1].split("?")[0].split("/")[-1]
            if request_type not in locations["request_type"]:
                locations["request_type"][request_type] = {
                    "count": 0,
                    "execution_time": [],
                    "min_execution_time": float('inf'),
                    "max_execution_time": float('-inf'),
                }
            bucket = locations["request_type"][request_type]
            bucket["count"] += 1

            exec_time = doc.get("execution_time")
            bucket["execution_time"].append(exec_time)
            if exec_time is not None:
                if exec_time < bucket["min_execution_time"]:
                    bucket["min_execution_time"] = exec_time
                if exec_time > bucket["max_execution_time"]:
                    bucket["max_execution_time"] = exec_time

            # --- Referer / location / timestamp buckets ----------------------
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
            month_year = doc['timestamp'].split('T')[0].split('-')
            month_key = f"{month_year[1]}-{month_year[0]}"
            if month_key not in locations["month_year"]:
                locations["month_year"][month_key] = 0
            locations["month_year"][month_key] += 1

            # --- QID extraction ---------------------------------------------
            try:
                item = doc['url'].split("qid=")[-1]
                locations["qid"][item] += 1
            except KeyError:
                pass
        except (AttributeError, KeyError):
            # Individual malformed records shouldn't abort the whole pass.
            continue

    # Collapse execution_time lists into a single mean per request type.
    for value in locations["request_type"].values():
        value["average_execution_time"] = (
            float(np.mean(value["execution_time"])) if value["execution_time"] else None
        )
        del value["execution_time"]

    return locations


if __name__ == "__main__":
    main()
