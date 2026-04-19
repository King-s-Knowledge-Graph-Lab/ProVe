# @repo: shared
# @description: MongoDB abstraction layer — manages all collections (html_content, entailment_results, status, queues); used by both API (reads) and processing (writes)
from typing import Dict, Any, Callable, List, Optional, Union
from datetime import datetime
from bson import ObjectId
import time
import uuid

import pandas as pd
from pymongo import MongoClient, collection, database, ReturnDocument

from .database.interface import DatabaseInterface
from .logger import logger


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
# Canonical database names are kept in one place so the schema can evolve
# without hunting through call sites. When Phase 2 adds a PostgreSQLHandler,
# these become table/schema names in the Postgres implementation.
_MAIN_DB = "wikidata_verification"
_USAGE_DB_PROD = "service_usage"
_USAGE_DB_DEV = "tmp_service_usage"  # dev/analysis mirror of prod usage data


class MongoDBHandler(DatabaseInterface):
    """
    MongoDB implementation of `DatabaseInterface`.

    This class is the *only* place in the codebase that should issue raw
    pymongo calls. Every other module (API handlers, background workers,
    decorators, analytics scripts) must go through the methods exposed here.

    The set of public methods defined here is the contract the
    `PostgreSQLHandler` also implements so the two backends can be swapped
    at runtime by `DatabaseOrchestrator` (see `prove_shared.database`).

    Responsibilities:
        - Own the MongoClient lifecycle (connect, reconnect, retry).
        - Expose typed methods for every read/write path the app needs.
        - Hide schema details (collection names, projections) from callers.
        - Own the Usage DBs (service_usage, tmp_service_usage) as well —
          previously these lived in ad-hoc subclasses (StatsDBHandler,
          TMPStatsDBHandler) that leaked raw MongoClient access.

    Args:
        connection_string: The MongoDB connection string.
        max_retries: Maximum number of retries for connecting to MongoDB.

    Raises:
        ConnectionError: If the connection cannot be established after retries.
    """

    def __init__(
        self,
        connection_string: str = "mongodb://localhost:27017/",
        max_retries: int = 3,
    ) -> None:
        # Connection parameters
        self.max_retries = max_retries
        self.connection_string = connection_string

        # Main DB handles (populated in connect)
        self.client: MongoClient = None
        self.db: database = None
        self.html_collection: collection = None
        self.entailment_collection: collection = None
        self.stats_collection: collection = None
        self.status_collection: collection = None
        self.summary_collection: collection = None
        self.random_collection: collection = None
        self.user_collection: collection = None

        # Queue lookup map — callers pass short string keys ("user", "random")
        # instead of pymongo Collection objects. This keeps the public API
        # backend-agnostic; Postgres will map the same keys to table names.
        self._queues: Dict[str, collection] = {}

        # Lazy handles for the usage databases. They live on the same Mongo
        # instance but are separate *databases*. We only open them on first
        # use because most code paths don't need them (they're used by the
        # @log_request decorator and the offline analytics script).
        self._usage_db_prod: database = None
        self._usage_db_dev: database = None

        if not self.connect(max_retries, connection_string):
            logger.error("Failed to connect to MongoDB")
            raise ConnectionError("Could not connect to MongoDB after multiple attempts")

    # ------------------------------------------------------------------ #
    # Connection lifecycle                                               #
    # ------------------------------------------------------------------ #
    def connect(self, max_retries: int, connection_string: str) -> bool:
        """
        Connect to MongoDB with retries. Returns True on success.
        """
        for attempt in range(max_retries):
            try:
                self.client = MongoClient(connection_string)
                self.ensure_connection(try_reconnect=False)
                break
            except Exception as e:
                logger.error(f"MongoDB connection attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    return False
                time.sleep(5)
                continue

        # Main application database
        self.db = self.client[_MAIN_DB]
        self.html_collection = self.db['html_content']
        self.entailment_collection = self.db['entailment_results']
        self.stats_collection = self.db['parser_stats']
        self.status_collection = self.db['status']
        self.summary_collection = self.db['summary']
        self.random_collection = self.db['random_queue']
        self.user_collection = self.db['user_queue']

        # Register queues under short keys. Public methods accept these keys
        # instead of Collection objects, which is the shape Postgres will use.
        self._queues = {
            'user': self.user_collection,
            'random': self.random_collection,
            'status': self.status_collection,
        }

        # Index for high-concurrency dequeue on the random queue.
        try:
            self.random_collection.create_index([('status', 1), ('requested_timestamp', 1)])
        except Exception as e:
            logger.error(f"Failed to create index {e}")

        logger.info("Successfully connected to WikiData verification MongoDB")
        return True

    def ensure_connection(self, try_reconnect: bool = True) -> None:
        """
        Ensure MongoDB connection is alive, reconnecting if needed.

        Raises ConnectionError if the connection cannot be re-established.
        """
        try:
            self.client.server_info()
        except Exception as e:
            logger.error("MongoDB connection lost, attempting to reconnect...")
            logger.error(f"Error details: {e}")

            if not try_reconnect:
                logger.error("Reconnection failed, please check MongoDB server status")
                raise ConnectionError("MongoDB connection lost") from e

            if not self.connect(self.max_retries, self.connection_string):
                logger.error("Reconnection failed, please check MongoDB server status")
                raise ConnectionError("Could not reconnect to MongoDB") from e

    # ------------------------------------------------------------------ #
    # Internal helpers                                                   #
    # ------------------------------------------------------------------ #
    def _resolve_queue(self, queue: Union[str, collection]) -> collection:
        """
        Accept either a short queue name (preferred) or a pymongo Collection
        (legacy) and return the underlying Collection.

        Dual acceptance keeps the Phase 1 rollout smooth: new callers pass
        "user" / "random" / "status"; older methods that already expected a
        Collection object continue to work without churn. Phase 2 will
        standardise on names only.
        """
        if isinstance(queue, str):
            try:
                return self._queues[queue]
            except KeyError as e:
                raise ValueError(
                    f"Unknown queue '{queue}'. Valid names: {list(self._queues)}"
                ) from e
        return queue

    # ==================================================================
    #                       EXISTING METHODS
    # ==================================================================
    # These predate the Phase 1 consolidation and are already called by the
    # queue-management workflow in queue_manager.py. They are kept as-is to
    # avoid collateral churn. The new methods below are where all previously
    # leaked queries now live.
    # ------------------------------------------------------------------
    def save_html_content(self, html_df: pd.DataFrame) -> None:
        """Save HTML content data with task_id."""
        try:
            if html_df.empty:
                logger.warning("html_df is empty")
                return

            html_df_without_html = html_df.drop('html', axis=1)

            logger.info(f"Attempting to save {len(html_df_without_html)} HTML records")
            records = html_df_without_html.to_dict('records')

            for record in records:
                try:
                    if 'reference_id' not in record:
                        logger.warning(f"Record missing reference_id: {record}")
                        continue

                    if 'fetch_timestamp' in record and isinstance(record['fetch_timestamp'], pd.Timestamp):
                        record['fetch_timestamp'] = record['fetch_timestamp'].to_pydatetime()

                    record['save_timestamp'] = datetime.now()

                    result = self.html_collection.update_one(
                        {
                            'reference_id': record['reference_id'],
                            'task_id': record['task_id']
                        },
                        {'$set': record},
                        upsert=True
                    )

                    logger.info(
                        f"Updated HTML document with reference_id {record['reference_id']}: "
                        f"matched={result.matched_count}, modified={result.modified_count}, "
                        f"upserted_id={result.upserted_id}"
                    )
                except Exception as e:
                    logger.error(f"Error saving HTML record: {record}")
                    logger.error(f"Error details: {e}")

        except Exception as e:
            logger.error(f"Error in save_html_content: {e}")
            raise RuntimeError(f"Failed to save HTML content: {e}") from e

    def save_entailment_results(self, entailment_df: pd.DataFrame) -> None:
        """Save entailment results to MongoDB."""
        try:
            if entailment_df.empty:
                logger.warning("entailment_df is empty")
                return

            logger.info(f"Attempting to save {len(entailment_df)} entailment records")
            records = entailment_df.to_dict('records')

            for record in records:
                try:
                    if 'processed_timestamp' in record:
                        record['processed_timestamp'] = datetime.strptime(
                            record['processed_timestamp'],
                            '%Y-%m-%dT%H:%M:%S.%f'
                        )

                    record['save_timestamp'] = datetime.now()

                    result = self.entailment_collection.insert_one(record)
                    logger.info(
                        f"Inserted new entailment document with reference_id {record['reference_id']}: "
                        f"inserted_id={result.inserted_id}"
                    )

                except Exception as e:
                    logger.error(f"Error saving entailment record: {record}")
                    logger.error(f"Error details: {e}")

        except Exception as e:
            logger.error(f"Error in save_entailment_results: {e}")
            raise RuntimeError(f"Failed to save entailment results: {e}") from e

    def save_parser_stats(self, stats_dict: Dict[str, Any]) -> None:
        """Save parser statistics to MongoDB."""
        try:
            if isinstance(stats_dict.get('parsing_start_timestamp'), pd.Timestamp):
                stats_dict['parsing_start_timestamp'] = stats_dict[
                    'parsing_start_timestamp'
                ].to_pydatetime()

            stats_dict['save_timestamp'] = datetime.now()

            self.stats_collection.update_one(
                {
                    'entity_id': stats_dict['entity_id'],
                    'task_id': stats_dict['task_id']
                },
                {'$set': stats_dict},
                upsert=True
            )

            logger.info(f"Updated parser stats for entity {stats_dict['entity_id']}")

        except Exception as e:
            logger.error(f"Error in save_parser_stats: {e}")
            raise RuntimeError(f"Failed to save parser statistics: {e}") from e

    def save_status(
        self,
        status_dict: Dict[str, Any],
        queue: Union[str, collection, None] = None,
    ) -> None:
        """
        Save or update a task status document.

        `queue` may be a short name ("user" / "random") or a Collection
        object. Defaults to the main status collection if omitted.
        """
        target: collection = (
            self.status_collection
            if queue is None
            else self._resolve_queue(queue)
        )

        try:
            timestamp_fields = [
                'requested_timestamp',
                'processing_start_timestamp',
                'completed_timestamp'
            ]

            for field in timestamp_fields:
                if status_dict.get(field) and status_dict[field] != 'null':
                    if isinstance(status_dict[field], str):
                        status_dict[field] = datetime.strptime(
                            status_dict[field].rstrip('Z'),
                            '%Y-%m-%dT%H:%M:%S.%f'
                        )

            status_dict['last_updated'] = datetime.now()

            existing_doc = target.find_one({
                'task_id': status_dict['task_id'],
                'qid': status_dict['qid']
            })

            if existing_doc:
                result = target.update_one(
                    {
                        'task_id': status_dict['task_id'],
                        'qid': status_dict['qid']
                    },
                    {'$set': status_dict}
                )
                logger.info(
                    f"Updated status for task {status_dict['task_id']}: "
                    f"matched={result.matched_count}, modified={result.modified_count}"
                )
            else:
                result = target.insert_one(status_dict)
                logger.info(
                    f"Created new status for task {status_dict['task_id']}: "
                    f"inserted_id={result.inserted_id}"
                )

        except Exception as e:
            logger.error(f"Error in save_status: {e}")
            raise RuntimeError(f"Failed to save status: {e}") from e

    def get_next_request(self, queue: collection) -> Union[Dict[str, Any], None]:
        """Atomically claim the next pending item from `queue`."""
        try:
            pending_request = queue.find_one_and_update(
                {
                    'status': 'in queue',
                    'processing_start_timestamp': None
                },
                {'$set': {
                    'status': 'processing',
                    'processing_start_timestamp': datetime.utcnow(),
                }},
                sort=[('requested_timestamp', 1)],
                return_document=ReturnDocument.AFTER
            )

            if pending_request:
                return pending_request
            return None
        except Exception as e:
            logger.error(f"Error getting next user request: {e}")
            raise RuntimeError(f"Failed to get next request: {e}") from e

    def get_request_by_id_and_reset(
        self,
        queue: collection,
        _id: str
    ) -> Union[Dict[str, Any], None]:
        return queue.find_one_and_update(
            {
                '_id': _id,
                'status': 'processing',
                'processing_start_timestamp': {'$not': {'$eq': None}}
            },
            {'$set': {
                'status': 'in queue',
                'processing_start_timestamp': None
            }},
            return_document=ReturnDocument.AFTER
        )

    def set_request_status_and_processing_time(
        self,
        queue: collection,
        status: str,
        processing_time: datetime,
        _id: str
    ) -> Union[Dict[str, Any], None]:
        return queue.find_one_and_update(
            {'_id': _id},
            {'$set': {
                'status': status,
                'processing_start_timestamp': processing_time
            }},
            return_document=ReturnDocument.AFTER
        )

    def get_request_by_id(self, queue: collection, _id: str) -> Union[Dict[str, Any], None]:
        return queue.find_one({'_id': ObjectId(_id)})

    def get_request_by_taskid(self, queue: collection, task_id: str) -> Union[Dict[str, Any], None]:
        return queue.find_one({'task_id': task_id})

    def get_all_request_in_progress(self, queue: collection) -> Union[Dict[str, Any], None]:
        return queue.find({'status': 'processing'})

    # ==================================================================
    #              NEW METHODS — Phase 1 query consolidation
    # ==================================================================
    # Every raw pymongo call that previously lived outside this class now
    # routes through one of the methods below. See the Phase 1 audit doc
    # for the full old-call-site → new-method mapping.
    #
    # Design rules:
    #   * Inputs are plain Python types (str, dict). No BSON leaks out.
    #   * Reads return Optional[dict] or List[dict] — cursors are always
    #     materialised so callers don't accidentally iterate twice.
    #   * Collection/field names are hidden from callers — they pass
    #     semantic args (qid, task_id, queue_name) and we look up internals.
    #   * Each method has a single, narrow responsibility so the Postgres
    #     implementation can mirror it one-for-one in Phase 2.
    # ------------------------------------------------------------------

    # ---- Status ------------------------------------------------------- #
    def get_latest_status_by_qid(self, qid: str) -> Optional[Dict[str, Any]]:
        """
        Return the most recent status document for `qid`, or None.

        "Most recent" is defined as the largest `requested_timestamp`.
        Used by the API to show the current state of a QID in GetItem and
        get_item.
        """
        return self.status_collection.find_one(
            {'qid': qid},
            sort=[('requested_timestamp', -1)],
        )

    def get_statuses_by_qid(
        self,
        qid: str,
        sort_by: Optional[str] = None,
        descending: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Return every status document for `qid`.

        Two historical call shapes:
          * `CheckItemStatus`        — unsorted, just wants the set.
          * `get_history`            — sorted by `completed_timestamp` desc.

        Passing `sort_by=None` preserves the existing unsorted semantics.
        """
        cursor = self.status_collection.find({'qid': qid})
        if sort_by is not None:
            cursor = cursor.sort(sort_by, -1 if descending else 1)
        return list(cursor)

    # ---- HTML content ------------------------------------------------- #
    def get_html_by_task_id(
        self,
        task_id: str,
        fields: Optional[Dict[str, int]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Return all HTML-content documents for `task_id`.

        `fields` is an optional Mongo-style projection. We accept it as a
        generic dict so the Postgres implementation can translate it to a
        SELECT column list. Pass None to fetch the full document.
        """
        if fields is None:
            cursor = self.html_collection.find({'task_id': task_id})
        else:
            cursor = self.html_collection.find({'task_id': task_id}, fields)
        return list(cursor)

    # ---- Entailment results ------------------------------------------- #
    def get_entailments_by_task_and_reference(
        self,
        task_id: str,
        reference_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Return every entailment row for a (task_id, reference_id) pair.

        Used by `GetItem` when selecting a verdict per reference. Postgres
        equivalent will be a simple `WHERE task_id = ? AND reference_id = ?`.
        """
        return list(
            self.entailment_collection.find(
                {'task_id': task_id, 'reference_id': reference_id},
            )
        )

    def aggregate_entailments_by_task_id(
        self,
        task_id: str,
        reference_ids: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Aggregate entailments for `task_id` restricted to `reference_ids`,
        sorted by entailment score (desc) and grouped by (reference_id, result).

        Only used by `get_item`. Doing this server-side avoids pulling every
        entailment row for a task. The Postgres equivalent will be
        `SELECT ... ORDER BY text_entailment_score DESC` materialised in
        Python (no native `$group` equivalent needed; a dict-reduce does it).

        Returns the raw aggregation output so the caller's existing
        post-processing keeps working unchanged.
        """
        pipeline = [
            {"$match": {
                "task_id": task_id,
                "reference_id": {"$in": reference_ids},
            }},
            {"$sort": {"text_entailment_score": -1}},
            {"$group": {
                "_id": {
                    "reference_id": "$reference_id",
                    "result": "$result",
                },
                "docs": {"$push": "$$ROOT"},
            }},
        ]
        return list(self.entailment_collection.aggregate(pipeline))

    # ---- Summaries ---------------------------------------------------- #
    def get_summary_by_id(self, target_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch the cached summary document for `target_id` (a QID).
        Returns None if no summary has been computed yet.
        """
        return self.summary_collection.find_one({'_id': target_id})

    def upsert_summary_by_id(self, target_id: str, data: Dict[str, Any]) -> None:
        """
        Insert or update the summary document for `target_id`.

        The previous implementation branched between `insert_one` and
        `update_one` depending on whether the document already existed. That
        pattern is race-prone (two workers both see "no doc" and both insert)
        and redundant — Mongo's `upsert=True` handles this atomically. This
        also gives the Postgres backend a clean `ON CONFLICT ... DO UPDATE`
        target.
        """
        self.summary_collection.update_one(
            {'_id': target_id},
            {'$set': data},
            upsert=True,
        )

    # ---- Parser stats ------------------------------------------------- #
    def get_parser_stats_by_task_and_entity(
        self,
        task_id: str,
        entity_id: str,
        fields: Optional[Dict[str, int]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a single parser-stats document keyed by (task_id, entity_id).

        `fields` is an optional projection. Every existing call site asks for
        `{'total_claims': 1, '_id': 0}`, so we default to that to keep call
        sites readable. Pass an explicit projection to override, or `{}` to
        fetch the full document.
        """
        projection = (
            {'total_claims': 1, '_id': 0} if fields is None else fields or None
        )
        return self.stats_collection.find_one(
            {'task_id': task_id, 'entity_id': entity_id},
            projection,
        )

    # ---- Generic queue operations ------------------------------------- #
    # The queue collections (user, random, status) share the same status
    # schema. Methods below are parameterised by queue name so callers don't
    # need to know which pymongo Collection to reach into. The Postgres
    # backend will expose the same queue_name-based API.

    def get_queue_items(
        self,
        queue_name: Union[str, collection],
        status: Optional[str] = None,
        sort_by: Optional[str] = None,
        ascending: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Return items from `queue_name`, optionally filtered by status.

        Call shapes this replaces:
          * `checkQueue`             → status='in queue', sort by requested_timestamp asc.
          * `retry_processing`       → status='processing', no sort.
        """
        target = self._resolve_queue(queue_name)
        query: Dict[str, Any] = {}
        if status is not None:
            query['status'] = status

        cursor = target.find(query)
        if sort_by is not None:
            cursor = cursor.sort(sort_by, 1 if ascending else -1)
        return list(cursor)

    def find_queue_item_by_qid(
        self,
        queue_name: Union[str, collection],
        qid: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Look up a single item by QID in `queue_name`. Returns None if absent.

        Used by the heuristic service to avoid enqueuing a QID that already
        exists in any known queue.
        """
        target = self._resolve_queue(queue_name)
        return target.find_one({'qid': qid})

    def increment_retry_by_id(
        self,
        queue_name: Union[str, collection],
        item_id: Any,
    ) -> None:
        """
        Atomically bump `retry_count` on the given queue item by 1.

        The old implementation read `retry_count`, added 1, then wrote it
        back — a classic lost-update race when multiple workers retried the
        same stuck item. Using `$inc` (atomic in Mongo, `UPDATE ... SET col =
        col + 1` in Postgres) closes that race.
        """
        target = self._resolve_queue(queue_name)
        target.update_one({'_id': item_id}, {'$inc': {'retry_count': 1}})

    def mark_queue_item_error_by_id(
        self,
        queue_name: Union[str, collection],
        item_id: Any,
        error_message: str = 'Max retry limit reached',
    ) -> None:
        """
        Mark a queue item as permanently failed (status='error').
        Called by the retry loop when an item has exhausted its retries.
        """
        target = self._resolve_queue(queue_name)
        target.update_one(
            {'_id': item_id},
            {'$set': {'status': 'error', 'error_message': error_message}},
        )

    def update_queue_status_by_task_and_qid(
        self,
        queue_name: Union[str, collection],
        task_id: str,
        qid: str,
        status: str,
    ) -> None:
        """
        Update the `status` field for the (task_id, qid) row in `queue_name`.
        Called by the main service when a job transitions to 'completed'.
        """
        target = self._resolve_queue(queue_name)
        target.update_one(
            {'task_id': task_id, 'qid': qid},
            {'$set': {'status': status}},
        )

    def enqueue_item(
        self,
        queue_name: Union[str, collection],
        item: Dict[str, Any],
    ) -> None:
        """
        Insert `item` into `queue_name` — the minimal "append to queue" op.

        Replaces the old pattern of passing `collection.insert_one` around
        as a raw callback (`save_function=mongo_handler.random_collection.insert_one`),
        which exposed pymongo to every caller.
        """
        target = self._resolve_queue(queue_name)
        target.insert_one(item)

    # ==================================================================
    #               USAGE DBs (service_usage / tmp_service_usage)
    # ==================================================================
    # Separate databases on the same Mongo instance, used by the API
    # decorators to log every request and by the offline analytics script
    # (`info.py`) to read them.
    #
    # Previously implemented as two subclasses (StatsDBHandler,
    # TMPStatsDBHandler) that each overrode `connect()` to point at a
    # different DB. That pattern leaked raw MongoClient access to every
    # usage call site — every one of those `self.client[...]` lines
    # counted as a leaked query.
    #
    # Folding them in here means a single handler now owns all Mongo access.
    # The DB handles are created lazily on first property access so the
    # 99% of code paths that never touch usage pay nothing.
    # ------------------------------------------------------------------

    @property
    def usage_collection(self) -> collection:
        """Lazy handle to `service_usage.usage` (production request logs)."""
        if self._usage_db_prod is None:
            self._usage_db_prod = self.client[_USAGE_DB_PROD]
        return self._usage_db_prod['usage']

    @property
    def tmp_usage_collection(self) -> collection:
        """Lazy handle to `tmp_service_usage.usage` (dev/analysis mirror)."""
        if self._usage_db_dev is None:
            self._usage_db_dev = self.client[_USAGE_DB_DEV]
        return self._usage_db_dev['usage']

    def log_usage(self, record: Dict[str, Any]) -> None:
        """
        Persist a single API-usage record to the production usage DB.

        Called from the `@log_request` decorator on every HTTP request. We
        intentionally swallow errors: a failure to log usage metadata must
        never surface as a 500 to the user.
        """
        try:
            self.usage_collection.insert_one(record)
        except Exception as e:
            logger.error(f"Failed to log usage record: {e}")

    def get_usage_records(
        self,
        use_dev_db: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Return every usage record for offline analysis.

        Args:
            use_dev_db: If True, read from `tmp_service_usage` (the dev
                mirror used by `info.py`). Otherwise read production data.

        Returns a list (not a cursor) because callers need length / indexing.
        """
        target = self.tmp_usage_collection if use_dev_db else self.usage_collection
        return list(target.find())


# ---------------------------------------------------------------------------
# Free-function helpers
# ---------------------------------------------------------------------------
# `requestItemProcessing` is kept as a module-level function for backward
# compatibility with existing importers. It is now just a thin wrapper that
# routes through the handler's new methods — no more raw pymongo callbacks.
# ---------------------------------------------------------------------------
def requestItemProcessing(
    qid: str,
    queue: Union[str, collection],
    db: "MongoDBHandler",
    request_type: str = 'userRequested',
    algo_version: str = '1.1.1',
) -> str:
    """
    Enqueue `qid` for processing if it isn't already pending.

    Args:
        qid: Wikidata identifier to enqueue.
        queue: Either a queue name ("user" / "random") or a pymongo Collection.
        db: The MongoDBHandler instance to route the reads/writes through.
        request_type: Origin of the request ('userRequested', 'Random_processing').
        algo_version: Pipeline version stamped on the new record.

    Returns:
        A human-readable status string (callers log it).
    """
    try:
        existing = db.find_queue_item_by_qid(queue, qid)
        if existing and existing.get('status') == 'in queue':
            return f"QID {qid} is already in queue. Skipping..."

        status_dict = _build_status_dict(qid, request_type, algo_version)
        db.enqueue_item(queue, status_dict)
        return f"Task {status_dict['task_id']} created for QID {qid}"

    except Exception as e:
        logger.error("Error in requestItemProcessing: %s", e)
        return f"An error occurred: {e}"


def _build_status_dict(
    qid: str,
    request_type: str,
    algo_version: str,
) -> Dict[str, Any]:
    """Construct the canonical 'in queue' status document for a new task."""
    return {
        'qid': qid,
        'task_id': str(uuid.uuid4()),
        'status': 'in queue',
        'algo_version': algo_version,
        'request_type': request_type,
        'requested_timestamp': datetime.utcnow(),
        'processing_start_timestamp': None,
        'completed_timestamp': None,
    }
