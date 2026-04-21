# @repo: shared
# @description: MongoDB implementation of DataStore (ABC) — owns every pymongo call in the codebase. Every other module accesses the DB through the methods defined here.
"""
MongoDB backend for the ProVe database layer.

This module is the *only* place in the codebase that may import from pymongo
or instantiate `MongoClient`. Every other module accesses the database through
the `DataStore` methods defined here.

Design notes:
    * Inputs are plain Python types (str, dict, list). No BSON leaks out.
    * Reads return Optional[dict] or List[dict] — pymongo `Cursor` objects are
      always materialised with `list(...)` so callers can't accidentally
      iterate twice (and so the same shape works for the Postgres backend).
    * Collection/field names are hidden from callers: they pass semantic
      args (qid, task_id, queue_name) and we resolve internals here.
"""
from typing import Any, Callable, Dict, List, Optional, Union
from datetime import datetime
from bson import ObjectId
import time
import uuid

import pandas as pd
from pymongo import MongoClient, ReturnDocument
from pymongo.collection import Collection
from pymongo.database import Database

from ..logger import logger
from .interface import DataStore


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
# Canonical database names are kept in one place so the schema can evolve
# without hunting through call sites. When the PostgreSQLHandler lands these
# become schema/table names in the Postgres implementation.
_MAIN_DB = "wikidata_verification"
_USAGE_DB_PROD = "service_usage"
_USAGE_DB_DEV = "tmp_service_usage"  # dev/analysis mirror of prod usage data


class MongoDBHandler(DataStore):
    """
    MongoDB implementation of `DataStore` (ABC).

    Owns all pymongo state for the process. The public methods form the
    contract the future `PostgreSQLHandler` also implements, so the
    `DatabaseOrchestrator` can swap backends at runtime.

    Usage DBs (`service_usage`, `tmp_service_usage`) are also owned here —
    previously they lived in ad-hoc subclasses (`StatsDBHandler`,
    `TMPStatsDBHandler`) which each re-opened `MongoClient`. Folding them in
    collapses three classes into one and removes several leaked
    `MongoClient(...)` call sites.

    Args:
        connection_string: MongoDB connection URI. Defaults to
            ``"mongodb://localhost:27017/"``.
        max_retries: Maximum number of retries for the initial connection.

    Attributes:
        client (MongoClient): Underlying pymongo client.
        db (database): Handle to the main `wikidata_verification` database.
        html_collection (collection): HTML-content documents.
        entailment_collection (collection): BERT-FEVER entailment results.
        stats_collection (collection): Parser statistics per (task, entity).
        status_collection (collection): Task status timeline.
        summary_collection (collection): Cached per-QID summaries.
        random_collection (collection): Random-selection worker queue.
        user_collection (collection): User-requested worker queue.

    Raises:
        ConnectionError: If the initial connection cannot be established
            after ``max_retries`` attempts.
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
        self.db: Database = None
        self.html_collection: Collection = None
        self.entailment_collection: Collection = None
        self.stats_collection: Collection = None
        self.status_collection: Collection = None
        self.summary_collection: Collection = None
        self.random_collection: Collection = None
        self.user_collection: Collection = None

        # Queue lookup map — callers pass short string keys ("user", "random")
        # rather than pymongo Collection objects. This keeps the public API
        # backend-agnostic; Postgres will map the same keys to table names.
        self._queues: Dict[str, Collection] = {}

        # Lazy handles for the usage databases. They live on the same Mongo
        # instance but are separate *databases*. We only open them on first
        # use because most code paths don't need them (only the @log_request
        # decorator and the offline analytics script do).
        self._usage_db_prod: Database = None
        self._usage_db_dev: Database = None

        if not self.connect(max_retries, connection_string):
            logger.error("Failed to connect to MongoDB")
            raise ConnectionError("Could not connect to MongoDB after multiple attempts")

    # ------------------------------------------------------------------ #
    # Connection lifecycle                                               #
    # ------------------------------------------------------------------ #
    def connect(self, max_retries: int, connection_string: str) -> bool:
        """
        Connect to MongoDB with retries and wire up collection handles.

        Args:
            max_retries: Maximum number of retries before giving up.
            connection_string: The MongoDB connection URI.

        Returns:
            ``True`` if the connection is successful, ``False`` otherwise.
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
        Verify the MongoDB connection is alive; reconnect if permitted.

        Args:
            try_reconnect: If ``True``, attempt to reconnect when the
                connection is down. If ``False``, raise immediately.

        Raises:
            ConnectionError: If the connection is down and either reconnection
                is disabled or retry attempts are exhausted.
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
    def _resolve_queue(self, queue: Union[str, Collection]) -> Collection:
        """
        Resolve a queue reference to an underlying pymongo Collection.

        Accepts either a short queue name (preferred, backend-agnostic) or
        a pymongo Collection (legacy). Dual acceptance keeps the migration
        smooth: new callers pass ``"user"`` / ``"random"`` / ``"status"``;
        older methods that already took Collection objects continue to work.

        Args:
            queue: Either a short queue name or a pymongo Collection.

        Returns:
            The underlying pymongo Collection.

        Raises:
            ValueError: If ``queue`` is a string that isn't a known queue name.
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
    # Bulk writes (existing methods, previously in the legacy handler)
    # ==================================================================
    def save_html_content(self, html_df: pd.DataFrame) -> None:
        """
        Upsert HTML content rows from a DataFrame.

        The raw HTML column is dropped before insert for storage efficiency.
        Rows are keyed on ``(reference_id, task_id)``; existing rows are
        updated in place.

        Args:
            html_df: DataFrame containing HTML content with columns:
                ``reference_id``, ``task_id``, ``html``, ``fetch_timestamp``.

        Raises:
            RuntimeError: If the batch write fails at the collection level.
                Per-record errors are logged and skipped, not re-raised.
        """
        try:
            if html_df.empty:
                logger.warning("html_df is empty")
                return

            # Drop raw HTML — we store metadata, not the full pages.
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
        """
        Insert entailment result rows from a DataFrame.

        Unlike ``save_html_content`` this is an append-only insert — we
        deliberately keep the full history of entailment verdicts per
        reference/claim pair for analytics.

        Args:
            entailment_df: DataFrame containing entailment results with
                columns including ``reference_id``, ``task_id``,
                ``processed_timestamp``.

        Raises:
            RuntimeError: If the batch insert fails. Per-record errors are
                logged and skipped, not re-raised.
        """
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
        """
        Upsert a parser-stats record keyed by ``(task_id, entity_id)``.

        Args:
            stats_dict: Parser statistics with at least
                ``entity_id``, ``task_id``, and ``parsing_start_timestamp``.

        Raises:
            RuntimeError: If the upsert fails.
        """
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
        queue: Union[str, Collection, None] = None,
    ) -> None:
        """
        Upsert a status document into the given queue.

        Existing records matching ``(task_id, qid)`` are updated in place;
        otherwise a new record is inserted. String timestamps in the input
        are parsed to ``datetime`` objects; ``last_updated`` is always set
        to the current wall-clock time.

        Args:
            status_dict: Status document. Must contain ``task_id`` and
                ``qid``. Optional fields: ``status``, ``algo_version``,
                ``request_type``, ``requested_timestamp``,
                ``processing_start_timestamp``, ``completed_timestamp``.
            queue: Either a short queue name (``"user"`` / ``"random"`` /
                ``"status"``) or a pymongo Collection. Defaults to the main
                status collection when omitted.

        Raises:
            RuntimeError: If the upsert fails.
        """
        target: Collection = (
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

    def get_next_request(self, queue: Collection) -> Optional[Dict[str, Any]]:
        """
        Atomically claim the next pending item from ``queue``.

        Uses ``find_one_and_update`` to flip status from ``"in queue"`` to
        ``"processing"`` in a single round-trip, avoiding any race window
        between read and write.

        Args:
            queue: The pymongo Collection to pull the next request from.

        Returns:
            The claimed document, or ``None`` if the queue is empty.

        Raises:
            RuntimeError: If the claim operation fails.
        """
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
        queue: Collection,
        _id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Return an in-flight item to ``"in queue"`` state (undo a claim).

        Only matches items currently in ``"processing"``; a worker that
        crashes mid-task calls this to release the row back to the pool.

        Args:
            queue: The pymongo Collection containing the item.
            _id: Primary key of the item.

        Returns:
            The updated document post-reset, or ``None`` if no match.
        """
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
        queue: Collection,
        status: str,
        processing_time: datetime,
        _id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Overwrite both ``status`` and ``processing_start_timestamp`` on an item.

        Args:
            queue: The pymongo Collection containing the item.
            status: New status string.
            processing_time: New ``processing_start_timestamp`` value.
            _id: Primary key of the item.

        Returns:
            The updated document post-write, or ``None`` if no match.
        """
        return queue.find_one_and_update(
            {'_id': _id},
            {'$set': {
                'status': status,
                'processing_start_timestamp': processing_time
            }},
            return_document=ReturnDocument.AFTER
        )

    def get_request_by_id(
        self,
        queue: Collection,
        _id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a single request by its primary key.

        Args:
            queue: The pymongo Collection containing the item.
            _id: Primary key (string; converted to ObjectId internally).

        Returns:
            The document, or ``None`` if not found.
        """
        return queue.find_one({'_id': ObjectId(_id)})

    def get_request_by_taskid(
        self,
        queue: Collection,
        task_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a single request by its ``task_id``.

        Args:
            queue: The pymongo Collection to search.
            task_id: Task identifier.

        Returns:
            The document, or ``None`` if not found.
        """
        return queue.find_one({'task_id': task_id})

    def get_all_request_in_progress(self, queue: Collection) -> Any:
        """
        Return a cursor over items currently in ``"processing"``.

        Args:
            queue: The pymongo Collection to scan.

        Returns:
            A pymongo cursor. (Kept as a cursor for backward compatibility
            with existing callers that iterate lazily.)
        """
        return queue.find({'status': 'processing'})

    # ==================================================================
    # NEW METHODS — Phase 1 query consolidation
    # ==================================================================
    # Every raw pymongo call that previously lived outside this class now
    # routes through one of the methods below. See the audit doc for the
    # full old-call-site → new-method mapping.

    # ---- Status ------------------------------------------------------- #
    def get_latest_status_by_qid(self, qid: str) -> Optional[Dict[str, Any]]:
        """
        Return the most recent status document for a QID.

        "Most recent" is the row with the largest ``requested_timestamp``.

        Args:
            qid: Wikidata identifier (e.g. ``"Q42"``).

        Returns:
            The status document, or ``None`` if the QID has never been
            enqueued.
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
        Return every status document for a QID.

        Args:
            qid: Wikidata identifier.
            sort_by: Optional field name to sort by. ``None`` preserves
                insertion order (matches the pre-refactor semantics of
                ``CheckItemStatus``).
            descending: Sort direction when ``sort_by`` is given.

        Returns:
            A list of status documents. Empty if the QID has no history.
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
        Return all HTML-content documents for a task.

        Args:
            task_id: Task identifier.
            fields: Optional Mongo-style projection ``{'name': 1, '_id': 0}``.
                Accepted as a generic dict so the Postgres backend can
                translate it to a SELECT column list. ``None`` fetches the
                full document.

        Returns:
            A list of HTML-content documents.
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
        Return entailment rows for a ``(task_id, reference_id)`` pair.

        Used by the per-reference verdict logic in ``GetItem``. Postgres
        equivalent: ``WHERE task_id = %s AND reference_id = %s``.

        Args:
            task_id: Task identifier.
            reference_id: Reference identifier.

        Returns:
            A list of entailment documents. Empty if no entailment has
            been computed for this pair yet.
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
        Group top entailments for a task, bucketed by (reference_id, result).

        Results are sorted by ``text_entailment_score`` descending. Only
        used by ``get_item`` when picking the top-scoring verdict per
        reference; doing it server-side avoids pulling every entailment row
        for a task.

        Args:
            task_id: Task identifier.
            reference_ids: Restrict aggregation to these references.

        Returns:
            The raw aggregation output — a list of
            ``{"_id": {"reference_id": ..., "result": ...}, "docs": [...]}``
            entries. The caller's existing grouping code expects this shape.
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
        Fetch the cached summary document for a QID.

        Args:
            target_id: Wikidata identifier (used as the document ``_id``).

        Returns:
            The summary document, or ``None`` if no summary has been
            computed yet.
        """
        return self.summary_collection.find_one({'_id': target_id})

    def upsert_summary_by_id(
        self,
        target_id: str,
        data: Dict[str, Any],
    ) -> None:
        """
        Insert or update the summary document for a QID.

        The previous implementation split this into explicit ``insert_one``
        and ``update_one`` branches depending on whether the document
        already existed. That pattern was race-prone (two workers both see
        "no doc" and both insert). ``upsert=True`` handles it atomically
        and maps cleanly to Postgres' ``ON CONFLICT ... DO UPDATE``.

        Args:
            target_id: Wikidata identifier (used as the document ``_id``).
            data: Fields to set on the summary.
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
        Fetch a single parser-stats document keyed by ``(task_id, entity_id)``.

        Args:
            task_id: Task identifier.
            entity_id: Wikidata identifier.
            fields: Optional projection. ``None`` (the default) returns just
                ``{'total_claims': 1, '_id': 0}`` — the shape every current
                caller needs. Pass an explicit projection to override, or
                ``{}`` to fetch the full document.

        Returns:
            The parser-stats document, or ``None`` if not found.
        """
        projection = (
            {'total_claims': 1, '_id': 0} if fields is None else fields or None
        )
        return self.stats_collection.find_one(
            {'task_id': task_id, 'entity_id': entity_id},
            projection,
        )

    # ---- Generic queue operations ------------------------------------- #
    def get_queue_items(
        self,
        queue_name: Union[str, Collection],
        status: Optional[str] = None,
        sort_by: Optional[str] = None,
        ascending: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Return items from a queue, optionally filtered by status.

        Args:
            queue_name: Either a short queue name (``"user"`` / ``"random"``
                / ``"status"``) or a pymongo Collection.
            status: Optional status filter (e.g. ``"in queue"``,
                ``"processing"``). ``None`` returns every row.
            sort_by: Optional field to sort by. ``None`` preserves insertion
                order.
            ascending: Sort direction when ``sort_by`` is given.

        Returns:
            A list of queue items.
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
        queue_name: Union[str, Collection],
        qid: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Look up a single item by QID in a queue.

        Used by the heuristic service to avoid enqueuing a QID that already
        exists in any known queue.

        Args:
            queue_name: Either a short queue name or a pymongo Collection.
            qid: Wikidata identifier.

        Returns:
            The queue item, or ``None`` if absent.
        """
        target = self._resolve_queue(queue_name)
        return target.find_one({'qid': qid})

    def increment_retry_by_id(
        self,
        queue_name: Union[str, Collection],
        item_id: Any,
    ) -> None:
        """
        Atomically bump ``retry_count`` on a queue item by 1.

        Uses ``$inc`` (atomic in Mongo, ``UPDATE ... SET col = col + 1`` in
        Postgres) to avoid the lost-update race the previous read-modify-
        write implementation had.

        Args:
            queue_name: Either a short queue name or a pymongo Collection.
            item_id: Primary key of the item.
        """
        target = self._resolve_queue(queue_name)
        target.update_one({'_id': item_id}, {'$inc': {'retry_count': 1}})

    def mark_queue_item_error_by_id(
        self,
        queue_name: Union[str, Collection],
        item_id: Any,
        error_message: str = 'Max retry limit reached',
    ) -> None:
        """
        Mark a queue item as permanently failed (``status = "error"``).

        Called by the retry loop when an item has exhausted its retries.

        Args:
            queue_name: Either a short queue name or a pymongo Collection.
            item_id: Primary key of the item.
            error_message: Human-readable reason for the failure.
        """
        target = self._resolve_queue(queue_name)
        target.update_one(
            {'_id': item_id},
            {'$set': {'status': 'error', 'error_message': error_message}},
        )

    def update_queue_status_by_task_and_qid(
        self,
        queue_name: Union[str, Collection],
        task_id: str,
        qid: str,
        status: str,
    ) -> None:
        """
        Update the ``status`` field for a ``(task_id, qid)`` row.

        Called by the main service when a job transitions to ``"completed"``.

        Args:
            queue_name: Either a short queue name or a pymongo Collection.
            task_id: Task identifier.
            qid: Wikidata identifier.
            status: New status string.
        """
        target = self._resolve_queue(queue_name)
        target.update_one(
            {'task_id': task_id, 'qid': qid},
            {'$set': {'status': status}},
        )

    def enqueue_item(
        self,
        queue_name: Union[str, Collection],
        item: Dict[str, Any],
    ) -> None:
        """
        Append a new item to a queue — the minimal "append" operation.

        Replaces the old pattern of passing ``collection.insert_one`` around
        as a raw callback, which exposed pymongo to every caller.

        Args:
            queue_name: Either a short queue name or a pymongo Collection.
            item: The document to insert.
        """
        target = self._resolve_queue(queue_name)
        target.insert_one(item)

    # ==================================================================
    # USAGE DBs (service_usage / tmp_service_usage)
    # ==================================================================
    # Separate databases on the same Mongo instance, used by the API
    # decorators to log every request and by the offline analytics script
    # (info.py) to read them.
    #
    # Previously implemented as two subclasses (StatsDBHandler,
    # TMPStatsDBHandler) each overriding `connect()` to point at a different
    # DB. That pattern leaked raw MongoClient access to every usage call
    # site. Folding them in here means a single handler now owns all Mongo
    # access; the DB handles are created lazily on first property access.
    # ------------------------------------------------------------------

    @property
    def usage_collection(self) -> Collection:
        """
        Lazy handle to ``service_usage.usage`` (production request logs).

        The database handle is opened on first access and cached for the
        lifetime of the process.
        """
        if self._usage_db_prod is None:
            self._usage_db_prod = self.client[_USAGE_DB_PROD]
        return self._usage_db_prod['usage']

    @property
    def tmp_usage_collection(self) -> Collection:
        """Lazy handle to ``tmp_service_usage.usage`` (dev/analysis mirror)."""
        if self._usage_db_dev is None:
            self._usage_db_dev = self.client[_USAGE_DB_DEV]
        return self._usage_db_dev['usage']

    def log_usage(self, record: Dict[str, Any]) -> None:
        """
        Persist a single API-usage record to the production usage DB.

        Called from the ``@log_request`` decorator on every HTTP request.
        Errors are intentionally swallowed — a usage-logging failure must
        never surface to the user as a 500.

        Args:
            record: Usage record (method, url, headers, body, timestamp,
                execution_time).
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
            use_dev_db: If ``True``, read from ``tmp_service_usage`` (the
                dev mirror used by ``info.py``). Otherwise read production.

        Returns:
            A materialised list of usage records (not a cursor) so callers
            can use ``len()``, ``tqdm``, and repeated iteration.
        """
        target = self.tmp_usage_collection if use_dev_db else self.usage_collection
        return list(target.find())


# ---------------------------------------------------------------------------
# Free-function helpers
# ---------------------------------------------------------------------------
def requestItemProcessing(
    qid: str,
    queue: Union[str, Collection],
    db: "MongoDBHandler",
    request_type: str = 'userRequested',
    algo_version: str = '1.1.1',
) -> str:
    """
    Enqueue a QID for processing if it isn't already pending.

    Thin wrapper over the handler's ``find_queue_item_by_qid`` +
    ``enqueue_item`` primitives. Kept as a module-level function because
    several callers import it directly.

    Args:
        qid: Wikidata identifier to enqueue.
        queue: Either a queue name (``"user"`` / ``"random"``) or a pymongo
            Collection.
        db: The handler instance to route reads/writes through.
        request_type: Origin tag for the request (``"userRequested"``,
            ``"Random_processing"``, ``"top_viewed"``, ...).
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
    """
    Construct the canonical "in queue" status document for a new task.

    Args:
        qid: Wikidata identifier.
        request_type: Origin tag for the request.
        algo_version: Pipeline version.

    Returns:
        A status dictionary ready for insertion.
    """
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
