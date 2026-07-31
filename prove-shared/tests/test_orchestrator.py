"""
Tests for `DatabaseOrchestrator` and the `get_database()` factory.

The orchestrator is the switching machinery: single vs dual-write modes,
primary vs fallback routing, config parsing. These tests mock out both
backends (Mongo + Postgres) with `MagicMock` and assert that calls flow
to the right place under each config.
"""
from unittest.mock import MagicMock

import pytest

from prove_shared.database.interface import DataStore
from prove_shared.database.orchestrator import (
    DatabaseOrchestrator,
    _build_from_config,
    get_database,
    reset_cached_database,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_backend_mock() -> MagicMock:
    """A MagicMock that passes `isinstance(x, DataStore)`."""
    # `spec=DataStore` makes MagicMock only accept interface methods
    # AND makes isinstance() work correctly.
    return MagicMock(spec=DataStore)


@pytest.fixture(autouse=True)
def _clear_factory_cache():
    """Ensure each test gets a fresh get_database() cache."""
    reset_cached_database()
    yield
    reset_cached_database()


# ===========================================================================
# DatabaseOrchestrator — read routing
# ===========================================================================
class TestReadRouting:
    def test_reads_go_to_primary_by_default(self):
        primary = _make_backend_mock()
        primary.get_latest_status_by_qid.return_value = {"qid": "Q42"}
        fallback = _make_backend_mock()
        orch = DatabaseOrchestrator(primary=primary, fallback=fallback)

        result = orch.get_latest_status_by_qid("Q42")

        assert result == {"qid": "Q42"}
        primary.get_latest_status_by_qid.assert_called_once_with("Q42")
        fallback.get_latest_status_by_qid.assert_not_called()

    def test_primary_read_failure_reraises_by_default(self):
        """Silent fallbacks hide outages — off by default."""
        primary = _make_backend_mock()
        primary.get_latest_status_by_qid.side_effect = RuntimeError("primary down")
        fallback = _make_backend_mock()
        orch = DatabaseOrchestrator(primary=primary, fallback=fallback)

        with pytest.raises(RuntimeError, match="primary down"):
            orch.get_latest_status_by_qid("Q42")
        fallback.get_latest_status_by_qid.assert_not_called()

    def test_fallback_engages_when_opt_in_flag_set(self):
        primary = _make_backend_mock()
        primary.get_latest_status_by_qid.side_effect = RuntimeError("primary down")
        fallback = _make_backend_mock()
        fallback.get_latest_status_by_qid.return_value = {"qid": "Q42", "from": "fallback"}
        orch = DatabaseOrchestrator(
            primary=primary, fallback=fallback, auto_fallback_on_read=True,
        )

        result = orch.get_latest_status_by_qid("Q42")

        assert result == {"qid": "Q42", "from": "fallback"}
        fallback.get_latest_status_by_qid.assert_called_once_with("Q42")


# ===========================================================================
# DatabaseOrchestrator — write routing
# ===========================================================================
class TestWriteRouting:
    def test_single_mode_writes_to_primary_only(self):
        primary = _make_backend_mock()
        fallback = _make_backend_mock()
        orch = DatabaseOrchestrator(primary=primary, fallback=fallback, dual_write=False)

        orch.upsert_summary_by_id("Q42", {"proveScore": 0.5})

        primary.upsert_summary_by_id.assert_called_once_with("Q42", {"proveScore": 0.5})
        fallback.upsert_summary_by_id.assert_not_called()

    def test_dual_write_mirrors_to_both(self):
        primary = _make_backend_mock()
        fallback = _make_backend_mock()
        orch = DatabaseOrchestrator(primary=primary, fallback=fallback, dual_write=True)

        orch.upsert_summary_by_id("Q42", {"proveScore": 0.5})

        primary.upsert_summary_by_id.assert_called_once_with("Q42", {"proveScore": 0.5})
        fallback.upsert_summary_by_id.assert_called_once_with("Q42", {"proveScore": 0.5})

    def test_dual_write_primary_succeeds_even_if_fallback_fails(self):
        """During migration, primary is source of truth. Fallback errors are logged."""
        primary = _make_backend_mock()
        fallback = _make_backend_mock()
        fallback.upsert_summary_by_id.side_effect = RuntimeError("fallback down")
        orch = DatabaseOrchestrator(primary=primary, fallback=fallback, dual_write=True)

        # Must NOT raise — the primary write succeeded.
        orch.upsert_summary_by_id("Q42", {"proveScore": 0.5})
        primary.upsert_summary_by_id.assert_called_once()

    def test_primary_write_failure_still_raises(self):
        primary = _make_backend_mock()
        primary.upsert_summary_by_id.side_effect = RuntimeError("primary down")
        fallback = _make_backend_mock()
        orch = DatabaseOrchestrator(primary=primary, fallback=fallback, dual_write=True)

        with pytest.raises(RuntimeError, match="primary down"):
            orch.upsert_summary_by_id("Q42", {"proveScore": 0.5})


# ===========================================================================
# DatabaseOrchestrator — log_usage contract
# ===========================================================================
class TestLogUsageContract:
    def test_orchestrator_log_usage_never_raises(self):
        """Interface contract: log_usage never raises, even through the orchestrator."""
        primary = _make_backend_mock()
        primary.log_usage.side_effect = RuntimeError("boom")
        orch = DatabaseOrchestrator(primary=primary)

        # Must not raise.
        orch.log_usage({"method": "GET"})


# ===========================================================================
# DatabaseOrchestrator — claim-operations bypass dual-write
# ===========================================================================
class TestClaimOperationsPrimaryOnly:
    """
    `get_next_request` and `get_request_by_id_and_reset` atomically
    read+write a single row (claiming / releasing). Dual-writing would
    double-claim the same row in two backends, which is wrong.
    """

    def test_get_next_request_is_primary_only_even_in_dual_write(self):
        primary = _make_backend_mock()
        primary.get_next_request.return_value = {"qid": "Q1"}
        fallback = _make_backend_mock()
        orch = DatabaseOrchestrator(primary=primary, fallback=fallback, dual_write=True)

        queue = MagicMock()
        result = orch.get_next_request(queue)

        assert result == {"qid": "Q1"}
        primary.get_next_request.assert_called_once_with(queue)
        fallback.get_next_request.assert_not_called()

    def test_get_request_by_id_and_reset_is_primary_only(self):
        primary = _make_backend_mock()
        fallback = _make_backend_mock()
        orch = DatabaseOrchestrator(primary=primary, fallback=fallback, dual_write=True)

        queue = MagicMock()
        orch.get_request_by_id_and_reset(queue, "some-id")

        primary.get_request_by_id_and_reset.assert_called_once()
        fallback.get_request_by_id_and_reset.assert_not_called()


# ===========================================================================
# DatabaseOrchestrator — attribute proxy for Mongo-specific fields
# ===========================================================================
class TestAttributeProxy:
    """Legacy callers reach for `.user_collection` etc. — proxy to primary."""

    def test_unknown_attribute_proxies_to_primary(self):
        primary = _make_backend_mock()
        primary.user_collection = "sentinel"
        orch = DatabaseOrchestrator(primary=primary)

        assert orch.user_collection == "sentinel"


# ===========================================================================
# _build_from_config — config parsing
# ===========================================================================
class TestBuildFromConfig:
    def test_default_empty_config_uses_mongo_single(self, monkeypatch):
        """Empty `database:` block defaults to single Mongo (no orchestrator)."""
        fake_mongo = _make_backend_mock()
        monkeypatch.setattr(
            "prove_shared.database.orchestrator._build_backend",
            lambda kind, _: fake_mongo if kind == "mongo" else None,
        )

        db = _build_from_config({})

        # No fallback + single mode = bare backend, not wrapped.
        assert db is fake_mongo

    def test_fallback_configured_wraps_in_orchestrator(self, monkeypatch):
        fake_mongo = _make_backend_mock()
        fake_pg = _make_backend_mock()
        monkeypatch.setattr(
            "prove_shared.database.orchestrator._build_backend",
            lambda kind, _: fake_mongo if kind == "mongo" else fake_pg,
        )

        db = _build_from_config({
            "primary": "mongo",
            "fallback": "postgres",
            "mode": "single",
        })

        assert isinstance(db, DatabaseOrchestrator)
        assert db.primary is fake_mongo
        assert db.fallback is fake_pg
        assert db.dual_write is False

    def test_dual_write_mode(self, monkeypatch):
        fake_mongo = _make_backend_mock()
        fake_pg = _make_backend_mock()
        monkeypatch.setattr(
            "prove_shared.database.orchestrator._build_backend",
            lambda kind, _: fake_mongo if kind == "mongo" else fake_pg,
        )

        db = _build_from_config({
            "primary": "mongo",
            "fallback": "postgres",
            "mode": "dual-write",
        })

        assert isinstance(db, DatabaseOrchestrator)
        assert db.dual_write is True

    def test_auto_fallback_on_read_flag(self, monkeypatch):
        fake_mongo = _make_backend_mock()
        fake_pg = _make_backend_mock()
        monkeypatch.setattr(
            "prove_shared.database.orchestrator._build_backend",
            lambda kind, _: fake_mongo if kind == "mongo" else fake_pg,
        )

        db = _build_from_config({
            "primary": "postgres",
            "fallback": "mongo",
            "auto_fallback_on_read": True,
        })

        assert isinstance(db, DatabaseOrchestrator)
        assert db.auto_fallback_on_read is True


# ===========================================================================
# get_database — factory + caching
# ===========================================================================
class TestGetDatabaseFactory:
    def test_inline_config_skips_cache(self, monkeypatch):
        build_calls = []

        def fake_build(kind, _settings):
            build_calls.append(kind)
            return _make_backend_mock()

        monkeypatch.setattr(
            "prove_shared.database.orchestrator._build_backend", fake_build,
        )

        # Two inline-config calls = two backend builds, no caching.
        get_database(config={"primary": "mongo"})
        get_database(config={"primary": "mongo"})

        assert len(build_calls) == 2

    def test_file_based_config_is_cached(self, monkeypatch, tmp_path):
        build_calls = []

        def fake_build(kind, _settings):
            build_calls.append(kind)
            return _make_backend_mock()

        monkeypatch.setattr(
            "prove_shared.database.orchestrator._build_backend", fake_build,
        )

        config_file = tmp_path / "config.yaml"
        config_file.write_text("database:\n  primary: mongo\n")

        db1 = get_database(config_path=str(config_file))
        db2 = get_database(config_path=str(config_file))

        assert db1 is db2
        assert len(build_calls) == 1  # built once, reused

    def test_missing_config_file_falls_back_to_mongo_default(self, monkeypatch):
        monkeypatch.setattr(
            "prove_shared.database.orchestrator._build_backend",
            lambda kind, _: _make_backend_mock() if kind == "mongo" else None,
        )

        db = get_database(config_path="/definitely/not/a/real/path.yaml")

        assert db is not None

    def test_refresh_bypasses_cache(self, monkeypatch, tmp_path):
        build_calls = []
        monkeypatch.setattr(
            "prove_shared.database.orchestrator._build_backend",
            lambda kind, _: (build_calls.append(kind), _make_backend_mock())[1],
        )

        config_file = tmp_path / "config.yaml"
        config_file.write_text("database:\n  primary: mongo\n")

        get_database(config_path=str(config_file))
        get_database(config_path=str(config_file), refresh=True)

        assert len(build_calls) == 2
