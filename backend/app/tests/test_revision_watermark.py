"""Incremental revision / watermark reconciliation against a scenario fixture.

The fixture ``delta_revision.jsonl`` is deliberately built so one run exercises
all three rules at once, against a watermark of ``2026-11-05T12:00:00Z`` and a
3600s lookback window (``[11:00, 12:00]``):

===========  =================  =====  ===================================
key          event_ts           rev    expected disposition
===========  =================  =====  ===================================
SKU-R1       09:00 (outside)    9      skipped_watermark  (no canonical write)
SKU-R2       11:30 (inside)     5>2    lookback_upsert    (overwrites rev 2)
SKU-R3       11:40 (inside)     3==3   skip_stale         (no overwrite)
SKU-R4       11:45 (inside)     7      quarantine         (diverges, rev 4 stays)
SKU-R5       11:50 (inside)     2<3    skip_stale         (no overwrite)
SKU-R6       12:10 (after)      3      insert
SKU-R7       12:20 (after)      1      insert
SKU-R8       12:05 (after)      1      insert
===========  =================  =====  ===================================

R8 is earlier than R7 but still newer than the run-start watermark, so it must
be admitted; the watermark must end at the true maximum (12:20) and must never
move because of a skipped or quarantined row.
"""

from __future__ import annotations

import pytest

from app import db, seed
from app.repositories import canonical as canon_repo
from app.repositories import runs as runs_repo
from app.repositories import sources as sources_repo
from app.services.ingest_service import run_ingest

WATERMARK = "2026-11-05T12:00:00Z"
SOURCE_ID = 4


def _canonical_item(
    key: str, title: str, price: float, category: str, rev: int, event_ts: str
) -> dict:
    payload = {
        "business_key": key,
        "title": title,
        "price": price,
        "category": category,
        "event_ts": event_ts,
        "source_revision": rev,
    }
    return {
        "business_key": key,
        "title": title,
        "price": price,
        "category": category,
        "event_ts": event_ts,
        "source_revision": rev,
        "payload": payload,
    }


def _seed_canonical(conn) -> None:
    rows = [
        ("SKU-R1", "Brass Hook", 12.5, "hardware", 2, "2026-11-05T09:30:00Z"),
        ("SKU-R2", "Pine Crate", 44.0, "storage", 2, "2026-11-05T11:10:00Z"),
        ("SKU-R3", "Cedar Bin", 30.0, "storage", 3, "2026-11-05T11:15:00Z"),
        ("SKU-R4", "Glass Vase", 59.0, "decor", 4, "2026-11-05T11:20:00Z"),
        ("SKU-R5", "Linen Runner", 22.0, "textile", 3, "2026-11-05T11:25:00Z"),
    ]
    for key, title, price, category, rev, ts in rows:
        canon_repo.upsert_item(conn, _canonical_item(key, title, price, category, rev, ts), 1)


@pytest.fixture()
def scenario_db(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "app.db")
    seed.init_db()
    conn = db.connect()
    conn.execute(
        """
        INSERT INTO sources(name, format, fixture_path, watermark_ts,
                            lookback_seconds, enabled, note)
        VALUES (?,?,?,?,?,?,?)
        """,
        (
            "Delta Revisions",
            "jsonl",
            "delta_revision.jsonl",
            WATERMARK,
            3600,
            1,
            "scenario: out-of-window late, in-window rev gap, conflict",
        ),
    )
    _seed_canonical(conn)
    conn.commit()
    yield conn
    conn.close()


def _snapshot(conn):
    return {r["business_key"]: dict(r) for r in canon_repo.list_items(conn)}


def test_out_of_window_late_row_is_skipped_without_canonical_write(scenario_db):
    before = _snapshot(scenario_db)
    run = run_ingest(SOURCE_ID, "incremental")
    after = _snapshot(scenario_db)

    # SKU-R1 is 3h before the watermark with a much higher revision; it is
    # still outside the 1h window and must not touch canonical.
    assert before["SKU-R1"] == after["SKU-R1"]
    assert after["SKU-R1"]["source_revision"] == 2
    assert after["SKU-R1"]["price"] == 12.5

    staging = {r["business_key"]: r for r in runs_repo.list_staging(scenario_db, run["id"])}
    assert staging["SKU-R1"]["disposition"] == "skipped_watermark"
    # The out-of-window row is far older than the watermark and cannot move it.
    assert run["report"]["watermark_ts"] == "2026-11-05T12:20:00Z"


def test_in_window_revision_gate_allows_only_strictly_newer(scenario_db):
    before = _snapshot(scenario_db)
    run = run_ingest(SOURCE_ID, "incremental")
    after = _snapshot(scenario_db)

    # Strictly newer inside the window -> lookback upsert.
    assert after["SKU-R2"]["source_revision"] == 5
    assert before["SKU-R2"]["source_revision"] == 2

    # Equal revision is a re-delivery, not an upgrade.
    assert after["SKU-R3"]["source_revision"] == before["SKU-R3"]["source_revision"] == 3

    # Lower revision must never overwrite a higher canonical revision.
    assert after["SKU-R5"]["source_revision"] == before["SKU-R5"]["source_revision"] == 3

    staging = {r["business_key"]: r["disposition"] for r in runs_repo.list_staging(scenario_db, run["id"])}
    assert staging["SKU-R2"] == "lookback_upsert"
    assert staging["SKU-R3"] == "skip_stale"
    assert staging["SKU-R5"] == "skip_stale"


def test_conflict_rows_never_reach_canonical(scenario_db):
    before = _snapshot(scenario_db)
    run = run_ingest(SOURCE_ID, "incremental")
    after = _snapshot(scenario_db)

    # SKU-R4 diverges on price and has a higher revision; it is quarantined.
    assert after["SKU-R4"] == before["SKU-R4"]
    assert after["SKU-R4"]["price"] == 59.0
    assert after["SKU-R4"]["source_revision"] == 4

    conflicts = runs_repo.list_conflicts(scenario_db, run["id"])
    assert [c["business_key"] for c in conflicts] == ["SKU-R4"]
    assert conflicts[0]["existing"]["price"] == 59.0
    assert conflicts[0]["incoming"]["price"] == 69.0

    staging = {r["business_key"]: r for r in runs_repo.list_staging(scenario_db, run["id"])}
    assert staging["SKU-R4"]["disposition"] == "quarantine"


def test_report_staging_canonical_conflicts_and_watermark_agree(scenario_db):
    before = _snapshot(scenario_db)
    run = run_ingest(SOURCE_ID, "incremental")
    report = run["report"]

    staging = runs_repo.list_staging(scenario_db, run["id"])
    staging_by_key = {r["business_key"]: r for r in staging}

    expected_dispositions = {
        "SKU-R1": "skipped_watermark",
        "SKU-R2": "lookback_upsert",
        "SKU-R3": "skip_stale",
        "SKU-R4": "quarantine",
        "SKU-R5": "skip_stale",
        "SKU-R6": "insert",
        "SKU-R7": "insert",
        "SKU-R8": "insert",
    }
    assert {k: v["disposition"] for k, v in staging_by_key.items()} == expected_dispositions

    # --- report is a pure summary of this run's staging rows ---
    assert report["parsed_count"] == len(staging) == 8
    assert report["disposition_counts"] == {
        "skipped_watermark": 1,
        "lookback_upsert": 1,
        "skip_stale": 2,
        "quarantine": 1,
        "insert": 3,
    }
    assert report["upserted_count"] == 4  # 1 lookback + 3 inserts
    assert report["conflict_count"] == 1
    assert report["skipped_count"] == 3
    assert report["upserted_count"] + report["conflict_count"] + report["skipped_count"] == report["parsed_count"]
    # Two in-window late rows pass the strict revision gate (R2 and R4); only
    # R2 ends as an upsert, R4 is quarantined for non-key divergence.
    assert report["lookback_candidate_count"] == 2
    assert report["lookback_upserted_count"] == 1

    # --- conflicts: exactly the quarantined staging rows ---
    conflicts = runs_repo.list_conflicts(scenario_db, run["id"])
    assert len(conflicts) == report["conflict_count"] == report["conflict_rows_count"]
    assert {c["business_key"] for c in conflicts} == {
        k for k, d in expected_dispositions.items() if d == "quarantine"
    }

    # --- canonical: only upsert dispositions are present/written ---
    after = _snapshot(scenario_db)
    written_keys = {k for k, d in expected_dispositions.items() if d in (
        "insert",
        "upsert",
        "lookback_insert",
        "lookback_upsert",
    )}
    assert written_keys == {"SKU-R2", "SKU-R6", "SKU-R7", "SKU-R8"}
    assert report["canonical_count"] == len(canon_repo.list_items(scenario_db)) == len(after)
    assert report["canonical_count"] == len(before) + 3  # 5 seeded + 3 inserts
    # Quarantined key is still the previously accepted copy, not the incoming one.
    assert after["SKU-R4"]["price"] == before["SKU-R4"]["price"]

    # --- watermark: only legitimate advancement, true maximum ---
    assert report["watermark_ts"] == "2026-11-05T12:20:00Z"
    source = sources_repo.get_source(scenario_db, SOURCE_ID)
    assert source["watermark_ts"] == report["watermark_ts"]
    # Every skipped / quarantined row is strictly older than the watermark and
    # could not have advanced it.
    for key in ("SKU-R1", "SKU-R3", "SKU-R4", "SKU-R5"):
        assert staging_by_key[key]["event_ts"] < source["watermark_ts"]
    # The watermark advanced past the run-start value because real upserts
    # landed after it.
    assert source["watermark_ts"] > WATERMARK


def test_second_incremental_run_is_idempotent(scenario_db):
    first = run_ingest(SOURCE_ID, "incremental")
    watermark_after_first = sources_repo.get_source(scenario_db, SOURCE_ID)["watermark_ts"]
    canonical_after_first = _snapshot(scenario_db)

    second = run_ingest(SOURCE_ID, "incremental")
    report = second["report"]

    # Everything in the fixture is now at or before the watermark.  The late
    # rows are re-evaluated against canonical as it now stands: R2 is already
    # at rev 5, R3/R5 are still not newer, R4 still diverges and stays
    # quarantined.  No canonical row changes and the watermark does not move.
    assert report["upserted_count"] == 0
    assert report["conflict_count"] == 1
    assert report["conflict_rows_count"] == 1
    assert report["skipped_count"] == report["parsed_count"] - 1 == 7
    assert sources_repo.get_source(scenario_db, SOURCE_ID)["watermark_ts"] == watermark_after_first
    assert _snapshot(scenario_db) == canonical_after_first
    assert first["report"]["canonical_count"] == report["canonical_count"]
