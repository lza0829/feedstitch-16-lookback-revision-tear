from __future__ import annotations

from typing import Any

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.merge_rules import (
    CONFLICT_DISPOSITIONS,
    LOOKBACK_DISPOSITIONS,
    SKIP_DISPOSITIONS,
    UPSERT_DISPOSITIONS,
    _is_after,
    decide_merge,
    eligible_for_incremental,
    within_lookback_window,
)
from app.engines.parse_feed import parse_fixture
from app.repositories import canonical as canon_repo
from app.repositories import runs as runs_repo
from app.repositories import sources as sources_repo


def preview_source(source_id: int) -> dict[str, Any]:
    conn = connect()
    try:
        source = sources_repo.get_source(conn, source_id)
        if not source:
            raise ValueError("source not found")
        path = FIXTURES_DIR / source["fixture_path"]
        items = parse_fixture(path, source["format"])
        return {
            "source": source,
            "count": len(items),
            "items": items[:50],
        }
    finally:
        conn.close()


def _existing_view(existing: dict[str, Any] | None) -> dict[str, Any] | None:
    if not existing:
        return None
    return {
        "business_key": existing["business_key"],
        "title": existing["title"],
        "price": existing["price"],
        "category": existing["category"],
        "event_ts": existing["event_ts"],
        "source_revision": existing["source_revision"],
        "payload": existing["payload"],
    }


def _advance_watermark(current: str | None, candidate: str) -> str:
    """Return the later of two timestamps (monotonic watermark)."""
    if current is None:
        return candidate
    return candidate if _is_after(candidate, current) else current


def _disposition_summary(conn, run_id: int) -> dict[str, int]:
    rows = conn.execute(
        """
        SELECT disposition, COUNT(*) AS c
        FROM staging_rows WHERE run_id = ?
        GROUP BY disposition
        """,
        (run_id,),
    ).fetchall()
    return {r["disposition"]: int(r["c"]) for r in rows}


def run_ingest(source_id: int, mode: str = "incremental") -> dict[str, Any]:
    if mode not in ("incremental", "full"):
        raise ValueError("mode must be incremental or full")

    conn = connect()
    try:
        source = sources_repo.get_source(conn, source_id)
        if not source:
            raise ValueError("source not found")
        if not source["enabled"]:
            raise ValueError("source disabled")

        if mode == "full":
            # A full rebuild restarts the source from scratch; every fixture
            # row is replayed through the same merge rules below.
            sources_repo.set_watermark(conn, source_id, None)
            source["watermark_ts"] = None

        lookback_seconds = int(source.get("lookback_seconds") or 0)
        run_id = runs_repo.create_run(conn, source_id, mode)
        path = FIXTURES_DIR / source["fixture_path"]

        try:
            items = parse_fixture(path, source["format"])
        except Exception as exc:  # noqa: BLE001
            runs_repo.finish_run(
                conn,
                run_id,
                status="failed",
                parsed=0,
                upserted=0,
                conflicts=0,
                skipped=0,
                report={"error": str(exc)},
            )
            conn.commit()
            raise

        watermark_ts = source.get("watermark_ts")
        max_event_ts: str | None = watermark_ts
        lookback_candidates = 0

        for item in items:
            # Late = not strictly newer than the watermark observed when this
            # run started.  A run must not move its own goalposts mid-loop.
            is_late = bool(watermark_ts) and not _is_after(item["event_ts"], watermark_ts)

            existing = canon_repo.get_by_key(conn, item["business_key"])
            existing_view = _existing_view(existing)

            if is_late:
                # A late row may only be re-admitted inside the lookback
                # window, and then only to overwrite its own canonical row
                # with a strictly higher revision.  Everything else is
                # skipped without touching canonical or the watermark.
                # The gate itself lives in merge_rules so the rule cannot
                # diverge from what the engine tests assert.
                gate_open = eligible_for_incremental(
                    item["event_ts"],
                    watermark_ts,
                    lookback_seconds,
                    existing_revision=(
                        int(existing_view["source_revision"])
                        if existing_view is not None
                        else None
                    ),
                    incoming_revision=int(item["source_revision"]),
                )
                if gate_open:
                    lookback_candidates += 1
                else:
                    in_window = within_lookback_window(
                        item["event_ts"], watermark_ts, lookback_seconds
                    )
                    disposition = "skip_stale" if in_window else "skipped_watermark"
                    runs_repo.insert_staging(
                        conn,
                        run_id=run_id,
                        source_id=source_id,
                        item=item,
                        disposition=disposition,
                    )
                    continue

            decision = decide_merge(existing_view, item)

            if decision in ("insert", "upsert"):
                canon_repo.upsert_item(conn, item, source_id)
                if is_late:
                    # Late rows reaching this point passed the strict revision
                    # gate, so an "insert" cannot occur; the mapping below is
                    # bookkeeping rather than a branch.
                    disposition = f"lookback_{decision}"
                else:
                    disposition = decision
                runs_repo.insert_staging(
                    conn,
                    run_id=run_id,
                    source_id=source_id,
                    item=item,
                    disposition=disposition,
                )
                if not is_late:
                    max_event_ts = _advance_watermark(max_event_ts, item["event_ts"])
            elif decision == "conflict":
                # Non-key divergence is quarantined only; canonical keeps the
                # previously accepted row and the watermark does not move.
                runs_repo.insert_conflict(
                    conn,
                    run_id=run_id,
                    source_id=source_id,
                    business_key=item["business_key"],
                    existing_payload=existing_view["payload"] if existing_view else {},
                    incoming_payload=item["payload"],
                    reason="non_key_field_mismatch",
                )
                runs_repo.insert_staging(
                    conn,
                    run_id=run_id,
                    source_id=source_id,
                    item=item,
                    disposition="quarantine",
                )
            else:
                runs_repo.insert_staging(
                    conn,
                    run_id=run_id,
                    source_id=source_id,
                    item=item,
                    disposition="skip_stale",
                )

        if max_event_ts:
            sources_repo.set_watermark(conn, source_id, max_event_ts)

        # Counts are derived from the staging rows of this run so that the
        # run report and the Staging page can never drift apart.
        disposition_counts = _disposition_summary(conn, run_id)
        parsed_count = sum(disposition_counts.values())
        upserted_count = sum(
            c for d, c in disposition_counts.items() if d in UPSERT_DISPOSITIONS
        )
        conflict_count = sum(
            c for d, c in disposition_counts.items() if d in CONFLICT_DISPOSITIONS
        )
        skipped_count = sum(
            c for d, c in disposition_counts.items() if d in SKIP_DISPOSITIONS
        )
        conflict_rows_count = int(
            conn.execute(
                "SELECT COUNT(*) AS c FROM conflicts WHERE run_id = ?", (run_id,)
            ).fetchone()["c"]
        )
        canonical_count = canon_repo.count_items(conn)

        report = {
            "source_id": source_id,
            "mode": mode,
            "lookback_seconds": lookback_seconds,
            "parsed_count": parsed_count,
            "upserted_count": upserted_count,
            "conflict_count": conflict_count,
            "skipped_count": skipped_count,
            "canonical_count": canonical_count,
            "watermark_ts": max_event_ts,
            "lookback_candidate_count": lookback_candidates,
            "lookback_upserted_count": sum(
                c for d, c in disposition_counts.items() if d in LOOKBACK_DISPOSITIONS
            ),
            "conflict_rows_count": conflict_rows_count,
            "disposition_counts": disposition_counts,
        }
        runs_repo.finish_run(
            conn,
            run_id,
            status="ok",
            parsed=parsed_count,
            upserted=upserted_count,
            conflicts=conflict_count,
            skipped=skipped_count,
            report=report,
        )
        conn.commit()
        run = runs_repo.get_run(conn, run_id)
        return run or report
    finally:
        conn.close()


def dashboard_stats() -> dict[str, Any]:
    conn = connect()
    try:
        sources = sources_repo.list_sources(conn)
        return {
            "source_count": len(sources),
            "enabled_sources": sum(1 for s in sources if s["enabled"]),
            "canonical_count": canon_repo.count_items(conn),
            "conflict_count": conn.execute("SELECT COUNT(*) c FROM conflicts").fetchone()["c"],
            "run_count": conn.execute("SELECT COUNT(*) c FROM runs").fetchone()["c"],
            "staging_count": conn.execute("SELECT COUNT(*) c FROM staging_rows").fetchone()["c"],
        }
    finally:
        conn.close()
