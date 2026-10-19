#!/usr/bin/env python3
"""
Targeted Re-Extraction for NBA Facebook Pipeline
Processes URLs collected in recent runs that were unaccepted or missing data,
using upgraded extraction logic (notification filtering and tooltip hover).
"""

import sys
import os
import time
import json
import signal
import logging
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

import facebook_dataset as fd
from facebook_pipeline import (
    PipelineDB,
    export_dataset_bundle,
    write_run_summary,
    write_quality_report,
    write_progress,
    check_facebook_access_issues,
    FINGERPRINT_SEED,
    PROFILE_DIR,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(BASE_DIR / "output" / "nba_60min" / "targeted_rerun.log"), encoding="utf-8"),
    ]
)
logger = logging.getLogger("targeted_rerun")

DATASET_SCOPE = "facebook:basketball:nba"
OUTPUT_DIR = BASE_DIR / "output" / "nba_60min"
DB_PATH = OUTPUT_DIR / "facebook_pipeline.sqlite"


def get_target_urls(db_path, include_run1=True, include_run2=True, limit=None):
    """Retrieves target URLs for re-extraction from SQLite."""
    import sqlite3
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    urls = []
    
    # Run 1: overnight run (notification toast false positive)
    if include_run1:
        cur.execute("""
            SELECT input_url, canonical_url, status, review_reasons 
            FROM pipeline_tasks 
            WHERE run_id = 'run_20261017T192245_498681Z' AND is_accepted = 0
            ORDER BY rowid ASC
        """)
        run1_rows = cur.fetchall()
        logger.info(f"Loaded {len(run1_rows)} unaccepted URLs from Run 1 (run_20261017T192245_498681Z)")
        for r in run1_rows:
            urls.append({"url": r[0], "canonical": r[1], "source_run": "run_20261017T192245_498681Z", "prev_status": r[2]})

    # Run 2: second-to-last run (missing_required timestamp issues + others)
    if include_run2:
        cur.execute("""
            SELECT input_url, canonical_url, status, review_reasons 
            FROM pipeline_tasks 
            WHERE run_id = 'run_20261017T061355_863975Z' AND is_accepted = 0
            ORDER BY 
                CASE WHEN status = 'missing_required' THEN 0 ELSE 1 END,
                rowid ASC
        """)
        run2_rows = cur.fetchall()
        logger.info(f"Loaded {len(run2_rows)} unaccepted URLs from Run 2 (run_20261017T061355_863975Z)")
        for r in run2_rows:
            urls.append({"url": r[0], "canonical": r[1], "source_run": "run_20261017T061355_863975Z", "prev_status": r[2]})

    conn.close()
    
    if limit:
        urls = urls[:limit]
        
    return urls


def run_targeted_extraction(
    target_urls,
    max_hours=8.0,
    export_every=20,
    headless=True,
    output_dir=OUTPUT_DIR,
):
    """Executes live re-extraction across targeted URLs."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    db = PipelineDB(DB_PATH)

    new_run_id = f"rerun_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S_%fZ')}"
    logger.info("=" * 65)
    logger.info(f"STARTING TARGETED RE-EXTRACTION [{new_run_id}]")
    logger.info(f"Total Target URLs to process: {len(target_urls)}")
    logger.info(f"Time Budget: {max_hours:.1f} hours | Headless: {headless}")
    logger.info("=" * 65)

    start_mono = time.monotonic()
    deadline_mono = (start_mono + max_hours * 3600.0) if max_hours else None
    interrupted = False

    def handle_signal(sig, frame):
        nonlocal interrupted
        logger.info("\nInterruption signal received. Gracefully finishing current post and exporting...")
        interrupted = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    counters = {
        "attempted": 0,
        "newly_accepted": 0,
        "video_skipped": 0,
        "unmatched": 0,
        "uncertain": 0,
        "missing_required": 0,
        "unavailable": 0,
        "failed": 0,
    }

    last_export_mono = time.monotonic()

    with fd.PersistentBrowserSession(PROFILE_DIR, fingerprint=FINGERPRINT_SEED, headless=headless) as context:
        page = context.new_page()

        for idx, item in enumerate(target_urls, start=1):
            if interrupted or (deadline_mono and time.monotonic() >= deadline_mono):
                logger.info("Stopping: time limit or interrupt signal reached.")
                break

            target_url = item["url"]
            source_run = item["source_run"]
            logger.info(f"[{idx}/{len(target_urls)}] Processing: {target_url} (from {source_run})")

            counters["attempted"] += 1
            t0 = time.monotonic()

            try:
                page.goto(target_url, wait_until="domcontentloaded", timeout=30000)
                time.sleep(3.0)

                access_issue = check_facebook_access_issues(page)
                if access_issue:
                    logger.warning(f"Facebook auth / checkpoint wall: {access_issue}")
                    counters["failed"] += 1
                    break

                record, meta = fd.extract_scoped_post_data(
                    page,
                    post_url=target_url,
                    default_sport="Basketball",
                    nba_policy="contextual",
                    view_policy="verified_only",
                )

                if meta.get("status") == "unavailable":
                    counters["unavailable"] += 1
                    db.record_failure(target_url, "Post unavailable or deleted", status="unavailable", canonical_url=page.url, run_id=new_run_id, dataset_scope=DATASET_SCOPE)
                    logger.info("  -> Result: unavailable (deleted/private)")
                    continue

                if record.get("media_type") == "video":
                    counters["video_skipped"] += 1
                    db.record_failure(target_url, "video_post_omitted_by_policy", status="video_skipped", canonical_url=record.get("post_url"), run_id=new_run_id, dataset_scope=DATASET_SCOPE)
                    logger.info("  -> Result: video_skipped")
                    continue

                is_accepted, rejections = db.record_success(
                    input_url=target_url,
                    canonical_url=record.get("post_url") or target_url,
                    post_id=record.get("post_id"),
                    author_id=record.get("author_id"),
                    record_17_col=record,
                    tech_meta=meta,
                    run_id=new_run_id,
                    dataset_scope=DATASET_SCOPE,
                )

                elapsed_step = time.monotonic() - t0
                if is_accepted:
                    counters["newly_accepted"] += 1
                    logger.info(f"  -> [ACCEPTED NBA POST] ID: {record['post_id']} | Likes: {record['like_count']} | Pub: {record['published_at']} | Text: {repr(record['text'][:50])} ({elapsed_step:.1f}s)")
                else:
                    nba_st = meta.get("nba_classification", {}).get("status")
                    if nba_st == "unmatched":
                        counters["unmatched"] += 1
                    elif nba_st == "uncertain":
                        counters["uncertain"] += 1
                    else:
                        counters["missing_required"] += 1
                    logger.info(f"  -> [REJECTED] {rejections} ({elapsed_step:.1f}s)")

            except Exception as exc:
                counters["failed"] += 1
                logger.error(f"  -> Error on {target_url}: {exc}")
                try:
                    db.record_failure(target_url, str(exc), status="failed", run_id=new_run_id, dataset_scope=DATASET_SCOPE)
                except Exception:
                    pass

            # Periodic export and progress write
            if (counters["attempted"] % export_every == 0) or (time.monotonic() - last_export_mono > 180.0):
                export_dataset_bundle(db, out_dir, dataset_scope=DATASET_SCOPE, filter_sport="Basketball")
                last_export_mono = time.monotonic()
                logger.info(f"[CHECKPOINT] Exported facebook_dataset.csv. Total Attempted: {counters['attempted']} | Newly Accepted: {counters['newly_accepted']}")

    # Final export
    export_dataset_bundle(db, out_dir, dataset_scope=DATASET_SCOPE, filter_sport="Basketball")
    db.close()

    elapsed_total = time.monotonic() - start_mono
    logger.info("=" * 65)
    logger.info("TARGETED RE-EXTRACTION COMPLETE")
    logger.info(f"Elapsed Time: {elapsed_total / 60:.1f} minutes")
    logger.info(f"Attempted:      {counters['attempted']}")
    logger.info(f"Newly Accepted: {counters['newly_accepted']}")
    logger.info(f"Video Skipped:  {counters['video_skipped']}")
    logger.info(f"Missing Req:    {counters['missing_required']}")
    logger.info(f"Unmatched:      {counters['unmatched']}")
    logger.info(f"Failed:         {counters['failed']}")
    logger.info(f"Dataset Output: {out_dir / 'facebook_dataset.csv'}")
    logger.info("=" * 65)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Targeted Re-Extraction for NBA Facebook Pipeline")
    parser.add_argument("--max-hours", type=float, default=8.0, help="Max run hours (default: 8.0)")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of URLs to process")
    parser.add_argument("--export-every", type=int, default=15, help="Batch size for export (default: 15)")
    parser.add_argument("--run1-only", action="store_true", help="Only process Run 1 URLs")
    parser.add_argument("--run2-only", action="store_true", help="Only process Run 2 URLs")
    parser.add_argument("--visible", action="store_true", help="Run browser in visible mode (default: headless)")
    args = parser.parse_args()

    inc_run1 = not args.run2_only
    inc_run2 = not args.run1_only

    target_urls = get_target_urls(DB_PATH, include_run1=inc_run1, include_run2=inc_run2, limit=args.limit)
    if not target_urls:
        logger.info("No target URLs found.")
        sys.exit(0)

    run_targeted_extraction(
        target_urls=target_urls,
        max_hours=args.max_hours,
        export_every=args.export_every,
        headless=not args.visible,
    )
