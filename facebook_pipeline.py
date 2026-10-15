"""Unified automated Facebook collection pipeline supporting URL lists, Page discovery,
SQLite-backed state management, incremental progress resumption, and 17-column schema export.

Usage Examples:
    # 1. Run from URL list (TXT, JSONL, or CSV)
    python facebook_pipeline.py --urls facebook_sports_30_urls.txt --limit 30

    # 2. Run from Page URL
    python facebook_pipeline.py --page https://www.facebook.com/fabrizioromanoherewego/ --limit 10

    # 3. Resume previous run (automatically skips completed posts)
    python facebook_pipeline.py --urls facebook_sports_30_urls.txt --resume
"""

import argparse
import csv
import json
import logging
import os
import re
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse
from zoneinfo import ZoneInfo

from playwright.sync_api import Error

import facebook_dataset as fd

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("facebook_pipeline")

BASE_DIR = Path(__file__).resolve().parent
PROFILE_DIR = BASE_DIR / "profiles" / "facebook"
OUTPUT_DIR = BASE_DIR / "output"
DB_PATH = OUTPUT_DIR / "facebook_pipeline.sqlite"
JSONL_PATH = OUTPUT_DIR / "facebook_dataset.jsonl"
FINGERPRINT_SEED = 12345
VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


class PipelineDB:
    """SQLite-backed task state manager supporting transactional updates and resumption."""

    def __init__(self, db_path=DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        self.conn.row_factory = sqlite3.Row
        self._init_tables()

    def _init_tables(self):
        with self.conn:
            self.conn.execute(r"""
                CREATE TABLE IF NOT EXISTS pipeline_tasks (
                    input_url TEXT PRIMARY KEY,
                    canonical_url TEXT,
                    post_id TEXT,
                    author_id TEXT,
                    status TEXT DEFAULT 'pending',
                    attempts INTEGER DEFAULT 0,
                    last_error TEXT,
                    review_reasons TEXT,
                    record_json TEXT,
                    tech_meta_json TEXT,
                    created_at TEXT,
                    updated_at TEXT
                )
            """)
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON pipeline_tasks(status)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_post_id ON pipeline_tasks(post_id)")
            # Backward compatibility column check
            cols = [c[1] for c in self.conn.execute("PRAGMA table_info(pipeline_tasks)").fetchall()]
            if "tech_meta_json" not in cols:
                self.conn.execute("ALTER TABLE pipeline_tasks ADD COLUMN tech_meta_json TEXT")

    def queue_urls(self, urls):
        """Enqueues input URLs with deduplication against existing entries."""
        now = datetime.now(timezone.utc).isoformat()
        added = 0
        with self.conn:
            for u in urls:
                cleaned = u.strip()
                if not cleaned:
                    continue
                cur = self.conn.execute(
                    "INSERT OR IGNORE INTO pipeline_tasks (input_url, status, attempts, created_at, updated_at) VALUES (?, 'pending', 0, ?, ?)",
                    (cleaned, now, now),
                )
                if cur.rowcount > 0:
                    added += 1
        return added

    def get_pending_tasks(self, limit=None, retry_failed=False):
        """Returns URLs ready for processing."""
        statuses = "('pending', 'processing')"
        if retry_failed:
            statuses = "('pending', 'processing', 'failed')"
        query = f"SELECT input_url, canonical_url, status, attempts FROM pipeline_tasks WHERE status IN {statuses} ORDER BY rowid ASC"
        if limit:
            query += f" LIMIT {int(limit)}"
        rows = self.conn.execute(query).fetchall()
        return [dict(r) for r in rows]

    def update_task_processing(self, input_url):
        now = datetime.now(timezone.utc).isoformat()
        with self.conn:
            self.conn.execute(
                "UPDATE pipeline_tasks SET status = 'processing', attempts = attempts + 1, updated_at = ? WHERE input_url = ?",
                (now, input_url),
            )

    def record_success(self, input_url, canonical_url, post_id, author_id, record_17_col, tech_meta):
        now = datetime.now(timezone.utc).isoformat()
        status = tech_meta.get("status", "success")
        reasons_json = json.dumps(tech_meta.get("review_reasons", []), ensure_ascii=False)
        record_json = json.dumps(record_17_col, ensure_ascii=False)
        tech_meta_json = json.dumps(tech_meta, ensure_ascii=False)
        with self.conn:
            self.conn.execute(
                """UPDATE pipeline_tasks 
                   SET status = ?, canonical_url = ?, post_id = ?, author_id = ?, 
                       last_error = NULL, review_reasons = ?, record_json = ?, tech_meta_json = ?, updated_at = ?
                   WHERE input_url = ?""",
                (status, canonical_url, post_id, author_id, reasons_json, record_json, tech_meta_json, now, input_url),
            )

    def record_failure(self, input_url, error_message, status="failed", canonical_url=None):
        now = datetime.now(timezone.utc).isoformat()
        with self.conn:
            self.conn.execute(
                """UPDATE pipeline_tasks 
                   SET status = ?, canonical_url = COALESCE(?, canonical_url), 
                       last_error = ?, updated_at = ?
                   WHERE input_url = ?""",
                (status, canonical_url, str(error_message), now, input_url),
            )

    def get_all_records(self):
        """Retrieves all completed or reviewed records for dataset bundling."""
        rows = self.conn.execute(
            "SELECT input_url, canonical_url, post_id, author_id, status, record_json, tech_meta_json, review_reasons, last_error FROM pipeline_tasks WHERE record_json IS NOT NULL ORDER BY rowid ASC"
        ).fetchall()
        dataset = []
        debug_list = []
        for r in rows:
            rec = json.loads(r["record_json"])
            dataset.append(rec)
            t_meta = {}
            if r["tech_meta_json"]:
                try:
                    t_meta = json.loads(r["tech_meta_json"])
                except Exception:
                    pass
            debug_list.append({
                "input_url": r["input_url"],
                "canonical_url": r["canonical_url"],
                "post_id": r["post_id"],
                "author_id": r["author_id"],
                "status": r["status"],
                "raw_timestamp": t_meta.get("raw_timestamp"),
                "time_precision": t_meta.get("time_precision"),
                "counts_meta": t_meta.get("counts_meta"),
                "review_reasons": json.loads(r["review_reasons"] or "[]"),
                "last_error": r["last_error"],
                "record": rec,
            })
        return dataset, debug_list

    def get_statistics(self):
        rows = self.conn.execute("SELECT status, COUNT(*) as cnt FROM pipeline_tasks GROUP BY status").fetchall()
        stats = {r["status"]: r["cnt"] for r in rows}
        total = self.conn.execute("SELECT COUNT(*) FROM pipeline_tasks").fetchone()[0]
        return total, stats

    def close(self):
        self.conn.close()


def load_input_urls(source_path):
    """Parses input URLs from TXT, JSONL, or CSV file."""
    path = Path(source_path)
    if not path.is_file():
        raise FileNotFoundError(f"Input source file not found: {source_path}")

    urls = []
    suffix = path.suffix.lower()

    if suffix == ".txt":
        for line in path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if s and not s.startswith("#") and "facebook.com" in s:
                urls.append(s)

    elif suffix == ".jsonl":
        for line in path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                url = obj.get("post_url") or obj.get("url") or obj.get("canonical_url")
                if url and "facebook.com" in url:
                    urls.append(url)
            except json.JSONDecodeError:
                pass

    elif suffix == ".csv":
        with path.open("r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("post_url") or row.get("url") or row.get("URL") or row.get("Post URL")
                if url and "facebook.com" in url:
                    urls.append(url.strip())

    else:
        # Default text parse
        for line in path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if s and not s.startswith("#") and "facebook.com" in s:
                urls.append(s)

    # Deduplicate while preserving order
    return list(dict.fromkeys(urls))


def check_facebook_access_issues(page):
    """Detects authentication requirement, checkpoints, or captcha."""
    path = urlparse(page.url).path.lower()
    if path.startswith(("/login", "/checkpoint", "/recover")):
        return "Facebook login or checkpoint required. Run cloak_facebook_login.py to re-authenticate."
    try:
        if page.locator('input[name="email"]').is_visible() and page.locator('input[name="pass"]').is_visible():
            return "Facebook session expired. Run cloak_facebook_login.py to re-authenticate."
    except Exception:
        pass
    return None


def run_pipeline(
    input_source=None,
    page_url=None,
    limit=None,
    headless=False,
    default_sport=None,
    retry_failed=False,
    fetch_followers=True,
    output_dir=OUTPUT_DIR,
):
    """Executes the automated end-to-end extraction pipeline."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    db = PipelineDB(output_dir / "facebook_pipeline.sqlite")

    # 1. Enqueue input URLs
    if input_source:
        parsed_urls = load_input_urls(input_source)
        enqueued = db.queue_urls(parsed_urls)
        logger.info(f"Loaded {len(parsed_urls)} URLs from {input_source} ({enqueued} newly queued).")

    # 2. Enqueue from Page Discovery if requested
    if page_url:
        logger.info(f"Running Page discovery on {page_url}...")
        try:
            import cloak_facebook_discover as disc
            discovered = disc.discover_page_posts(page_url, limit=limit or 20)
            disc_urls = [p["post_url"] for p in discovered if "post_url" in p]
            enqueued_disc = db.queue_urls(disc_urls)
            logger.info(f"Discovered {len(disc_urls)} permalinks from {page_url} ({enqueued_disc} newly queued).")
        except Exception as exc:
            logger.error(f"Page discovery failed: {exc}")

    # 3. Retrieve pending tasks
    tasks = db.get_pending_tasks(limit=limit, retry_failed=retry_failed)
    total_db, status_counts = db.get_statistics()
    logger.info(f"Current DB state: {total_db} total URLs {status_counts}. Tasks to run this batch: {len(tasks)}")

    if not tasks:
        logger.info("No pending tasks. Pipeline is complete or all URLs already extracted.")
        export_dataset_bundle(db, output_dir)
        db.close()
        return 0

    if not PROFILE_DIR.is_dir():
        logger.error(f"Profile directory not found: {PROFILE_DIR}")
        logger.error("Please run cloak_facebook_login.py first to establish the authenticated session.")
        db.close()
        return 1

    followers_cache = {}
    stop_early_reason = None
    successful_in_batch = 0

    logger.info(f"Starting single browser session (headless={headless})...")
    try:
        with fd.PersistentBrowserSession(PROFILE_DIR, fingerprint=FINGERPRINT_SEED, headless=headless) as context:
            page = context.new_page()

            for idx, task in enumerate(tasks, start=1):
                raw_url = task["input_url"]
                logger.info(f"\n[{idx}/{len(tasks)}] Processing: {raw_url}")
                db.update_task_processing(raw_url)

                try:
                    # Navigate to target post URL (handles redirects e.g. /3211971012344948 -> /posts/pfbid...)
                    page.goto(raw_url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(3500)

                    # Check for login / checkpoint blocks
                    access_issue = check_facebook_access_issues(page)
                    if access_issue:
                        stop_early_reason = access_issue
                        db.record_failure(raw_url, access_issue, status="failed", canonical_url=page.url)
                        logger.warning(f"Session blocked: {access_issue}")
                        break

                    # Check for unavailable / deleted post
                    record, meta = fd.extract_scoped_post_data(page, post_url=raw_url, default_sport=default_sport)

                    if meta["status"] == "unavailable":
                        db.record_failure(raw_url, "Post unavailable or deleted", status="unavailable", canonical_url=page.url)
                        logger.info("  -> Status: unavailable (deleted or private)")
                        continue

                    # If author followers fetching requested and author handle is known
                    if fetch_followers and record.get("author_id"):
                        author = record["author_id"]
                        followers = fd.fetch_author_followers(context, author, cache=followers_cache)
                        if followers is not None:
                            record["author_followers"] = followers
                            logger.info(f"  -> Author {author} followers: {followers:,}")

                    # Append to incremental JSONL
                    with JSONL_PATH.open("a", encoding="utf-8") as jf:
                        jf.write(json.dumps(record, ensure_ascii=False) + "\n")

                    # Update database with success or needs_review
                    db.record_success(
                        input_url=raw_url,
                        canonical_url=record["post_url"],
                        post_id=record["post_id"],
                        author_id=record["author_id"],
                        record_17_col=record,
                        tech_meta=meta,
                    )

                    successful_in_batch += 1
                    logger.info(f"  -> Extracted: {meta['status']} | ID: {record['post_id']} | Text: {len(record['text'] or '')} chars | Date: {record['published_at']}")

                except (Error, Exception) as exc:
                    logger.error(f"  -> Failed processing {raw_url}: {exc}")
                    db.record_failure(raw_url, str(exc), status="failed", canonical_url=page.url if "page" in locals() else None)

            try:
                page.close()
            except Exception:
                pass

    except KeyboardInterrupt:
        logger.info("\nPipeline paused by user (Ctrl+C). Completed tasks have been safely committed.")
    except Exception as exc:
        logger.error(f"Pipeline session error: {exc}")

    # Generate final exported dataset files and quality report
    export_dataset_bundle(db, output_dir)
    print_quality_report(db)
    db.close()

    if stop_early_reason:
        print(f"\n[ALERT] Pipeline stopped: {stop_early_reason}")
        return 2

    return 0


def export_dataset_bundle(db, output_dir):
    """Exports all valid records from database into standard 17-column JSON and debug logs."""
    dataset, debug_list = db.get_all_records()
    if not dataset:
        return None

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    out_json = output_dir / f"facebook_dataset_auto_{stamp}.json"
    out_debug = output_dir / f"facebook_posts_auto_debug_{stamp}.json"

    out_json.write_text(json.dumps(dataset, ensure_ascii=False, indent=2), encoding="utf-8")
    out_debug.write_text(json.dumps(debug_list, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(f"\nSaved standardized 17-column dataset ({len(dataset)} items): {out_json}")
    logger.info(f"Saved technical debug records: {out_debug}")
    return out_json


def print_quality_report(db):
    """Prints a detailed statistics and field quality report."""
    total, stats = db.get_statistics()
    dataset, _ = db.get_all_records()

    print("\n" + "=" * 60)
    print("FACEBOOK PIPELINE EXECUTION REPORT")
    print("=" * 60)
    print(f"Total Input URLs in Database: {total}")
    for st, count in stats.items():
        print(f"  - {st.upper():<15}: {count}")

    if not dataset:
        print("\nNo records extracted yet.")
        print("=" * 60)
        return

    print("\nField Coverage & Missing Rates (Total Processed: {}):".format(len(dataset)))
    print("-" * 60)
    for col in fd.REQUIRED_COLUMNS:
        populated = sum(1 for r in dataset if r.get(col) is not None and r.get(col) != "")
        missing = len(dataset) - populated
        pct = (missing / len(dataset)) * 100
        print(f"  {col:<20}: {populated:>3} populated | {missing:>3} missing ({pct:>5.1f}%)")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Facebook Automated Scraper Pipeline")
    parser.add_argument("--urls", type=str, help="Path to input URL file (.txt, .jsonl, .csv)")
    parser.add_argument("--page", type=str, help="Facebook Page URL to discover posts from")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of URLs to process in this run")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--sport", type=str, default="Athletics", help="Default sport category (default: Athletics for Valencia Marathon)")
    parser.add_argument("--retry-failed", action="store_true", help="Retry previously failed tasks")
    parser.add_argument("--no-followers", action="store_true", help="Skip author follower lookups to maximize speed")
    parser.add_argument("--output-dir", type=str, default=str(OUTPUT_DIR), help="Output directory")

    args = parser.parse_args()

    if not args.urls and not args.page and not DB_PATH.exists():
        parser.print_help()
        print("\nError: Please provide --urls <file> or --page <url> to start the pipeline.")
        return 1

    return run_pipeline(
        input_source=args.urls,
        page_url=args.page,
        limit=args.limit,
        headless=args.headless,
        default_sport=args.sport,
        retry_failed=args.retry_failed,
        fetch_followers=not args.no_followers,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    raise SystemExit(main())
