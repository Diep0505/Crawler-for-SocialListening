"""Unified automated Facebook collection pipeline supporting URL lists, Page discovery,
SQLite-backed state management, incremental progress resumption,
and 17-column schema export.

Usage Examples:
    # 1. Run from URL list (permalink mode)
    python facebook_pipeline.py --urls facebook_sports_30_urls.txt --limit 30

    # 2. Run from a Facebook Page (discovers permalinks and collects complete records)
    python facebook_pipeline.py --page https://www.facebook.com/nba/ --limit 20

    # 3. Run multiple Pages with sport hint and max hours
    python facebook_pipeline.py --pages sources/pages.txt --sport-hint Basketball --max-hours 2.0 --limit 50

    # 4. Export CSV with sport filter (e.g. Football)
    python facebook_pipeline.py --urls facebook_sports_30_urls.txt --filter-sport Football
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
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse
from zoneinfo import ZoneInfo

from playwright.sync_api import Error

import facebook_dataset as fd
import cloak_facebook_discover as disc

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

            self.conn.execute(r"""
                CREATE TABLE IF NOT EXISTS post_aliases (
                    alias_url TEXT PRIMARY KEY,
                    canonical_post_id TEXT,
                    resolved_url TEXT,
                    created_at TEXT
                )
            """)

            self.conn.execute(r"""
                CREATE TABLE IF NOT EXISTS author_followers_cache (
                    author_id TEXT PRIMARY KEY,
                    followers INTEGER,
                    raw_value TEXT,
                    is_approximate INTEGER,
                    source TEXT,
                    observed_at TEXT,
                    expires_at TEXT
                )
            """)

            # Non-destructive migration: check columns in pipeline_tasks
            cols = [c[1] for c in self.conn.execute("PRAGMA table_info(pipeline_tasks)").fetchall()]
            if "tech_meta_json" not in cols:
                self.conn.execute("ALTER TABLE pipeline_tasks ADD COLUMN tech_meta_json TEXT")

    def queue_urls(self, urls):
        """Enqueues input URLs with deduplication against existing entries.
        Rejects root Fanpage profile URLs (e.g. facebook.com/nba/) so only post permalinks are queued.
        """
        now = datetime.now(timezone.utc).isoformat()
        added = 0
        with self.conn:
            for u in urls:
                cleaned = u.strip()
                if not cleaned:
                    continue
                # Validate that URL looks like a post candidate, not a root Page URL
                parsed = urlparse(cleaned)
                path = parsed.path.strip("/")
                if not path and not parsed.query:
                    continue
                # Single alphabetic slug without query is a Fanpage URL (e.g. /nba or /nba/)
                if not path.isdigit() and "/" not in path and not parsed.query:
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
        """Upserts card task record before updating, checking affected row count to prevent orphaned JSONL entries."""
        now = datetime.now(timezone.utc).isoformat()
        status = tech_meta.get("status", "success")
        reasons_json = json.dumps(tech_meta.get("review_reasons", []), ensure_ascii=False)
        record_json = json.dumps(record_17_col, ensure_ascii=False)
        tech_meta_json = json.dumps(tech_meta, ensure_ascii=False)

        with self.conn:
            # Ensure task exists first so UPDATE affects at least 1 row
            self.conn.execute(
                """INSERT OR IGNORE INTO pipeline_tasks 
                   (input_url, canonical_url, post_id, author_id, status, attempts, created_at, updated_at) 
                   VALUES (?, ?, ?, ?, 'processing', 1, ?, ?)""",
                (input_url, canonical_url, post_id, author_id, now, now),
            )
            cur = self.conn.execute(
                """UPDATE pipeline_tasks 
                   SET status = ?, canonical_url = ?, post_id = ?, author_id = ?, 
                       last_error = NULL, review_reasons = ?, record_json = ?, tech_meta_json = ?, updated_at = ?
                   WHERE input_url = ?""",
                (status, canonical_url, post_id, author_id, reasons_json, record_json, tech_meta_json, now, input_url),
            )
            if cur.rowcount == 0:
                raise RuntimeError(f"Failed to record success in SQLite for {input_url} (rowcount=0)")

            # Record alias if input URL differs from canonical post URL
            if canonical_url and input_url != canonical_url and post_id:
                self.conn.execute(
                    "INSERT OR REPLACE INTO post_aliases (alias_url, canonical_post_id, resolved_url, created_at) VALUES (?, ?, ?, ?)",
                    (input_url, post_id, canonical_url, now),
                )

    def record_failure(self, input_url, error_message, status="failed", canonical_url=None):
        now = datetime.now(timezone.utc).isoformat()
        with self.conn:
            self.conn.execute(
                """INSERT OR IGNORE INTO pipeline_tasks (input_url, status, attempts, created_at, updated_at) 
                   VALUES (?, 'pending', 0, ?, ?)""",
                (input_url, now, now),
            )
            self.conn.execute(
                """UPDATE pipeline_tasks 
                   SET status = ?, canonical_url = COALESCE(?, canonical_url), 
                       last_error = ?, updated_at = ?
                   WHERE input_url = ?""",
                (status, canonical_url, str(error_message), now, input_url),
            )

    def is_post_completed(self, post_id):
        """Checks if a post_id has already been successfully recorded in SQLite."""
        if not post_id:
            return False
        row = self.conn.execute(
            "SELECT 1 FROM pipeline_tasks WHERE post_id = ? AND status IN ('success', 'needs_review') LIMIT 1",
            (post_id,),
        ).fetchone()
        return bool(row)

    def get_followers_cache(self, author_id, max_age_hours=24):
        """Retrieves cached followers count for author if not expired."""
        if not author_id:
            return None
        row = self.conn.execute(
            "SELECT followers, raw_value, is_approximate, source, observed_at, expires_at FROM author_followers_cache WHERE author_id = ?",
            (author_id.lower(),),
        ).fetchone()
        if not row:
            return None
        now = datetime.now(timezone.utc).isoformat()
        if row["expires_at"] and now > row["expires_at"]:
            return None
        return dict(row)

    def set_followers_cache(self, author_id, followers, raw_value=None, is_approximate=False, source="profile", ttl_hours=24):
        """Stores author followers in SQLite cache. Never permanently caches None or failed lookups."""
        if not author_id or followers is None:
            return
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat()
        expires = (now_dt + timedelta(hours=ttl_hours)).isoformat()
        with self.conn:
            self.conn.execute(
                """INSERT OR REPLACE INTO author_followers_cache 
                   (author_id, followers, raw_value, is_approximate, source, observed_at, expires_at) 
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (author_id.lower(), int(followers), str(raw_value) if raw_value else str(followers), 1 if is_approximate else 0, source, now, expires),
            )

    def get_all_records(self):
        """Retrieves all completed or reviewed records for dataset bundling.
        Maps raw timestamp and count approximation based on key existence without losing False or empty dicts.
        """
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

            raw_ts = t_meta["raw_published_at"] if "raw_published_at" in t_meta else t_meta.get("raw_timestamp")
            is_approx = t_meta["is_approximate"] if "is_approximate" in t_meta else t_meta.get("counts_meta")

            debug_list.append({
                "input_url": r["input_url"],
                "canonical_url": r["canonical_url"],
                "post_id": r["post_id"],
                "author_id": r["author_id"],
                "status": r["status"],
                "raw_timestamp": raw_ts,
                "time_precision": t_meta.get("time_precision"),
                "is_approximate": is_approx,
                "sport_classification": t_meta.get("sport_classification"),
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
        for line in path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if s and not s.startswith("#") and "facebook.com" in s:
                urls.append(s)

    return list(dict.fromkeys(urls))


def load_page_sources(page=None, pages_file=None):
    """Parses Page sources from --page and/or --pages file."""
    sources = []
    if page:
        sources.append(page.strip())
    if pages_file:
        p_path = Path(pages_file)
        if not p_path.is_file():
            raise FileNotFoundError(f"Pages file not found: {pages_file}")
        for line in p_path.read_text(encoding="utf-8-sig").splitlines():
            s = line.strip()
            if s and not s.startswith("#") and "facebook.com" in s:
                sources.append(s)
    return list(dict.fromkeys(sources))


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


def export_csv_dataset(db, output_dir, filter_sport=None):
    """Exports dataset from SQLite to CSV using atomic rename (.tmp -> .csv).
    Strictly follows fd.REQUIRED_COLUMNS order, utf-8-sig, newline='', empty cell for null.
    Returns (csv_path, kept_count, excluded_count).
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_final = out_dir / "facebook_dataset.csv"
    csv_tmp = out_dir / "facebook_dataset.csv.tmp"

    dataset, _ = db.get_all_records()
    if not dataset:
        return csv_final, 0, 0

    kept_rows = []
    excluded_count = 0
    clean_filter = filter_sport.strip().lower() if filter_sport else None

    for rec in dataset:
        sport = rec.get("sport_category") or ""
        if clean_filter:
            sports = [s.strip().lower() for s in sport.split(",") if s.strip()]
            if clean_filter not in sports:
                excluded_count += 1
                continue

        row = {}
        for col in fd.REQUIRED_COLUMNS:
            val = rec.get(col)
            if val is None:
                row[col] = ""
            elif col in ("hashtags", "mentions"):
                if isinstance(val, (list, tuple)):
                    row[col] = json.dumps(val, ensure_ascii=False)
                elif isinstance(val, str) and val.startswith("["):
                    row[col] = val
                else:
                    row[col] = "[]"
            elif col in ("post_id", "author_id"):
                row[col] = str(val)
            elif col in ("like_count", "comment_count", "share_count", "view_count", "author_followers"):
                try:
                    row[col] = int(val)
                except (ValueError, TypeError):
                    row[col] = ""
            else:
                row[col] = str(val)
        kept_rows.append(row)

    with csv_tmp.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fd.REQUIRED_COLUMNS)
        writer.writeheader()
        for r in kept_rows:
            writer.writerow(r)
        f.flush()
        os.fsync(f.fileno())

    csv_tmp.replace(csv_final)
    return csv_final, len(kept_rows), excluded_count


def export_dataset_bundle(db, output_dir, filter_sport=None):
    """Exports dataset to atomic CSV, timestamped JSON snapshot, and debug logs."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path, kept_count, excluded_count = export_csv_dataset(db, out_dir, filter_sport=filter_sport)

    dataset, debug_list = db.get_all_records()
    if not dataset:
        return None

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    out_json = out_dir / f"facebook_dataset_auto_{stamp}.json"
    out_debug = out_dir / f"facebook_posts_auto_debug_{stamp}.json"

    out_json.write_text(json.dumps(dataset, ensure_ascii=False, indent=2), encoding="utf-8")
    out_debug.write_text(json.dumps(debug_list, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(f"Exported primary CSV ({kept_count} rows, {excluded_count} filtered out): {csv_path}")
    logger.info(f"Saved dataset JSON: {out_json}")
    logger.info(f"Saved technical debug records: {out_debug}")
    return csv_path


def print_quality_report(db, filter_sport=None, elapsed_seconds=0, counters=None):
    """Prints a detailed statistics, field quality, and execution report."""
    total, stats = db.get_statistics()
    dataset, _ = db.get_all_records()

    print("\n" + "=" * 60)
    print("FACEBOOK PIPELINE EXECUTION REPORT")
    print("=" * 60)
    print(f"Total Execution Time: {elapsed_seconds:.1f}s ({elapsed_seconds / 60:.2f} min)")
    if counters:
        print(f"Counters: attempted={counters.get('attempted', 0)}, success={counters.get('success', 0)}, "
              f"needs_review={counters.get('needs_review', 0)}, failed={counters.get('failed', 0)}, "
              f"unavailable={counters.get('unavailable', 0)}, refreshes={counters.get('refreshes', 0)}")
    print(f"Total Input/Card URLs in Database: {total}")
    for st, count in stats.items():
        print(f"  - {st.upper():<15}: {count}")

    if filter_sport:
        clean_filter = filter_sport.strip().lower()
        matching = sum(1 for r in dataset if clean_filter in [s.strip().lower() for s in (r.get("sport_category") or "").split(",") if s.strip()])
        print(f"  - FILTER ({filter_sport}): {matching} matching posts for CSV ({len(dataset) - matching} excluded)")

    if not dataset:
        print("\nNo records extracted yet.")
        print("=" * 60)
        return

    print("\nField Coverage & Missing Rates (Total In DB: {}):".format(len(dataset)))
    print("-" * 60)
    for col in fd.REQUIRED_COLUMNS:
        populated = sum(1 for r in dataset if r.get(col) is not None and r.get(col) != "")
        missing = len(dataset) - populated
        pct = (missing / len(dataset)) * 100
        print(f"  {col:<20}: {populated:>3} populated | {missing:>3} missing ({pct:>5.1f}%)")
    print("=" * 60)


def run_pipeline(
    input_source=None,
    page_url=None,
    pages_file=None,
    limit=None,
    headless=False,
    sport_hint=None,
    filter_sport=None,
    retry_failed=False,
    fetch_followers=True,
    output_dir=OUTPUT_DIR,
    max_hours=None,
    idle_rounds=10,
    max_refreshes=2,
    refresh_wait=15,
    mode="permalink",
    **kwargs,
):
    """Executes the automated end-to-end extraction pipeline."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    jsonl_output = output_dir / "facebook_dataset.jsonl"
    db = PipelineDB(output_dir / "facebook_pipeline.sqlite")

    start_mono = time.monotonic()
    deadline_mono = (start_mono + max_hours * 3600.0) if (max_hours and max_hours > 0) else None

    def is_deadline_reached():
        return deadline_mono is not None and time.monotonic() >= deadline_mono

    def interruptible_sleep(seconds, step=0.5):
        target = time.monotonic() + seconds
        while time.monotonic() < target:
            if is_deadline_reached():
                break
            time.sleep(min(step, target - time.monotonic()))

    counters = {
        "attempted": 0,
        "success": 0,
        "needs_review": 0,
        "failed": 0,
        "unavailable": 0,
        "duplicates": 0,
        "fallback": 0,
        "refreshes": 0,
    }

    last_export_mono = time.monotonic()
    stop_early_reason = None

    if not PROFILE_DIR.is_dir():
        logger.error(f"Profile directory not found: {PROFILE_DIR}")
        logger.error("Please run cloak_facebook_login.py first to establish authenticated session.")
        db.close()
        return 1

    # -------------------------------------------------------------
    # PERMALINK & PAGE DISCOVERY COLLECTION PIPELINE
    # -------------------------------------------------------------
    logger.info(f"Starting pipeline in permalink mode with limit={limit}, max_hours={max_hours}...")

    # 1. Enqueue input URLs
    if input_source:
        parsed_urls = load_input_urls(input_source)
        enqueued = db.queue_urls(parsed_urls)
        logger.info(f"Loaded {len(parsed_urls)} URLs from {input_source} ({enqueued} newly queued).")

    # 2. Enqueue from Page Discovery if requested
    page_sources = load_page_sources(page=page_url, pages_file=pages_file)
    for ps in page_sources:
        if is_deadline_reached():
            stop_early_reason = "max_hours_deadline_reached"
            break
        logger.info(f"Running Page discovery on {ps} (headless={headless})...")
        try:
            discovered = disc.discover_page_posts(
                ps,
                limit=limit or 20,
                headless=headless,
                idle_rounds=idle_rounds,
                max_refreshes=max_refreshes,
                refresh_wait=refresh_wait,
            )
            disc_urls = [p["post_url"] for p in discovered if "post_url" in p]
            enqueued_disc = db.queue_urls(disc_urls)
            logger.info(f"Discovered {len(disc_urls)} permalinks from {ps} ({enqueued_disc} newly queued).")
        except Exception as exc:
            logger.error(f"Page discovery failed on {ps}: {exc}")

    # 3. Retrieve pending tasks
    tasks = db.get_pending_tasks(limit=limit, retry_failed=retry_failed)
    total_db, status_counts = db.get_statistics()
    logger.info(f"Current DB state: {total_db} total URLs {status_counts}. Tasks to run this batch: {len(tasks)}")

    if not tasks:
        logger.info("No pending tasks. Pipeline is complete or all URLs already extracted.")
        export_dataset_bundle(db, output_dir, filter_sport=filter_sport)
        db.close()
        return 0

    followers_cache = {}

    try:
        with fd.PersistentBrowserSession(PROFILE_DIR, fingerprint=FINGERPRINT_SEED, headless=headless) as context:
            page = context.new_page()

            for idx, task in enumerate(tasks, start=1):
                if is_deadline_reached():
                    stop_early_reason = "max_hours_deadline_reached"
                    break

                raw_url = task["input_url"]
                counters["attempted"] += 1
                logger.info(f"\n[{idx}/{len(tasks)}] Processing: {raw_url}")
                db.update_task_processing(raw_url)

                try:
                    page.goto(raw_url, wait_until="domcontentloaded", timeout=30000)
                    interruptible_sleep(3.5)

                    access_issue = check_facebook_access_issues(page)
                    if access_issue:
                        stop_early_reason = access_issue
                        db.record_failure(raw_url, access_issue, status="failed", canonical_url=page.url)
                        logger.warning(f"Session blocked: {access_issue}")
                        break

                    record, meta = fd.extract_scoped_post_data(page, post_url=raw_url, default_sport=sport_hint)

                    if meta["status"] == "unavailable":
                        counters["unavailable"] += 1
                        db.record_failure(raw_url, "Post unavailable or deleted", status="unavailable", canonical_url=page.url)
                        logger.info("  -> Status: unavailable (deleted or private)")
                        continue

                    # Author followers lookup/caching
                    if fetch_followers and record.get("author_id"):
                        author = record["author_id"]
                        cached_fol = db.get_followers_cache(author)
                        if cached_fol:
                            record["author_followers"] = cached_fol["followers"]
                        else:
                            f_val = fd.fetch_author_followers(context, author, cache=followers_cache)
                            if f_val is not None:
                                db.set_followers_cache(author, f_val, source="profile")
                                record["author_followers"] = f_val

                    # Commit to SQLite
                    db.record_success(
                        input_url=raw_url,
                        canonical_url=record["post_url"],
                        post_id=record["post_id"],
                        author_id=record["author_id"],
                        record_17_col=record,
                        tech_meta=meta,
                    )

                    # Append to JSONL
                    with jsonl_output.open("a", encoding="utf-8") as jf:
                        jf.write(json.dumps(record, ensure_ascii=False) + "\n")

                    if meta["status"] == "success":
                        counters["success"] += 1
                    else:
                        counters["needs_review"] += 1

                    # Periodic CSV export
                    if time.monotonic() - last_export_mono >= 60.0:
                        export_csv_dataset(db, output_dir, filter_sport=filter_sport)
                        last_export_mono = time.monotonic()

                except (Error, Exception) as exc:
                    counters["failed"] += 1
                    logger.error(f"  -> Failed processing {raw_url}: {exc}")
                    db.record_failure(raw_url, str(exc), status="failed", canonical_url=page.url if "page" in locals() else None)

            try:
                page.close()
            except Exception:
                pass

    except KeyboardInterrupt:
        logger.info("\nPipeline paused by user (Ctrl+C). Completed tasks safely committed.")
    except Exception as exc:
        logger.error(f"Pipeline session error: {exc}")

    # Export final dataset files and report
    elapsed_total = time.monotonic() - start_mono
    export_dataset_bundle(db, output_dir, filter_sport=filter_sport)
    print_quality_report(db, filter_sport=filter_sport, elapsed_seconds=elapsed_total, counters=counters)
    db.close()

    if stop_early_reason:
        print(f"\n[ALERT] Pipeline stopped: {stop_early_reason}")
        return 2

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Facebook Automated Scraper Pipeline (Permalink & Page Discovery)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--mode", type=str, default="permalink", help=argparse.SUPPRESS)
    parser.add_argument("--urls", type=str, help="Path to input URL file (.txt, .jsonl, .csv)")
    parser.add_argument("--page", type=str, help="Single Facebook Page URL to discover posts from")
    parser.add_argument("--pages", type=str, help="UTF-8 text file containing Page URLs, one per line")
    parser.add_argument("--limit", type=int, default=None, help="Max posts to process")
    parser.add_argument("--max-hours", type=float, default=None, help="Maximum execution time in hours (up to 8.0h)")
    parser.add_argument("--idle-rounds", type=int, default=10, help="Consecutive idle rounds before refresh during discovery (default: 10)")
    parser.add_argument("--max-refreshes", type=int, default=2, help="Max reload attempts per page when scrolling stalls; 0 disables (default: 2)")
    parser.add_argument("--refresh-wait", type=int, default=15, help="Seconds to wait before reload (default: 15)")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")

    # Sport arguments
    parser.add_argument("--sport", type=str, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--sport-hint", type=str, default=None, help="Source topic hint for metadata (default: None; does not override post evidence)")
    parser.add_argument("--filter-sport", type=str, default=None, help="Optional filter to select canonical sport for exported CSV (default: None)")

    parser.add_argument("--retry-failed", action="store_true", help="Retry previously failed tasks")
    parser.add_argument("--resume", action="store_true", help="Resume previous run (default behavior; skips completed tasks)")
    parser.add_argument("--no-followers", action="store_true", help="Skip author follower lookups to maximize speed")
    parser.add_argument("--output-dir", type=str, default=str(OUTPUT_DIR), help="Output directory for SQLite, CSV, JSONL, and reports")

    args = parser.parse_args()

    # Handle deprecated mode argument
    if args.mode == "feed":
        logger.warning("Feed streaming mode has been removed due to Facebook Comet DOM instability. Automatically running in robust Permalink/Discovery mode.")

    # Validate deprecated --sport alias
    if args.sport is not None:
        logger.warning("Flag --sport is deprecated and treated as --sport-hint. Default sport is None; post categories are derived from post content.")
        if not args.sport_hint:
            args.sport_hint = args.sport

    if not args.urls and not args.page and not args.pages and not (Path(args.output_dir) / "facebook_pipeline.sqlite").exists():
        parser.error("Please provide --urls <file>, --page <url>, or --pages <file> to start pipeline.")

    return run_pipeline(
        input_source=args.urls,
        page_url=args.page,
        pages_file=args.pages,
        limit=args.limit,
        headless=args.headless,
        sport_hint=args.sport_hint,
        filter_sport=args.filter_sport,
        retry_failed=args.retry_failed,
        fetch_followers=not args.no_followers,
        output_dir=args.output_dir,
        max_hours=args.max_hours,
        idle_rounds=args.idle_rounds,
        max_refreshes=args.max_refreshes,
        refresh_wait=args.refresh_wait,
    )


if __name__ == "__main__":
    raise SystemExit(main())
