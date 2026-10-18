"""Unified automated Facebook collection pipeline supporting URL lists, Page discovery,
SQLite-backed state management, incremental progress resumption,
and 17-column schema export.

Usage Examples:
    # 1. 60-Minute Pilot Benchmark for NBA (recommended handover command):
    caffeinate -i python3 -u facebook_pipeline.py \
      --pages sources/nba_pages.txt \
      --sport-hint Basketball \
      --filter-sport Basketball \
      --competition NBA \
      --batch-size 20 \
      --max-hours 1 \
      --max-refreshes 0 \
      --output-dir output/nba_60min

    # 2. Run from specific URL list:
    python facebook_pipeline.py --urls sources/facebook_sports_30_urls.txt --competition NBA

    # 3. Reference 8-Hour Production run:
    caffeinate -i python3 -u facebook_pipeline.py \
      --pages sources/nba_pages.txt \
      --sport-hint Basketball \
      --filter-sport Basketball \
      --competition NBA \
      --batch-size 20 \
      --max-hours 8 \
      --max-refreshes 0 \
      --output-dir output/nba_60min
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
logger = logging.getLogger("facebook_pipeline")
logger.setLevel(logging.INFO)
BASE_DIR = Path(__file__).resolve().parent
PROFILE_DIR = BASE_DIR / "profiles" / "facebook"
OUTPUT_DIR = BASE_DIR / "output"
DB_PATH = OUTPUT_DIR / "facebook_pipeline.sqlite"
FINGERPRINT_SEED = 12345
VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


class PipelineLock:
    """Guarantees that only one crawler process runs against a specific output directory
    and browser profile at a time.
    """
    def __init__(self, output_dir):
        self.lock_file = Path(output_dir) / "pipeline.lock"

    def acquire(self):
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        if self.lock_file.exists():
            try:
                data = json.loads(self.lock_file.read_text(encoding="utf-8"))
                pid = data.get("pid")
                if pid:
                    try:
                        os.kill(pid, 0)
                        raise RuntimeError(
                            f"Another pipeline process (PID {pid}) is currently active in {self.lock_file.parent}. "
                            f"To prevent data corruption, only one process may use this profile and output directory at a time."
                        )
                    except OSError:
                        pass
            except (json.JSONDecodeError, OSError) as exc:
                if isinstance(exc, RuntimeError):
                    raise exc
            self.lock_file.unlink(missing_ok=True)

        info = {
            "pid": os.getpid(),
            "acquired_at": datetime.now(timezone.utc).isoformat(),
        }
        self.lock_file.write_text(json.dumps(info), encoding="utf-8")

    def release(self):
        try:
            if self.lock_file.exists():
                data = json.loads(self.lock_file.read_text(encoding="utf-8"))
                if data.get("pid") == os.getpid():
                    self.lock_file.unlink(missing_ok=True)
        except Exception:
            pass


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
                    dataset_scope TEXT,
                    competition TEXT,
                    is_accepted INTEGER DEFAULT 0,
                    run_id TEXT,
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

            # Non-destructive migrations: verify and add missing columns
            cols = [c[1] for c in self.conn.execute("PRAGMA table_info(pipeline_tasks)").fetchall()]
            if "tech_meta_json" not in cols:
                self.conn.execute("ALTER TABLE pipeline_tasks ADD COLUMN tech_meta_json TEXT")
            if "dataset_scope" not in cols:
                self.conn.execute("ALTER TABLE pipeline_tasks ADD COLUMN dataset_scope TEXT")
            if "competition" not in cols:
                self.conn.execute("ALTER TABLE pipeline_tasks ADD COLUMN competition TEXT")
            if "is_accepted" not in cols:
                self.conn.execute("ALTER TABLE pipeline_tasks ADD COLUMN is_accepted INTEGER DEFAULT 0")
            if "run_id" not in cols:
                self.conn.execute("ALTER TABLE pipeline_tasks ADD COLUMN run_id TEXT")

            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_scope ON pipeline_tasks(dataset_scope)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_accepted ON pipeline_tasks(is_accepted)")

    def recover_interrupted_tasks(self, dataset_scope=None):
        """Resets any 'processing' tasks left behind by crashed or interrupted sessions back to 'pending'."""
        with self.conn:
            if dataset_scope:
                cur = self.conn.execute(
                    "UPDATE pipeline_tasks SET status = 'pending' WHERE status = 'processing' AND (dataset_scope = ? OR dataset_scope IS NULL)",
                    (dataset_scope,),
                )
            else:
                cur = self.conn.execute("UPDATE pipeline_tasks SET status = 'pending' WHERE status = 'processing'")
            if cur.rowcount > 0:
                logger.info(f"Recovered {cur.rowcount} interrupted 'processing' tasks back to 'pending'.")

    def queue_urls(self, urls, dataset_scope="facebook:basketball:nba"):
        """Enqueues input URLs with deduplication against existing entries.
        Rejects root Fanpage profile URLs (e.g. facebook.com/nba/) so only post permalinks are queued.
        Normalizes tracking parameters and checks against existing accepted/completed posts.
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
                if not path.isdigit() and "/" not in path and not parsed.query:
                    continue

                normalized = fd.normalize_facebook_post_url(cleaned)
                cand_post_id = fd.extract_post_id(normalized)
                if cand_post_id and self.is_post_completed(cand_post_id):
                    continue

                cur = self.conn.execute(
                    """INSERT OR IGNORE INTO pipeline_tasks 
                       (input_url, canonical_url, post_id, dataset_scope, status, attempts, created_at, updated_at) 
                       VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)""",
                    (normalized, normalized, cand_post_id, dataset_scope, now, now),
                )
                if cur.rowcount > 0:
                    added += 1
        return added

    def queue_parsed_post(self, parsed_post, dataset_scope="facebook:basketball:nba"):
        """Enqueues a single discovered post dictionary directly into SQLite."""
        post_url = parsed_post.get("post_url")
        post_id = parsed_post.get("post_id")
        if not post_url:
            return False
        normalized = fd.normalize_facebook_post_url(post_url)
        if post_id and self.is_post_completed(post_id):
            return False

        now = datetime.now(timezone.utc).isoformat()
        with self.conn:
            cur = self.conn.execute(
                """INSERT OR IGNORE INTO pipeline_tasks 
                   (input_url, canonical_url, post_id, dataset_scope, status, attempts, created_at, updated_at) 
                   VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)""",
                (normalized, normalized, post_id, dataset_scope, now, now),
            )
            return cur.rowcount > 0

    def get_pending_tasks(self, limit=None, dataset_scope=None, retry_failed=False):
        """Returns URLs ready for processing in the specified scope."""
        where_clauses = []
        params = []

        if dataset_scope:
            where_clauses.append("(dataset_scope = ? OR dataset_scope IS NULL)")
            params.append(dataset_scope)

        if retry_failed:
            where_clauses.append("status IN ('pending', 'processing', 'failed') AND attempts < 3")
        else:
            where_clauses.append("status IN ('pending', 'processing') AND attempts < 3")

        where_str = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        query = f"SELECT input_url, canonical_url, post_id, status, attempts FROM pipeline_tasks {where_str} ORDER BY rowid ASC"
        if limit:
            query += f" LIMIT {int(limit)}"
        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def get_pending_tasks_count(self, dataset_scope=None):
        where_clauses = ["status IN ('pending', 'processing')", "attempts < 3"]
        params = []
        if dataset_scope:
            where_clauses.append("(dataset_scope = ? OR dataset_scope IS NULL)")
            params.append(dataset_scope)
        query = f"SELECT COUNT(*) FROM pipeline_tasks WHERE {' AND '.join(where_clauses)}"
        return self.conn.execute(query, params).fetchone()[0]

    def get_all_seen_keys(self):
        """Returns set of all post_id and input_url tokens to avoid re-discovering known items."""
        rows = self.conn.execute("SELECT input_url, canonical_url, post_id FROM pipeline_tasks").fetchall()
        keys = set()
        for r in rows:
            if r["input_url"]:
                keys.add(r["input_url"])
            if r["canonical_url"]:
                keys.add(r["canonical_url"])
            if r["post_id"]:
                keys.add(f"facebook:post:{r['post_id']}")
        return keys

    def update_task_processing(self, input_url):
        now = datetime.now(timezone.utc).isoformat()
        with self.conn:
            self.conn.execute(
                "UPDATE pipeline_tasks SET status = 'processing', attempts = attempts + 1, updated_at = ? WHERE input_url = ?",
                (now, input_url),
            )

    def record_success(
        self,
        input_url,
        canonical_url,
        post_id,
        author_id,
        record_17_col,
        tech_meta,
        run_id=None,
        dataset_scope="facebook:basketball:nba",
    ):
        """Upserts task record and evaluates strict dataset acceptance."""
        now = datetime.now(timezone.utc).isoformat()
        status = tech_meta.get("status", "success")
        reasons_json = json.dumps(tech_meta.get("review_reasons", []), ensure_ascii=False)
        record_json = json.dumps(record_17_col, ensure_ascii=False)
        tech_meta_json = json.dumps(tech_meta, ensure_ascii=False)

        # Evaluate acceptance for this dataset scope
        is_accepted, acceptance_rejections = fd.evaluate_record_acceptance(
            record_17_col,
            tech_meta=tech_meta,
            target_sport="Basketball" if "basketball" in (dataset_scope or "").lower() else None,
            target_competition="NBA" if "nba" in (dataset_scope or "").lower() else None,
        )
        is_accepted_int = 1 if is_accepted else 0
        comp_str = tech_meta.get("nba_classification", {}).get("competition") or "NBA"

        all_reasons = list(tech_meta.get("review_reasons", []))
        if not is_accepted and acceptance_rejections:
            all_reasons.extend(acceptance_rejections)
        all_reasons = list(dict.fromkeys(all_reasons))
        reasons_json = json.dumps(all_reasons, ensure_ascii=False)

        with self.conn:
            self.conn.execute(
                """INSERT OR IGNORE INTO pipeline_tasks 
                   (input_url, canonical_url, post_id, author_id, dataset_scope, run_id, status, attempts, created_at, updated_at) 
                   VALUES (?, ?, ?, ?, ?, ?, 'processing', 1, ?, ?)""",
                (input_url, canonical_url, post_id, author_id, dataset_scope, run_id, now, now),
            )
            cur = self.conn.execute(
                """UPDATE pipeline_tasks 
                   SET status = ?, canonical_url = ?, post_id = ?, author_id = ?, 
                       dataset_scope = COALESCE(dataset_scope, ?), run_id = COALESCE(?, run_id),
                       competition = ?, is_accepted = ?,
                       last_error = NULL, review_reasons = ?, record_json = ?, tech_meta_json = ?, updated_at = ?
                   WHERE input_url = ?""",
                (status, canonical_url, post_id, author_id, dataset_scope, run_id, comp_str, is_accepted_int, reasons_json, record_json, tech_meta_json, now, input_url),
            )
            if cur.rowcount == 0:
                raise RuntimeError(f"Failed to record success in SQLite for {input_url} (rowcount=0)")

            if canonical_url and input_url != canonical_url and post_id:
                self.conn.execute(
                    "INSERT OR REPLACE INTO post_aliases (alias_url, canonical_post_id, resolved_url, created_at) VALUES (?, ?, ?, ?)",
                    (input_url, post_id, canonical_url, now),
                )
        return is_accepted, acceptance_rejections

    def record_failure(
        self,
        input_url,
        error_message,
        status="failed",
        canonical_url=None,
        run_id=None,
        dataset_scope="facebook:basketball:nba",
    ):
        now = datetime.now(timezone.utc).isoformat()
        with self.conn:
            self.conn.execute(
                """INSERT OR IGNORE INTO pipeline_tasks 
                   (input_url, status, attempts, dataset_scope, run_id, created_at, updated_at) 
                   VALUES (?, 'pending', 0, ?, ?, ?, ?)""",
                (input_url, dataset_scope, run_id, now, now),
            )
            self.conn.execute(
                """UPDATE pipeline_tasks 
                   SET status = ?, canonical_url = COALESCE(?, canonical_url), 
                       last_error = ?, run_id = COALESCE(?, run_id), updated_at = ?
                   WHERE input_url = ?""",
                (status, canonical_url, str(error_message), run_id, now, input_url),
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

    def get_accepted_records(self, dataset_scope="facebook:basketball:nba", filter_sport=None):
        """Retrieves distinct accepted records for dataset bundling (CSV, JSON, JSONL).
        Guarantees that CSV, JSON, and JSONL contain the exact same set of records,
        deduplicated by post_id so each accepted post appears at most once.
        """
        where_clauses = ["is_accepted = 1", "record_json IS NOT NULL"]
        params = []
        if dataset_scope:
            where_clauses.append("(dataset_scope = ? OR dataset_scope IS NULL)")
            params.append(dataset_scope)

        query = f"SELECT post_id, record_json, tech_meta_json, updated_at FROM pipeline_tasks WHERE {' AND '.join(where_clauses)} ORDER BY rowid ASC"
        rows = self.conn.execute(query, params).fetchall()

        seen_post_ids = set()
        dataset = []
        clean_filter = filter_sport.strip().lower() if filter_sport else None

        for r in rows:
            pid = r["post_id"]
            if pid and pid in seen_post_ids:
                continue
            rec = json.loads(r["record_json"])
            if clean_filter:
                sport = rec.get("sport_category") or ""
                sports = [s.strip().lower() for s in sport.split(",") if s.strip()]
                if clean_filter not in sports:
                    continue
            if pid:
                seen_post_ids.add(pid)
            dataset.append(rec)
        return dataset

    def get_all_records(self, dataset_scope=None):
        """Retrieves all completed or reviewed records for technical debug bundling."""
        where_clauses = ["record_json IS NOT NULL"]
        params = []
        if dataset_scope:
            where_clauses.append("(dataset_scope = ? OR dataset_scope IS NULL)")
            params.append(dataset_scope)

        query = f"SELECT input_url, canonical_url, post_id, author_id, status, is_accepted, record_json, tech_meta_json, review_reasons, last_error FROM pipeline_tasks WHERE {' AND '.join(where_clauses)} ORDER BY rowid ASC"
        rows = self.conn.execute(query, params).fetchall()
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

            raw_ts = t_meta.get("raw_published_at") or t_meta.get("raw_timestamp")
            is_approx = t_meta.get("is_approximate") or t_meta.get("counts_meta")

            debug_list.append({
                "input_url": r["input_url"],
                "canonical_url": r["canonical_url"],
                "post_id": r["post_id"],
                "author_id": r["author_id"],
                "status": r["status"],
                "is_accepted": bool(r["is_accepted"]),
                "raw_timestamp": raw_ts,
                "time_precision": t_meta.get("time_precision"),
                "is_approximate": is_approx,
                "sport_classification": t_meta.get("sport_classification"),
                "nba_classification": t_meta.get("nba_classification"),
                "review_reasons": json.loads(r["review_reasons"] or "[]"),
                "last_error": r["last_error"],
                "record": rec,
            })
        return dataset, debug_list

    def get_statistics(self, dataset_scope=None):
        where_clause = "WHERE (dataset_scope = ? OR dataset_scope IS NULL)" if dataset_scope else ""
        params = [dataset_scope] if dataset_scope else []
        rows = self.conn.execute(f"SELECT status, COUNT(*) as cnt FROM pipeline_tasks {where_clause} GROUP BY status", params).fetchall()
        stats = {r["status"]: r["cnt"] for r in rows}
        total = self.conn.execute(f"SELECT COUNT(*) FROM pipeline_tasks {where_clause}", params).fetchone()[0]
        accepted_cnt = self.conn.execute(f"SELECT COUNT(DISTINCT post_id) FROM pipeline_tasks {where_clause} {'AND' if where_clause else 'WHERE'} is_accepted = 1", params).fetchone()[0]
        return total, stats, accepted_cnt

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


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
    if page is None:
        return None
    try:
        path = urlparse(page.url).path.lower()
        if path.startswith(("/login", "/checkpoint", "/recover")):
            return "Facebook login or checkpoint required. Run cloak_facebook_login.py to re-authenticate."
        if page.locator('input[name="email"]').is_visible() and page.locator('input[name="pass"]').is_visible():
            return "Facebook session expired. Run cloak_facebook_login.py to re-authenticate."
    except Exception:
        pass
    return None


def export_dataset_bundle(db, output_dir, dataset_scope="facebook:basketball:nba", filter_sport=None):
    """Exports dataset to unified CSV, timestamped and canonical JSON snapshots, and JSONL.
    Strictly follows 17-column order, utf-8-sig, newline='', empty cell for null.
    Returns (csv_path, accepted_count).
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    dataset = db.get_accepted_records(dataset_scope=dataset_scope, filter_sport=filter_sport)

    # 1. Export CSV atomically
    csv_final = out_dir / "facebook_dataset.csv"
    csv_tmp = out_dir / "facebook_dataset.csv.tmp"
    with csv_tmp.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fd.REQUIRED_COLUMNS)
        writer.writeheader()
        for rec in dataset:
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
            writer.writerow(row)
        f.flush()
        os.fsync(f.fileno())
    csv_tmp.replace(csv_final)

    # 2. Export canonical JSON atomically
    json_final = out_dir / "facebook_dataset.json"
    json_tmp = out_dir / "facebook_dataset.json.tmp"
    json_tmp.write_text(json.dumps(dataset, ensure_ascii=False, indent=2), encoding="utf-8")
    json_tmp.replace(json_final)

    # 3. Export canonical JSONL atomically
    jsonl_final = out_dir / "facebook_dataset.jsonl"
    jsonl_tmp = out_dir / "facebook_dataset.jsonl.tmp"
    with jsonl_tmp.open("w", encoding="utf-8") as jf:
        for rec in dataset:
            jf.write(json.dumps(rec, ensure_ascii=False) + "\n")
        jf.flush()
        os.fsync(jf.fileno())
    jsonl_tmp.replace(jsonl_final)

    # 4. Save technical debug records atomically & purge older debug dumps
    _, debug_list = db.get_all_records(dataset_scope=dataset_scope)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    out_debug = out_dir / f"facebook_posts_auto_debug_{stamp}.json"
    debug_tmp = out_debug.with_suffix(".json.tmp")
    debug_tmp.write_text(json.dumps(debug_list, ensure_ascii=False, indent=2), encoding="utf-8")
    debug_tmp.replace(out_debug)

    # Automatically purge older debug files so only the single latest comprehensive file is kept
    for old_file in out_dir.glob("facebook_posts_auto_debug_*.json"):
        if old_file.resolve() != out_debug.resolve():
            try:
                old_file.unlink()
            except OSError:
                pass

    logger.info(f"Exported unified dataset: {len(dataset)} accepted records to CSV, JSON, and JSONL.")
    return csv_final, len(dataset)


def reprocess_dataset_offline(
    db_path,
    output_dir=None,
    nba_policy="contextual",
    view_policy="verified_only",
    scope_filter="facebook:basketball:nba",
    run_id_filter=None,
    apply_to_db=True,
):
    """Reprocesses historical pipeline tasks offline without opening browser or network calls.

    Applies:
      1. Singleton statistical Lingua language detector.
      2. Contextual verified NBA source registry + updated NBA rules.
      3. Conservative view count policy (legacy unverified views set to null with provenance).
      4. Sport category synchronization with Basketball.
      5. Full acceptance re-evaluation with exact rejection codes.
      6. Clean 17-column dataset export and audit reconciliation CSV + JSON.
    """
    db_path = Path(db_path)
    if not db_path.is_file():
        raise FileNotFoundError(f"Database not found at {db_path}")

    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    audit_run_id = f"audit_run_{timestamp_str}"

    if output_dir:
        out_dir = Path(output_dir)
    else:
        out_dir = db_path.parent / audit_run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    db = PipelineDB(db_path)

    # Query tasks
    where_clauses = []
    params = []
    if scope_filter:
        where_clauses.append("(dataset_scope = ? OR dataset_scope IS NULL)")
        params.append(scope_filter)
    if run_id_filter:
        where_clauses.append("run_id = ?")
        params.append(run_id_filter)

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    query = f"SELECT rowid, input_url, canonical_url, post_id, author_id, dataset_scope, run_id, status, is_accepted, review_reasons, record_json, tech_meta_json, last_error FROM pipeline_tasks {where_sql} ORDER BY rowid ASC"
    rows = db.conn.execute(query, params).fetchall()

    logger.info(f"Starting offline audit/reprocess on {len(rows)} tasks from {db_path.name}...")

    audit_trail = []
    clean_accepted_records = []
    seen_accepted_post_ids = set()

    counters_before = {
        "accepted": 0,
        "unmatched": 0,
        "uncertain": 0,
        "missing_required": 0,
        "video_skipped": 0,
        "failed": 0,
        "total": len(rows),
    }
    counters_after = {
        "accepted": 0,
        "unmatched": 0,
        "uncertain": 0,
        "missing_required": 0,
        "video_skipped": 0,
        "failed": 0,
        "total": len(rows),
    }

    transitions = {
        "recovered_accepted": 0,
        "demoted_from_accepted": 0,
        "maintained_accepted": 0,
        "maintained_unaccepted": 0,
        "skipped_video": 0,
        "failed_extraction": 0,
    }

    lang_stats = {"before": {}, "after": {}}
    views_stats = {"before_non_null": 0, "after_non_null": 0, "legacy_unverified_nullified": 0}

    now_iso = datetime.now(timezone.utc).isoformat()

    with db.conn:
        for r in rows:
            u_input = r["input_url"]
            u_canon = r["canonical_url"]
            p_id = r["post_id"]
            a_id = r["author_id"]
            old_st = r["status"]
            old_is_acc = bool(r["is_accepted"])
            old_reasons = json.loads(r["review_reasons"] or "[]")
            r_json = r["record_json"]
            t_json = r["tech_meta_json"]
            r_id = r["run_id"]
            l_err = r["last_error"]

            if not r_json:
                if old_st == "video_skipped":
                    counters_before["video_skipped"] += 1
                    counters_after["video_skipped"] += 1
                    transitions["skipped_video"] += 1
                    action = "skipped_video"
                    new_st = "video_skipped"
                    new_is_acc = False
                    new_reasons = ["video_post_omitted_by_policy"]
                else:
                    counters_before["failed"] += 1
                    counters_after["failed"] += 1
                    transitions["failed_extraction"] += 1
                    action = "failed_extraction"
                    new_st = "failed"
                    new_is_acc = False
                    new_reasons = [l_err or "navigation_or_extraction_failed"]

                audit_trail.append({
                    "task_rowid": r["rowid"],
                    "input_url": u_input,
                    "canonical_url": u_canon,
                    "post_id": p_id,
                    "author_id": a_id,
                    "run_id": r_id,
                    "old_status": old_st,
                    "new_status": new_st,
                    "old_is_accepted": old_is_acc,
                    "new_is_accepted": new_is_acc,
                    "old_sport": None,
                    "new_sport": None,
                    "old_language": None,
                    "new_language": None,
                    "old_views": None,
                    "new_views": None,
                    "classification_basis": "none",
                    "audit_action": action,
                    "old_reasons": "; ".join(old_reasons),
                    "new_reasons": "; ".join(new_reasons),
                })
                continue

            rec = json.loads(r_json)
            meta = json.loads(t_json) if t_json else {}

            # Baseline metrics
            old_lang = rec.get("language") or "unknown"
            lang_stats["before"][old_lang] = lang_stats["before"].get(old_lang, 0) + 1
            old_views = rec.get("view_count")
            if old_views is not None:
                views_stats["before_non_null"] += 1
            old_sport = rec.get("sport_category")

            if old_is_acc:
                counters_before["accepted"] += 1
            else:
                nba_prev = meta.get("nba_classification", {}).get("status")
                if nba_prev == "unmatched":
                    counters_before["unmatched"] += 1
                elif nba_prev == "uncertain":
                    counters_before["uncertain"] += 1
                else:
                    counters_before["missing_required"] += 1

            # 1. Reprocess Language Detection via Lingua
            caption_text = rec.get("text")
            new_lang, lang_meta = fd.detect_language_with_meta(caption_text)
            rec["language"] = new_lang
            meta["language_meta"] = lang_meta
            lang_stats["after"][new_lang] = lang_stats["after"].get(new_lang, 0) + 1

            # 2. Reprocess NBA and Sport Classification
            author_or_src = rec.get("author_id") or a_id or u_input
            sport_cat, sport_meta = fd.classify_post_sport(caption_text, sport_hint="Basketball")
            nba_res = fd.classify_nba_post(caption_text, sport_hint="Basketball", author_or_source=author_or_src, policy=nba_policy)
            meta["nba_classification"] = nba_res
            meta["sport_classification"] = sport_meta

            if nba_res["matched"]:
                rec["sport_category"] = "Basketball"
            elif sport_cat:
                rec["sport_category"] = sport_cat
            else:
                rec["sport_category"] = None
            new_sport = rec["sport_category"]

            # 3. View count handling
            if view_policy == "off":
                rec["view_count"] = None
                meta["view_meta"] = {"status": "off", "reason": "view_policy_disabled"}
            else:
                if old_views is not None:
                    meta["legacy_view_count"] = old_views
                    rec["view_count"] = None
                    meta["view_meta"] = {
                        "status": "legacy_unverified",
                        "reason": "legacy_view_lacks_dom_candidate_provenance",
                        "raw_legacy_value": str(old_views),
                    }
                    views_stats["legacy_unverified_nullified"] += 1
                else:
                    meta["view_meta"] = {
                        "status": "not_displayed",
                        "reason": "metric_not_present_in_dom",
                    }
            new_views = rec["view_count"]
            if new_views is not None:
                views_stats["after_non_null"] += 1

            # Set reprocessed metadata
            meta["reprocessed_at"] = now_iso
            meta["audit_run_id"] = audit_run_id
            meta["reprocess_version"] = "2.0.0-audit"

            # 4. Strict Quality Acceptance Evaluation
            new_is_acc, new_rejections = fd.evaluate_record_acceptance(
                rec,
                tech_meta=meta,
                target_sport="Basketball" if "basketball" in (scope_filter or "").lower() else None,
                target_competition="NBA" if "nba" in (scope_filter or "").lower() else None,
            )

            if new_is_acc:
                new_st = "success"
                counters_after["accepted"] += 1
                if not old_is_acc:
                    transitions["recovered_accepted"] += 1
                    action = "recovered"
                else:
                    transitions["maintained_accepted"] += 1
                    action = "maintained_accepted"
            else:
                n_st = nba_res.get("status")
                if n_st == "unmatched":
                    new_st = "unmatched"
                    counters_after["unmatched"] += 1
                elif n_st == "uncertain":
                    new_st = "uncertain"
                    counters_after["uncertain"] += 1
                else:
                    new_st = "missing_required"
                    counters_after["missing_required"] += 1

                if old_is_acc:
                    transitions["demoted_from_accepted"] += 1
                    action = "demoted"
                else:
                    transitions["maintained_unaccepted"] += 1
                    action = "maintained_rejected"

            # 5. Apply to SQLite DB if requested
            if apply_to_db:
                new_reasons_json = json.dumps(new_rejections, ensure_ascii=False)
                new_record_json = json.dumps(rec, ensure_ascii=False)
                new_tech_meta_json = json.dumps(meta, ensure_ascii=False)
                comp_str = nba_res.get("competition") or "NBA"
                db.conn.execute(
                    """UPDATE pipeline_tasks
                       SET status = ?, is_accepted = ?, review_reasons = ?,
                           competition = ?, record_json = ?, tech_meta_json = ?, updated_at = ?
                       WHERE rowid = ?""",
                    (new_st, 1 if new_is_acc else 0, new_reasons_json, comp_str, new_record_json, new_tech_meta_json, now_iso, r["rowid"])
                )

            # 6. Add to clean dataset if accepted
            if new_is_acc:
                post_key = rec.get("post_id") or u_canon or u_input
                if post_key not in seen_accepted_post_ids:
                    seen_accepted_post_ids.add(post_key)
                    clean_accepted_records.append(rec)

            audit_trail.append({
                "task_rowid": r["rowid"],
                "input_url": u_input,
                "canonical_url": u_canon,
                "post_id": p_id,
                "author_id": a_id,
                "run_id": r_id,
                "old_status": old_st,
                "new_status": new_st,
                "old_is_accepted": old_is_acc,
                "new_is_accepted": new_is_acc,
                "old_sport": old_sport,
                "new_sport": new_sport,
                "old_language": old_lang,
                "new_language": new_lang,
                "old_views": old_views,
                "new_views": new_views,
                "classification_basis": nba_res.get("classification_basis", "unknown"),
                "audit_action": action,
                "old_reasons": "; ".join(old_reasons),
                "new_reasons": "; ".join(new_rejections),
            })

    # Export clean 17-column CSV atomically
    csv_clean_path = out_dir / "facebook_dataset_clean.csv"
    csv_tmp = csv_clean_path.with_suffix(".csv.tmp")
    with csv_tmp.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fd.REQUIRED_COLUMNS)
        writer.writeheader()
        for rec in clean_accepted_records:
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
            writer.writerow(row)
        f.flush()
        os.fsync(f.fileno())
    csv_tmp.replace(csv_clean_path)

    # Export canonical clean JSON & JSONL atomically
    json_clean_path = out_dir / "facebook_dataset_clean.json"
    json_clean_tmp = json_clean_path.with_suffix(".json.tmp")
    json_clean_tmp.write_text(json.dumps(clean_accepted_records, ensure_ascii=False, indent=2), encoding="utf-8")
    json_clean_tmp.replace(json_clean_path)

    jsonl_clean_path = out_dir / "facebook_dataset_clean.jsonl"
    jsonl_clean_tmp = jsonl_clean_path.with_suffix(".jsonl.tmp")
    with jsonl_clean_tmp.open("w", encoding="utf-8") as jf:
        for rec in clean_accepted_records:
            jf.write(json.dumps(rec, ensure_ascii=False) + "\n")
        jf.flush()
        os.fsync(jf.fileno())
    jsonl_clean_tmp.replace(jsonl_clean_path)

    # Export audit reconciliation CSV atomically
    audit_csv_path = out_dir / "audit_reconciliation.csv"
    audit_csv_tmp = audit_csv_path.with_suffix(".csv.tmp")
    audit_cols = [
        "task_rowid", "input_url", "canonical_url", "post_id", "author_id", "run_id",
        "old_status", "new_status", "old_is_accepted", "new_is_accepted",
        "old_sport", "new_sport", "old_language", "new_language",
        "old_views", "new_views", "classification_basis", "audit_action",
        "old_reasons", "new_reasons"
    ]
    with audit_csv_tmp.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=audit_cols)
        writer.writeheader()
        for item in audit_trail:
            writer.writerow(item)
        f.flush()
        os.fsync(f.fileno())
    audit_csv_tmp.replace(audit_csv_path)

    # Export audit summary JSON atomically
    summary_data = {
        "audit_run_id": audit_run_id,
        "database_file": str(db_path),
        "executed_at": now_iso,
        "policies": {
            "nba_policy": nba_policy,
            "view_policy": view_policy,
            "language_detector": "lingua-language-detector (2.2.0)",
            "scope_filter": scope_filter,
            "run_id_filter": run_id_filter,
        },
        "total_tasks_audited": len(rows),
        "transitions": transitions,
        "counters_before": counters_before,
        "counters_after": counters_after,
        "clean_accepted_posts_count": len(clean_accepted_records),
        "distinct_accepted_posts": len(seen_accepted_post_ids),
        "language_distribution": lang_stats,
        "views_distribution": views_stats,
        "invariants_verification": {
            "sum_before_equals_total": sum(counters_before[k] for k in ["accepted", "unmatched", "uncertain", "missing_required", "video_skipped", "failed"]) == len(rows),
            "sum_after_equals_total": sum(counters_after[k] for k in ["accepted", "unmatched", "uncertain", "missing_required", "video_skipped", "failed"]) == len(rows),
            "transitions_sum_equals_total": sum(transitions.values()) == len(rows),
        },
    }
    summary_json_path = out_dir / "audit_summary.json"
    summary_json_tmp = summary_json_path.with_suffix(".json.tmp")
    summary_json_tmp.write_text(json.dumps(summary_data, ensure_ascii=False, indent=2), encoding="utf-8")
    summary_json_tmp.replace(summary_json_path)

    # If reprocessing the entire primary dataset, also update primary exports in parent output_dir
    if apply_to_db and out_dir.resolve() != db_path.parent.resolve():
        export_dataset_bundle(db, db_path.parent, dataset_scope=scope_filter, filter_sport="Basketball")

    print("\n" + "=" * 70)
    print("           OFFLINE AUDIT & REPROCESSING COMPLETE")
    print("=" * 70)
    print(f"Audit Run ID:              {audit_run_id}")
    print(f"Total Tasks Audited:       {len(rows)}")
    print(f"Accepted Posts Before:     {counters_before['accepted']}")
    print(f"Accepted Posts After:      {counters_after['accepted']} (+{transitions['recovered_accepted']} recovered, -{transitions['demoted_from_accepted']} demoted)")
    print(f"Clean Distinct Posts:      {len(seen_accepted_post_ids)}")
    print(f"Legacy Views Nullified:    {views_stats['legacy_unverified_nullified']} (provenance preserved in tech_meta)")
    print(f"Unknown Languages:         {lang_stats['before'].get('unknown', 0)} -> {lang_stats['after'].get('unknown', 0)}")
    print(f"Clean CSV Export:          {csv_clean_path}")
    print(f"Audit Reconciliation CSV:  {audit_csv_path}")
    print(f"Audit Summary JSON:        {summary_json_path}")
    print("=" * 70 + "\n")

    return summary_data


def write_run_summary(output_dir, summary_data):
    """Writes run_summary.json atomically with per-run stats and 8-hour projections."""
    path = Path(output_dir) / "run_summary.json"
    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(summary_data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def write_quality_report(output_dir, db, dataset_scope=None, filter_sport=None, sources_stats=None, counters=None):
    """Writes quality_report.json covering 17-column coverage, missing breakdowns,
    NBA classification breakdown, and source retention rates.
    """
    dataset = db.get_accepted_records(dataset_scope=dataset_scope, filter_sport=filter_sport)
    _, all_debug = db.get_all_records(dataset_scope=dataset_scope)

    coverage = {}
    for col in fd.REQUIRED_COLUMNS:
        pop = sum(1 for r in dataset if r.get(col) is not None and r.get(col) != "")
        missing = len(dataset) - pop
        pct = (missing / len(dataset) * 100) if dataset else 0.0
        coverage[col] = {
            "populated": pop,
            "missing": missing,
            "missing_pct": round(pct, 2),
        }

    # Classification distribution across all debug posts
    nba_dist = {"matched": 0, "unmatched": 0, "uncertain": 0, "no_evidence": 0}
    for item in all_debug:
        meta = item.get("nba_classification") or {}
        st = meta.get("status", "no_evidence")
        nba_dist[st] = nba_dist.get(st, 0) + 1

    # Extract 5 sample records for manual inspection
    samples = []
    for item in all_debug[:5]:
        rec = item.get("record") or {}
        nba_meta = item.get("nba_classification") or {}
        caption = rec.get("text") or ""
        samples.append({
            "post_id": rec.get("post_id"),
            "post_url": rec.get("post_url"),
            "media_type": rec.get("media_type"),
            "like_count": rec.get("like_count"),
            "published_at": rec.get("published_at"),
            "nba_status": nba_meta.get("status"),
            "nba_evidence": nba_meta.get("evidence", []),
            "caption_preview": caption[:150] + ("..." if len(caption) > 150 else ""),
            "is_accepted": item.get("is_accepted", False),
        })

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_accepted_in_dataset": len(dataset),
        "total_attempted_in_scope": len(all_debug),
        "field_coverage": coverage,
        "nba_classification_distribution": nba_dist,
        "sources_breakdown": sources_stats or {},
        "counters": counters or {},
        "sample_manual_check": samples,
    }

    path = Path(output_dir) / "quality_report.json"
    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def write_progress(output_dir, progress_data):
    """Writes progress.json atomically for real-time monitoring."""
    path = Path(output_dir) / "progress.json"
    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(progress_data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def print_quality_report(db, dataset_scope=None, filter_sport=None, elapsed_seconds=0, counters=None, run_summary=None):
    """Prints a detailed statistics, field quality, and execution report to console."""
    total, stats, accepted_cnt = db.get_statistics(dataset_scope=dataset_scope)
    dataset = db.get_accepted_records(dataset_scope=dataset_scope, filter_sport=filter_sport)

    print("\n" + "=" * 65)
    print("FACEBOOK NBA PIPELINE EXECUTION REPORT")
    print("=" * 65)
    print(f"Elapsed Time: {elapsed_seconds:.1f}s ({elapsed_seconds / 60:.2f} min)")
    if counters:
        print(f"Run Counters: Attempted={counters.get('attempted', 0)}, "
              f"Valid NBA New={counters.get('valid_new_nba_posts', 0)}, "
              f"Video Skipped={counters.get('video_skipped', 0)}, "
              f"Unmatched Sport={counters.get('unmatched_sport', 0)}, "
              f"Uncertain NBA={counters.get('uncertain_nba', 0)}, "
              f"Missing Required={counters.get('missing_required', 0)}, "
              f"Unavailable={counters.get('unavailable', 0)}, "
              f"Duplicates={counters.get('duplicates', 0)}, "
              f"Failed={counters.get('failed', 0)}")
    print(f"Database Scope: {dataset_scope or 'All'}")
    print(f"Total Enqueued URLs in DB: {total}")
    print(f"Total Unique Accepted NBA Posts in DB: {accepted_cnt}")

    if run_summary and "performance" in run_summary:
        rate = run_summary["performance"].get("valid_posts_per_hour_realtime", 0.0)
        print(f"Realtime Rate: {rate:.1f} valid NBA posts / hour")

    if not dataset:
        print("\nNo accepted records extracted yet.")
        print("=" * 65)
        return

    print("\n17-Column Field Coverage in Accepted Dataset (Total: {}):".format(len(dataset)))
    print("-" * 65)
    for col in fd.REQUIRED_COLUMNS:
        populated = sum(1 for r in dataset if r.get(col) is not None and r.get(col) != "")
        missing = len(dataset) - populated
        pct = (missing / len(dataset)) * 100
        print(f"  {col:<20}: {populated:>3} populated | {missing:>3} missing ({pct:>5.1f}%)")
    print("=" * 65)


def run_pipeline(
    input_source=None,
    page_url=None,
    pages_file=None,
    competition="NBA",
    sport_hint="Basketball",
    filter_sport="Basketball",
    nba_policy="contextual",
    view_policy="verified_only",
    batch_size=20,
    max_hours=1.0,
    target_valid=None,
    max_attempts=None,
    max_new_urls=None,
    source_max_scrolls=100,
    max_refreshes=0,
    idle_rounds=10,
    backlog_limit=150,
    retry_failed=False,
    fetch_followers=True,
    headless=False,
    output_dir=OUTPUT_DIR,
    **kwargs,
):
    """Executes the automated end-to-end continuous discovery and batch extraction pipeline."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Attach file handler for pipeline.log
    log_file = out_dir / "pipeline.log"
    file_handler = logging.FileHandler(str(log_file), encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", "%H:%M:%S"))
    logger.addHandler(file_handler)

    # Acquire single-process lock
    lock = PipelineLock(out_dir)
    try:
        lock.acquire()
    except RuntimeError as lock_err:
        logger.error(str(lock_err))
        return 1

    dataset_scope = f"facebook:{sport_hint.lower() if sport_hint else 'basketball'}:{competition.lower() if competition else 'nba'}"
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S_%fZ')}"
    db = PipelineDB(out_dir / "facebook_pipeline.sqlite")

    # Recover any crashed tasks
    db.recover_interrupted_tasks(dataset_scope=dataset_scope)

    start_mono = time.monotonic()
    deadline_mono = (start_mono + max_hours * 3600.0) if (max_hours and max_hours > 0) else None

    interrupted = False

    def handle_signal(sig, frame):
        nonlocal interrupted
        logger.info("\nSignal received (Ctrl+C / SIGTERM). Gracefully pausing pipeline and committing state...")
        interrupted = True

    prev_sigint = signal.signal(signal.SIGINT, handle_signal)
    prev_sigterm = signal.signal(signal.SIGTERM, handle_signal)

    def is_deadline_reached():
        return interrupted or (deadline_mono is not None and time.monotonic() >= deadline_mono)

    def interruptible_sleep(seconds, step=0.3):
        target = time.monotonic() + seconds
        while time.monotonic() < target:
            if is_deadline_reached():
                break
            time.sleep(min(step, target - time.monotonic()))

    counters = {
        "discovered_urls": 0,
        "attempted": 0,
        "valid_new_nba_posts": 0,
        "video_skipped": 0,
        "unmatched_sport": 0,
        "uncertain_nba": 0,
        "missing_required": 0,
        "unavailable": 0,
        "duplicates": 0,
        "failed": 0,
        "browser_recoveries": 0,
    }

    time_breakdown = {
        "discovery": 0.0,
        "extraction": 0.0,
        "followers": 0.0,
        "cooldown_and_wait": 0.0,
        "export": 0.0,
    }

    sources_stats = {}
    last_export_mono = time.monotonic()
    stop_early_reason = None
    current_source = None

    if not PROFILE_DIR.is_dir():
        logger.error(f"Profile directory not found: {PROFILE_DIR}")
        logger.error("Please run cloak_facebook_login.py first to establish authenticated session.")
        lock.release()
        db.close()
        return 1

    logger.info("=" * 65)
    logger.info(f"STARTING FACEBOOK NBA PIPELINE [{run_id}]")
    logger.info(f"Scope: {dataset_scope} | Output: {out_dir}")
    logger.info(f"Time Budget: {max_hours:.2f}h | Batch Size: {batch_size} | Source Max Scrolls: {source_max_scrolls}")
    logger.info("=" * 65)

    # 1. Enqueue input URLs from file if provided
    if input_source:
        parsed_urls = load_input_urls(input_source)
        enqueued = db.queue_urls(parsed_urls, dataset_scope=dataset_scope)
        logger.info(f"Loaded {len(parsed_urls)} URLs from {input_source} ({enqueued} newly queued).")

    page_sources = load_page_sources(page=page_url, pages_file=pages_file)

    def process_extraction_batch(context, extraction_page, limit_batch):
        """Pulls pending tasks from SQLite and extracts them sequentially on extraction_page."""
        nonlocal stop_early_reason, last_export_mono
        tasks = db.get_pending_tasks(limit=limit_batch, dataset_scope=dataset_scope, retry_failed=retry_failed)
        if not tasks:
            return 0

        followers_cache = {}
        processed = 0

        for idx, task in enumerate(tasks, start=1):
            if is_deadline_reached():
                stop_early_reason = "max_hours_deadline_reached" if not interrupted else "user_interrupted"
                break

            if max_attempts and counters["attempted"] >= max_attempts:
                stop_early_reason = "max_attempts_reached"
                break

            if target_valid and counters["valid_new_nba_posts"] >= target_valid:
                stop_early_reason = "target_valid_reached"
                break

            raw_url = task["input_url"]
            counters["attempted"] += 1
            processed += 1
            logger.info(f"\n[Task {counters['attempted']}] Extracting: {raw_url}")
            db.update_task_processing(raw_url)

            t_ext_start = time.monotonic()
            try:
                # Calculate navigation timeout constrained by remaining time budget
                remaining_s = (deadline_mono - time.monotonic()) if deadline_mono else 30.0
                nav_timeout = max(5000, min(30000, int(remaining_s * 1000)))

                extraction_page.goto(raw_url, wait_until="domcontentloaded", timeout=nav_timeout)
                interruptible_sleep(3.0)

                access_issue = check_facebook_access_issues(extraction_page)
                if access_issue:
                    stop_early_reason = "login_or_checkpoint_required"
                    db.record_failure(raw_url, access_issue, status="failed", canonical_url=extraction_page.url, run_id=run_id, dataset_scope=dataset_scope)
                    logger.warning(f"Facebook authentication checkpoint detected: {access_issue}")
                    break

                record, meta = fd.extract_scoped_post_data(extraction_page, post_url=raw_url, default_sport=sport_hint, nba_policy=nba_policy, view_policy=view_policy)

                # Check deleted / unavailable status
                if meta.get("status") == "unavailable":
                    counters["unavailable"] += 1
                    db.record_failure(raw_url, "Post unavailable or deleted", status="unavailable", canonical_url=extraction_page.url, run_id=run_id, dataset_scope=dataset_scope)
                    logger.info("  -> Status: unavailable (deleted or restricted)")
                    continue

                # Check video post policy: video posts omit caption text and are not accepted
                if record.get("media_type") == "video":
                    counters["video_skipped"] += 1
                    db.record_failure(raw_url, "video_post_omitted_by_policy", status="video_skipped", canonical_url=record.get("post_url"), run_id=run_id, dataset_scope=dataset_scope)
                    logger.info("  -> Status: video_skipped (video post omitted by quality policy)")
                    continue

                # Author followers lookup/caching
                if fetch_followers and record.get("author_id"):
                    author = record["author_id"]
                    cached_fol = db.get_followers_cache(author)
                    if cached_fol:
                        record["author_followers"] = cached_fol["followers"]
                    else:
                        t_fol_start = time.monotonic()
                        f_val = fd.fetch_author_followers(context, author, cache=followers_cache)
                        time_breakdown["followers"] += (time.monotonic() - t_fol_start)
                        if f_val is not None:
                            db.set_followers_cache(author, f_val, source="profile")
                            record["author_followers"] = f_val

                # Commit record to SQLite with acceptance evaluation
                is_accepted, rejections = db.record_success(
                    input_url=raw_url,
                    canonical_url=record["post_url"],
                    post_id=record["post_id"],
                    author_id=record["author_id"],
                    record_17_col=record,
                    tech_meta=meta,
                    run_id=run_id,
                    dataset_scope=dataset_scope,
                )

                if is_accepted:
                    counters["valid_new_nba_posts"] += 1
                    logger.info(f"  -> [ACCEPTED NBA POST] ID: {record['post_id']} | Likes: {record['like_count']} | Sport: {record['sport_category']}")
                else:
                    nba_st = meta.get("nba_classification", {}).get("status")
                    if nba_st == "unmatched":
                        counters["unmatched_sport"] += 1
                        logger.info(f"  -> [REJECTED] Non-NBA content: {rejections}")
                    elif nba_st == "uncertain":
                        counters["uncertain_nba"] += 1
                        logger.info(f"  -> [REJECTED] Uncertain NBA evidence: {rejections}")
                    else:
                        counters["missing_required"] += 1
                        logger.info(f"  -> [REJECTED] Missing required fields: {rejections}")

            except (Error, Exception) as exc:
                counters["failed"] += 1
                logger.error(f"  -> Extraction failed for {raw_url}: {exc}")
                db.record_failure(raw_url, str(exc), status="failed", canonical_url=extraction_page.url if extraction_page else None, run_id=run_id, dataset_scope=dataset_scope)

            finally:
                time_breakdown["extraction"] += (time.monotonic() - t_ext_start)

            # Periodic exports & progress reporting
            if time.monotonic() - last_export_mono >= 60.0:
                last_export_mono = time.monotonic()
                t_exp = time.monotonic()
                export_dataset_bundle(db, out_dir, dataset_scope=dataset_scope, filter_sport=filter_sport)
                time_breakdown["export"] += (time.monotonic() - t_exp)

                elapsed_now = time.monotonic() - start_mono
                rate_now = (counters["valid_new_nba_posts"] / (elapsed_now / 3600.0)) if elapsed_now > 0 else 0.0
                write_progress(out_dir, {
                    "run_id": run_id,
                    "status": "running",
                    "elapsed_seconds": round(elapsed_now, 1),
                    "remaining_seconds": round(max(0.0, (deadline_mono - time.monotonic())), 1) if deadline_mono else None,
                    "valid_new_nba_posts": counters["valid_new_nba_posts"],
                    "attempted": counters["attempted"],
                    "current_rate_per_hour": round(rate_now, 1),
                    "pending_tasks": db.get_pending_tasks_count(dataset_scope=dataset_scope),
                    "current_source": current_source,
                })
                write_quality_report(out_dir, db, dataset_scope=dataset_scope, filter_sport=filter_sport, sources_stats=sources_stats, counters=counters)

        return processed

    # Main browser session loop with crash recovery
    try:
        with fd.PersistentBrowserSession(PROFILE_DIR, fingerprint=FINGERPRINT_SEED, headless=headless) as context:
            extraction_page = context.new_page()

            # Iterate through sources with continuous batching
            for ps in page_sources:
                if is_deadline_reached() or stop_early_reason:
                    break

                current_source = ps
                sources_stats[ps] = {
                    "discovered_urls": 0,
                    "scroll_rounds": 0,
                    "status": "running",
                }
                logger.info(f"\nOpening discovery session on source: {ps}")

                discovery_session = disc.PageDiscoverySession(
                    page_url=ps,
                    context=context,
                    max_scrolls=source_max_scrolls,
                    idle_rounds=idle_rounds,
                    max_refreshes=max_refreshes,
                    deadline_mono=deadline_mono,
                    existing_seen_keys=db.get_all_seen_keys(),
                )

                try:
                    discovery_session.start()

                    while not discovery_session.is_exhausted:
                        if is_deadline_reached() or stop_early_reason:
                            break

                        # Check queue backlog
                        pending_count = db.get_pending_tasks_count(dataset_scope=dataset_scope)
                        if pending_count < backlog_limit:
                            t_disc = time.monotonic()
                            try:
                                def on_discovered(item):
                                    is_new = db.queue_parsed_post(item, dataset_scope=dataset_scope)
                                    if is_new:
                                        counters["discovered_urls"] += 1
                                        sources_stats[ps]["discovered_urls"] += 1
                                    return is_new

                                discovery_session.discover_batch(
                                    batch_size=batch_size,
                                    on_url_discovered=on_discovered,
                                    deadline_mono=deadline_mono,
                                )
                            finally:
                                time_breakdown["discovery"] += (time.monotonic() - t_disc)

                        sources_stats[ps]["scroll_rounds"] = discovery_session.scroll_round

                        # Process extraction batch
                        process_extraction_batch(context, extraction_page, limit_batch=batch_size)

                        if max_attempts and counters["attempted"] >= max_attempts:
                            stop_early_reason = "max_attempts_reached"
                            break

                        if target_valid and counters["valid_new_nba_posts"] >= target_valid:
                            stop_early_reason = "target_valid_reached"
                            break

                    sources_stats[ps]["status"] = discovery_session.stop_reason or "source_completed"

                except Exception as exc:
                    logger.error(f"Error in discovery session on {ps}: {exc}")
                    sources_stats[ps]["status"] = f"error: {exc}"
                    if "login" in str(exc).lower() or "checkpoint" in str(exc).lower():
                        stop_early_reason = "login_or_checkpoint_required"
                        break
                finally:
                    discovery_session.close()

            # Process remaining tasks in queue
            while not is_deadline_reached() and not stop_early_reason:
                processed = process_extraction_batch(context, extraction_page, limit_batch=batch_size)
                if processed == 0:
                    break

            try:
                extraction_page.close()
            except Exception:
                pass

    except Exception as exc:
        logger.error(f"Browser session terminated: {exc}")
        if stop_early_reason is None:
            stop_early_reason = f"session_error: {exc}"

    finally:
        lock.release()
        signal.signal(signal.SIGINT, prev_sigint)
        signal.signal(signal.SIGTERM, prev_sigterm)

    # Determine final stop reason
    elapsed_total = time.monotonic() - start_mono
    if interrupted:
        stop_early_reason = "user_interrupted"
    elif is_deadline_reached() and not stop_early_reason:
        stop_early_reason = "max_hours_deadline_reached"
    elif not stop_early_reason:
        stop_early_reason = "all_sources_and_tasks_completed"

    # Export final dataset files
    t_exp = time.monotonic()
    export_dataset_bundle(db, out_dir, dataset_scope=dataset_scope, filter_sport=filter_sport)
    time_breakdown["export"] += (time.monotonic() - t_exp)

    # Build and write run summary
    total_db, status_counts, accepted_in_db = db.get_statistics(dataset_scope=dataset_scope)
    rate_per_hour = (counters["valid_new_nba_posts"] / (elapsed_total / 3600.0)) if elapsed_total > 0 else 0.0
    projected_8h = int(round(rate_per_hour * 8.0))

    summary_data = {
        "run_id": run_id,
        "dataset_scope": dataset_scope,
        "competition": competition,
        "sport_hint": sport_hint,
        "filter_sport": filter_sport,
        "started_at": datetime.fromtimestamp(time.time() - elapsed_total, tz=timezone.utc).isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(elapsed_total, 1),
        "time_budget_hours": max_hours,
        "stop_reason": stop_early_reason,
        "sources_attempted": len(sources_stats),
        "sources_list": page_sources,
        "browser_recoveries": counters["browser_recoveries"],
        "tasks_summary": {
            "discovered_urls_this_run": counters["discovered_urls"],
            "attempted_urls_this_run": counters["attempted"],
            "valid_new_nba_posts_this_run": counters["valid_new_nba_posts"],
            "total_valid_nba_posts_in_db": accepted_in_db,
            "video_posts_skipped": counters["video_skipped"],
            "unmatched_sport_posts": counters["unmatched_sport"],
            "uncertain_nba_posts": counters["uncertain_nba"],
            "missing_required_fields_posts": counters["missing_required"],
            "unavailable_or_deleted_posts": counters["unavailable"],
            "failed_after_retries": counters["failed"],
            "pending_in_queue": db.get_pending_tasks_count(dataset_scope=dataset_scope),
        },
        "performance": {
            "valid_posts_per_hour_realtime": round(rate_per_hour, 2),
            "total_elapsed_minutes": round(elapsed_total / 60.0, 2),
            "time_breakdown_seconds": {k: round(v, 1) for k, v in time_breakdown.items()},
        },
        "extrapolation_8h_estimate": {
            "basis": f"valid_posts_per_hour_realtime ({rate_per_hour:.1f}) * 8.0",
            "projected_8h_posts": projected_8h,
            "condition_note": "Ngoại suy ước tính dựa trên tốc độ thời gian thực của phiên pilot, với điều kiện nguồn fanpage có đủ bài đăng mới và phiên duy trì hoạt động ổn định liên tục. Không phải cam kết số lượng cố định.",
        },
        "required_fields_policy": {
            "post_id": "required, non-empty string",
            "post_url": "required, valid Facebook URL",
            "text": "required, non-empty caption text, video captions omitted",
            "published_at": "required, ISO 8601 timestamp with timezone offset",
            "collected_at": "required, ISO 8601 timestamp with timezone offset",
            "sport_category": "required, Basketball",
            "like_count": "required, non-negative integer engagement indicator",
            "view_count": "optional, null if not displayed",
            "author_followers": "optional integer, cached with 24h TTL",
            "competition_rule": "strict NBA evidence required (franchise, player, explicit league token)",
            "video_policy": "video/reel captions omitted and skipped from research dataset",
        },
    }

    write_run_summary(out_dir, summary_data)
    write_quality_report(out_dir, db, dataset_scope=dataset_scope, filter_sport=filter_sport, sources_stats=sources_stats, counters=counters)
    write_progress(out_dir, {
        "run_id": run_id,
        "status": "completed",
        "elapsed_seconds": round(elapsed_total, 1),
        "valid_new_nba_posts": counters["valid_new_nba_posts"],
        "attempted": counters["attempted"],
        "stop_reason": stop_early_reason,
    })

    print_quality_report(
        db,
        dataset_scope=dataset_scope,
        filter_sport=filter_sport,
        elapsed_seconds=elapsed_total,
        counters=counters,
        run_summary=summary_data,
    )
    db.close()

    if stop_early_reason == "login_or_checkpoint_required":
        print("\n[ALERT] Facebook authentication wall or checkpoint detected. Saved state safely.")
        print("Please run python cloak_facebook_login.py manually to re-authenticate.\n")
        return 2

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Facebook Automated NBA Scraper Pipeline (Continuous Discovery & Extraction)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--mode", type=str, default="permalink", help=argparse.SUPPRESS)
    parser.add_argument("--urls", type=str, help="Path to input URL file (.txt, .jsonl, .csv)")
    parser.add_argument("--page", type=str, help="Single Facebook Page URL to discover posts from")
    parser.add_argument("--pages", "--sources", dest="pages", type=str, help="UTF-8 text file containing Page URLs, one per line")
    parser.add_argument("--competition", type=str, default="NBA", help="Target league / competition scope (default: NBA)")
    parser.add_argument("--sport-hint", type=str, default="Basketball", help="Source topic hint for metadata (default: Basketball)")
    parser.add_argument("--filter-sport", type=str, default="Basketball", help="Filter canonical sport for exported CSV (default: Basketball)")
    parser.add_argument("--batch-size", type=int, default=20, help="Max new URLs to discover before yielding to extraction (default: 20)")
    parser.add_argument("--max-hours", type=float, default=1.0, help="Maximum execution time in hours (default: 1.0 for 60-min pilot)")
    parser.add_argument("--target-valid", type=int, default=None, help="Target valid accepted NBA posts to stop early (default: None)")
    parser.add_argument("--max-attempts", type=int, default=None, help="Hard cap on tasks attempted in this run (default: None)")
    parser.add_argument("--limit", type=int, default=None, help="Deprecated alias for --max-attempts")
    parser.add_argument("--max-new-urls", type=int, default=None, help="Cap on new URLs enqueued in this run (default: None)")
    parser.add_argument("--source-max-scrolls", "--max-scrolls", dest="source_max_scrolls", type=int, default=100, help="Max scan/scroll rounds per source page (default: 100)")
    parser.add_argument("--max-refreshes", type=int, default=0, help="Max reload attempts when scrolling stalls; 0 disables (default: 0)")
    parser.add_argument("--idle-rounds", type=int, default=10, help="Consecutive idle rounds before source stall (default: 10)")
    parser.add_argument("--backlog-limit", type=int, default=150, help="Max pending queue backlog before discovery pauses (default: 150)")
    parser.add_argument("--retry-failed", action="store_true", help="Retry eligible failed tasks")
    parser.add_argument("--resume", action="store_true", help="Resume previous run (default behavior; skips completed tasks)")
    parser.add_argument("--no-followers", action="store_true", help="Skip author follower lookups to maximize speed")
    parser.add_argument("--output-dir", "--output", dest="output_dir", type=str, default=str(OUTPUT_DIR), help="Output directory for SQLite, CSV, JSONL, and reports")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")

    parser.add_argument("--nba-policy", type=str, choices=["contextual", "strict"], default="contextual", help="NBA classification policy (default: contextual)")
    parser.add_argument("--view-policy", type=str, choices=["verified_only", "off"], default="verified_only", help="View count metric extraction policy (default: verified_only)")
    parser.add_argument("--reprocess-offline", action="store_true", help="Reprocess all database records offline using improved classifiers and export clean dataset and audit reconciliation")
    parser.add_argument("--audit-dir", type=str, default=None, help="Directory to save offline audit reports and clean dataset")
    parser.add_argument("--run-id-filter", type=str, default=None, help="Filter offline audit to a specific run_id (e.g. run_20261017T061355_863975Z)")
    parser.add_argument("--no-db-update", action="store_true", help="Do not write reprocessed results back to SQLite during offline audit")

    # Sport deprecated argument
    parser.add_argument("--sport", type=str, default=None, help=argparse.SUPPRESS)

    args = parser.parse_args()

    # Offline reprocess execution branch
    if args.reprocess_offline:
        db_path = Path(args.output_dir) / "facebook_pipeline.sqlite"
        if not db_path.exists():
            parser.error(f"Cannot reprocess offline: SQLite database not found at {db_path}")
        reprocess_dataset_offline(
            db_path=db_path,
            output_dir=args.audit_dir,
            nba_policy=args.nba_policy,
            view_policy=args.view_policy,
            scope_filter=f"facebook:{args.sport_hint.lower() if args.sport_hint else 'basketball'}:{args.competition.lower() if args.competition else 'nba'}",
            run_id_filter=args.run_id_filter,
            apply_to_db=not args.no_db_update,
        )
        return 0

    # Validate numeric parameters
    if args.max_hours is not None and args.max_hours <= 0:
        parser.error("--max-hours must be a positive number greater than 0.")
    if args.batch_size <= 0:
        parser.error("--batch-size must be a positive integer greater than 0.")
    if args.source_max_scrolls <= 0:
        parser.error("--source-max-scrolls must be a positive integer greater than 0.")
    if args.idle_rounds <= 0:
        parser.error("--idle-rounds must be a positive integer greater than 0.")
    if args.max_refreshes < 0:
        parser.error("--max-refreshes must be 0 or greater.")
    if args.target_valid is not None and args.target_valid <= 0:
        parser.error("--target-valid must be a positive integer greater than 0.")

    # Handle deprecated --limit alias
    max_attempts = args.max_attempts
    if args.limit is not None:
        logger.warning("Flag --limit is deprecated as a row limit; treated as alias for --max-attempts.")
        if max_attempts is not None and max_attempts != args.limit:
            parser.error("Conflicting values provided for --max-attempts and deprecated --limit.")
        max_attempts = args.limit

    # Handle deprecated mode argument
    if args.mode == "feed":
        logger.warning("Feed streaming mode has been removed due to Facebook Comet DOM instability. Automatically running in robust Permalink/Discovery mode.")

    # Handle deprecated --sport alias
    if args.sport is not None:
        logger.warning("Flag --sport is deprecated and treated as --sport-hint.")
        if not args.sport_hint:
            args.sport_hint = args.sport

    if not args.urls and not args.page and not args.pages and not (Path(args.output_dir) / "facebook_pipeline.sqlite").exists():
        parser.error("Please provide --urls <file>, --page <url>, or --pages <file> to start pipeline.")

    return run_pipeline(
        input_source=args.urls,
        page_url=args.page,
        pages_file=args.pages,
        competition=args.competition,
        sport_hint=args.sport_hint,
        filter_sport=args.filter_sport,
        nba_policy=args.nba_policy,
        view_policy=args.view_policy,
        batch_size=args.batch_size,
        max_hours=args.max_hours,
        target_valid=args.target_valid,
        max_attempts=max_attempts,
        max_new_urls=args.max_new_urls,
        source_max_scrolls=args.source_max_scrolls,
        max_refreshes=args.max_refreshes,
        idle_rounds=args.idle_rounds,
        backlog_limit=args.backlog_limit,
        retry_failed=args.retry_failed,
        fetch_followers=not args.no_followers,
        headless=args.headless,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    raise SystemExit(main())
