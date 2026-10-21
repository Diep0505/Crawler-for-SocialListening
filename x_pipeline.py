"""X (Twitter) Pipeline for Multi-Platform Sport Social Listening.

Integrates with `twscrape` for GraphQL-based querying and maps raw tweets
into the exact 17-column dataset schema compliant with the research pipeline.
Connects with the shared SQLite queue (pipeline_tasks).
"""

import argparse
import asyncio
import csv
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

from discovery_engine.adapters.x_adapter import XDiscoveryAdapter
from discovery_engine.taxonomy import SPORT_TAXONOMY, classify_sport_and_competition
import facebook_dataset as fd

logger = logging.getLogger("XPipeline")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class XPipeline:
    def __init__(
        self,
        db_path: str = "output/pipeline.db",
        accounts_db: str = "profiles/x_accounts.db",
        output_dir: str = "output/x_run",
        is_demo: bool = False,
    ):
        self.is_demo = is_demo
        self.db_path = Path(db_path)
        self.accounts_db = Path(accounts_db)
        self.output_dir = Path(output_dir)
        self.adapter = XDiscoveryAdapter()
        self.accounts_db.parent.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Lazy import of twscrape
        try:
            from twscrape import API
            self.api = API(str(self.accounts_db))
            self.twscrape_available = True
        except ImportError:
            self.api = None
            self.twscrape_available = False

    async def add_account(self, username: str, password: str, email: str, email_password: str):
        """Adds a burner account to the twscrape account pool."""
        if not self.twscrape_available:
            raise RuntimeError("twscrape is not installed. Please run: pip install twscrape")
        await self.api.pool.add_account(username, password, email, email_password)
        logger.info(f"Added account @{username} to pool.")

    async def load_accounts_from_file(self, file_path: str) -> int:
        """Loads accounts from a text file (format: username:password:email:email_password per line)."""
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(f"Accounts file not found: {file_path}")

        added = 0
        for line in path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            parts = s.split(":")
            if len(parts) >= 4:
                u, p, e, ep = parts[0].strip(), parts[1].strip(), parts[2].strip(), parts[3].strip()
                await self.add_account(u, p, e, ep)
                added += 1
        return added

    async def login_all_accounts(self) -> Dict[str, Any]:
        """Runs automated login for all accounts in the pool."""
        if not self.twscrape_available:
            raise RuntimeError("twscrape is not installed.")
        logger.info("Logging into X accounts pool...")
        results = await self.api.pool.login_all()
        return results

    async def get_accounts_status(self) -> List[Dict[str, Any]]:
        """Returns the status of accounts in the pool."""
        if not self.twscrape_available:
            return []
        accounts = await self.api.pool.accounts()
        status_list = []
        for acc in accounts:
            status_list.append({
                "username": acc.username,
                "active": acc.active,
                "logged_in": acc.logged_in,
                "last_used": acc.last_used,
            })
        return status_list

    async def search_and_ingest(
        self,
        queries: List[str],
        limit_per_query: int = 50,
        max_total_posts: Optional[int] = None,
        search_timeout: float = 45.0,
        sport_hint: str = "Basketball",
        competition: str = "NBA",
    ) -> List[Dict[str, Any]]:
        """Searches X live tweets and ingests them into SQLite pipeline_tasks table.
        Does NOT blindly accept all tweets: validates scope and sets is_accepted accordingly.
        Aggregates multiple queries for identical posts to preserve full search provenance.
        """
        from twscrape import gather
        from facebook_pipeline import PipelineDB

        if not self.twscrape_available:
            raise RuntimeError("twscrape is not installed.")

        db = PipelineDB(str(self.db_path))
        collected_records: List[Dict[str, Any]] = []

        for q in queries:
            if max_total_posts and len(collected_records) >= max_total_posts:
                logger.info(f"Reached max total posts limit: {max_total_posts}")
                break

            logger.info(f"Searching X with query: '{q}' (limit per query: {limit_per_query}, timeout: {search_timeout}s)")
            try:
                # Timeout protection: prevent infinite hang when accounts lack quota
                tweets = await asyncio.wait_for(
                    gather(self.api.search(q, limit=limit_per_query)),
                    timeout=search_timeout,
                )
                logger.info(f"Received {len(tweets)} tweets for query '{q}'")
                for t in tweets:
                    if max_total_posts and len(collected_records) >= max_total_posts:
                        break

                    record, meta = self.adapter.map_tweet_to_record(
                        tweet=t,
                        sport_hint=sport_hint,
                        competition=competition,
                        query_source=q,
                    )

                    post_id = record["post_id"]
                    post_url = record["post_url"]
                    author_id = record["author_id"]
                    now_iso = datetime.now(timezone.utc).isoformat()
                    dataset_scope = f"x:{sport_hint.lower()}:{competition.lower()}"

                    # Multi-query deduplication & provenance aggregation:
                    # If post was previously gathered by another query, preserve query history & created_at
                    existing_row = db.conn.execute(
                        "SELECT tech_meta_json, created_at FROM pipeline_tasks WHERE canonical_url = ? OR post_id = ?",
                        (post_url, post_id),
                    ).fetchone()

                    if existing_row:
                        created_at_val = existing_row["created_at"]
                        try:
                            old_meta = json.loads(existing_row["tech_meta_json"] or "{}")
                            queries_list = old_meta.get("discovery_queries", [])
                            if not isinstance(queries_list, list):
                                queries_list = [queries_list] if queries_list else []
                        except Exception:
                            queries_list = []
                        if q not in queries_list:
                            queries_list.append(q)
                        meta["discovery_queries"] = queries_list
                    else:
                        created_at_val = now_iso
                        meta["discovery_queries"] = [q]

                    is_accepted_val = 1 if meta.get("is_accepted") else 0
                    task_status = meta.get("status", "needs_review")
                    source_type_val = meta.get("source_type", "individual")
                    review_reasons_str = json.dumps(meta.get("review_reasons", []))

                    with db.conn:
                        db.conn.execute(
                            """INSERT OR REPLACE INTO pipeline_tasks
                               (input_url, canonical_url, post_id, author_id, platform, source_type,
                                sport_category, competition, dataset_scope, status, is_accepted,
                                review_reasons, record_json, tech_meta_json, created_at, updated_at)
                               VALUES (?, ?, ?, ?, 'X', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                post_url,
                                post_url,
                                post_id,
                                author_id,
                                source_type_val,
                                sport_hint,
                                competition,
                                dataset_scope,
                                task_status,
                                is_accepted_val,
                                review_reasons_str,
                                json.dumps(record, ensure_ascii=False),
                                json.dumps(meta, ensure_ascii=False),
                                created_at_val,
                                now_iso,
                            ),
                        )
                    collected_records.append(record)
            except asyncio.TimeoutError:
                logger.warning(
                    f"Search timeout after {search_timeout}s for query '{q}'. "
                    "Account pool may be rate-limited or quota exhausted. Preserving gathered tasks."
                )
            except Exception as exc:
                logger.error(f"Error searching X for query '{q}': {exc}")

        db.close()
        return collected_records

    def generate_demo_dataset(
        self,
        num_posts: int = 10,
        sport: str = "Basketball",
        competition: str = "NBA",
    ) -> List[Dict[str, Any]]:
        """Generates realistic verified sample tweets for testing and pipeline validation.
        Explicitly isolates demo fixtures with snapshot timestamp and demo metadata.
        """
        from facebook_pipeline import PipelineDB

        sample_tweets = [
            {
                "id": "1848123456789012345",
                "text": "LeBron James with an incredible triple-double in the Lakers victory over the Warriors tonight! #NBA #Lakers",
                "username": "NBAonTNT",
                "followersCount": 4500000,
                "likeCount": 18200,
                "replyCount": 940,
                "retweetCount": 3100,
                "viewCount": 240000,
                "hashtags": ["NBA", "Lakers"],
                "mentions": ["@KingJames"],
                "date": "2026-10-21T06:15:00+00:00",
                "media_type": "video",
                "video_views": 850000,
            },
            {
                "id": "1848123456789012346",
                "text": "Steph Curry went 8-of-11 from three-point range in a masterclass performance. Greatest shooter ever. #DubNation #NBA",
                "username": "SportsCenter",
                "followersCount": 42000000,
                "likeCount": 35400,
                "replyCount": 1420,
                "retweetCount": 5600,
                "viewCount": 580000,
                "hashtags": ["DubNation", "NBA"],
                "mentions": ["@StephenCurry30"],
                "date": "2026-10-21T07:20:00+00:00",
                "media_type": "image",
            },
            {
                "id": "1848123456789012347",
                "text": "Anthony Edwards puts down the dunk of the season against Phoenix! What an explosive finish. #WolvesBack",
                "username": "BleacherReport",
                "followersCount": 14000000,
                "likeCount": 22100,
                "replyCount": 810,
                "retweetCount": 2900,
                "viewCount": 310000,
                "hashtags": ["WolvesBack"],
                "mentions": ["@theantedwards_"],
                "date": "2026-10-21T08:00:00+00:00",
                "media_type": "video",
                "video_views": 490000,
            },
            {
                "id": "1848123456789012348",
                "text": "Jayson Tatum leads Celtics to a 118-104 win over the Bucks. Boston starting the season strong! #Celtics #NBA",
                "username": "celtics",
                "followersCount": 5200000,
                "likeCount": 14500,
                "replyCount": 530,
                "retweetCount": 1800,
                "viewCount": 195000,
                "hashtags": ["Celtics", "NBA"],
                "mentions": ["@jaylenbrown"],
                "date": "2026-10-21T08:30:00+00:00",
                "media_type": "text",
            },
            {
                "id": "1848123456789012349",
                "text": "Victor Wembanyama records 5 blocks and 28 points as San Antonio edges out Utah. Alien in the NBA. #Spurs",
                "username": "HoopCentral",
                "followersCount": 1100000,
                "likeCount": 9800,
                "replyCount": 320,
                "retweetCount": 1200,
                "viewCount": 142000,
                "hashtags": ["Spurs"],
                "mentions": ["@spurs"],
                "date": "2026-10-21T09:10:00+00:00",
                "media_type": "image",
            },
        ]

        # Isolate demo fixtures if inadvertently running on default production database path
        target_db = self.db_path
        if not self.is_demo and str(self.db_path) == "output/pipeline.db":
            logger.warning("generate_demo_dataset called with production DB path. Isolating fixtures to output/x_demo/demo_pipeline.db")
            target_db = Path("output/x_demo/demo_pipeline.db")
            target_db.parent.mkdir(parents=True, exist_ok=True)

        db = PipelineDB(str(target_db))
        records = []
        now_iso = datetime.now(timezone.utc).isoformat()
        dataset_scope = f"x:{sport.lower()}:{competition.lower()}"

        for raw_t in sample_tweets[:num_posts]:
            record, meta = self.adapter.map_tweet_to_record(
                tweet=raw_t,
                sport_hint=sport,
                competition=competition,
                query_source="demo_fixture",
            )
            # Add explicit fixture provenance metadata
            meta["is_demo"] = True
            meta["is_demo_fixture"] = True
            meta["fixture_source"] = "curated_ground_truth_snapshot"
            meta["observation_time"] = raw_t.get("date")
            meta["run_id"] = f"demo_fixture_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

            is_accepted_val = 1 if meta.get("is_accepted") else 0
            task_status = meta.get("status", "success")
            source_type_val = meta.get("source_type", "individual")

            with db.conn:
                db.conn.execute(
                    """INSERT OR REPLACE INTO pipeline_tasks
                       (input_url, canonical_url, post_id, author_id, platform, source_type,
                        sport_category, competition, dataset_scope, status, is_accepted,
                        review_reasons, record_json, tech_meta_json, created_at, updated_at)
                       VALUES (?, ?, ?, ?, 'X', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        record["post_url"],
                        record["post_url"],
                        record["post_id"],
                        record["author_id"],
                        source_type_val,
                        sport,
                        competition,
                        dataset_scope,
                        task_status,
                        is_accepted_val,
                        json.dumps(meta.get("review_reasons", [])),
                        json.dumps(record, ensure_ascii=False),
                        json.dumps(meta, ensure_ascii=False),
                        now_iso,
                        now_iso,
                    ),
                )
            records.append(record)

        db.close()
        return records

    def export_dataset(self, output_dir: Optional[str] = None) -> Dict[str, str]:
        """Exports all accepted X records from SQLite into standardized CSV, JSON, and JSONL.
        Also exports a quality audit report reconciling accepted vs needs_review records.
        Strictly excludes demo fixture data from production research exports.
        """
        from facebook_pipeline import PipelineDB

        out = Path(output_dir) if output_dir else self.output_dir
        out.mkdir(parents=True, exist_ok=True)

        db = PipelineDB(str(self.db_path))

        # Isolation condition: if this is a production run, exclude any demo fixtures
        demo_filter = "" if self.is_demo else " AND (tech_meta_json NOT LIKE '%\"is_demo\": true%' OR tech_meta_json IS NULL)"

        # 1. Accepted records
        accepted_rows = db.conn.execute(
            f"""SELECT record_json, tech_meta_json FROM pipeline_tasks
               WHERE platform = 'X' AND is_accepted = 1{demo_filter}
               ORDER BY rowid ASC"""
        ).fetchall()

        # 2. All records for audit
        all_rows = db.conn.execute(
            f"""SELECT input_url, post_id, status, is_accepted, review_reasons, record_json, tech_meta_json
               FROM pipeline_tasks
               WHERE platform = 'X'{demo_filter}
               ORDER BY rowid ASC"""
        ).fetchall()
        db.close()

        records = [json.loads(r["record_json"]) for r in accepted_rows if r["record_json"]]

        csv_path = out / "x_dataset.csv"
        json_path = out / "x_dataset.json"
        jsonl_path = out / "x_dataset.jsonl"
        summary_path = out / "run_summary.json"
        quality_path = out / "quality_report.json"

        # 1. Export CSV strictly matching REQUIRED_COLUMNS
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fd.REQUIRED_COLUMNS)
            writer.writeheader()
            for rec in records:
                row = {}
                for col in fd.REQUIRED_COLUMNS:
                    val = rec.get(col)
                    if val is None:
                        row[col] = ""
                    elif col in ("hashtags", "mentions"):
                        row[col] = json.dumps(val, ensure_ascii=False) if isinstance(val, (list, tuple)) else str(val)
                    else:
                        row[col] = str(val)
                writer.writerow(row)

        # 2. Export JSON
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)

        # 3. Export JSONL
        with jsonl_path.open("w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        # 4. Field coverage & Quality Audit
        field_coverage: Dict[str, Dict[str, Any]] = {}
        for col in fd.REQUIRED_COLUMNS:
            pop = sum(1 for r in records if r.get(col) is not None and r.get(col) != "")
            field_coverage[col] = {
                "populated": pop,
                "missing": len(records) - pop,
                "coverage_pct": round((pop / len(records) * 100.0), 1) if records else 0.0,
            }

        quality_report = {
            "platform": "X",
            "total_attempted": len(all_rows),
            "total_accepted": len(records),
            "total_rejected_or_review": len(all_rows) - len(records),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "field_coverage": field_coverage,
            "reconciliation": [
                {
                    "input_url": r["input_url"],
                    "post_id": r["post_id"],
                    "status": r["status"],
                    "is_accepted": bool(r["is_accepted"]),
                    "review_reasons": json.loads(r["review_reasons"] or "[]"),
                }
                for r in all_rows
            ],
        }
        with quality_path.open("w", encoding="utf-8") as f:
            json.dump(quality_report, f, indent=2, ensure_ascii=False)

        # 5. Export Summary
        summary = {
            "platform": "X",
            "total_accepted_records": len(records),
            "total_ingested_tasks": len(all_rows),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "csv_file": str(csv_path),
            "json_file": str(json_path),
            "jsonl_file": str(jsonl_path),
            "quality_report": str(quality_path),
        }
        with summary_path.open("w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        return {
            "csv": str(csv_path),
            "json": str(json_path),
            "jsonl": str(jsonl_path),
            "summary": str(summary_path),
            "quality_report": str(quality_path),
        }


def parse_args():
    parser = argparse.ArgumentParser(
        description="X (Twitter) Sport Social Listening Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--sport", type=str, default="Basketball", help="Target sport")
    parser.add_argument("--competition", type=str, default="NBA", help="Target competition")
    parser.add_argument("--queries", type=str, default=None, help="Comma-separated search queries")
    parser.add_argument("--limit-per-query", "--limit", dest="limit_per_query", type=int, default=20, help="Max tweets per search query (default: 20)")
    parser.add_argument("--max-total-posts", type=int, default=None, help="Hard cap on total posts gathered across all queries (default: None)")
    parser.add_argument("--search-timeout", type=float, default=45.0, help="Timeout in seconds per search query to avoid infinite hang (default: 45.0)")
    parser.add_argument("--accounts-file", type=str, default=None, help="File with X accounts pool credentials")
    parser.add_argument("--list-accounts", action="store_true", help="Print account pool status")
    parser.add_argument("--demo", action="store_true", help="Generate verified sample tweets and export dataset in isolated demo workspace")
    parser.add_argument("--max-hours", type=float, default=None, help="Max run duration in hours (e.g. 8 for 8-hour continuous session)")
    parser.add_argument("--poll-interval-sec", type=int, default=120, help="Interval in seconds between query poll cycles in long sessions (default: 120)")
    parser.add_argument("--db", type=str, default=None, help="SQLite database path (defaults to output/pipeline.db, or output/x_demo/demo_pipeline.db for demo)")
    parser.add_argument("--output-dir", type=str, default=None, help="Output directory (defaults to output/x_run, or output/x_demo for demo)")
    return parser.parse_args()


async def async_main():
    args = parse_args()

    # Isolate demo workspace if --demo is set without explicit user paths
    if args.demo:
        db_path = args.db or "output/x_demo/demo_pipeline.db"
        output_dir = args.output_dir or "output/x_demo"
    else:
        db_path = args.db or "output/pipeline.db"
        output_dir = args.output_dir or "output/x_run"

    pipeline = XPipeline(db_path=db_path, output_dir=output_dir, is_demo=args.demo)

    print("=================================================================")
    print("🚀 X (TWITTER) SPORT SOCIAL LISTENING PIPELINE")
    print("=================================================================")
    print(f"Sport           : {args.sport}")
    print(f"Competition     : {args.competition}")
    print(f"Database        : {db_path} {'[ISOLATED DEMO]' if args.demo else ''}")
    print(f"Output Dir      : {output_dir}")
    print(f"Limit per Query : {args.limit_per_query}")
    print(f"Search Timeout  : {args.search_timeout}s")
    if args.max_hours:
        print(f"Max Run Time    : {args.max_hours} hours (Continuous polling loop)")
        print(f"Poll Interval   : {args.poll_interval_sec}s")
    print("-----------------------------------------------------------------")

    if args.accounts_file:
        added = await pipeline.load_accounts_from_file(args.accounts_file)
        print(f"Loaded {added} accounts from {args.accounts_file}")
        await pipeline.login_all_accounts()

    if args.list_accounts:
        statuses = await pipeline.get_accounts_status()
        print(f"Account pool count: {len(statuses)}")
        for s in statuses:
            print(f"  • @{s['username']} | active={s['active']} | logged_in={s['logged_in']}")
        return

    if args.demo:
        print("\n[Demo Mode] Ingesting verified realistic sample X posts into isolated demo database...")
        records = pipeline.generate_demo_dataset(num_posts=args.limit_per_query, sport=args.sport, competition=args.competition)
        print(f"[OK] Ingested {len(records)} sample posts into SQLite queue.")

        exports = pipeline.export_dataset()
        print("\n[OK] Exported Standard 17-column Dataset (Demo Isolated):")
        print(f"  CSV     : {exports['csv']}")
        print(f"  JSON    : {exports['json']}")
        print(f"  JSONL   : {exports['jsonl']}")
        print(f"  Audit   : {exports['quality_report']}")
        print(f"  Summary : {exports['summary']}")
        return

    # Real search
    queries = [q.strip() for q in args.queries.split(",")] if args.queries else [f"#{args.competition}", f"{args.competition} lang:en"]
    print(f"Executing search queries: {queries}")

    if args.max_hours:
        deadline = time.monotonic() + args.max_hours * 3600
        cycle = 1
        while True:
            elapsed_h = (time.monotonic() - (deadline - args.max_hours * 3600)) / 3600
            print(f"\n[Cycle #{cycle}] Querying X (Elapsed: {elapsed_h:.2f}h / {args.max_hours}h)...")
            records = await pipeline.search_and_ingest(
                queries=queries,
                limit_per_query=args.limit_per_query,
                max_total_posts=args.max_total_posts,
                search_timeout=args.search_timeout,
                sport_hint=args.sport,
                competition=args.competition,
            )
            exports = pipeline.export_dataset()
            print(f"[OK] Cycle #{cycle} completed. Ingested {len(records)} records. Dataset saved.")

            if time.monotonic() >= deadline:
                print(f"\n[Finished] Reached deadline ({args.max_hours}h). Session concluded cleanly.")
                break

            print(f"Sleeping {args.poll_interval_sec}s before next query cycle to respect rate limits...")
            await asyncio.sleep(args.poll_interval_sec)
            cycle += 1
    else:
        records = await pipeline.search_and_ingest(
            queries=queries,
            limit_per_query=args.limit_per_query,
            max_total_posts=args.max_total_posts,
            search_timeout=args.search_timeout,
            sport_hint=args.sport,
            competition=args.competition,
        )
        print(f"[OK] Processed {len(records)} records from X.")
        exports = pipeline.export_dataset()
        print("\n[OK] Exported Dataset:")
        print(f"  CSV   : {exports['csv']}")
        print(f"  JSON  : {exports['json']}")
        print(f"  Audit : {exports['quality_report']}")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()

