"""Unit tests for Phase 2: X (Twitter) Integration & Autonomous Discovery."""

import csv
import json
import sqlite3
import pytest
from pathlib import Path

from x_pipeline import XPipeline
from discovery_engine.adapters.x_adapter import XDiscoveryAdapter
from discovery_engine.adapters.facebook_adapter import FacebookDiscoveryAdapter
import facebook_dataset as fd


def test_x_adapter_tweet_mapping():
    adapter = XDiscoveryAdapter()
    sample_tweet = {
        "id": "1849000111222333444",
        "text": "Luka Doncic hits a game-winning stepback three over the Clippers! 42 points. #MFFL #NBA",
        "username": "dallasmavs",
        "followersCount": 2300000,
        "likeCount": 15400,
        "replyCount": 820,
        "retweetCount": 2400,
        "viewCount": 210000,
        "hashtags": ["MFFL", "NBA"],
        "mentions": ["@luka7doncic"],
        "date": "2026-10-21T07:30:00+00:00",
        "media_type": "video",
    }

    record, meta = adapter.map_tweet_to_record(sample_tweet, sport_hint="Basketball", competition="NBA")

    # Verify 17 columns
    for col in fd.REQUIRED_COLUMNS:
        assert col in record, f"Missing required column: {col}"

    assert record["platform"] == "X"
    assert record["post_id"] == "1849000111222333444"
    assert record["author_id"] == "dallasmavs"
    assert record["like_count"] == 15400
    assert record["comment_count"] == 820
    assert record["share_count"] == 2400
    assert record["view_count"] == 210000
    assert record["author_followers"] == 2300000
    assert record["media_type"] == "video"
    assert record["sport_category"] == "Basketball"
    assert "https://x.com/dallasmavs/status/" in record["post_url"]


def test_x_pipeline_ingest_and_export(tmp_path):
    test_db = tmp_path / "x_pipeline.db"
    out_dir = tmp_path / "x_output"

    pipeline = XPipeline(db_path=str(test_db), output_dir=str(out_dir), is_demo=True)

    # Ingest demo verified posts
    records = pipeline.generate_demo_dataset(num_posts=3, sport="Basketball", competition="NBA")
    assert len(records) == 3

    # Check SQLite database
    conn = sqlite3.connect(str(test_db))
    rows = conn.execute("SELECT post_id, platform, status, is_accepted FROM pipeline_tasks WHERE platform = 'X'").fetchall()
    conn.close()

    assert len(rows) == 3
    for r in rows:
        assert r[1] == "X"
        assert r[2] == "success"
        assert r[3] == 1

    # Export dataset
    exports = pipeline.export_dataset(output_dir=str(out_dir))
    csv_file = Path(exports["csv"])
    assert csv_file.is_file()

    with csv_file.open("r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        header = next(reader)
        assert header == fd.REQUIRED_COLUMNS
        rows_csv = list(reader)
        assert len(rows_csv) == 3


def test_mention_graph_snowballing():
    fb_adapter = FacebookDiscoveryAdapter()

    records = [
        {"mentions": ["@KingJames", "@NBA", "@wojespn"]},
        {"mentions": ["@StephenCurry30", "@warriors", "@here", "@followers"]},
    ]

    profiles = fb_adapter.extract_mention_profiles(records)

    # Should filter out generic mentions like @here, @followers
    assert "https://www.facebook.com/KingJames/" in profiles
    assert "https://www.facebook.com/NBA/" in profiles
    assert "https://www.facebook.com/StephenCurry30/" in profiles
    assert not any("here" in p.lower() for p in profiles)
    assert not any("followers" in p.lower() for p in profiles)


def test_view_count_exact_none_and_video_isolation():
    """Validates Rule 2: view_count must preserve None, 0, and not substitute video views."""
    adapter = XDiscoveryAdapter()

    # Case A: viewCount is None, but video views exist
    tweet_a = {
        "id": "1001",
        "text": "LeBron James fastbreak slam dunk in the NBA finals! #Lakers",
        "username": "Lakers",
        "likeCount": 100,
        "viewCount": None,
        "video_views": 500000,
        "date": "2026-10-21T05:00:00+00:00",
    }
    rec_a, meta_a = adapter.map_tweet_to_record(tweet_a, sport_hint="Basketball", competition="NBA")
    assert rec_a["view_count"] is None
    assert meta_a["metrics_details"]["video_views"] == 500000

    # Case B: viewCount is 0
    tweet_b = {
        "id": "1002",
        "text": "Late night game analysis for NBA fans. #NBA",
        "username": "celtics",
        "likeCount": 5,
        "viewCount": 0,
        "date": "2026-10-21T05:05:00+00:00",
    }
    rec_b, meta_b = adapter.map_tweet_to_record(tweet_b, sport_hint="Basketball", competition="NBA")
    assert rec_b["view_count"] == 0


def test_placeholder_author_followers_none():
    """Validates Rule 2: Missing or placeholder author profile must NOT be counted as 0 followers."""
    adapter = XDiscoveryAdapter()
    tweet = {
        "id": "1003",
        "text": "Giannis Antetokounmpo dunks over two defenders in NBA action! #Bucks",
        "username": "deleted_user_123",
        "is_placeholder_user": True,
        "followersCount": 0,
        "date": "2026-10-21T05:10:00+00:00",
    }
    rec, meta = adapter.map_tweet_to_record(tweet, sport_hint="Basketball", competition="NBA")
    assert rec["author_followers"] is None
    assert meta["author_metadata"]["is_placeholder_user"] is True
    assert "Null indicates missing or placeholder" in meta["author_metadata"]["author_followers_note"]


def test_scope_gate_ambiguous_keyword_needs_review():
    """Validates Rule 1: 'Warriors' movie/medieval post without NBA context must be routed to needs_review."""
    adapter = XDiscoveryAdapter()

    # Ambiguous tweet matching 'warriors' without any basketball context
    ambiguous_tweet = {
        "id": "2001",
        "text": "The ancient warriors fought bravely with shields and broadswords in the medieval castle.",
        "username": "history_buff_99",
        "followersCount": 1500,
        "likeCount": 12,
        "date": "2026-10-21T06:00:00+00:00",
    }
    rec_amb, meta_amb = adapter.map_tweet_to_record(ambiguous_tweet, sport_hint="Basketball", competition="NBA")
    assert meta_amb["is_accepted"] is False
    assert meta_amb["status"] == "needs_review"
    assert any("ambiguous_scope_keyword" in r or "scope_unmatched" in r for r in meta_amb["review_reasons"])

    # Legitimate NBA Warriors tweet
    legit_tweet = {
        "id": "2002",
        "text": "Steph Curry scores 38 points as the Warriors defeat the Nuggets in NBA season opener! #DubNation",
        "username": "warriors",
        "followersCount": 5000000,
        "likeCount": 25000,
        "date": "2026-10-21T06:15:00+00:00",
    }
    rec_legit, meta_legit = adapter.map_tweet_to_record(legit_tweet, sport_hint="Basketball", competition="NBA")
    assert meta_legit["is_accepted"] is True
    assert meta_legit["status"] == "success"
    assert len(meta_legit["review_reasons"]) == 0


def test_quote_tweet_metrics_isolation():
    """Validates Rule 2 & 5: Quote tweet metrics must not overwrite outer tweet metrics."""
    adapter = XDiscoveryAdapter()
    tweet = {
        "id": "3001",
        "text": "Look at this incredible play from last night's NBA game!",
        "username": "nba_analyst",
        "likeCount": 45,
        "replyCount": 3,
        "retweetCount": 10,
        "viewCount": 1200,
        "date": "2026-10-21T06:30:00+00:00",
        "is_quote": True,
        "quotedTweet": {
            "id": "2999",
            "username": "KingJames",
            "text": "Back in the gym preparing for the season.",
            "likeCount": 150000,
            "viewCount": 3000000,
        },
    }
    rec, meta = adapter.map_tweet_to_record(tweet, sport_hint="Basketball", competition="NBA")
    # Outer metrics preserved
    assert rec["like_count"] == 45
    assert rec["view_count"] == 1200
    assert meta["post_type"] == "quote"
    assert meta["metrics_details"]["is_quote"] is True
    assert meta["metrics_details"]["quoted_tweet"]["id"] == "2999"
    assert meta["metrics_details"]["quoted_tweet"]["like_count"] == 150000


def test_demo_isolation_from_research_dataset(tmp_path):
    """Validates Rule 3: Demo fixtures must never leak into production research exports."""
    db_file = tmp_path / "shared_pipeline.db"
    prod_out = tmp_path / "prod_out"
    demo_out = tmp_path / "demo_out"

    # 1. Pipeline running in demo mode
    demo_pipeline = XPipeline(db_path=str(db_file), output_dir=str(demo_out), is_demo=True)
    demo_records = demo_pipeline.generate_demo_dataset(num_posts=2)
    assert len(demo_records) == 2

    demo_exports = demo_pipeline.export_dataset()
    with open(demo_exports["csv"], "r", encoding="utf-8-sig") as f:
        demo_rows = list(csv.reader(f))
        assert len(demo_rows) == 3  # Header + 2 posts

    # 2. Pipeline running in production research mode on the same database
    prod_pipeline = XPipeline(db_path=str(db_file), output_dir=str(prod_out), is_demo=False)
    prod_exports = prod_pipeline.export_dataset()
    with open(prod_exports["csv"], "r", encoding="utf-8-sig") as f:
        prod_rows = list(csv.reader(f))
        # Demo fixtures MUST NOT leak into production export
        assert len(prod_rows) == 1  # Only header, 0 data rows


def test_multi_query_deduplication_and_provenance(tmp_path):
    """Validates Rule 6: Identical tweet appearing in multiple queries preserves all sources."""
    db_file = tmp_path / "multi_query.db"
    out_dir = tmp_path / "out"

    pipeline = XPipeline(db_path=str(db_file), output_dir=str(out_dir))
    
    # Mock search by directly inserting two queries for same post_url into pipeline_tasks
    post_url = "https://x.com/NBA/status/999999999"
    now_1 = "2026-10-21T07:00:00+00:00"
    now_2 = "2026-10-21T07:05:00+00:00"

    from facebook_pipeline import PipelineDB
    db = PipelineDB(str(db_file))

    # Query 1: #NBA
    sample_tweet = {
        "id": "999999999",
        "text": "Jayson Tatum buzzer beater for Celtics in NBA action! #NBA",
        "username": "NBA",
        "followersCount": 45000000,
        "likeCount": 10000,
        "date": "2026-10-21T06:55:00+00:00",
    }
    rec1, meta1 = pipeline.adapter.map_tweet_to_record(sample_tweet, sport_hint="Basketball", competition="NBA", query_source="#NBA")
    meta1["discovery_queries"] = ["#NBA"]

    with db.conn:
        db.conn.execute(
            """INSERT INTO pipeline_tasks
               (input_url, canonical_url, post_id, author_id, platform, source_type,
                sport_category, competition, dataset_scope, status, is_accepted,
                review_reasons, record_json, tech_meta_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, 'X', 'organization', 'Basketball', 'NBA', 'x:basketball:nba', 'success', 1, '[]', ?, ?, ?, ?)""",
            (post_url, post_url, "999999999", "NBA", json.dumps(rec1), json.dumps(meta1), now_1, now_1),
        )

    # Query 2: "Celtics buzzer beater" (Simulate pipeline deduplication logic)
    existing = db.conn.execute("SELECT tech_meta_json, created_at FROM pipeline_tasks WHERE canonical_url = ?", (post_url,)).fetchone()
    assert existing is not None
    assert existing["created_at"] == now_1

    existing_meta = json.loads(existing["tech_meta_json"])
    queries = existing_meta.get("discovery_queries", [])
    queries.append("Celtics buzzer beater")
    existing_meta["discovery_queries"] = queries

    with db.conn:
        db.conn.execute(
            """UPDATE pipeline_tasks
               SET tech_meta_json = ?, updated_at = ?
               WHERE canonical_url = ?""",
            (json.dumps(existing_meta), now_2, post_url),
        )

    # Verify no duplicate task row, both queries recorded, original created_at preserved
    tasks = db.conn.execute("SELECT post_id, created_at, updated_at, tech_meta_json FROM pipeline_tasks WHERE canonical_url = ?", (post_url,)).fetchall()
    assert len(tasks) == 1
    assert tasks[0]["created_at"] == now_1
    assert tasks[0]["updated_at"] == now_2
    final_meta = json.loads(tasks[0]["tech_meta_json"])
    assert final_meta["discovery_queries"] == ["#NBA", "Celtics buzzer beater"]
    db.close()
