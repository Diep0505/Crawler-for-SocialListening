"""Unit tests for Multi-Platform Sport Social Listening Discovery Engine."""

import sqlite3
import pytest
from pathlib import Path
from discovery_engine.taxonomy import (
    SPORT_TAXONOMY,
    get_sport_queries,
    get_sport_hashtags,
    classify_sport_and_competition,
)
from discovery_engine.models import DiscoveredPost
from discovery_engine.adapters.facebook_adapter import FacebookDiscoveryAdapter
from discovery_engine.adapters.x_adapter import XDiscoveryAdapter
from discovery_engine.adapters.instagram_adapter import InstagramDiscoveryAdapter
from discovery_engine.adapters.threads_adapter import ThreadsDiscoveryAdapter
from discovery_engine.engine import DiscoveryEngine
from facebook_pipeline import PipelineDB


def test_taxonomy_hierarchy():
    assert "Basketball" in SPORT_TAXONOMY
    assert "Football" in SPORT_TAXONOMY
    assert "F1" in SPORT_TAXONOMY
    assert "Athletics" in SPORT_TAXONOMY

    # Verify primary competitions
    assert "NBA" in SPORT_TAXONOMY["Basketball"]["competitions"]
    assert "EPL" in SPORT_TAXONOMY["Football"]["competitions"]
    assert "F1" in SPORT_TAXONOMY["F1"]["competitions"]
    assert "World Athletics" in SPORT_TAXONOMY["Athletics"]["competitions"]


def test_sport_queries_and_hashtags():
    nba_queries = get_sport_queries(sport="Basketball", competition="NBA")
    assert any("nba" in q.lower() for q in nba_queries)
    assert any("lakers" in q.lower() or "celtics" in q.lower() for q in nba_queries)

    epl_tags = get_sport_hashtags(sport="Football", competition="EPL")
    assert any("premierleague" in t.lower() or "epl" in t.lower() for t in epl_tags)


def test_classify_sport_and_competition():
    # NBA test
    s, c, conf, meta = classify_sport_and_competition("Steph Curry drops 35 points as the Warriors win thriller against Lakers!")
    assert s == "Basketball"
    assert c == "NBA"
    assert conf > 0.5
    assert meta["status"] == "matched"

    # EPL Football test
    s, c, conf, meta = classify_sport_and_competition("Manchester United secure crucial victory in the Premier League derby.")
    assert s == "Football"
    assert c == "EPL"

    # F1 test
    s, c, conf, meta = classify_sport_and_competition("Max Verstappen takes pole position for Red Bull Racing at the Monaco Grand Prix.")
    assert s == "F1"
    assert c == "F1"

    # Marathon test
    s, c, conf, meta = classify_sport_and_competition("Kipchoge smashes the world record at the Berlin Marathon!")
    assert s == "Athletics"
    assert c == "Marathon"


def test_discovered_post_model():
    p = DiscoveredPost(
        url="https://x.com/NBA/status/1234567890123456789",
        platform="x",
        post_id="1234567890123456789",
        author_id="NBA",
        source_type="fanpage",
        sport="Basketball",
        competition="NBA",
    )
    assert p.platform == "X"
    assert p.to_pipeline_scope() == "x:basketball:nba"

    d = p.to_dict()
    assert d["post_id"] == "1234567890123456789"
    assert d["platform"] == "X"

    reconstructed = DiscoveredPost.from_dict(d)
    assert reconstructed.url == p.url
    assert reconstructed.platform == "X"


def test_facebook_adapter_link_parsing():
    fb = FacebookDiscoveryAdapter()

    # 1. Page post
    page_url = "https://www.facebook.com/warriors/posts/10161234567890123"
    assert fb.normalize_post_url(page_url) == "https://www.facebook.com/warriors/posts/10161234567890123/"
    assert fb.extract_post_id(page_url) == "10161234567890123"
    assert fb.determine_source_type(page_url) == "fanpage"

    # 2. Public Group post
    group_url = "https://www.facebook.com/groups/nbavietnam/posts/9876543210/"
    assert fb.extract_post_id(group_url) == "9876543210"
    assert fb.determine_source_type(group_url) == "group"

    # 3. Individual user profile post
    user_url = "https://www.facebook.com/profile.php?id=100012345678&story_fbid=555666777"
    assert fb.extract_post_id(user_url) == "555666777"
    assert fb.determine_source_type(user_url) == "individual"

    posts = fb.parse_discovered_links(
        [page_url, group_url, user_url],
        sport="Basketball",
        competition="NBA",
    )
    assert len(posts) == 3
    assert any(p.source_type == "group" for p in posts)
    assert any(p.source_type == "individual" for p in posts)
    assert any(p.source_type == "fanpage" for p in posts)


def test_x_adapter_link_parsing():
    x = XDiscoveryAdapter()
    tweet_url = "https://twitter.com/KingJames/status/1789012345678901234"
    normalized = x.normalize_post_url(tweet_url)
    assert normalized == "https://x.com/KingJames/status/1789012345678901234"
    assert x.extract_post_id(normalized) == "1789012345678901234"
    assert x.extract_author_id(normalized) == "KingJames"

    posts = x.parse_discovered_links([tweet_url], sport="Basketball", competition="NBA")
    assert len(posts) == 1
    assert posts[0].platform == "X"
    assert posts[0].author_id == "KingJames"


def test_instagram_adapter_link_parsing():
    ig = InstagramDiscoveryAdapter()
    post_url = "https://www.instagram.com/p/DB12345Xyz_/?utm_source=ig_web_copy_link"
    normalized = ig.normalize_post_url(post_url)
    assert normalized == "https://www.instagram.com/p/DB12345Xyz_/"
    assert ig.extract_post_id(normalized) == "DB12345Xyz_"

    reel_url = "https://www.instagram.com/reel/DC67890Abc_/"
    assert ig.normalize_post_url(reel_url) == "https://www.instagram.com/reel/DC67890Abc_/"

    posts = ig.parse_discovered_links([post_url, reel_url], sport="Football", competition="EPL")
    assert len(posts) == 2
    assert posts[0].platform == "Instagram"


def test_threads_adapter_link_parsing():
    th = ThreadsDiscoveryAdapter()
    thread_url = "https://www.threads.net/@stephencurry30/post/DA12345Xyz"
    normalized = th.normalize_post_url(thread_url)
    assert normalized == "https://www.threads.net/@stephencurry30/post/DA12345Xyz"
    assert th.extract_post_id(normalized) == "DA12345Xyz"
    assert th.extract_author_id(normalized) == "stephencurry30"

    posts = th.parse_discovered_links([thread_url], sport="Basketball", competition="NBA")
    assert len(posts) == 1
    assert posts[0].platform == "Threads"
    assert posts[0].author_id == "stephencurry30"


def test_sqlite_pipeline_ingestion(tmp_path):
    test_db = tmp_path / "test_pipeline.db"
    engine = DiscoveryEngine(db_path=str(test_db))

    posts = [
        DiscoveredPost(
            url="https://www.facebook.com/nba/posts/10161234567890123/",
            platform="Facebook",
            post_id="10161234567890123",
            author_id="nba",
            source_type="fanpage",
            sport="Basketball",
            competition="NBA",
        ),
        DiscoveredPost(
            url="https://www.facebook.com/groups/nbafans/posts/999888777/",
            platform="Facebook",
            post_id="999888777",
            source_type="group",
            sport="Basketball",
            competition="NBA",
        ),
        DiscoveredPost(
            url="https://x.com/FabrizioRomano/status/1789999999999999999",
            platform="X",
            post_id="1789999999999999999",
            author_id="FabrizioRomano",
            source_type="individual",
            sport="Football",
            competition="EPL",
        ),
    ]

    stats = engine.enqueue_posts(posts, db_path=str(test_db))
    assert stats["total_submitted"] == 3
    assert stats["enqueued"] == 3
    assert stats["skipped_or_existing"] == 0

    # Test deduplication
    stats_dupe = engine.enqueue_posts(posts, db_path=str(test_db))
    assert stats_dupe["enqueued"] == 0
    assert stats_dupe["skipped_or_existing"] == 3

    # Verify rows in database
    conn = sqlite3.connect(str(test_db))
    rows = conn.execute("SELECT input_url, platform, source_type, sport_category, competition FROM pipeline_tasks").fetchall()
    conn.close()

    assert len(rows) == 3
    assert rows[0][1] == "Facebook"
    assert rows[0][2] == "fanpage"
    assert rows[1][2] == "group"
    assert rows[2][1] == "X"
    assert rows[2][2] == "individual"
    assert rows[2][3] == "Football"
    assert rows[2][4] == "EPL"
