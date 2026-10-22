"""Comprehensive offline test suite for Facebook Keyword Discovery & Extraction pipeline.

Verifies:
1. Query normalization, UTF-8 file reading, stable collection IDs, and Facebook search URL encoding.
2. CLI argument parsing and mode-specific validations (keyword vs permalink).
3. Keyword matching semantics (Unicode word boundaries, "AI" vs "Thailand", hashtags, any/all, exclusions, accents).
4. Candidate URL normalization and post ID extraction (groups, profiles, comment stripping).
5. Quality gate & 17-column export contract for non-sports topics (empty sport_category, null metrics, video captions).
6. SQLite queue: multi-query deduplication, provenance tracking, cached extraction reuse, and collection isolation.
7. End-to-end keyword pipeline orchestration with mocked search discovery and extraction.
"""

import csv
import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import facebook_dataset as fd
import facebook_pipeline as fp
import cloak_facebook_discover as disc


class TestFacebookKeywordPipelineOffline(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_dir = Path(self.temp_dir.name)
        self.db_path = self.output_dir / "test_pipeline.sqlite"

    def tearDown(self):
        self.temp_dir.cleanup()

    # -------------------------------------------------------------
    # 1. Query Normalization, UTF-8 File & Collection ID
    # -------------------------------------------------------------
    def test_parse_queries_string_and_file(self):
        """Verifies parsing comma-separated queries, UTF-8 file, trimming, and deduplication."""
        # Comma-separated with spaces and duplicates
        q_str = "xe điện,  pin LFP , xe điện, VinFast VF8"
        res = fp.parse_queries(queries_str=q_str)
        self.assertEqual(res, ["xe điện", "pin LFP", "VinFast VF8"])

        # UTF-8 file with quotes and phrases containing commas
        queries_file = self.output_dir / "queries.txt"
        queries_file.write_text(
            '# Comment line\n'
            'kem chống nắng\n'
            '"serum B5, phục hồi"\n'
            'kem chống nắng\n'
            '  retinol 0.5%  \n',
            encoding="utf-8"
        )
        file_res = fp.parse_queries(queries_file=str(queries_file))
        self.assertEqual(file_res, ["kem chống nắng", "serum B5, phục hồi", "retinol 0.5%"])

        # Merging both sources with deduplication
        merged = fp.parse_queries(queries_str="kem chống nắng, toner AHA", queries_file=str(queries_file))
        self.assertEqual(merged, ["kem chống nắng", "toner AHA", "serum B5, phục hồi", "retinol 0.5%"])

    def test_stable_collection_id_generation(self):
        """Verifies collection ID generation is deterministic, slug-safe, and non-sports."""
        queries = ["Xe Điện Thông Minh", "Pin LFP 2026!"]
        cid1 = fp.generate_stable_collection_id(queries)
        cid2 = fp.generate_stable_collection_id(queries)
        self.assertEqual(cid1, cid2)
        self.assertTrue(cid1.startswith("col_"))
        self.assertIn("xe_dien", cid1)
        # Should not contain NBA or sports defaults
        self.assertNotIn("nba", cid1.lower())

    def test_facebook_search_url_encoding(self):
        """Verifies search URLs encode Vietnamese, spaces, and special symbols correctly."""
        url = disc.build_facebook_search_url("xe điện & pin LFP #2026")
        self.assertTrue(url.startswith("https://www.facebook.com/search/posts/?q="))
        self.assertIn("xe+%C4%91i%E1%BB%87n", url)
        self.assertIn("%26", url)
        self.assertIn("%232026", url)

    # -------------------------------------------------------------
    # 2. CLI Argument Validation Per Mode
    # -------------------------------------------------------------
    def test_cli_mode_keyword_validation(self):
        """Keyword mode succeeds without sports/competition/url flags."""
        parser = fp.build_arg_parser()

        # Valid keyword mode args
        args = parser.parse_args([
            "--mode", "keyword",
            "--queries", "xe điện,pin LFP",
            "--collection-id", "test-ev",
            "--limit", "50",
            "--limit-per-query", "20"
        ])
        fp.validate_args(args)
        self.assertEqual(args.mode, "keyword")
        self.assertEqual(args.limit, 50)
        self.assertEqual(args.limit_per_query, 20)

        # Keyword mode without queries or queries-file must fail
        args_no_query = parser.parse_args(["--mode", "keyword"])
        with self.assertRaises(ValueError) as ctx:
            fp.validate_args(args_no_query)
        self.assertIn("--queries or --queries-file", str(ctx.exception))

        # Permalink mode with empty directory requires target page/url
        args_permalink_missing = parser.parse_args([
            "--mode", "permalink",
            "--output-dir", str(self.output_dir)
        ])
        with self.assertRaises(ValueError) as ctx:
            fp.validate_args(args_permalink_missing)
        self.assertIn("Missing target Facebook page", str(ctx.exception))

    # -------------------------------------------------------------
    # 3. Keyword Matching Semantics
    # -------------------------------------------------------------
    def test_keyword_matching_word_boundaries(self):
        """Verifies that 'AI' does NOT match inside 'Thailand' or 'Taiwan', but matches standalone 'AI' or '#AI'."""
        queries = ["AI"]

        # Rejection: "Thailand" contains "AI" as substring but not word
        is_match, matched, reasons = fd.match_keywords(
            "Du lịch Thailand mùa hè tuyệt đẹp cùng gia đình",
            include_keywords=queries,
            match_mode="any"
        )
        self.assertFalse(is_match)
        self.assertEqual(matched, [])

        # Rejection: "Taiwan"
        is_match, matched, reasons = fd.match_keywords(
            "Thăm quan bảo tàng tại Taiwan",
            include_keywords=queries,
            match_mode="any"
        )
        self.assertFalse(is_match)

        # Match: Standalone "AI"
        is_match, matched, reasons = fd.match_keywords(
            "Xu hướng ứng dụng AI trong doanh nghiệp năm 2026",
            include_keywords=queries,
            match_mode="any"
        )
        self.assertTrue(is_match)
        self.assertEqual(matched, ["AI"])

        # Match: Hashtag "#AI"
        is_match, matched, reasons = fd.match_keywords(
            "Công nghệ chuyển đổi số mới nhất #AI #Tech",
            include_keywords=queries,
            match_mode="any"
        )
        self.assertTrue(is_match)
        self.assertEqual(matched, ["AI"])

        # Match: Punctuation boundary "AI."
        is_match, matched, reasons = fd.match_keywords(
            "Tương lai phụ thuộc vào AI.",
            include_keywords=queries,
            match_mode="any"
        )
        self.assertTrue(is_match)

    def test_keyword_matching_modes_and_exclusions(self):
        """Verifies match_mode any vs all and exclude_keywords."""
        text = "Trải nghiệm xe điện VinFast VF8 với pin LFP tiên tiến"

        # Match any: one matches
        ok_any, _, _ = fd.match_keywords(text, include_keywords=["xe điện", "xe xăng"], match_mode="any")
        self.assertTrue(ok_any)

        # Match all: one missing -> fails
        ok_all_fail, _, _ = fd.match_keywords(text, include_keywords=["xe điện", "xe xăng"], match_mode="all")
        self.assertFalse(ok_all_fail)

        # Match all: both present -> succeeds
        ok_all_pass, _, _ = fd.match_keywords(text, include_keywords=["xe điện", "pin LFP"], match_mode="all")
        self.assertTrue(ok_all_pass)

        # Exclude keywords: triggered
        ok_excluded, _, reasons = fd.match_keywords(
            text,
            include_keywords=["xe điện"],
            exclude_keywords=["VinFast"],
            match_mode="any"
        )
        self.assertFalse(ok_excluded)
        self.assertTrue(any("excluded_keyword" in r for r in reasons))

        # Exclude keywords: not triggered
        ok_not_excluded, _, _ = fd.match_keywords(
            text,
            include_keywords=["xe điện"],
            exclude_keywords=["xe máy"],
            match_mode="any"
        )
        self.assertTrue(ok_not_excluded)

    def test_vietnamese_accent_stripping_option(self):
        """Verifies that strip_accents=True matches unaccented text, while default False requires accents."""
        text = "Chia sẻ kinh nghiệm sử dụng xe dien gia re"

        # Default (strip_accents=False): "xe điện" does not match "xe dien"
        ok_strict, _, _ = fd.match_keywords(text, include_keywords=["xe điện"], strip_accents=False)
        self.assertFalse(ok_strict)

        # With strip_accents=True: "xe điện" matches "xe dien"
        ok_stripped, _, _ = fd.match_keywords(text, include_keywords=["xe điện"], strip_accents=True)
        self.assertTrue(ok_stripped)

    # -------------------------------------------------------------
    # 4. URL Normalization and Post ID Extraction
    # -------------------------------------------------------------
    def test_url_normalization_and_id_extraction(self):
        """Verifies Facebook post URL normalization for groups, permalinks, and comment dropping."""
        # Group post
        group_url = "https://www.facebook.com/groups/evvietnam/posts/987654321/?comment_id=111&reply_comment_id=222"
        norm_group = fd.normalize_facebook_post_url(group_url)
        self.assertEqual(norm_group, "https://www.facebook.com/groups/evvietnam/posts/987654321/")
        self.assertEqual(fd.extract_post_id(norm_group), "987654321")

        # Group permalink with mobile domain canonicalization
        group_perm = "https://m.facebook.com/groups/techvn/permalink/555666777/?sfnsn=mo"
        norm_perm = fd.normalize_facebook_post_url(group_perm)
        self.assertEqual(norm_perm, "https://www.facebook.com/groups/techvn/permalink/555666777/")
        self.assertEqual(fd.extract_post_id(norm_perm), "555666777")

        # Profile with story_fbid
        profile_url = "https://www.facebook.com/profile.php?id=100012345&story_fbid=77889900"
        norm_profile = fd.normalize_facebook_post_url(profile_url)
        self.assertIn("story_fbid=77889900", norm_profile)
        self.assertEqual(fd.extract_post_id(norm_profile), "77889900")

        # External redirect discarded
        self.assertEqual(fd.normalize_facebook_post_url("https://l.facebook.com/l.php?u=https%3A%2F%2Fexample.com"), "")

    # -------------------------------------------------------------
    # 5. Quality Gate & Acceptance for Generic Collections
    # -------------------------------------------------------------
    def test_generic_topic_quality_gate(self):
        """Verifies generic topics accept short captions, unknown language, video captions, and null metrics."""
        generic_record = {
            "post_id": "12345",
            "platform": "facebook",
            "text": "Xe điện VinFast VF8 ra mắt.",  # Short text (< 5 words)
            "published_at": "2026-10-20T10:00:00+00:00",
            "collected_at": "2026-10-22T10:00:00+00:00",
            "author_id": "vinfastauto",
            "language": "unknown",  # Unknown language
            "hashtags": ["#VinFast", "#EV"],
            "mentions": [],
            "like_count": None,  # Metric missing from Facebook UI (null)
            "comment_count": 15,
            "share_count": None,
            "view_count": None,
            "media_type": "video",  # Video post with caption
            "author_followers": None,
            "post_url": "https://www.facebook.com/vinfastauto/posts/12345",
            "sport_category": None,  # Non-sports
        }

        # Under generic mode settings (target_sport=None, allow_video=True, require_like_count=False)
        is_accepted, reasons = fd.evaluate_record_acceptance(
            generic_record,
            target_sport=None,
            target_competition=None,
            allow_video=True,
            require_like_count=False,
        )
        self.assertTrue(is_accepted, f"Expected accepted, got reasons: {reasons}")
        self.assertEqual(reasons, [])

    def test_nba_mode_still_enforces_sport_rules(self):
        """Verifies NBA mode still strictly enforces basketball category and non-video."""
        nba_record = {
            "post_id": "12345",
            "platform": "facebook",
            "text": "Check out this video of the game",
            "published_at": "2026-10-20T10:00:00+00:00",
            "collected_at": "2026-10-22T10:00:00+00:00",
            "author_id": "nba",
            "language": "en",
            "hashtags": [],
            "mentions": [],
            "like_count": 100,
            "comment_count": 10,
            "share_count": 5,
            "view_count": None,
            "media_type": "video",  # Video rejected in NBA pipeline
            "author_followers": None,
            "post_url": "https://www.facebook.com/nba/posts/12345",
            "sport_category": None,  # Missing Basketball
        }
        is_accepted, reasons = fd.evaluate_record_acceptance(
            nba_record,
            target_sport="Basketball",
            target_competition="NBA",
            allow_video=False,
            require_like_count=True,
        )
        self.assertFalse(is_accepted)
        self.assertTrue(any("video" in r for r in reasons))
        self.assertTrue(any("sport_category_mismatch" in r for r in reasons))

    # -------------------------------------------------------------
    # 6. Database Collections, Deduplication & Cached Reuse
    # -------------------------------------------------------------
    def test_pipeline_db_collections_and_deduplication(self):
        """Verifies multi-query deduplication, provenance tracking, and independent collections in SQLite."""
        db = fp.PipelineDB(self.db_path)

        col_ev = "col-ev-2026"
        col_battery = "col-battery-2026"
        db.register_collection(col_ev, {"queries": ["xe điện", "pin LFP"]})
        db.register_collection(col_battery, {"queries": ["pin LFP", "solid state"]})

        post_url = "https://www.facebook.com/autodaily/posts/999888/"

        # 1. Enqueue under col_ev with query "xe điện"
        cand1 = {
            "post_url": post_url,
            "canonical_url": post_url,
            "post_id": "999888",
            "discovery_query": "xe điện",
            "search_url": "https://www.facebook.com/search/posts/?q=xe+%C4%91i%E1%BB%87n",
            "preview_snippet": "Trải nghiệm xe điện mới",
        }
        is_new1, reused1, cached1 = db.enqueue_keyword_candidate(
            cand1,
            collection_id=col_ev,
            collection_config={"queries": ["xe điện", "pin LFP"]},
        )
        self.assertTrue(is_new1)
        self.assertFalse(reused1)
        self.assertIsNone(cached1)

        # 2. Enqueue same post under col_ev with second query "pin LFP" (same collection)
        cand2 = dict(cand1)
        cand2["discovery_query"] = "pin LFP"
        is_new2, reused2, cached2 = db.enqueue_keyword_candidate(
            cand2,
            collection_id=col_ev,
            collection_config={"queries": ["xe điện", "pin LFP"]},
        )
        self.assertFalse(is_new2)  # Not a new post in collection, deduplicated

        # Verify discovery_queries_json has both queries
        item = db.get_collection_item(col_ev, post_url)
        queries_recorded = json.loads(item["discovery_queries_json"])
        self.assertIn("xe điện", queries_recorded)
        self.assertIn("pin LFP", queries_recorded)

        # 3. Simulate successful extraction and acceptance for col_ev
        sample_record = {
            "post_id": "999888",
            "platform": "facebook",
            "text": "Đánh giá xe điện sử dụng pin LFP bền bỉ.",
            "published_at": "2026-10-21T08:00:00+00:00",
            "collected_at": "2026-10-22T08:00:00+00:00",
            "author_id": "autodaily",
            "language": "vi",
            "hashtags": ["#xe_dien", "#pin_LFP"],
            "mentions": [],
            "like_count": None,  # Facebook omitted like count
            "comment_count": 50,
            "share_count": 12,
            "view_count": None,
            "media_type": "text",
            "author_followers": None,
            "post_url": post_url,
            "sport_category": None,
        }
        is_col_acc, matched_kws, reasons = db.record_keyword_success(
            input_url=post_url,
            canonical_url=post_url,
            post_id="999888",
            author_id="autodaily",
            record_17_col=sample_record,
            tech_meta={"status": "success"},
            collection_id=col_ev,
            collection_config={"queries": ["xe điện", "pin LFP"]},
        )
        self.assertTrue(is_col_acc)
        self.assertEqual(matched_kws, ["xe điện", "pin LFP"])

        # col_ev item is accepted
        self.assertTrue(db.is_collection_item_accepted(col_ev, post_url))
        accepted_ev = db.get_collection_accepted_records(col_ev)
        self.assertEqual(len(accepted_ev), 1)

        # 4. Now col_battery discovers the SAME post
        # Because it's already extracted in pipeline_tasks, it should reuse extraction
        cand3 = {
            "post_url": post_url,
            "canonical_url": post_url,
            "post_id": "999888",
            "discovery_query": "pin LFP",
            "search_url": "https://www.facebook.com/search/posts/?q=pin+LFP",
        }
        is_new3, reused3, cached3 = db.enqueue_keyword_candidate(
            cand3,
            collection_id=col_battery,
            collection_config={"queries": ["pin LFP", "solid state"]},
        )
        self.assertTrue(is_new3)
        self.assertTrue(reused3)
        self.assertIsNotNone(cached3)
        self.assertEqual(cached3.get("post_id"), "999888")

        # col_battery gets its own independent evaluation and status in collection_items
        self.assertTrue(db.is_collection_item_accepted(col_battery, post_url))

        # Check total count in pipeline_tasks: exactly 1 task row created despite multiple queries/collections
        cur = db.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM pipeline_tasks WHERE canonical_url = ?", (post_url,))
        task_count = cur.fetchone()[0]
        self.assertEqual(task_count, 1)

    # -------------------------------------------------------------
    # 7. 17-Column Export Bundle Contract for Non-Sports Collections
    # -------------------------------------------------------------
    def test_export_dataset_bundle_generic_contract(self):
        """Verifies exported CSV has exact 17 columns in order, sport_category is empty string, missing metrics are null."""
        db = fp.PipelineDB(self.db_path)
        col_id = "test-skincare-2026"
        db.register_collection(col_id, {"queries": ["kem chống nắng"]})

        post_url = "https://www.facebook.com/beautyblog/posts/11223344/"
        db.enqueue_keyword_candidate(
            {"post_url": post_url, "canonical_url": post_url, "post_id": "11223344", "discovery_query": "kem chống nắng"},
            collection_id=col_id,
            collection_config={"queries": ["kem chống nắng"]},
        )

        rec = {
            "post_id": "11223344",
            "platform": "facebook",
            "text": "Review kem chống nắng quang phổ rộng cho da dầu mụn",
            "published_at": "2026-10-15T09:00:00+00:00",
            "collected_at": "2026-10-22T09:00:00+00:00",
            "author_id": "beautyblog",
            "language": "vi",
            "hashtags": ["#skincare", "#kcn"],
            "mentions": ["@larocheposay"],
            "like_count": None,  # No reactions count available
            "comment_count": 28,
            "share_count": None,
            "view_count": None,
            "media_type": "image",
            "author_followers": None,
            "post_url": post_url,
            "sport_category": None,  # Non-sports
        }
        db.record_keyword_success(
            input_url=post_url,
            canonical_url=post_url,
            post_id="11223344",
            author_id="beautyblog",
            record_17_col=rec,
            tech_meta={"status": "success"},
            collection_id=col_id,
            collection_config={"queries": ["kem chống nắng"]},
        )

        fp.export_dataset_bundle(db, self.output_dir, collection_id=col_id)
        csv_path = self.output_dir / "facebook_dataset.csv"
        json_path = self.output_dir / "facebook_dataset.json"
        jsonl_path = self.output_dir / "facebook_dataset.jsonl"

        self.assertTrue(csv_path.exists())
        self.assertTrue(json_path.exists())
        self.assertTrue(jsonl_path.exists())

        # Verify CSV columns and headers
        with csv_path.open("r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader)
            self.assertEqual(header, fd.REQUIRED_COLUMNS)
            self.assertEqual(len(header), 17)

            row = next(reader)
            self.assertEqual(len(row), 17)
            # Verify sport_category column (index 16) is empty string
            self.assertEqual(row[16], "")
            # Verify like_count (index 9) is empty string (not "0")
            self.assertEqual(row[9], "")
            # Verify comment_count (index 10) is "28"
            self.assertEqual(row[10], "28")

        # Verify JSON
        with json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            self.assertEqual(len(data), 1)
            self.assertIsNone(data[0]["sport_category"])
            self.assertIsNone(data[0]["like_count"])
            self.assertEqual(data[0]["comment_count"], 28)

    # -------------------------------------------------------------
    # 8. Truncated Preview Does Not Disqualify Before Full Extraction
    # -------------------------------------------------------------
    def test_truncated_preview_not_prematurely_rejected(self):
        """Verifies that a post whose search preview is cut off ('Xem thêm...') is not rejected at candidate stage."""
        db = fp.PipelineDB(self.db_path)
        col_id = "test-preview-col"
        db.register_collection(col_id, {"queries": ["pin LFP"]})

        post_url = "https://www.facebook.com/evgroup/posts/777888999/"
        # Preview is truncated, does NOT contain "pin LFP"
        preview = "Hôm nay chúng tôi xin giới thiệu dòng xe mới được trang bị công nghệ thế hệ tiếp theo... Xem thêm"

        cand = {
            "post_url": post_url,
            "canonical_url": post_url,
            "post_id": "777888999",
            "discovery_query": "pin LFP",
            "search_url": "https://www.facebook.com/search/posts/?q=pin+LFP",
            "preview_snippet": preview,
        }
        is_new, reused, cached = db.enqueue_keyword_candidate(
            cand,
            collection_id=col_id,
            collection_config={"queries": ["pin LFP"]},
        )
        self.assertTrue(is_new)
        self.assertFalse(reused)

        # Post is queued as pending, NOT rejected.
        # Now full extraction runs and recovers full text containing the keyword:
        full_text = (
            "Hôm nay chúng tôi xin giới thiệu dòng xe mới được trang bị công nghệ thế hệ tiếp theo. "
            "Xe sử dụng pin LFP với độ an toàn cao và tuổi thọ trên 15 năm."
        )
        full_record = {
            "post_id": "777888999",
            "platform": "facebook",
            "text": full_text,
            "published_at": "2026-10-21T08:00:00+00:00",
            "collected_at": "2026-10-22T08:00:00+00:00",
            "author_id": "evgroup",
            "language": "vi",
            "hashtags": [],
            "mentions": [],
            "like_count": 50,
            "comment_count": 10,
            "share_count": 2,
            "view_count": None,
            "media_type": "text",
            "author_followers": None,
            "post_url": post_url,
            "sport_category": None,
        }

        # When evaluated with full extraction, it is accepted into the collection
        is_acc, matched, _ = db.record_keyword_success(
            input_url=post_url,
            canonical_url=post_url,
            post_id="777888999",
            author_id="evgroup",
            record_17_col=full_record,
            tech_meta={"status": "success"},
            collection_id=col_id,
            collection_config={"queries": ["pin LFP"]},
        )
        self.assertTrue(is_acc)
        self.assertEqual(matched, ["pin LFP"])
        self.assertTrue(db.is_collection_item_accepted(col_id, post_url))

    # -------------------------------------------------------------
    # 9. Irrelevant Post in Search Results is Rejected with Clear Reason
    # -------------------------------------------------------------
    def test_irrelevant_caption_in_search_results_rejected(self):
        """If Facebook search returns a post whose caption has nothing to do with the query, it is rejected."""
        db = fp.PipelineDB(self.db_path)
        col_id = "test-relevance"
        db.register_collection(col_id, {"queries": ["xe điện"]})

        post_url = "https://www.facebook.com/random/posts/111222/"
        db.enqueue_keyword_candidate(
            {"post_url": post_url, "canonical_url": post_url, "post_id": "111222", "discovery_query": "xe điện"},
            collection_id=col_id,
            collection_config={"queries": ["xe điện"]},
        )

        # Full extracted caption is about cooking recipes
        extracted_text = "Hôm nay chia sẻ công thức làm món sườn xào chua ngọt siêu ngon cho bữa tối gia đình."
        cooking_record = {
            "post_id": "111222",
            "platform": "facebook",
            "text": extracted_text,
            "published_at": "2026-10-21T08:00:00+00:00",
            "collected_at": "2026-10-22T08:00:00+00:00",
            "author_id": "random",
            "language": "vi",
            "hashtags": [],
            "mentions": [],
            "like_count": 10,
            "comment_count": 2,
            "share_count": 0,
            "view_count": None,
            "media_type": "text",
            "author_followers": None,
            "post_url": post_url,
            "sport_category": None,
        }

        # Stored with rejection reason
        is_acc, matched, reasons = db.record_keyword_success(
            input_url=post_url,
            canonical_url=post_url,
            post_id="111222",
            author_id="random",
            record_17_col=cooking_record,
            tech_meta={"status": "success"},
            collection_id=col_id,
            collection_config={"queries": ["xe điện"]},
        )

        self.assertFalse(is_acc)
        self.assertEqual(matched, [])
        self.assertTrue(any("keyword_not_matched" in r for r in reasons))

        self.assertFalse(db.is_collection_item_accepted(col_id, post_url))
        accepted = db.get_collection_accepted_records(col_id)
        self.assertEqual(len(accepted), 0)

        # Audit/debug records keep the rejected record with reasons
        accepted_ds, debug_records = db.get_collection_debug_records(col_id)
        self.assertEqual(len(accepted_ds), 0)
        self.assertEqual(len(debug_records), 1)
        self.assertTrue(any("keyword_not_matched" in r for r in debug_records[0]["rejection_reasons"]))

    # -------------------------------------------------------------
    # 10. End-to-End Keyword Pipeline Mocked Orchestration
    # -------------------------------------------------------------
    @patch("time.sleep", return_value=None)
    @patch("facebook_dataset.PersistentBrowserSession")
    @patch("cloak_facebook_discover.FacebookSearchDiscoverySession")
    @patch("facebook_dataset.extract_scoped_post_data")
    @patch("facebook_pipeline.check_facebook_access_issues", return_value=None)
    def test_run_keyword_pipeline_orchestration(
        self,
        mock_access,
        mock_extract_scoped,
        mock_search_session_cls,
        mock_browser_session_cls,
        mock_sleep,
    ):
        """Verifies end-to-end run_keyword_pipeline loop with mocked browser discovery and extraction."""
        # Setup mock browser context and extraction page
        mock_context = MagicMock()
        mock_browser_session_cls.return_value.__enter__.return_value = mock_context
        mock_extraction_page = MagicMock()
        mock_context.new_page.return_value = mock_extraction_page

        # Setup mock search discovery session
        mock_search_session = MagicMock()
        mock_search_session_cls.return_value = mock_search_session

        cand1 = {
            "post_url": "https://www.facebook.com/evnews/posts/101/",
            "canonical_url": "https://www.facebook.com/evnews/posts/101/",
            "post_id": "101",
            "preview_snippet": "Bài viết về xe điện mới nhất",
            "author_id": "evnews",
            "source_type": "fanpage",
            "platform": "Facebook",
            "discovery_method": "keyword_search",
            "discovery_query": "xe điện",
            "search_url": "https://www.facebook.com/search/posts/?q=xe+%C4%91i%E1%BB%87n",
            "discovered_at": "2026-10-22T10:00:00+00:00",
            "discovery_key": "facebook:post:101",
        }
        cand2 = {
            "post_url": "https://www.facebook.com/evnews/posts/102/",
            "canonical_url": "https://www.facebook.com/evnews/posts/102/",
            "post_id": "102",
            "preview_snippet": "Bài viết không liên quan về ẩm thực",
            "author_id": "evnews",
            "source_type": "fanpage",
            "platform": "Facebook",
            "discovery_method": "keyword_search",
            "discovery_query": "xe điện",
            "search_url": "https://www.facebook.com/search/posts/?q=xe+%C4%91i%E1%BB%87n",
            "discovered_at": "2026-10-22T10:00:01+00:00",
            "discovery_key": "facebook:post:102",
        }

        def mock_discover(batch_size=None, on_candidate_discovered=None, deadline_mono=None):
            if on_candidate_discovered:
                on_candidate_discovered(cand1)
                on_candidate_discovered(cand2)
            mock_search_session.is_exhausted = True
            return [cand1, cand2]

        mock_search_session.discover_batch.side_effect = mock_discover

        # Setup mock post extraction
        def mock_extract(page, post_url, **kwargs):
            if "101" in post_url:
                return (
                    {
                        "post_id": "101",
                        "platform": "facebook",
                        "text": "Đánh giá chi tiết mẫu xe điện mới vừa ra mắt tại Việt Nam.",
                        "published_at": "2026-10-21T10:00:00+00:00",
                        "collected_at": "2026-10-22T10:00:00+00:00",
                        "author_id": "evnews",
                        "language": "vi",
                        "hashtags": ["#xedien"],
                        "mentions": [],
                        "like_count": 150,
                        "comment_count": 20,
                        "share_count": 5,
                        "view_count": None,
                        "media_type": "text",
                        "author_followers": None,
                        "post_url": post_url,
                        "sport_category": None,
                    },
                    {"status": "success", "review_reasons": []},
                )
            else:
                return (
                    {
                        "post_id": "102",
                        "platform": "facebook",
                        "text": "Hướng dẫn nấu canh chua cá lóc thơm ngon đậm đà.",
                        "published_at": "2026-10-21T11:00:00+00:00",
                        "collected_at": "2026-10-22T10:00:00+00:00",
                        "author_id": "evnews",
                        "language": "vi",
                        "hashtags": [],
                        "mentions": [],
                        "like_count": 40,
                        "comment_count": 2,
                        "share_count": 0,
                        "view_count": None,
                        "media_type": "text",
                        "author_followers": None,
                        "post_url": post_url,
                        "sport_category": None,
                    },
                    {"status": "success", "review_reasons": []},
                )

        mock_extract_scoped.side_effect = mock_extract

        # Run pipeline with limit=1 so it stops immediately upon accepting post 101
        ret = fp.run_keyword_pipeline(
            queries=["xe điện"],
            collection_id="test-e2e-ev",
            keyword_match="any",
            exclude_keywords=None,
            strip_accents=False,
            limit=2,
            limit_per_query=5,
            max_hours=0.01,
            poll_interval_sec=0,
            output_dir=self.output_dir,
            headless=True,
        )

        self.assertEqual(ret, 0)

        # Verify exported files exist and contain 1 accepted record
        csv_file = self.output_dir / "facebook_dataset.csv"
        self.assertTrue(csv_file.exists())
        with csv_file.open("r", encoding="utf-8-sig") as f:
            reader = list(csv.reader(f))
            self.assertEqual(len(reader), 2)  # Header + 1 accepted row
            self.assertIn("101", reader[1][0])

        summary_file = self.output_dir / "run_summary.json"
        self.assertTrue(summary_file.exists())
        with summary_file.open("r", encoding="utf-8") as f:
            summary = json.load(f)
            self.assertEqual(summary["tasks_summary"]["accepted_this_run"], 1)
            self.assertEqual(summary["tasks_summary"]["rejected_keyword_mismatch"], 1)


if __name__ == "__main__":
    unittest.main()
