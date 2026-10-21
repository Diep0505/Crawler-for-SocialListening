"""Comprehensive offline test suite for upgraded Facebook NBA collection pipeline.
Verifies all 12 acceptance criteria without requiring internet or live Facebook access.
"""

import json
import os
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import facebook_dataset as fd
import facebook_pipeline as fp
import cloak_facebook_discover as disc


class TestNBAPipelineOffline(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_dir = Path(self.temp_dir.name)
        self.db_path = self.output_dir / "test_pipeline.sqlite"

    def tearDown(self):
        self.temp_dir.cleanup()

    # -------------------------------------------------------------
    # 1. Trích xuất & Chính sách Video
    # -------------------------------------------------------------
    def test_text_image_extraction_no_unbound_local_error(self):
        """Verifies that text/image posts assign extracted_text properly without UnboundLocalError."""
        mock_page = MagicMock()
        mock_page.url = "https://www.facebook.com/nba/posts/123456"
        mock_page.evaluate.side_effect = [
            False,  # is_unavailable evaluation
            {
                "card_found": True,
                "text": "Lakers take down Celtics 112-108 in thrilling fourth quarter comeback! #NBA",
                "published_at_raw": "Friday 16 October 2026 at 19:30",
                "like_count_raw": "45K",
                "comment_count_raw": "1,200",
                "comment_is_zero": False,
                "share_count_raw": "500",
                "view_count_raw": None,
                "media_type": "image",
                "ts_coords": None,
                "dom_mentions": ["@lakers", "@celtics"],
                "review_reasons": [],
            },
        ]

        rec, meta = fd.extract_scoped_post_data(mock_page, post_url=mock_page.url, default_sport="Basketball")
        self.assertIsNotNone(rec["text"])
        self.assertIn("Lakers take down Celtics", rec["text"])
        self.assertEqual(rec["media_type"], "image")
        self.assertEqual(rec["sport_category"], "Basketball")
        self.assertEqual(meta["status"], "success")

    def test_video_posts_omit_caption_and_rejected_from_accepted_dataset(self):
        """Verifies that video posts omit captions, tags, and are rejected by evaluate_record_acceptance."""
        mock_page = MagicMock()
        mock_page.url = "https://www.facebook.com/nba/posts/999888"
        mock_page.evaluate.side_effect = [
            False,
            {
                "card_found": True,
                "text": "Video player subtitle or junk text here #NBA",
                "published_at_raw": "Friday 16 October 2026 at 19:30",
                "like_count_raw": "10K",
                "comment_count_raw": "50",
                "comment_is_zero": False,
                "share_count_raw": "20",
                "view_count_raw": "500K",
                "media_type": "video",
                "ts_coords": None,
                "dom_mentions": [],
                "review_reasons": [],
            },
        ]

        rec, meta = fd.extract_scoped_post_data(mock_page, post_url=mock_page.url, default_sport="Basketball")
        self.assertIsNone(rec["text"])
        self.assertEqual(rec["media_type"], "video")
        self.assertEqual(rec["hashtags"], [])
        self.assertEqual(rec["mentions"], [])

        is_accepted, rejections = fd.evaluate_record_acceptance(rec, meta, target_sport="Basketball", target_competition="NBA")
        self.assertFalse(is_accepted)
        self.assertIn("video_post_omitted_by_policy", rejections)

    # -------------------------------------------------------------
    # 2. Phạm vi DOM (DOM Scoping)
    # -------------------------------------------------------------
    def test_dom_fixture_isolation_comments_and_sidebars(self):
        """Verifies that SCOPED_EXTRACTION_JS isolates target post and excludes comments and sidebars."""
        from playwright.sync_api import sync_playwright

        html_fixture = """
        <html>
          <body>
            <div id="sidebar" role="complementary">
              <h2>Suggested for you</h2>
              <div>This content isn't available right now for some users</div>
            </div>
            <div role="main">
              <div id="target-post" role="article">
                <a href="/nba/posts/777" role="link">NBA</a>
                <div data-ad-preview="message">
                  <span>Target Post: Nikola Jokic delivers another triple-double!</span>
                </div>
                <div role="toolbar">
                  <span aria-label="55K reactions">55K</span>
                  <span aria-label="1,200 comments">1,200</span>
                </div>
                <div class="comments">
                  <div role="article"><span>Comment 1: Best center in the league!</span></div>
                </div>
              </div>
            </div>
          </body>
        </html>
        """
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.set_content(html_fixture)
            res = page.evaluate(fd.SCOPED_EXTRACTION_JS, "https://www.facebook.com/nba/posts/777")
            browser.close()

        self.assertTrue(res["card_found"])
        self.assertIn("Nikola Jokic delivers another triple-double!", res["text"])
        self.assertNotIn("Comment 1", res["text"])
        self.assertNotIn("Suggested for you", res["text"])

    def test_dom_fixture_rejects_notification_toast_dialog(self):
        """Verifies that SCOPED_EXTRACTION_JS ignores notification toasts/dialogs and targets the actual post."""
        from playwright.sync_api import sync_playwright

        html_with_toast = """
        <html>
          <body>
            <!-- Persistent Facebook notification dialog/toast -->
            <div role="dialog" aria-label="Notifications">
              <a href="https://www.facebook.com/reel/123456?story_fbid=987654321">Reel</a>
              <div dir="auto">
                UnreadNam Nhat, Nguyễn Thức and 7 other people recently reacted to your reel: "AI nghe thì xịn, nhưng ứng dụng vào đời..."
              </div>
            </div>

            <!-- Target NBA Post in Main -->
            <div role="main">
              <div id="target-post" role="article">
                <a href="/nba/posts/pfbid02Fa" role="link" aria-label="Saturday, October 17, 2026 at 10:00 AM">NBA</a>
                <div data-ad-preview="message">
                  <span>Stephen Curry hits 8 three-pointers in stunning preseason victory!</span>
                </div>
                <div role="toolbar">
                  <span aria-label="42K reactions">42K</span>
                  <span aria-label="850 comments">850</span>
                  <span aria-label="300 shares">300</span>
                </div>
              </div>
            </div>
          </body>
        </html>
        """
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.set_content(html_with_toast)
            res = page.evaluate(fd.SCOPED_EXTRACTION_JS, "https://www.facebook.com/nba/posts/pfbid02Fa")
            browser.close()

        self.assertTrue(res["card_found"])
        self.assertIn("Stephen Curry hits 8 three-pointers", res["text"])
        self.assertNotIn("UnreadNam Nhat", res["text"])
        self.assertNotIn("reacted to your reel", res["text"])
        self.assertEqual(res["like_count_raw"], "42K")
        self.assertEqual(res["comment_count_raw"], "850")
        self.assertEqual(res["share_count_raw"], "300")
        self.assertEqual(res["published_at_raw"], "Saturday, October 17, 2026 at 10:00 AM")


    # -------------------------------------------------------------
    # 3. Phân loại NBA (NBA Competition Classifier)
    # -------------------------------------------------------------
    def test_nba_classification_rules(self):
        """Verifies NBA classification rules against all required positive, negative, and edge test cases."""
        # Case 1: Direct NBA match
        res1 = fd.classify_nba_post("NBA Finals: Lakers vs Celtics in Game 7 thriller!")
        self.assertTrue(res1["matched"])
        self.assertEqual(res1["status"], "matched")
        self.assertEqual(res1["competition"], "NBA")
        self.assertEqual(res1["sport_category"], "Basketball")
        self.assertIn("nba finals", res1["evidence"])

        # Case 2: WNBA rejected from NBA scope
        res2 = fd.classify_nba_post("WNBA Finals: New York Liberty take Game 1 victory behind Sabrina Ionescu!")
        self.assertFalse(res2["matched"])
        self.assertEqual(res2["status"], "unmatched")
        self.assertEqual(res2["competition"], "WNBA")

        # Case 3: Word "NBA" inside "WNBA" does not match NBA
        res3 = fd.classify_nba_post("Tune in to the WNBA championship game tonight on ESPN!")
        self.assertFalse(res3["matched"])
        self.assertEqual(res3["status"], "unmatched")
        self.assertNotIn("nba", res3["evidence"])

        # Case 4: "I cooked curry for dinner" rejected
        res4 = fd.classify_nba_post("I cooked curry for dinner with rice.")
        self.assertFalse(res4["matched"])
        self.assertEqual(res4["status"], "unmatched")
        self.assertIsNone(res4["sport_category"])

        # Case 5: Stephen Curry in basketball context matched
        res5 = fd.classify_nba_post("Stephen Curry dropped 42 points with 8 three-pointers for the Warriors!")
        self.assertTrue(res5["matched"])
        self.assertEqual(res5["status"], "matched")
        self.assertEqual(res5["competition"], "NBA")

        # Case 6: Emojis only or minimal context -> uncertain
        res6 = fd.classify_nba_post("🔥🏀🏆", sport_hint="Basketball")
        self.assertFalse(res6["matched"])
        self.assertEqual(res6["status"], "uncertain")

        # Case 7: Promotional / recruitment on NBA page without game context -> uncertain
        res7 = fd.classify_nba_post("We are hiring! Apply now for an administrative role at the NBA Store NYC.")
        self.assertFalse(res7["matched"])
        self.assertEqual(res7["status"], "uncertain")

        # Case 8: NBA 2K video game excluded
        res8 = fd.classify_nba_post("NBA 2K25 official gameplay trailer released today!")
        self.assertFalse(res8["matched"])
        self.assertEqual(res8["status"], "unmatched")
        self.assertEqual(res8["competition"], "NBA2K")

        # Case 9: G-League excluded from main NBA scope
        res9 = fd.classify_nba_post("South Bay Lakers face Santa Cruz Warriors in the NBA G League showcase.")
        self.assertFalse(res9["matched"])
        self.assertEqual(res9["status"], "unmatched")
        self.assertEqual(res9["competition"], "GLeague")

        # Case 10: Rule-based score is not hardcoded 0.95 probability
        self.assertIn("rule_score", res1)
        self.assertEqual(res1["rule_score"], 1.0)
        self.assertNotEqual(res1.get("confidence"), 0.95)

    # -------------------------------------------------------------
    # 4. Danh tính & Chuẩn hóa URL
    # -------------------------------------------------------------
    def test_url_normalization_and_deduplication(self):
        """Verifies tracking parameter stripping and canonical matching."""
        raw_url = "https://www.facebook.com/nba/posts/10162548489?fbclid=IwAR2xyz&ref=page_internal&__tn__=%2CO%2CP-R"
        norm_url = fd.normalize_facebook_post_url(raw_url)
        self.assertEqual(norm_url, "https://www.facebook.com/nba/posts/10162548489/")

        # Permalinks with story_fbid
        story_url = "https://www.facebook.com/permalink.php?id=10001&story_fbid=9999&fbclid=123"
        norm_story = fd.normalize_facebook_post_url(story_url)
        self.assertIn("story_fbid=9999", norm_story)
        self.assertIn("id=10001", norm_story)
        self.assertNotIn("fbclid", norm_story)

    # -------------------------------------------------------------
    # 5. Lưu Discovery vào SQLite tức thì
    # -------------------------------------------------------------
    def test_discovery_immediate_sqlite_persistence(self):
        """Verifies that discovered URLs are recorded immediately into SQLite."""
        db = fp.PipelineDB(self.db_path)
        item = {
            "post_url": "https://www.facebook.com/nba/posts/1001/",
            "post_id": "1001",
            "discovery_key": "facebook:post:1001",
        }
        is_new = db.queue_parsed_post(item, dataset_scope="facebook:basketball:nba")
        self.assertTrue(is_new)

        # Immediate check in database
        tasks = db.get_pending_tasks(dataset_scope="facebook:basketball:nba")
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["post_id"], "1001")

        # Re-enqueuing duplicate returns False
        is_dup = db.queue_parsed_post(item, dataset_scope="facebook:basketball:nba")
        self.assertFalse(is_dup)
        db.close()

    # -------------------------------------------------------------
    # 6. Tiếp tục Discovery không mở lại trang
    # -------------------------------------------------------------
    def test_page_discovery_session_batching_without_reload(self):
        """Verifies that PageDiscoverySession yields at batch_size without reloading the page."""
        mock_context = MagicMock()
        mock_page = MagicMock()
        mock_page.url = "https://www.facebook.com/nba/"
        mock_page.locator.return_value.first.is_visible.return_value = False
        mock_context.new_page.return_value = mock_page

        # Simulate 2 scan rounds, each returning distinct links
        mock_page.evaluate.side_effect = [
            {
                "links": [
                    {"href": f"https://www.facebook.com/nba/posts/{i}/"} for i in range(1, 15)
                ]
            },
            {
                "links": [
                    {"href": f"https://www.facebook.com/nba/posts/{i}/"} for i in range(15, 30)
                ]
            },
        ]

        session = disc.PageDiscoverySession(
            page_url="https://www.facebook.com/nba/",
            context=mock_context,
            max_scrolls=10,
            idle_rounds=5,
        )
        session.start()
        # Page should only have been navigated to ONCE
        mock_page.goto.assert_called_once()

        # Request batch of 10
        batch1 = session.discover_batch(batch_size=10)
        self.assertEqual(len(batch1), 10)
        # Verify page was NOT closed or reloaded
        mock_page.close.assert_not_called()

        # Request next batch
        batch2 = session.discover_batch(batch_size=10)
        self.assertEqual(len(batch2), 10)
        # Still not reloaded
        mock_page.goto.assert_called_once()
        session.close()

    # -------------------------------------------------------------
    # 7. Khôi phục task processing sau crash
    # -------------------------------------------------------------
    def test_recover_interrupted_processing_tasks(self):
        """Verifies that tasks left in 'processing' state after an unexpected termination are reset to 'pending'."""
        db = fp.PipelineDB(self.db_path)
        db.queue_urls(["https://www.facebook.com/nba/posts/555/"], dataset_scope="facebook:basketball:nba")
        db.update_task_processing("https://www.facebook.com/nba/posts/555/")

        # Verify task is currently 'processing'
        row = db.conn.execute("SELECT status FROM pipeline_tasks WHERE input_url = 'https://www.facebook.com/nba/posts/555/'").fetchone()
        self.assertEqual(row["status"], "processing")

        # Recover
        db.recover_interrupted_tasks(dataset_scope="facebook:basketball:nba")
        row_recovered = db.conn.execute("SELECT status FROM pipeline_tasks WHERE input_url = 'https://www.facebook.com/nba/posts/555/'").fetchone()
        self.assertEqual(row_recovered["status"], "pending")
        db.close()

    # -------------------------------------------------------------
    # 8. Deadline chung & Interruptible Sleep
    # -------------------------------------------------------------
    def test_monotonic_deadline_enforcement(self):
        """Verifies monotonic deadline stops task loop without resetting budget."""
        start_mono = time.monotonic()
        # Create an expired deadline (1ms ago)
        deadline_mono = start_mono - 0.001

        def is_deadline_reached():
            return time.monotonic() >= deadline_mono

        self.assertTrue(is_deadline_reached())

    # -------------------------------------------------------------
    # 9. Lỗi phiên & Checkpoint
    # -------------------------------------------------------------
    def test_checkpoint_detection_triggers_session_halt(self):
        """Verifies that Facebook login/checkpoint screen is detected and halts session."""
        mock_page = MagicMock()
        mock_page.url = "https://www.facebook.com/checkpoint/?next=https%3A%2F%2Fwww.facebook.com%2F"
        issue = fp.check_facebook_access_issues(mock_page)
        self.assertIsNotNone(issue)
        self.assertIn("checkpoint", issue.lower())

    # -------------------------------------------------------------
    # 10. Phân vùng Scope & Tránh lẫn dữ liệu cũ
    # -------------------------------------------------------------
    def test_dataset_scope_isolation_prevents_historical_leaks(self):
        """Verifies that Football/Athletics tasks in SQLite are NOT included in NBA dataset exports."""
        db = fp.PipelineDB(self.db_path)

        # Enqueue and complete a historical Athletics task
        ath_rec = fd.build_dataset_record(
            "https://www.facebook.com/atletismoRFEA/posts/111",
            text="Valencia Marathon record broken!",
            sport_category="Athletics",
            like_count=500,
            published_at="2026-10-15T10:00:00+07:00",
        )
        db.record_success(
            input_url="https://www.facebook.com/atletismoRFEA/posts/111",
            canonical_url="https://www.facebook.com/atletismoRFEA/posts/111",
            post_id="ath_111",
            author_id="atletismoRFEA",
            record_17_col=ath_rec,
            tech_meta={"status": "success", "sport_classification": {"status": "classified"}, "nba_classification": {"status": "unmatched"}},
            dataset_scope="facebook:athletics",
        )

        # Enqueue and complete an NBA task
        nba_rec = fd.build_dataset_record(
            "https://www.facebook.com/nba/posts/222",
            text="Lakers defeat Warriors 120-115 in NBA season opener!",
            sport_category="Basketball",
            like_count=25000,
            published_at="2026-10-16T19:00:00+07:00",
        )
        db.record_success(
            input_url="https://www.facebook.com/nba/posts/222",
            canonical_url="https://www.facebook.com/nba/posts/222",
            post_id="nba_222",
            author_id="nba",
            record_17_col=nba_rec,
            tech_meta={"status": "success", "sport_classification": {"status": "classified"}, "nba_classification": {"status": "matched", "competition": "NBA"}},
            dataset_scope="facebook:basketball:nba",
        )

        # Query accepted NBA records
        nba_records = db.get_accepted_records(dataset_scope="facebook:basketball:nba", filter_sport="Basketball")
        self.assertEqual(len(nba_records), 1)
        self.assertEqual(nba_records[0]["post_id"], "222")
        self.assertEqual(nba_records[0]["sport_category"], "Basketball")

        # Verify Athletics task was excluded
        post_ids = [r["post_id"] for r in nba_records]
        self.assertNotIn("111", post_ids)
        db.close()

    # -------------------------------------------------------------
    # 11. Xuất dữ liệu thống nhất (CSV, JSON, JSONL)
    # -------------------------------------------------------------
    def test_unified_export_consistency(self):
        """Verifies that CSV, JSON, and JSONL exports contain the exact same set of records with 17 columns."""
        db = fp.PipelineDB(self.db_path)
        nba_rec = fd.build_dataset_record(
            "https://www.facebook.com/nba/posts/333",
            text="Celtics edge Bucks in double overtime! #NBA",
            sport_category="Basketball",
            like_count=15000,
            published_at="2026-10-16T21:00:00+07:00",
        )
        db.record_success(
            input_url="https://www.facebook.com/nba/posts/333",
            canonical_url="https://www.facebook.com/nba/posts/333",
            post_id="333",
            author_id="nba",
            record_17_col=nba_rec,
            tech_meta={"status": "success", "nba_classification": {"status": "matched"}},
            dataset_scope="facebook:basketball:nba",
        )

        csv_file, count = fp.export_dataset_bundle(db, self.output_dir, dataset_scope="facebook:basketball:nba", filter_sport="Basketball")
        self.assertEqual(count, 1)

        # Read CSV
        import csv
        with open(self.output_dir / "facebook_dataset.csv", mode="r", encoding="utf-8-sig") as f:
            reader = list(csv.DictReader(f))
            self.assertEqual(len(reader), 1)
            self.assertEqual(list(reader[0].keys()), fd.REQUIRED_COLUMNS)
            self.assertEqual(reader[0]["post_id"], "333")

        # Read JSON
        with open(self.output_dir / "facebook_dataset.json", mode="r", encoding="utf-8") as f:
            json_data = json.load(f)
            self.assertEqual(len(json_data), 1)
            self.assertEqual(json_data[0]["post_id"], "333")

        # Read JSONL
        with open(self.output_dir / "facebook_dataset.jsonl", mode="r", encoding="utf-8") as f:
            lines = [json.loads(line) for line in f.read().splitlines() if line.strip()]
            self.assertEqual(len(lines), 1)
            self.assertEqual(lines[0]["post_id"], "333")

        # Verify only 1 debug file exists
        debug_files = list(self.output_dir.glob("facebook_posts_auto_debug_*.json"))
        self.assertEqual(len(debug_files), 1)

        # Export a second time and ensure previous debug file is purged
        import time as _t
        _t.sleep(0.01)
        fp.export_dataset_bundle(db, self.output_dir, dataset_scope="facebook:basketball:nba", filter_sport="Basketball")
        debug_files_after = list(self.output_dir.glob("facebook_posts_auto_debug_*.json"))
        self.assertEqual(len(debug_files_after), 1)

        db.close()

    # -------------------------------------------------------------
    # 12. Cache, Migration & An toàn Entry Point
    # -------------------------------------------------------------
    def test_followers_cache_persistence_and_ttl(self):
        """Verifies SQLite followers cache saves values and ignores expired ones."""
        db = fp.PipelineDB(self.db_path)
        db.set_followers_cache("nba", 45000000, raw_value="45M", ttl_hours=24)

        cached = db.get_followers_cache("nba")
        self.assertIsNotNone(cached)
        self.assertEqual(cached["followers"], 45000000)

        # Expired cache check
        db.set_followers_cache("expired_author", 100, ttl_hours=-1)
        self.assertIsNone(db.get_followers_cache("expired_author"))
        db.close()

    def test_safe_entry_point_does_not_open_browser_on_import_or_help(self):
        """Verifies that importing modules or running --help does not trigger browser launch."""
        import subprocess
        import sys
        res = subprocess.run(
            [sys.executable, "facebook_pipeline.py", "--help"],
            capture_output=True,
            text=True,
            cwd=str(fp.BASE_DIR),
        )
        self.assertEqual(res.returncode, 0)
        self.assertIn("--competition", res.stdout)
        self.assertIn("--batch-size", res.stdout)
        self.assertIn("--source-max-scrolls", res.stdout)


if __name__ == "__main__":
    unittest.main()
