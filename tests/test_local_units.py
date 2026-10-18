"""Unit tests for Facebook scraper modules: regex, normalization, language detection,
datetime parsing, and SQLite state resumption.
"""

import sqlite3
import unittest
from pathlib import Path

import facebook_dataset as fd
from facebook_pipeline import PipelineDB, load_input_urls


class TestFacebookScraperUnits(unittest.TestCase):

    def test_unicode_and_vietnamese_diacritics_regex(self):
        """Verifies that mathematical bold and Vietnamese characters match without re.PatternError."""
        bold_text = "𝐌𝐨𝐮𝐫𝐢𝐧𝐡𝐨’𝐬 𝐦𝐞𝐬𝐬𝐚𝐠𝐞 𝐨𝐧 𝐕𝐢𝐧𝐢 𝐉𝐫."
        words = fd.re.findall(r"[\w\U0001D400-\U0001D7FF]{3,}", bold_text, flags=fd.re.UNICODE)
        self.assertIn("𝐌𝐨𝐮𝐫𝐢𝐧𝐡𝐨", words)
        self.assertIn("𝐦𝐞𝐬𝐬𝐚𝐠𝐞", words)

        vn_text = "Giải chạy điền kinh tiếp sức Việt Nam 2026"
        vn_words = fd.re.findall(r"[\w\U0001D400-\U0001D7FF]{3,}", vn_text, flags=fd.re.UNICODE)
        self.assertIn("chạy", vn_words)
        self.assertIn("điền", vn_words)
        self.assertIn("kinh", vn_words)

    def test_numeric_count_normalization(self):
        """Verifies multi-format number normalization and approximation flags."""
        val, approx = fd.parse_numeric_count_with_meta("1.2K")
        self.assertEqual(val, 1200)
        self.assertTrue(approx)

        val, approx = fd.parse_numeric_count_with_meta("1,2K")
        self.assertEqual(val, 1200)
        self.assertTrue(approx)

        val, approx = fd.parse_numeric_count_with_meta("1,234")
        self.assertEqual(val, 1234)
        self.assertFalse(approx)

        val, approx = fd.parse_numeric_count_with_meta("1.234")
        self.assertEqual(val, 1234)
        self.assertFalse(approx)

        val, approx = fd.parse_numeric_count_with_meta("1,2 nghìn")
        self.assertEqual(val, 1200)
        self.assertTrue(approx)

        val, approx = fd.parse_numeric_count_with_meta("2.5M")
        self.assertEqual(val, 2500000)
        self.assertTrue(approx)

        self.assertIsNone(fd.parse_numeric_count(None))
        self.assertIsNone(fd.parse_numeric_count("No reactions"))

    def test_timestamp_standardization(self):
        """Verifies ISO 8601 formatting with timezone and insufficient evidence handling."""
        # Exact English
        iso, prec = fd.parse_facebook_timestamp("Friday 30 December 2022 at 01:52")
        self.assertEqual(prec, "exact")
        self.assertTrue(iso.startswith("2022-12-30T01:52:00"))

        # Exact Spanish
        iso_es, prec_es = fd.parse_facebook_timestamp("Miércoles 21 de diciembre de 2022 a las 19:43")
        self.assertEqual(prec_es, "exact")
        self.assertTrue(iso_es.startswith("2022-12-21T19:43:00"))

        # Insufficient evidence (relative time)
        iso_rel, prec_rel = fd.parse_facebook_timestamp("2 h")
        self.assertIsNone(iso_rel)
        self.assertEqual(prec_rel, "insufficient_evidence")

        iso_yr, prec_yr = fd.parse_facebook_timestamp("3y")
        self.assertIsNone(iso_yr)
        self.assertEqual(prec_yr, "insufficient_evidence")

    def test_sport_category_normalization(self):
        """Verifies sports mapping and marathon/athletics categorization."""
        self.assertEqual(fd.validate_or_normalize_sport("Marathon"), "Athletics")
        self.assertEqual(fd.validate_or_normalize_sport("maratón"), "Athletics")
        self.assertEqual(fd.validate_or_normalize_sport("running"), "Athletics")
        self.assertEqual(fd.validate_or_normalize_sport("atletismo"), "Athletics")
        self.assertEqual(fd.validate_or_normalize_sport("football"), "Football")
        self.assertEqual(fd.validate_or_normalize_sport("bóng đá"), "Football")
        self.assertEqual(fd.validate_or_normalize_sport("cycling"), "Cycling")
        # Non-sports rejection
        self.assertIsNone(fd.validate_or_normalize_sport("politics"))
        self.assertIsNone(fd.validate_or_normalize_sport("weather"))

    def test_language_detection(self):
        """Verifies vi, es, en, and unknown fallback without defaulting Latin to English."""
        # Spanish text
        es_text = "Dicen que recordar es volver a vivir, así que recordamos juntos los grandes momentos del atletismo en 2022."
        self.assertEqual(fd.detect_language(es_text), "es")

        # English text
        en_text = "Mourinho's message on Vini Jr: what hurts me and makes me sad is this situation with the team."
        self.assertEqual(fd.detect_language(en_text), "en")

        # Vietnamese text
        vn_text = "Giải vô địch điền kinh quốc gia đã diễn ra tại Hà Nội với nhiều kỷ lục mới."
        self.assertEqual(fd.detect_language(vn_text), "vi")

        # Undetermined / short Latin
        self.assertEqual(fd.detect_language("Queropalca Peru"), "unknown")

    def test_sqlite_pipeline_resumption(self):
        """Verifies SQLite queueing, deduplication, and task resumption."""
        db_path = Path("output/test_pipeline_temp.sqlite")
        if db_path.exists():
            db_path.unlink()

        db = PipelineDB(db_path)
        urls = [
            "https://www.facebook.com/3211971012344948",
            "https://www.facebook.com/587466646719913",
            "https://www.facebook.com/3211971012344948",  # Duplicate
        ]
        added = db.queue_urls(urls)
        self.assertEqual(added, 2)  # Deduplicated

        pending = db.get_pending_tasks()
        self.assertEqual(len(pending), 2)

        # Mark first as success
        rec = fd.build_dataset_record(urls[0], text="Sample text", sport_category="Athletics")
        db.record_success(urls[0], urls[0], "post_321", "author_1", rec, {"status": "success"})

        # Subsequent query should only have 1 pending task (resumption!)
        remaining = db.get_pending_tasks()
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["input_url"], urls[1])

        db.close()
        if db_path.exists():
            db_path.unlink()

    def test_dom_fixture_isolation_comments_and_sidebars(self):
        """Verifies that extraction extracts ONLY the target post caption and ignores comments and adjacent posts."""
        from playwright.sync_api import sync_playwright

        html_fixture = """
        <html>
          <body>
            <div id="feed-container">
              <!-- Adjacent post 1 -->
              <div class="adjacent-post" role="article">
                <div data-ad-preview="message">Adjacent post caption: Breaking news on football!</div>
                <div>Other comments here</div>
              </div>

              <!-- TARGET MAIN POST CARD -->
              <div id="target-post" role="article" class="main-card">
                <div class="header">
                  <a href="/atletismoRFEA/posts/12345" role="link">Posted 2022-12-30</a>
                </div>
                <div data-ad-preview="message" class="caption-container">
                  <span>Target Post Headline</span><br/>
                  <span>Las 12 campanadas del atletismo en 2022. Valencia Marathon summary!</span>
                </div>
                <div class="interaction-stats">
                  <span>44</span>
                  <span>2</span>
                  <span>6</span>
                </div>
                <div role="toolbar">
                  <button aria-label="Like"></button>
                  <button aria-label="Comment"></button>
                  <button aria-label="Share"></button>
                </div>
                <div class="comments-section">
                  <div role="article" class="comment-item">
                    <span>Commenter 1: Gran carrera, enhorabuena a todos los participantes!</span>
                  </div>
                  <div role="article" class="comment-item">
                    <span>Commenter 2: ¿Dónde puedo ver las clasificaciones oficiales?</span>
                  </div>
                </div>
              </div>
            </div>
          </body>
        </html>
        """

        target_url = "https://www.facebook.com/atletismoRFEA/posts/12345"

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.set_content(html_fixture)

            # Execute JS scoping logic targeting the specific post
            data = page.evaluate(fd.SCOPED_EXTRACTION_JS, target_url)
            browser.close()

        self.assertTrue(data["card_found"])
        self.assertIn("Target Post Headline", data["text"])
        self.assertIn("Las 12 campanadas del atletismo", data["text"])
        # Ensure comments are NOT in caption
        self.assertNotIn("Gran carrera", data["text"])
        self.assertNotIn("clasificaciones oficiales", data["text"])
        # Ensure adjacent post text is NOT in caption
        self.assertNotIn("Adjacent post caption", data["text"])
        self.assertNotIn("Breaking news on football", data["text"])

    def test_multilingual_numeric_parsing(self):
        """Verifies Spanish and French number parsing."""
        val, approx = fd.parse_numeric_count_with_meta("1,9 mil")
        self.assertEqual(val, 1900)
        self.assertTrue(approx)

        val, approx = fd.parse_numeric_count_with_meta("2 millones")
        self.assertEqual(val, 2000000)
        self.assertTrue(approx)

    def test_vietnamese_timestamp_parsing(self):
        """Verifies Vietnamese timestamp format standardization."""
        iso, prec = fd.parse_facebook_timestamp("30 tháng 12, 2022 lúc 18:30")
        self.assertEqual(prec, "exact")
        self.assertTrue(iso.startswith("2022-12-30T18:30:00"))

    def test_mentions_email_exclusion(self):
        """Verifies that email addresses are not falsely extracted as account mentions."""
        text = "Liên hệ với chúng tôi qua contact@valenciamarathon.com hoặc tag @valenciamarathon."
        mentions = fd.extract_mentions(text)
        self.assertEqual(mentions, ["@valenciamarathon"])
        self.assertNotIn("@valenciamarathon.com", mentions)

    def test_mentions_combined_text_and_dom_tags(self):
        """Verifies merging of plain text @mentions and DOM anchor tags, stripping trailing punctuation."""
        text = "Check out @valenciamarathon! and @running_spain."
        dom_tags = ["@maratonvalencia", "energiaedp", "@valenciamarathon"]
        combined = fd.extract_mentions(text, additional_mentions=dom_tags)
        self.assertEqual(combined, ["@valenciamarathon", "@running_spain", "@maratonvalencia", "@energiaedp"])

    def test_dom_fixture_tagged_link_mention_extraction(self):
        """Verifies that SCOPED_EXTRACTION_JS extracts tagged profile/page anchors from the post caption."""
        from playwright.sync_api import sync_playwright

        html_fixture = """
        <html>
          <body>
            <div id="target-post" role="article" class="main-card">
              <div class="header">
                <a href="/energiaedp/posts/12345" role="link">energiaedp</a>
              </div>
              <div data-ad-preview="message" class="caption-container">
                <span>Os esperamos en el stand del </span>
                <a href="https://www.facebook.com/maratonvalencia?__cft__[0]=AZ123&__tn__=-]K-R" role="link">Maratón Valencia</a>
                <span> con la mejor energía. </span>
                <a href="https://www.facebook.com/hashtag/edprunfortomorrow">#EDPRunForTomorrow</a>
              </div>
              <div role="toolbar">
                <button aria-label="Like"></button>
              </div>
            </div>
          </body>
        </html>
        """
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.set_content(html_fixture)
            data = page.evaluate(fd.SCOPED_EXTRACTION_JS, "https://www.facebook.com/energiaedp/posts/12345")
            browser.close()

        self.assertEqual(data.get("dom_mentions"), ["@maratonvalencia"])


    def test_numeric_object_url_author_rejection(self):
        """Verifies that numeric object URLs are NOT treated as author handles."""
        # Top-level numeric slug must return None, not the object ID
        author = fd.extract_author_id("https://www.facebook.com/3211971012344948")
        self.assertIsNone(author)

        # Standard post URL with author handle
        author_valid = fd.extract_author_id("https://www.facebook.com/atletismoRFEA/posts/12345")
        self.assertEqual(author_valid, "atletismoRFEA")

        # Profile.php with numeric ID query
        author_profile = fd.extract_author_id("https://www.facebook.com/profile.php?id=100054321")
        self.assertEqual(author_profile, "100054321")

    def test_collected_at_tz_compliance(self):
        """Verifies collected_at defaults to Vietnam timezone (+07:00) and converts +00:00 to +07:00."""
        rec = fd.build_dataset_record("https://www.facebook.com/atletismoRFEA/posts/123")
        collected_at = rec["collected_at"]
        self.assertTrue(collected_at.endswith("+07:00"))

        rec_conv = fd.build_dataset_record(
            "https://www.facebook.com/atletismoRFEA/posts/123",
            collected_at="2026-10-16T18:25:29.712611+00:00"
        )
        self.assertEqual(rec_conv["collected_at"], "2026-10-17T01:25:29.712611+07:00")

    def test_video_media_type_omits_text(self):
        """Verifies that posts with media_type='video' omit caption text, hashtags, mentions, and sport."""
        rec = fd.build_dataset_record(
            "https://www.facebook.com/nba/posts/123",
            text="Junk or video player caption here #NBA @nba",
            media_type="video",
            sport_category="Basketball"
        )
        self.assertIsNone(rec["text"])
        self.assertIsNone(rec["language"])
        self.assertEqual(rec["hashtags"], [])
        self.assertEqual(rec["mentions"], [])
        self.assertIsNone(rec["sport_category"])
        self.assertEqual(rec["media_type"], "video")

    def test_discover_post_url_parsing(self):
        """Verifies discover tool parsing of both /posts/ and /share/p/ URLs."""
        import cloak_facebook_discover as disc
        p1 = disc.parse_post_url("https://www.facebook.com/author1/posts/123456789/")
        self.assertIsNotNone(p1)
        self.assertEqual(p1["post_id"], "123456789")

        p2 = disc.parse_post_url("https://www.facebook.com/share/p/1Lj5fMfmVg/")
        self.assertIsNotNone(p2)
        self.assertEqual(p2["post_id"], "1Lj5fMfmVg")


if __name__ == "__main__":
    unittest.main()
