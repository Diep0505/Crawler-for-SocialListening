"""Captures raw text and screenshot from a Facebook page using saved auth session.
Usage: python facebook_capture.py
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from playwright.sync_api import Error, sync_playwright

BASE_DIR = Path(__file__).resolve().parent
AUTH_FILE = BASE_DIR / "playwright" / ".auth" / "facebook.json"
OUTPUT_DIR = BASE_DIR / "output"


def main():
    if not AUTH_FILE.is_file():
        print("Auth file playwright/.auth/facebook.json not found.")
        print("Place this Python script in the playwright-demo directory.")
        return 1

    OUTPUT_DIR.mkdir(exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        try:
            context = browser.new_context(
                storage_state=str(AUTH_FILE),
                viewport={"width": 1280, "height": 900},
            )
            page = context.new_page()
            page.goto(
                "https://www.facebook.com/",
                wait_until="domcontentloaded",
                timeout=60000,
            )

            print("\nIn the opened browser tab:")
            print("1. If session expired, log in and verify manually.")
            print("2. Open a public post in THIS SAME TAB.")
            print("3. Click 'See more' to expand full post content; wait until loaded.")
            input("\nOnce the post is displayed, return to Terminal and press Enter to save...")

            if input("Is the target public post displayed? Enter y to save: ").strip().lower() != "y":
                print("Cancelled; page data was not saved.")
                return 0

            raw_text = page.locator("body").inner_text(timeout=30000)
            captured_at = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))
            stamp = captured_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
            json_path = OUTPUT_DIR / f"facebook_{stamp}.json"
            image_path = OUTPUT_DIR / f"facebook_{stamp}.png"

            data = {
                "source": "facebook",
                "url": page.url,
                "title": page.title(),
                "collected_at": captured_at.isoformat(),
                "capture_type": "raw_page_text",
                "raw_page_text": raw_text,
                "note": (
                    "Raw page text: may contain menus, comments, and other posts. "
                    "Unstructured single-post extraction."
                ),
            }

            page.screenshot(path=str(image_path))
            json_path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            print(f"Saved JSON: {json_path}")
            print(f"Saved screenshot: {image_path}")
            print(f"Characters extracted: {len(raw_text)}")
            input("Press Enter to close browser...")
            context.close()
            return 0

        except (Error, OSError, ValueError) as exc:
            print(f"Run failed: {exc}")
            return 1
        finally:
            browser.close()


if __name__ == "__main__":
    raise SystemExit(main())
