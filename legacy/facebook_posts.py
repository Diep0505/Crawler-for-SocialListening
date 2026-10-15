"""Standard Playwright test script for reading Facebook posts.
Usage: python facebook_posts.py
Auth file: playwright/.auth/facebook.json
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from playwright.sync_api import Error, sync_playwright

BASE_DIR = Path(__file__).resolve().parent
AUTH_FILE = BASE_DIR / "playwright" / ".auth" / "facebook.json"
OUTPUT_DIR = BASE_DIR / "output"
VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

POSTS = [
    {
        "sample_id": "post_1",
        "url": "https://www.facebook.com/share/p/19VvgBAgMc/",
        "anchors": [
            "Hitler Mussolini Simeon",
            "As candidate of the far-left Ahora Nacion Party, he won 34.5% of the votes.",
        ],
    },
    {
        "sample_id": "post_2",
        "url": "https://www.facebook.com/share/p/1Lj5fMfmVg/",
        "anchors": [
            "𝐌𝐨𝐮𝐫𝐢𝐧𝐡𝐨’𝐬 𝐦𝐞𝐬𝐬𝐚𝐠𝐞 𝐨𝐧 𝐕𝐢𝐧𝐢 𝐉𝐫. “What hurts me, so makes",
            "𝐌𝐨𝐮𝐫𝐢𝐧𝐡𝐨’𝐬 𝐦𝐞𝐬𝐬𝐚𝐠𝐞 𝐨𝐧 𝐕𝐢𝐧𝐢 𝐉𝐫.",
        ],
    },
]


def normalized(text):
    return " ".join(text.split())


def read_candidates(page, anchors):
    """Reads visible text matching anchors without relying on session-volatile IDs."""
    texts = []
    details = []

    for anchor in anchors:
        locator = page.get_by_text(anchor, exact=False)
        matches = []
        for index in range(locator.count()):
            node = locator.nth(index)
            if node.is_visible():
                text = node.inner_text(timeout=5000).strip()
                if text and text not in matches:
                    matches.append(text)

        detail = {"anchor": anchor, "visible_matches": len(matches)}
        if not matches:
            detail["status"] = "not_found"
            print(f"Anchor text not found: {anchor}")
        elif len(matches) == 1:
            texts.append(matches[0])
            detail["status"] = "matched"
        else:
            print(f"\nMultiple matches for anchor: {anchor}")
            for index, text in enumerate(matches, start=1):
                print(f"\n[{index}]\n{text}")
            choice = input("Enter correct match index; press Enter to skip: ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(matches):
                texts.append(matches[int(choice) - 1])
                detail["status"] = "selected_manually"
            else:
                detail["status"] = "ambiguous"
        details.append(detail)

    unique_texts = list(dict.fromkeys(texts))
    selected = [
        text for text in unique_texts
        if not any(
            text != other
            and normalized(text) != normalized(other)
            and normalized(text) in normalized(other)
            for other in unique_texts
        )
    ]
    deduplicated = []
    for text in selected:
        if normalized(text) not in [normalized(item) for item in deduplicated]:
            deduplicated.append(text)
    return "\n\n".join(deduplicated), details


def main():
    if not AUTH_FILE.is_file():
        print("Auth file playwright/.auth/facebook.json not found.")
        print("Place facebook_posts.py in the playwright-demo directory.")
        return 1

    OUTPUT_DIR.mkdir(exist_ok=True)
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    output_file = OUTPUT_DIR / f"facebook_posts_{run_stamp}.json"
    records = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        try:
            context = browser.new_context(
                storage_state=str(AUTH_FILE),
                viewport={"width": 1280, "height": 900},
            )
            page = context.new_page()

            for post in POSTS:
                print(f"\n--- {post['sample_id']} ---")
                record = {
                    "sample_id": post["sample_id"],
                    "source": "facebook",
                    "requested_url": post["url"],
                    "post_url": None,
                    "text": None,
                    "candidate_text": None,
                    "published_at": None,
                    "collected_at": None,
                    "reaction_count": None,
                    "comment_count": None,
                    "share_count": None,
                    "collection_status": "failed",
                    "text_status": "unverified",
                    "missing_fields": [
                        "published_at", "reaction_count",
                        "comment_count", "share_count",
                    ],
                    "locator_details": [],
                    "error": None,
                }
                try:
                    page.goto(
                        post["url"],
                        wait_until="domcontentloaded",
                        timeout=60000,
                    )
                    print("If prompted, log in or verify manually in this tab.")
                    print("Expand 'See more' if needed; wait for content to load.")
                    print("Keep the target post active in this browser tab.")
                    input("Return to terminal and press Enter to read post...")

                    candidate, details = read_candidates(page, post["anchors"])
                    record["post_url"] = page.url
                    record["collected_at"] = datetime.now(VIETNAM_TZ).isoformat()
                    record["candidate_text"] = candidate or None
                    record["locator_details"] = details

                    if candidate:
                        print(f"\nEXTRACTED TEXT:\n{candidate}\n")
                        answer = input(
                            "Verify on Facebook: Is this the FULL post text "
                            "without menus/comments? Enter y if confirmed: "
                        ).strip().lower()
                        if answer == "y":
                            record["text"] = candidate
                            record["text_status"] = "user_confirmed"
                            record["collection_status"] = "ok_text_only"
                        else:
                            record["text_status"] = "needs_review"
                            record["collection_status"] = "needs_review"
                    else:
                        record["error"] = "No unique content matched for anchors."

                except Error as exc:
                    record["collected_at"] = datetime.now(VIETNAM_TZ).isoformat()
                    record["error"] = str(exc)
                    print(f"Error reading post: {exc}")

                records.append(record)
                output_file.write_text(
                    json.dumps(records, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                print(f"Saved: {output_file}")

            context.close()
            print("\nCompleted test run.")
            input("Press Enter to close browser...")
            return 0

        except (Error, OSError, ValueError) as exc:
            print(f"Run failed: {exc}")
            return 1
        finally:
            browser.close()


if __name__ == "__main__":
    raise SystemExit(main())
