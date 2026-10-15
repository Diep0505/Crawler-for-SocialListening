"""Interactive test runner for extracting Facebook posts using CloakBrowser.
Usage: python cloak_facebook_posts.py
Profile: profiles/facebook (logged-in persistent profile).
Extracts text and metrics according to the 17-column dataset specification.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from playwright.sync_api import Error
from cloakbrowser import launch_persistent_context
import facebook_dataset as fd

BASE_DIR = Path(__file__).resolve().parent
PROFILE_DIR = BASE_DIR / "profiles" / "facebook"
FINGERPRINT_SEED = 12345
OUTPUT_DIR = BASE_DIR / "output"
VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

POSTS = [
    {
        "sample_id": "post_1",
        "sport_category": None,  # Non-sports topic (Peru mayor election) -> None
        "url": "https://www.facebook.com/share/p/19VvgBAgMc/",
        "anchors": [
            "Hitler Mussolini Simeon",
            "As candidate of the far-left Ahora Nacion Party, he won 34.5% of the votes.",
        ],
    },
    {
        "sample_id": "post_2",
        "sport_category": "Football",
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
    """Reads visible text elements matching anchors without relying on session-volatile IDs."""
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

    # Deduplicate candidate texts and remove nested substrings
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
    if not PROFILE_DIR.is_dir():
        print(f"Profile directory not found: {PROFILE_DIR}")
        print("Please run cloak_facebook_login.py first to initialize the profile.")
        return 1

    OUTPUT_DIR.mkdir(exist_ok=True)
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    dataset_file = OUTPUT_DIR / f"facebook_dataset_{run_stamp}.json"
    debug_file = OUTPUT_DIR / f"facebook_posts_debug_{run_stamp}.json"
    dataset_records = []
    debug_records = []
    author_followers_cache = {}

    try:
        print(f"Launching CloakBrowser with managed profile: {PROFILE_DIR}", flush=True)
        with fd.PersistentBrowserSession(PROFILE_DIR, fingerprint=FINGERPRINT_SEED, headless=False) as context:
            page = context.new_page()

            for post in POSTS:
                print(f"\n--- {post['sample_id']} ---")
                debug_record = {
                    "sample_id": post["sample_id"],
                    "requested_url": post["url"],
                    "post_url": None,
                    "candidate_text": None,
                    "collection_status": "failed",
                    "text_status": "unverified",
                    "locator_details": [],
                    "error": None,
                }
                clean_record = None
                try:
                    page.goto(
                        post["url"],
                        wait_until="domcontentloaded",
                        timeout=60000,
                    )
                    print("If prompted, log in or solve verification manually in this tab.")
                    print("Expand 'See more' if needed; wait for full content to load.")
                    print("Keep the target post active in this browser tab.")
                    input("Return to terminal and press Enter to read post...")

                    candidate, details = read_candidates(page, post["anchors"])
                    debug_record["post_url"] = page.url
                    debug_record["candidate_text"] = candidate or None
                    debug_record["locator_details"] = details

                    # Extract interaction metrics and media type scoped to the post
                    metrics = fd.extract_page_metrics(page, anchors=post.get("anchors"))

                    confirmed_text = None
                    if candidate:
                        print(f"\nEXTRACTED TEXT:\n{candidate}\n")
                        answer = input(
                            "Verify on Facebook: Is this the FULL post text "
                            "without menus/comments? Enter y if confirmed: "
                        ).strip().lower()
                        if answer == "y":
                            confirmed_text = candidate
                            debug_record["text_status"] = "user_confirmed"
                            debug_record["collection_status"] = "ok_text_only"
                        else:
                            debug_record["text_status"] = "needs_review"
                            debug_record["collection_status"] = "needs_review"
                    else:
                        debug_record["error"] = "No unique content matched for given anchors."

                    # Resolve author followers via profile
                    author_id = fd.extract_author_id(page.url)
                    author_followers = fd.fetch_author_followers(context, author_id, cache=author_followers_cache)

                    # Build clean 17-column record
                    clean_record = fd.build_dataset_record(
                        post_url=page.url,
                        text=confirmed_text,
                        published_at=metrics.get("published_at"),
                        author_id=author_id,
                        post_id=fd.extract_post_id(page.url),
                        sport_category=post.get("sport_category"),
                        like_count=metrics.get("like_count_raw"),
                        comment_count=metrics.get("comment_count_raw"),
                        share_count=metrics.get("share_count_raw"),
                        view_count=metrics.get("view_count_raw"),
                        media_type=metrics.get("media_type", "text"),
                        author_followers=author_followers,
                    )

                except Error as exc:
                    debug_record["error"] = str(exc)
                    print(f"Error reading post: {exc}")
                    clean_record = fd.build_dataset_record(
                        post_url=page.url if "page" in locals() else post["url"],
                        text=None,
                        sport_category=post.get("sport_category"),
                    )

                debug_records.append(debug_record)
                dataset_records.append(clean_record)

                # Persist incrementally after each post
                dataset_file.write_text(
                    json.dumps(dataset_records, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                debug_file.write_text(
                    json.dumps(debug_records, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                print(f"Saved standard dataset (17 columns): {dataset_file}")
                print(f"Saved technical debug log: {debug_file}")

            print("\nFinished run.")
            print(f"Dataset has exactly {len(fd.REQUIRED_COLUMNS)} columns matching specification.")
            return 0

    except (Error, OSError, ValueError) as exc:
        print(f"Run failed: {exc}")
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted by user. Completed posts were preserved in output.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
