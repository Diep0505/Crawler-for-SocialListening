"""Automated reader for configured Facebook posts using persistent CloakBrowser profile.
Usage: python cloak_facebook_posts_auto.py
Profile: profiles/facebook (seed 12345).
Runs headless or headed without requiring manual terminal confirmation.
Extracts text and interaction metrics conforming to the 17-column specification.
"""

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from cloakbrowser import launch_persistent_context
from playwright.sync_api import Error
import facebook_dataset as fd

BASE_DIR = Path(__file__).resolve().parent
PROFILE_DIR = BASE_DIR / "profiles" / "facebook"
FINGERPRINT_SEED = 12345
OUTPUT_DIR = BASE_DIR / "output"
VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
WAIT_SECONDS = 30
MORE_LABELS = re.compile(r"^(?:See more|Xem thêm)$", re.IGNORECASE)

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


def unique_visible_nodes(locator):
    return [locator.nth(i) for i in range(locator.count()) if locator.nth(i).is_visible()]


def access_issue(page):
    path = urlparse(page.url).path.lower()
    if path.startswith(("/login", "/checkpoint", "/recover")):
        return "Facebook login/checkpoint required. Run cloak_facebook_login.py first."
    if page.locator('input[name="email"]').is_visible() and page.locator('input[name="pass"]').is_visible():
        return "Facebook session expired or not logged in. Run cloak_facebook_login.py first."
    return None


def wait_for_anchor(page, anchors):
    """Waits for known content without relying on networkidle on Facebook."""
    deadline = time.monotonic() + WAIT_SECONDS
    while time.monotonic() < deadline:
        issue = access_issue(page)
        if issue:
            raise ValueError(issue)
        for anchor in anchors:
            if unique_visible_nodes(page.get_by_text(anchor, exact=False)):
                return
        time.sleep(0.4)
    raise ValueError("Configured anchor text was not found within timeout.")


def expand_known_post(page, anchors):
    """Attempts to click 'See more' button scoped near anchor content."""
    actions = []
    for _ in range(3):
        target = None
        for anchor in anchors:
            nodes = unique_visible_nodes(page.get_by_text(anchor, exact=False))
            if len(nodes) != 1:
                continue
            scope = nodes[0]
            for _level in range(8):
                buttons = unique_visible_nodes(scope.get_by_role("button", name=MORE_LABELS))
                if len(buttons) == 1:
                    target = buttons[0]
                    break
                if len(buttons) > 1:
                    actions.append({"status": "ambiguous_more_button"})
                    break
                parent = scope.locator("xpath=..")
                if parent.count() != 1:
                    break
                tag = parent.evaluate("el => el.tagName.toLowerCase()")
                role = parent.get_attribute("role")
                if tag in ("html", "body") or role in ("dialog", "feed", "main"):
                    break
                scope = parent
            if target is not None:
                break
        if target is None:
            break
        try:
            target.click(timeout=5000)
            actions.append({"status": "clicked_more"})
            time.sleep(0.4)
        except Error as exc:
            actions.append({"status": "more_click_failed", "error": str(exc)})
            break
    return actions


def read_candidates(page, anchors):
    texts = []
    details = []
    for anchor in anchors:
        matches = []
        for node in unique_visible_nodes(page.get_by_text(anchor, exact=False)):
            text = node.inner_text(timeout=5000).strip()
            if text and normalized(text) not in [normalized(x) for x in matches]:
                matches.append(text)
        detail = {"anchor": anchor, "visible_matches": len(matches)}
        if len(matches) == 1:
            texts.append(matches[0])
            detail["status"] = "matched"
        elif not matches:
            detail["status"] = "not_found"
        else:
            detail["status"] = "ambiguous"
            detail["candidates"] = matches
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
        if normalized(text) not in [normalized(x) for x in deduplicated]:
            deduplicated.append(text)
    return "\n\n".join(deduplicated), details


def wait_for_stable_candidates(page, anchors):
    """Waits for candidate extraction locator results to stabilize."""
    deadline = time.monotonic() + WAIT_SECONDS
    previous = None
    stable_since = None
    result = ("", [])
    while time.monotonic() < deadline:
        result = read_candidates(page, anchors)
        snapshot = json.dumps(result, ensure_ascii=False, sort_keys=True)
        now = time.monotonic()
        if snapshot != previous:
            previous = snapshot
            stable_since = now
        all_matched = all(d["status"] == "matched" for d in result[1])
        required_seconds = 1.2 if all_matched else 4.0
        if result[0] and stable_since is not None and now - stable_since >= required_seconds:
            return result
        time.sleep(0.4)
    return result


def has_more_label(text):
    return re.search(r"(?:See more|Xem thêm)\s*$", text, re.IGNORECASE) is not None


def save_records(output_file, records):
    temporary = output_file.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(output_file)


def save_review_screenshot(page, output_file, sample_id, record):
    try:
        image_file = output_file.with_name(f"{output_file.stem}_{sample_id}.png")
        page.screenshot(path=str(image_file), timeout=5000)
        record["review_screenshot"] = image_file.name
    except Error as exc:
        record["screenshot_error"] = str(exc)


def main():
    if not PROFILE_DIR.is_dir():
        print(f"Profile directory not found: {PROFILE_DIR}")
        print("Please run cloak_facebook_login.py first to initialize profile.")
        return 1
    OUTPUT_DIR.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    dataset_file = OUTPUT_DIR / f"facebook_dataset_auto_{stamp}.json"
    debug_file = OUTPUT_DIR / f"facebook_posts_auto_debug_{stamp}.json"
    dataset_records = []
    debug_records = []
    context = None
    try:
        print("Launching CloakBrowser...", flush=True)
        context = launch_persistent_context(
            str(PROFILE_DIR),
            headless=False,
            args=[f"--fingerprint={FINGERPRINT_SEED}"],
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()
        for post in POSTS:
            print(f"Reading {post['sample_id']}: {post['url']}", flush=True)
            debug_record = {
                "sample_id": post["sample_id"],
                "requested_url": post["url"],
                "post_url": None,
                "candidate_text": None,
                "collection_status": "failed",
                "text_status": "unverified",
                "locator_details": [],
                "expansion_actions": [],
                "review_reasons": [],
                "error": None,
            }
            clean_record = None
            stop_for_login = False
            try:
                page.goto(post["url"], wait_until="domcontentloaded", timeout=60000)
                wait_for_anchor(page, post["anchors"])
                debug_record["expansion_actions"] = expand_known_post(page, post["anchors"])
                candidate, details = wait_for_stable_candidates(page, post["anchors"])
                debug_record["candidate_text"] = candidate or None
                debug_record["locator_details"] = details

                # Scoped interaction and media metrics extraction
                metrics = fd.extract_page_metrics(page, anchors=post.get("anchors"))

                extracted_text = None
                if candidate:
                    reasons = debug_record["review_reasons"]
                    if any(d["status"] != "matched" for d in details):
                        reasons.append("Some anchors were not found or returned multiple matches.")
                    if has_more_label(candidate):
                        reasons.append("Post content may not be fully expanded.")
                    if any(a["status"] != "clicked_more" for a in debug_record["expansion_actions"]):
                        reasons.append("Could not reliably identify or click 'See more' button.")
                    if reasons:
                        debug_record["text_status"] = "needs_review"
                        debug_record["collection_status"] = "needs_review"
                    else:
                        extracted_text = candidate
                        debug_record["text_status"] = "extracted_unverified"
                        debug_record["collection_status"] = "extracted_text_only"
                    print(f"Extracted {len(candidate)} characters; status: {debug_record['text_status']}", flush=True)
                else:
                    debug_record["error"] = "No unique content matched for anchors."

                # Build clean 17-column record
                clean_record = fd.build_dataset_record(
                    post_url=page.url,
                    text=extracted_text,
                    published_at=metrics.get("published_at"),
                    author_id=fd.extract_author_id(page.url),
                    post_id=fd.extract_post_id(page.url),
                    sport_category=post.get("sport_category"),
                    like_count=metrics.get("like_count_raw"),
                    comment_count=metrics.get("comment_count_raw"),
                    share_count=metrics.get("share_count_raw"),
                    view_count=metrics.get("view_count_raw"),
                    media_type=metrics.get("media_type", "text"),
                    author_followers=metrics.get("author_followers_raw"),
                )

            except (Error, ValueError) as exc:
                debug_record["error"] = str(exc)
                stop_for_login = access_issue(page) is not None
                print(f"Error: {exc}", flush=True)
                clean_record = fd.build_dataset_record(
                    post_url=page.url if "page" in locals() else post["url"],
                    text=None,
                    sport_category=post.get("sport_category"),
                )

            debug_record["post_url"] = page.url
            if debug_record["collection_status"] in ("failed", "needs_review"):
                save_review_screenshot(page, debug_file, post["sample_id"], debug_record)

            debug_records.append(debug_record)
            dataset_records.append(clean_record)

            save_records(dataset_file, dataset_records)
            save_records(debug_file, debug_records)

            if stop_for_login:
                print("Stopping: login/verification required in login profile.")
                break
        print(f"Saved standard dataset (17 columns): {dataset_file}")
        print(f"Saved technical debug log: {debug_file}")
        return 0 if dataset_records and all(r["text"] is not None for r in dataset_records) else 1
    except (Error, OSError, ValueError) as exc:
        print(f"Run failed: {exc}")
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted by user. Completed posts were preserved.")
        return 130
    finally:
        if context is not None:
            context.close()


if __name__ == "__main__":
    raise SystemExit(main())
