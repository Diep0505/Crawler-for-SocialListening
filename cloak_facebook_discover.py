"""Step 1: Automatically discover Facebook post permalinks from Pages using a persistent profile.

Placed alongside cloak_facebook_login.py, uses profiles/facebook and seed 12345.
Example:
    python -u cloak_facebook_discover.py \
        --source https://www.facebook.com/fabrizioromanoherewego/ --limit 30

Can also use --sources sources.txt (one Page URL per line).
Output: output/facebook_discovery/post_queue.jsonl and discovery_report.json.

This is a URL discovery tool for local testing, not a complete archival collector.
Accepts permalinks matching /posts/, story.php, permalink.php.
Does not treat photo/video/reel URLs as primary post IDs.
candidate_preview is truncated at 2000 chars and may contain UI/comments.
Do not use candidate_preview as the final dataset text.
Does not persist cookies, passwords, full page HTML, or screenshots.
"""

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

BASE_DIR = Path(__file__).resolve().parent
FINGERPRINT_SEED = 12345
FACEBOOK_HOSTS = {"facebook.com", "www.facebook.com", "m.facebook.com", "mbasic.facebook.com", "web.facebook.com"}
TOKEN = re.compile(r"(?:[0-9]+|pfbid[A-Za-z0-9]+)\Z")
POST_PATH = re.compile(r"/([^/]+)/posts/([^/]+)/?\Z")
RESERVED = {"groups", "watch", "reel", "reels", "photo", "photos", "share", "login", "checkpoint", "search", "home.php", "feed", "events", "hashtag", "marketplace", "gaming", "story.php", "permalink.php"}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def facebook_url(value):
    """Only accepts absolute Facebook URLs; rejects pseudo hosts and user credentials."""
    try:
        parsed = urlparse(value)
        if parsed.scheme not in {"http", "https"} or parsed.hostname not in FACEBOOK_HOSTS:
            return None
        if parsed.username or parsed.password or parsed.port not in (None, 80, 443):
            return None
        return parsed
    except (ValueError, TypeError):
        return None


def parse_post_url(value):
    parsed = facebook_url(value)
    if parsed is None:
        return None
    match = POST_PATH.fullmatch(parsed.path)
    if match:
        owner, token = match.groups()
        if owner.lower() in RESERVED or not TOKEN.fullmatch(token):
            return None
        return {
            "post_url": f"https://www.facebook.com/{owner}/posts/{token}/",
            "post_id": token,
            "discovery_key": f"facebook:post:{token}",
            "owner_token": owner,
        }
    if parsed.path.rstrip("/") in {"/story.php", "/permalink.php"}:
        query = parse_qs(parsed.query)
        tokens = query.get("story_fbid", [])
        owners = query.get("id", [])
        if len(tokens) != 1 or len(owners) != 1:
            return None
        token, owner = tokens[0], owners[0]
        if not TOKEN.fullmatch(token) or not owner.isdigit():
            return None
        return {
            "post_url": "https://www.facebook.com/permalink.php?" + urlencode({"story_fbid": token, "id": owner}),
            "post_id": token,
            "discovery_key": f"facebook:post:{token}",
            "owner_token": owner,
        }
    return None


def parse_source_url(value):
    """Accepts Page URLs like /pagename or profile.php?id=...; rejects post URLs."""
    parsed = facebook_url(value.strip())
    if parsed is None:
        raise ValueError(f"Invalid source URL: {value}")
    path = parsed.path.strip("/")
    if path == "profile.php":
        ids = parse_qs(parsed.query).get("id", [])
        if len(ids) == 1 and ids[0].isdigit():
            return "https://www.facebook.com/profile.php?" + urlencode({"id": ids[0]}), ids[0]
    if path and "/" not in path and path.lower() not in RESERVED and "." not in path:
        return f"https://www.facebook.com/{path}/", path
    if path and "/" not in path and path.lower() not in RESERVED and not path.lower().endswith(".php"):
        return f"https://www.facebook.com/{path}/", path
    raise ValueError(f"Expected Page URL, not post/group/homepage: {value}")


def append_records(path, records):
    if not records:
        return
    with path.open("a", encoding="utf-8") as stream:
        for record in records:
            stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def existing_keys(path):
    result = set()
    if path.exists():
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            try:
                record = json.loads(line)
                result.add(record["discovery_key"])
            except (json.JSONDecodeError, KeyError, TypeError) as exc:
                raise ValueError(f"Corrupt queue at line {number}; keeping file for inspection.") from exc
    return result


def save_report(path, report):
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


SCAN_JS = r"""() => {
    const rendered = el => el.getClientRects().length > 0
        && getComputedStyle(el).visibility !== 'hidden';
    const main = document.querySelector('[role="main"]') || document.body;
    const anchors = [...main.querySelectorAll('a[href]')].filter(rendered).slice(0, 5000);
    const links = anchors.map(a => {
        const card = a.closest('[role="article"]');
        return {
            href: a.href,
            label: (a.innerText || a.getAttribute('aria-label') || '').slice(0, 250),
            in_article: Boolean(card),
            candidate_preview: card ? (card.innerText || '').slice(0, 2000) : null
        };
    });
    return {
        article_count: [...main.querySelectorAll('[role="article"]')].filter(rendered).length,
        link_count: links.length,
        links
    };
}"""


def access_issue(page):
    parsed = urlparse(page.url)
    if parsed.path.lower().startswith(("/login", "/checkpoint", "/recover")):
        return "Facebook requires login/verification. Halting to resolve with login script."
    email = page.locator('input[name="email"]').first
    password = page.locator('input[name="pass"]').first
    if email.is_visible() and password.is_visible():
        return "Login screen detected. Run cloak_facebook_login.py first."
    return None


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("Value must be greater than 0.")
    return number


def get_arguments():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", action="append", default=[], help="Page URL; can be repeated.")
    parser.add_argument("--sources", type=Path, help="UTF-8 file, one Page URL per line, '#' comments ignored.")
    parser.add_argument("--limit", type=positive_int, default=30, help="Max new unique URLs for this run (default: 30).")
    parser.add_argument("--max-scrolls", type=positive_int, default=60, help="Max scroll rounds per source.")
    parser.add_argument("--idle-rounds", type=positive_int, default=10, help="Stop source after this many idle rounds.")
    parser.add_argument("--profile", type=Path, default=BASE_DIR / "profiles" / "facebook")
    parser.add_argument("--output", type=Path, default=BASE_DIR / "output" / "facebook_discovery")
    args = parser.parse_args()
    values = list(args.source)
    if args.sources:
        try:
            values.extend(line.strip() for line in args.sources.read_text(encoding="utf-8-sig").splitlines()
                          if line.strip() and not line.lstrip().startswith("#"))
        except OSError as exc:
            parser.error(str(exc))
    if not values:
        parser.error("Specify --source PAGE_URL or --sources sources.txt.")
    try:
        args.seeds = list(dict.fromkeys(parse_source_url(value) for value in values))
    except ValueError as exc:
        parser.error(str(exc))
    return args


def main():
    args = get_arguments()
    if not args.profile.is_dir():
        print(f"Profile directory not found: {args.profile}")
        print("Run cloak_facebook_login.py first to configure profile.")
        return 1
    try:
        from cloakbrowser import launch_persistent_context
        from playwright.sync_api import Error
    except ImportError:
        print("Missing required libraries. Run in configured virtual environment.")
        return 1
    args.output.mkdir(parents=True, exist_ok=True)
    queue_file = args.output / "post_queue.jsonl"
    report_file = args.output / "discovery_report.json"
    try:
        queue_file.touch(exist_ok=True)
        seen = existing_keys(queue_file)
    except (OSError, ValueError) as exc:
        print(str(exc))
        return 1
    report = {
        "started_at": utc_now(), "finished_at": None, "status": "running",
        "requested_new_urls": args.limit, "previous_unique_urls": len(seen),
        "new_unique_urls": 0, "total_unique_urls": len(seen), "sources": [],
        "scope": "Page permalink discovery only; not a verified dataset or complete Page archive",
        "metric_definitions": {"like_count": "total post reactions", "author_followers": "follower count only"},
    }
    context = None
    try:
        context = launch_persistent_context(
            str(args.profile), headless=False, args=[f"--fingerprint={FINGERPRINT_SEED}"],
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()
        stop_for_access = False
        for source_url, owner in args.seeds:
            if report["new_unique_urls"] >= args.limit:
                break
            detail = {"source_url": source_url, "final_url": None, "new_urls": 0,
                      "rounds": 0, "status": "running", "error": None,
                      "article_count_last": 0, "link_count_last": 0,
                      "recognized_permalink_count_last": 0, "other_owner_links_skipped": 0,
                      "candidate_link_examples": []}
            report["sources"].append(detail)
            print(f"Source: {source_url}", flush=True)
            try:
                page.goto(source_url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(4)
                owners = {owner.lower()}
                try:
                    _, redirected_owner = parse_source_url(page.url)
                    owners.add(redirected_owner.lower())
                except ValueError:
                    pass
                idle = 0
                source_seen = set()
                for number in range(1, args.max_scrolls + 1):
                    detail["rounds"] = number
                    issue = access_issue(page)
                    if issue:
                        detail["status"], detail["error"] = "access_required", issue
                        stop_for_access = True
                        break
                    snapshot = page.evaluate(SCAN_JS)
                    detail["article_count_last"] = snapshot["article_count"]
                    detail["link_count_last"] = snapshot["link_count"]
                    detail["recognized_permalink_count_last"] = 0
                    batch = []
                    discovered_this_round = 0
                    for link in snapshot["links"]:
                        parsed_post = parse_post_url(link["href"])
                        if not parsed_post:
                            continue
                        detail["recognized_permalink_count_last"] += 1
                        post_owner = parsed_post["owner_token"].lower()
                        if post_owner in owners:
                            relation = "url_owner_matches_seed"
                        elif post_owner.isdigit() and not any(x.isdigit() for x in owners):
                            relation = "numeric_owner_unverified"
                        else:
                            detail["other_owner_links_skipped"] += 1
                            continue
                        key = parsed_post["discovery_key"]
                        if key not in source_seen:
                            source_seen.add(key)
                            discovered_this_round += 1
                        if key in seen:
                            continue
                        if report["new_unique_urls"] + len(batch) >= args.limit:
                            break
                        batch.append({
                            **parsed_post, "platform": "facebook", "source_url": source_url,
                            "source_relation": relation, "discovered_at": utc_now(),
                            "discovery_status": "url_found_unverified", "link_label": link["label"],
                            "in_article": link["in_article"],
                            "candidate_preview": link["candidate_preview"],
                            "preview_status": "mixed_ui_text_not_dataset_text",
                        })
                        seen.add(key)
                        if len(detail["candidate_link_examples"]) < 5:
                            detail["candidate_link_examples"].append(parsed_post["post_url"])
                    append_records(queue_file, batch)
                    added = len(batch)
                    report["new_unique_urls"] += added
                    report["total_unique_urls"] = len(seen)
                    detail["new_urls"] += added
                    detail["final_url"] = page.url
                    print(f"  Round {number}: +{added} URLs; new {report['new_unique_urls']}/{args.limit}; "
                          f"article={snapshot['article_count']}", flush=True)
                    save_report(report_file, report)
                    if report["new_unique_urls"] >= args.limit:
                        detail["status"] = "run_limit_reached"
                        break
                    idle = idle + 1 if discovered_this_round == 0 else 0
                    if idle >= args.idle_rounds:
                        detail["status"] = "no_new_links_in_loaded_dom"
                        break
                    page.mouse.wheel(0, 750)
                    time.sleep(3)
                else:
                    detail["status"] = "max_scrolls_reached"
            except Error as exc:
                detail["status"], detail["error"] = "browser_error", str(exc)[:1500]
            finally:
                detail["final_url"] = page.url
                save_report(report_file, report)
            if stop_for_access:
                print(detail["error"], flush=True)
                break
        report["status"] = ("access_required" if stop_for_access else
                            "limit_reached" if report["new_unique_urls"] >= args.limit else "partial")
    except KeyboardInterrupt:
        report["status"] = "interrupted"
        print("Interrupted. Completed rounds were saved.", flush=True)
    except (Error, OSError, ValueError) as exc:
        report["status"], report["error"] = "failed", str(exc)[:1500]
        print(f"Error: {exc}", flush=True)
    finally:
        report["finished_at"] = utc_now()
        save_report(report_file, report)
        if context is not None:
            try:
                context.close()
            except Error:
                pass
        print(f"New URLs: {report['new_unique_urls']}; Total discovered: {report['total_unique_urls']}")
        print(f"Queue: {queue_file}\nReport: {report_file}")
    return 0 if report["status"] == "limit_reached" else 2


if __name__ == "__main__":
    raise SystemExit(main())
