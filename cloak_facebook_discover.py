"""Step 1: Automatically discover Facebook post permalinks from Pages using a persistent profile.

Placed alongside cloak_facebook_login.py, uses profiles/facebook and seed 12345.
Example:
    python -u cloak_facebook_discover.py \
        --source https://www.facebook.com/fabrizioromanoherewego/ --limit 30

Can also use --sources sources.txt (one Page URL per line).
Output: output/facebook_discovery/post_queue.jsonl and discovery_report.json.

When scrolling stalls, waits and reloads up to --max-refreshes times per source
(default: 2). Use --max-refreshes 0 to disable recovery. Saved URLs are retained
and duplicates skipped after reload. Reloading may return to the top; it does
not guarantee access to older posts or remove Facebook-side restrictions.
--limit and --max-scrolls remain hard caps, including after recovery.

This is a URL discovery tool for local testing, not a complete archival collector.
Accepts permalinks matching /posts/, story.php, permalink.php.
Does not treat photo/video/reel URLs as primary post IDs.
candidate_preview is truncated at 2000 chars and may contain UI/comments.
Do not use candidate_preview as the final dataset text.
Does not persist cookies, passwords, full page HTML, or screenshots.
"""

import argparse
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, quote_plus, urlencode, urlparse
import facebook_dataset as fd

logger = logging.getLogger("facebook_discovery")

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
            "is_share_alias": False,
        }
    share_match = re.fullmatch(r"/share/p/([^/]+)/?\Z", parsed.path)
    if share_match:
        token = share_match.group(1)
        return {
            "post_url": f"https://www.facebook.com/share/p/{token}/",
            "post_id": token,
            "discovery_key": f"facebook:post:{token}",
            "owner_token": "share",
            "is_share_alias": True,
        }
    if parsed.path.rstrip("/") in {"/story.php", "/permalink.php"}:
        query = parse_qs(parsed.query)
        tokens = query.get("story_fbid", []) or query.get("fbid", [])
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
            "is_share_alias": False,
        }
    return None


def parse_source_url(value):
    """Accepts Page URLs like /pagename or profile.php?id=..., or Home/News Feed (/ or /feed)."""
    parsed = facebook_url(value.strip())
    if parsed is None:
        raise ValueError(f"Invalid source URL: {value}")
    path = parsed.path.strip("/")
    if not path or path.lower() in ("feed", "home.php"):
        return f"{parsed.scheme}://{parsed.netloc}/", "news_feed"
    if path == "profile.php":
        ids = parse_qs(parsed.query).get("id", [])
        if len(ids) == 1 and ids[0].isdigit():
            return "https://www.facebook.com/profile.php?" + urlencode({"id": ids[0]}), ids[0]
    if path and "/" not in path and path.lower() not in RESERVED and "." not in path:
        return f"https://www.facebook.com/{path}/", path
    if path and "/" not in path and path.lower() not in RESERVED and not path.lower().endswith(".php"):
        return f"https://www.facebook.com/{path}/", path
    raise ValueError(f"Expected Page URL or Feed URL, not post/group: {value}")


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
    if page is None or not hasattr(page, "url"):
        return None
    page_url = str(page.url) if (page.url and isinstance(page.url, str)) else ""
    if page_url:
        parsed = urlparse(page_url)
        if parsed.path.lower().startswith(("/login", "/checkpoint", "/recover")):
            return "Facebook requires login/verification. Halting to resolve with login script."
    try:
        email = page.locator('input[name="email"]').first
        password = page.locator('input[name="pass"]').first
        if email.is_visible() and password.is_visible():
            return "Login screen detected. Run cloak_facebook_login.py first."
    except Exception:
        pass
    return None


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("Value must be greater than 0.")
    return number


def nonnegative_int(value):
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("Value must be 0 or greater.")
    return number


def refresh_after_idle(page, cooldown_seconds):
    """Reload a stalled feed in its current session; never reload a login wall.

    Returns an access issue, or None when reloaded. Navigation failures are
    handled by the existing source-level browser error handler.
    """
    issue = access_issue(page)
    if issue:
        return issue
    if cooldown_seconds:
        page.wait_for_timeout(cooldown_seconds * 1000)
        issue = access_issue(page)
        if issue:
            return issue
    page.reload(wait_until="domcontentloaded", timeout=60000)
    issue = access_issue(page)
    if issue:
        return issue
    # DOMContentLoaded does not mean the dynamically rendered feed is ready.
    # Later scan rounds continue waiting if posts have not appeared yet.
    page.wait_for_timeout(3000)
    return access_issue(page)


class PageDiscoverySession:
    """Manages an active, stateful Facebook Page discovery tab.
    Allows discovering URLs in batches while keeping the browser tab open and scrolled,
    so that resuming discovery does not re-navigate or reload the Page from the top.
    """
    def __init__(
        self,
        page_url,
        context,
        max_scrolls=100,
        idle_rounds=10,
        max_refreshes=0,
        refresh_wait=15,
        deadline_mono=None,
        existing_seen_keys=None,
    ):
        self.raw_page_url = page_url
        self.clean_url, self.owner = parse_source_url(page_url)
        self.is_news_feed = (self.owner == "news_feed")
        self.context = context
        self.max_scrolls = max_scrolls
        self.idle_rounds = idle_rounds
        self.max_refreshes = max_refreshes
        self.refresh_wait = refresh_wait
        self.deadline_mono = deadline_mono
        self.seen = set(existing_seen_keys or [])
        self.source_seen = set()
        self.page = None
        self.owners = {self.owner.lower()}
        self.scroll_round = 0
        self.idle = 0
        self.refreshes = 0
        self.is_exhausted = False
        self.stop_reason = None
        self.total_discovered = 0

    def start(self, timeout=60000):
        if self.page is None:
            self.page = self.context.new_page()
            self.page.goto(self.clean_url, wait_until="domcontentloaded", timeout=timeout)
            time.sleep(3.5)
            try:
                _, redirected_owner = parse_source_url(self.page.url)
                self.owners.add(redirected_owner.lower())
            except ValueError:
                pass

    def is_deadline_reached(self):
        return self.deadline_mono is not None and time.monotonic() >= self.deadline_mono

    def discover_batch(self, batch_size=20, on_url_discovered=None, deadline_mono=None):
        """Scans and scrolls the Page, discovering permalinks until batch_size new URLs are found,
        the scroll limit is reached, or the source stalls.
        The page tab remains OPEN and at its current position after returning!
        """
        if deadline_mono:
            self.deadline_mono = deadline_mono

        if self.page is None:
            self.start()

        batch_new = []

        while self.scroll_round < self.max_scrolls:
            if self.is_deadline_reached():
                self.stop_reason = "deadline_reached"
                break

            issue = access_issue(self.page)
            if issue:
                self.stop_reason = issue
                raise ValueError(issue)

            snapshot = self.page.evaluate(SCAN_JS)
            discovered_this_round = 0

            for link in snapshot["links"]:
                parsed_post = parse_post_url(link["href"])
                if not parsed_post:
                    continue
                post_owner = parsed_post["owner_token"].lower()
                if not self.is_news_feed and post_owner not in self.owners and not (post_owner.isdigit() and not any(x.isdigit() for x in self.owners)):
                    continue

                key = parsed_post["discovery_key"]
                if key not in self.source_seen:
                    self.source_seen.add(key)
                    discovered_this_round += 1

                if key not in self.seen:
                    self.seen.add(key)
                    item = {
                        **parsed_post,
                        "platform": "facebook",
                        "source_url": self.clean_url,
                        "discovered_at": utc_now(),
                    }
                    self.total_discovered += 1
                    is_new_to_queue = True
                    if on_url_discovered:
                        try:
                            cb_res = on_url_discovered(item)
                            if cb_res is False:
                                is_new_to_queue = False
                        except Exception as exc:
                            pass

                    if is_new_to_queue:
                        batch_new.append(item)
                        if len(batch_new) >= batch_size:
                            # Yield control to extraction; keep page open!
                            return batch_new

            self.scroll_round += 1

            if discovered_this_round == 0:
                self.idle += 1
            else:
                self.idle = 0

            if self.idle >= self.idle_rounds:
                if self.refreshes < self.max_refreshes:
                    self.refreshes += 1
                    issue = refresh_after_idle(self.page, self.refresh_wait)
                    if issue:
                        self.stop_reason = issue
                        raise ValueError(issue)
                    self.source_seen.clear()
                    self.idle = 0
                    continue
                else:
                    self.is_exhausted = True
                    self.stop_reason = "idle_rounds_exhausted"
                    break

            # Scroll down
            self.page.mouse.wheel(0, 750)
            target = time.monotonic() + 2.5
            while time.monotonic() < target:
                if self.is_deadline_reached():
                    break
                time.sleep(0.3)

        if self.scroll_round >= self.max_scrolls:
            self.is_exhausted = True
            self.stop_reason = "max_scrolls_budget_reached"

        return batch_new

    def close(self):
        if self.page is not None:
            try:
                self.page.close()
            except Exception:
                pass
            self.page = None


SEARCH_SCAN_JS = r"""() => {
    const rendered = el => el.getClientRects().length > 0
        && getComputedStyle(el).visibility !== 'hidden';
    const main = document.querySelector('[role="main"]') || document.body;

    const emptyPhrases = [
        "we didn't find any results",
        "không tìm thấy kết quả",
        "no results found",
        "không có kết quả nào",
        "không tìm thấy bài viết nào"
    ];
    const mainText = (main.innerText || '').toLowerCase();
    const hasNoResults = emptyPhrases.some(p => mainText.includes(p));

    const isPostHref = (h) => {
        if (!h) return false;
        if (h.includes('comment_id=') || h.includes('reply_comment_id=')) return false;
        if (h.includes('l.facebook.com') || h.includes('/hashtag/')) return false;
        const low = h.toLowerCase();
        return low.includes('/posts/') || low.includes('/permalink/') || low.includes('/share/p/') ||
               low.includes('story_fbid=') || low.includes('permalink.php') ||
               (low.includes('/photos/') && !low.endsWith('/photos/')) ||
               (low.includes('photo.php') && low.includes('fbid=')) ||
               (low.includes('/videos/') && !low.endsWith('/videos/')) ||
               (low.includes('/reel/') && !low.endsWith('/reel/')) ||
               (low.includes('/watch') && low.includes('v='));
    };

    const items = [];
    const seen = new Set();

    // 1. Articles / feed unit cards
    const articles = Array.from(main.querySelectorAll('[role="article"], div[data-pagelet*="FeedUnit"]')).filter(rendered);
    for (const art of articles) {
        const anchors = Array.from(art.querySelectorAll('a[href]')).filter(rendered);
        let bestHref = null;
        let bestLabel = '';
        for (const a of anchors) {
            const h = a.href || '';
            if (isPostHref(h)) {
                const aria = (a.getAttribute('aria-label') || '').trim();
                const text = (a.innerText || '').trim();
                bestHref = h;
                bestLabel = aria || text;
                if (aria && /\d/.test(aria)) break;
                if (h.includes('/posts/') || h.includes('/permalink/') || h.includes('story_fbid=')) break;
            }
        }
        if (bestHref && !seen.has(bestHref)) {
            seen.add(bestHref);
            const msgEl = art.querySelector('div[data-ad-preview="message"], div[data-ad-comet-preview="message"], div[dir="auto"]');
            const preview = msgEl ? (msgEl.innerText || '').trim().slice(0, 2000) : '';
            items.push({
                href: bestHref,
                label: bestLabel.slice(0, 250),
                preview: preview,
                in_article: true
            });
        }
    }

    // 2. Extra anchors in main
    const allAnchors = Array.from(main.querySelectorAll('a[href]')).filter(rendered);
    for (const a of allAnchors) {
        const h = a.href || '';
        if (isPostHref(h) && !seen.has(h)) {
            seen.add(h);
            const card = a.closest('[role="article"]') || a.closest('div[data-pagelet*="FeedUnit"]');
            const preview = card ? (card.innerText || '').slice(0, 2000) : '';
            items.push({
                href: h,
                label: (a.innerText || a.getAttribute('aria-label') || '').slice(0, 250),
                preview: preview,
                in_article: Boolean(card)
            });
        }
    }

    return {
        has_no_results: hasNoResults,
        card_count: articles.length,
        item_count: items.length,
        items: items
    };
}"""


def build_facebook_search_url(query: str) -> str:
    """Builds a standardized Facebook Post Search URL with proper URL encoding."""
    return f"https://www.facebook.com/search/posts/?q={quote_plus(str(query).strip())}"


class FacebookSearchDiscoverySession:
    """Manages an active Facebook Post Search tab for a specific keyword query.

    Navigates to https://www.facebook.com/search/posts/?q=...
    Scans results, isolates post cards, extracts real permalinks,
    and supports scrolling up to limit_per_query candidates or idle exhaustion.
    """
    def __init__(
        self,
        query,
        context,
        limit_per_query=30,
        max_scrolls=50,
        idle_rounds=5,
        deadline_mono=None,
        existing_seen_keys=None,
    ):
        self.query = query.strip()
        self.search_url = build_facebook_search_url(self.query)
        self.context = context
        self.limit_per_query = limit_per_query
        self.max_scrolls = max_scrolls
        self.idle_rounds = idle_rounds
        self.deadline_mono = deadline_mono
        self.seen = set(existing_seen_keys or [])
        self.query_seen = set()
        self.page = None
        self.scroll_round = 0
        self.idle = 0
        self.is_exhausted = False
        self.stop_reason = None
        self.total_discovered = 0
        self.has_no_results = False

    def start(self, timeout=60000):
        if self.page is None:
            self.page = self.context.new_page()
            self.page.goto(self.search_url, wait_until="domcontentloaded", timeout=timeout)
            time.sleep(3.5)

    def is_deadline_reached(self):
        return self.deadline_mono is not None and time.monotonic() >= self.deadline_mono

    def discover_batch(self, batch_size=None, on_candidate_discovered=None, deadline_mono=None):
        if deadline_mono:
            self.deadline_mono = deadline_mono

        if self.page is None:
            self.start()

        target_batch = batch_size or self.limit_per_query
        batch_new = []

        while self.scroll_round < self.max_scrolls:
            if self.is_deadline_reached():
                self.stop_reason = "deadline_reached"
                break

            issue = access_issue(self.page)
            if issue:
                self.stop_reason = issue
                raise ValueError(issue)

            snapshot = self.page.evaluate(SEARCH_SCAN_JS)
            if snapshot.get("has_no_results") and snapshot.get("item_count", 0) == 0:
                self.has_no_results = True
                self.stop_reason = "no_results_found"
                self.is_exhausted = True
                break

            discovered_this_round = 0

            for item in snapshot.get("items", []):
                raw_href = item.get("href")
                if not raw_href:
                    continue

                clean_url = fd.normalize_facebook_post_url(raw_href)
                if not clean_url:
                    continue

                post_id = fd.extract_post_id(clean_url)
                if not post_id:
                    continue

                author_id = fd.extract_author_id(clean_url)

                lower_url = clean_url.lower()
                if "/groups/" in lower_url:
                    source_type = "group"
                elif "profile.php" in lower_url or "/people/" in lower_url:
                    source_type = "individual"
                elif any(token in lower_url for token in ["/posts/", "/photos/", "/videos/"]):
                    source_type = "fanpage"
                else:
                    source_type = "unknown"

                key = f"facebook:post:{post_id}"
                if key not in self.query_seen:
                    self.query_seen.add(key)
                    discovered_this_round += 1

                candidate = {
                    "post_url": clean_url,
                    "canonical_url": clean_url,
                    "post_id": post_id,
                    "author_id": author_id,
                    "source_type": source_type,
                    "platform": "Facebook",
                    "discovery_method": "keyword_search",
                    "discovery_query": self.query,
                    "search_url": self.search_url,
                    "candidate_preview": item.get("preview"),
                    "discovered_at": utc_now(),
                    "discovery_key": key,
                }

                is_new_candidate = True
                if on_candidate_discovered:
                    try:
                        res = on_candidate_discovered(candidate)
                        if res is False:
                            is_new_candidate = False
                    except Exception as exc:
                        logger.warning(f"Error in on_candidate_discovered callback: {exc}")

                if key not in self.seen:
                    self.seen.add(key)
                    self.total_discovered += 1
                    if is_new_candidate:
                        batch_new.append(candidate)
                        if len(batch_new) >= target_batch:
                            return batch_new

            self.scroll_round += 1

            if discovered_this_round == 0:
                self.idle += 1
            else:
                self.idle = 0

            if self.idle >= self.idle_rounds:
                self.is_exhausted = True
                self.stop_reason = "idle_rounds_exhausted"
                break

            # Scroll down
            self.page.mouse.wheel(0, 800)
            target = time.monotonic() + 2.5
            while time.monotonic() < target:
                if self.is_deadline_reached():
                    break
                time.sleep(0.3)

        if self.scroll_round >= self.max_scrolls:
            self.is_exhausted = True
            if not self.stop_reason:
                self.stop_reason = "max_scrolls_budget_reached"

        return batch_new

    def close(self):
        if self.page is not None:
            try:
                self.page.close()
            except Exception:
                pass
            self.page = None


def discover_page_posts(
    page_url,
    limit=30,
    profile_dir=BASE_DIR / "profiles" / "facebook",
    headless=False,
    max_scrolls=60,
    idle_rounds=10,
    max_refreshes=2,
    refresh_wait=15,
    max_hours=None,
    existing_seen_keys=None,
    context=None,
):
    """Discovers post permalinks from a given Facebook Page URL using shared recovery and refresh logic.
    Returns a list of dicts with post_url, post_id, etc.
    """
    profile_path = Path(profile_dir)
    owns_context = False
    if context is None:
        if not profile_path.is_dir():
            raise FileNotFoundError(f"Profile directory not found: {profile_path}")
        import facebook_dataset as fd
        fd.clean_profile_locks(profile_path)
        from cloakbrowser import launch_persistent_context
        context = launch_persistent_context(
            str(profile_path),
            headless=headless,
            args=[f"--fingerprint={FINGERPRINT_SEED}"],
            viewport={"width": 1280, "height": 900},
        )
        owns_context = True

    discovered = []
    deadline = (time.monotonic() + max_hours * 3600) if (max_hours and max_hours > 0) else None

    session = PageDiscoverySession(
        page_url=page_url,
        context=context,
        max_scrolls=max_scrolls,
        idle_rounds=idle_rounds,
        max_refreshes=max_refreshes,
        refresh_wait=refresh_wait,
        deadline_mono=deadline,
        existing_seen_keys=existing_seen_keys,
    )

    try:
        session.start()
        while not session.is_exhausted and len(discovered) < limit:
            batch = session.discover_batch(batch_size=min(20, limit - len(discovered)), deadline_mono=deadline)
            discovered.extend(batch)
            if not batch and session.is_exhausted:
                break
    finally:
        session.close()
        if owns_context:
            try:
                context.close()
            except Exception:
                pass
            import facebook_dataset as fd
            fd.clean_profile_locks(profile_path)

    return discovered


def get_arguments():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", action="append", default=[], help="Page URL; can be repeated.")
    parser.add_argument("--sources", type=Path, help="UTF-8 file, one Page URL per line, '#' comments ignored.")
    parser.add_argument("--limit", type=positive_int, default=30, help="Max new unique URLs for this run (default: 30).")
    parser.add_argument("--max-scrolls", type=positive_int, default=60, help="Total scan rounds per source, including after refresh.")
    parser.add_argument("--idle-rounds", type=positive_int, default=10, help="Consecutive rounds with no newly encountered post URLs before recovery.")
    parser.add_argument("--max-refreshes", type=nonnegative_int, default=2, help="Max reload attempts per source when scrolling stalls; 0 disables recovery.")
    parser.add_argument("--refresh-wait", type=nonnegative_int, default=15, help="Seconds to wait before each reload (default: 15).")
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
        "recovery_settings": {"max_refreshes_per_source": args.max_refreshes,
                              "refresh_wait_seconds": args.refresh_wait,
                              "idle_rounds": args.idle_rounds,
                              "max_scan_rounds_per_source": args.max_scrolls},
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
                      "refreshes": 0, "refresh_events": [],
                      "article_count_last": 0, "link_count_last": 0,
                      "recognized_permalink_count_last": 0, "other_owner_links_skipped": 0,
                      "candidate_link_examples": []}
            report["sources"].append(detail)
            print(f"Source: {source_url}", flush=True)
            try:
                is_news_feed = (owner == "news_feed")
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
                        if is_news_feed or post_owner in owners:
                            relation = "feed_post" if is_news_feed else "url_owner_matches_seed"
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
                    if number >= args.max_scrolls:
                        detail["status"] = "max_scrolls_reached"
                        break
                    idle = idle + 1 if discovered_this_round == 0 else 0
                    if idle >= args.idle_rounds:
                        if detail["refreshes"] >= args.max_refreshes:
                            detail["status"] = ("no_new_links_after_refreshes" if detail["refreshes"]
                                                else "no_new_links_in_loaded_dom")
                            break
                        detail["refreshes"] += 1
                        event = {"attempt": detail["refreshes"], "round": number,
                                 "started_at": utc_now(), "finished_at": None,
                                 "new_urls_before_refresh": detail["new_urls"],
                                 "status": "attempting"}
                        detail["refresh_events"].append(event)
                        detail["status"] = "refreshing"
                        save_report(report_file, report)
                        print(f"  No new links for {idle} rounds. Waiting {args.refresh_wait}s, "
                              f"then refresh {detail['refreshes']}/{args.max_refreshes}.", flush=True)
                        try:
                            issue = refresh_after_idle(page, args.refresh_wait)
                        except Error as exc:
                            event["status"], event["error"] = "browser_error", str(exc)[:1500]
                            raise
                        finally:
                            event["finished_at"] = utc_now()
                        if issue:
                            event["status"] = "access_required"
                            detail["status"], detail["error"] = "access_required", issue
                            stop_for_access = True
                            break
                        event["status"] = "reloaded"
                        detail["status"] = "running"
                        # Reset only this pass's traversal tracker, not the saved
                        # deduplication keys. This lets us scroll past old posts
                        # again without treating them as new dataset records.
                        source_seen.clear()
                        idle = 0
                        save_report(report_file, report)
                        continue
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
