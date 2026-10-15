"""Dataset standardization and extraction helpers enforcing the 17-column schema.
Specification source: sources/dataset_requirements.xlsx

17 required columns:
1.  post_id
2.  platform
3.  text
4.  published_at
5.  collected_at
6.  author_id
7.  language
8.  hashtags
9.  mentions
10. like_count
11. comment_count
12. share_count
13. view_count
14. media_type
15. author_followers
16. post_url
17. sport_category
"""

import atexit
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# Exact list of 17 required columns in order
REQUIRED_COLUMNS = [
    "post_id",
    "platform",
    "text",
    "published_at",
    "collected_at",
    "author_id",
    "language",
    "hashtags",
    "mentions",
    "like_count",
    "comment_count",
    "share_count",
    "view_count",
    "media_type",
    "author_followers",
    "post_url",
    "sport_category",
]

# Standard sport categories in English
VALID_SPORTS = {
    "Football",
    "Basketball",
    "Tennis",
    "Volleyball",
    "Badminton",
    "Swimming",
    "Motorsport",
    "Athletics",
    "Martial Arts",
    "Table Tennis",
    "Golf",
    "Rugby",
    "Baseball",
    "Gymnastics",
    "Chess",
    "Shooting",
    "Weightlifting",
}

# Mapping common Vietnamese and English synonyms to canonical English sport names
SPORT_SYNONYMS = {
    "bóng đá": "Football",
    "soccer": "Football",
    "football": "Football",
    "bóng rổ": "Basketball",
    "basketball": "Basketball",
    "quần vợt": "Tennis",
    "tennis": "Tennis",
    "bóng chuyền": "Volleyball",
    "volleyball": "Volleyball",
    "cầu lông": "Badminton",
    "badminton": "Badminton",
    "bơi lội": "Swimming",
    "swimming": "Swimming",
    "đua xe": "Motorsport",
    "motorsport": "Motorsport",
    "f1": "Motorsport",
    "điền kinh": "Athletics",
    "athletics": "Athletics",
    "võ thuật": "Martial Arts",
    "martial arts": "Martial Arts",
    "mma": "Martial Arts",
    "boxing": "Martial Arts",
    "bóng bàn": "Table Tennis",
    "table tennis": "Table Tennis",
    "ping pong": "Table Tennis",
    "golf": "Golf",
    "bóng bầu dục": "Rugby",
    "rugby": "Rugby",
    "bóng chày": "Baseball",
    "baseball": "Baseball",
    "thể dục dụng cụ": "Gymnastics",
    "gymnastics": "Gymnastics",
    "cờ vua": "Chess",
    "chess": "Chess",
    "bắn súng": "Shooting",
    "shooting": "Shooting",
    "cử tạ": "Weightlifting",
    "weightlifting": "Weightlifting",
}

# Distinct Vietnamese diacritics for language detection
VIETNAMESE_DISTINCT_RE = re.compile(
    r"[ăâđêôơưĂÂĐÊÔƠƯàảãạằẳẵặầẩẫậèẻẽẹềểễệìỉĩịòỏõọồổỗộờởỡợùủũụừửữựỳỷỹỵ]",
    re.IGNORECASE,
)
COMMON_VN_WORDS = {
    "và", "của", "là", "có", "được", "trong", "cho", "với",
    "không", "người", "này", "khi", "đã", "sẽ", "về", "như", "bài",
}

# Regex to extract hashtags (#word)
HASHTAG_RE = re.compile(r"#\w+", re.UNICODE)

# Regex to extract mentions (@word)
MENTION_RE = re.compile(r"@[\w\.]+", re.UNICODE)

# Browser JS evaluation script scoped accurately to the post container
DOM_EXTRACT_JS = r"""(anchorsArg) => {
    const res = {
        published_at: null,
        like_count_raw: null,
        comment_count_raw: null,
        share_count_raw: null,
        view_count_raw: null,
        author_followers_raw: null,
        media_type: "text",
        debug_container_found: false
    };

    function isValidTextNode(node) {
        if (!node || !node.parentElement) return false;
        const tag = node.parentElement.tagName;
        return tag !== 'SCRIPT' && tag !== 'STYLE' && tag !== 'NOSCRIPT';
    }

    const anchorList = Array.isArray(anchorsArg) ? anchorsArg : (anchorsArg ? [anchorsArg] : []);

    // 1. Locate the post card container
    let postContainer = null;
    for (const rawAnchor of anchorList) {
        if (!rawAnchor) continue;
        const needle = rawAnchor.trim().slice(0, 30);
        const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
        let node;
        while ((node = walker.nextNode())) {
            if (isValidTextNode(node) && node.nodeValue && node.nodeValue.includes(needle)) {
                let curr = node.parentElement;
                for (let i = 0; i < 15 && curr && curr !== document.body; i++) {
                    const hasActions = !!curr.querySelector('[aria-label*="Like" i], [aria-label*="Thích" i], [aria-label*="Leave a comment" i], [aria-label*="comment" i]');
                    const hasMedia = !!curr.querySelector('img, video, a[href*="/photo"]');
                    if (hasActions && hasMedia) {
                        postContainer = curr;
                        break;
                    }
                    if (hasActions) {
                        postContainer = curr;
                    }
                    curr = curr.parentElement;
                }
                if (postContainer) break;
            }
        }
        if (postContainer) break;
    }

    if (postContainer) {
        res.debug_container_found = true;
    } else {
        postContainer = document.querySelector('[role="main"]') || document.body;
    }

    // 2. Identify media_type
    // Priority: video -> image (excluding avatars & emojis) -> link -> text
    const vids = Array.from(postContainer.querySelectorAll('video')).filter(v => {
        const rect = v.getBoundingClientRect();
        return rect.width >= 150 && rect.height >= 150;
    });
    const hasVideo = vids.length > 0 || !!postContainer.querySelector('[data-video-id]');
    if (hasVideo) {
        res.media_type = "video";
    } else {
        const contentImgs = Array.from(postContainer.querySelectorAll('img')).filter(img => {
            const w = img.naturalWidth || img.clientWidth || img.width || 0;
            const h = img.naturalHeight || img.clientHeight || img.height || 0;
            const src = img.src || "";
            const alt = img.alt || "";
            const isEmoji = src.includes("emoji.php") || src.includes("rsrc.php") || src.startsWith("data:image/svg+xml");
            const isAvatar = (w < 100 && h < 100) || !!img.closest('[aria-label*="profile" i], [role="img"][aria-label*="avatar" i], [aria-label*="story" i]');
            const isFbPhotoAlt = /^May be an image of/i.test(alt) || /^Có thể là hình ảnh về/i.test(alt) || /^No photo description/i.test(alt);
            return (!isEmoji && !isAvatar && (w >= 140 || h >= 140)) || isFbPhotoAlt;
        });
        const hasPhotoLink = !!postContainer.querySelector('a[href*="/photo"], a[href*="/photos/"], a[href*="photo.php"]');
        if (contentImgs.length > 0 || hasPhotoLink) {
            res.media_type = "image";
        } else {
            const hasExternalLink = !!postContainer.querySelector('a[target="_blank"][rel*="nofollow"], a[href*="l.facebook.com/l.php"]');
            if (hasExternalLink) {
                res.media_type = "link";
            } else {
                res.media_type = "text";
            }
        }
    }

    // 3. published_at: Look for timestamp links on the post
    const candidateLinks = Array.from(document.querySelectorAll('a[role="link"], a[href]'));
    for (const a of candidateLinks) {
        const aria = (a.getAttribute('aria-label') || '').trim();
        const txt = (a.innerText || '').trim();
        // Check aria-label first (e.g. "Friday 9 October 2026 at 18:54" or "Saturday 10 October 2026 at 17:54")
        if (aria && /^(?:\w+day\s+\d{1,2}\s+\w+|\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|tháng)|\d+\s*(?:h|m|d|s|hr|hrs|min|mins|day|days|sec|secs|ngày|giờ|phút|giây)|yesterday|hôm qua|just now|vừa xong)/i.test(aria)) {
            res.published_at = aria;
            break;
        }
        if (txt && /^(\d+\s*(?:h|m|d|s|hr|hrs|min|mins|day|days|sec|secs|ngày|giờ|phút|giây)|yesterday|hôm qua|just now|vừa xong|\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec|tháng))$/i.test(txt)) {
            res.published_at = txt;
            break;
        }
    }

    // 4. Interaction Metrics (like_count, comment_count, share_count, view_count)
    // 4.1. Like button count
    const likeButtons = Array.from(postContainer.querySelectorAll('[role="button"], button')).filter(btn => {
        const aria = (btn.getAttribute('aria-label') || '').trim();
        return /^like$/i.test(aria) || /^thích$/i.test(aria) || /like/i.test(aria);
    });
    for (const btn of likeButtons) {
        const txt = btn.innerText.trim();
        if (/^([\d.,]+[KkMm]?|\d+\s*(?:nghìn|ngàn|triệu|tr))$/i.test(txt)) {
            res.like_count_raw = txt;
            break;
        }
    }
    // Fallback for like_count: reaction badge
    if (!res.like_count_raw) {
        const reactionBadges = Array.from(postContainer.querySelectorAll('[aria-label*="reacted to this" i], [aria-label*="bày tỏ cảm xúc" i], [aria-label*="reactions" i]'));
        for (const badge of reactionBadges) {
            const aria = badge.getAttribute('aria-label') || '';
            const m = aria.match(/([\d.,]+[KkMm]?|\d+\s*(?:nghìn|ngàn|triệu|tr))/i);
            if (m) {
                res.like_count_raw = m[1];
                break;
            }
            const txt = badge.innerText.trim();
            if (/^([\d.,]+[KkMm]?|\d+\s*(?:nghìn|ngàn|triệu|tr))$/i.test(txt)) {
                res.like_count_raw = txt;
                break;
            }
        }
    }

    // 4.2. Comment count
    const commentButtons = Array.from(postContainer.querySelectorAll('[role="button"], button')).filter(btn => {
        const aria = (btn.getAttribute('aria-label') || '').trim();
        return /leave a comment/i.test(aria) || /bình luận/i.test(aria) || /comments?/i.test(aria);
    });
    for (const btn of commentButtons) {
        const txt = btn.innerText.trim();
        if (/^([\d.,]+[KkMm]?|\d+\s*(?:nghìn|ngàn|triệu|tr))$/i.test(txt)) {
            res.comment_count_raw = txt;
            break;
        }
    }
    if (!res.comment_count_raw) {
        const postText = postContainer.innerText || "";
        const m = postText.match(/([\d.,]+[KkMm]?|\d+\s*(?:nghìn|ngàn|triệu|tr))\s*(?:comments?|bình luận)/i);
        if (m) res.comment_count_raw = m[1];
    }

    // 4.3. Share count
    const shareButtons = Array.from(postContainer.querySelectorAll('[role="button"], button')).filter(btn => {
        const aria = (btn.getAttribute('aria-label') || '').trim();
        return /share/i.test(aria) || /chia sẻ/i.test(aria);
    });
    for (const btn of shareButtons) {
        const txt = btn.innerText.trim();
        if (/^([\d.,]+[KkMm]?|\d+\s*(?:nghìn|ngàn|triệu|tr))$/i.test(txt)) {
            res.share_count_raw = txt;
            break;
        }
    }
    if (!res.share_count_raw) {
        const postText = postContainer.innerText || "";
        const m = postText.match(/([\d.,]+[KkMm]?|\d+\s*(?:nghìn|ngàn|triệu|tr))\s*(?:shares?|lượt chia sẻ)/i);
        if (m) res.share_count_raw = m[1];
    }

    // 4.4. View count: Extracted from ANY post format whenever displayed by Facebook
    const postText = postContainer.innerText || "";
    const viewMatch = postText.match(/([\d.,]+[KkMm]?|\d+\s*(?:nghìn|ngàn|triệu|tr))\s*(?:views?|lượt xem)/i);
    if (viewMatch) {
        res.view_count_raw = viewMatch[1];
    } else {
        const viewEl = Array.from(postContainer.querySelectorAll('[aria-label*="view" i], [aria-label*="lượt xem" i]')).find(el => {
            const aria = el.getAttribute('aria-label') || '';
            return /([\d.,]+[KkMm]?|\d+\s*(?:nghìn|ngàn|triệu|tr))\s*(?:views?|lượt xem)/i.test(aria);
        });
        if (viewEl) {
            const m = (viewEl.getAttribute('aria-label') || '').match(/([\d.,]+[KkMm]?|\d+\s*(?:nghìn|ngàn|triệu|tr))/i);
            if (m) res.view_count_raw = m[1];
        }
    }

    return res;
};"""


def parse_numeric_count(value):
    """Converts counts like 20K, 1.5M, 20 nghìn, 1,200 into clean Python integers."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip().lower()
    if not text or not re.search(r"\d", text):
        return None

    # Replace language-specific suffixes
    text = re.sub(r"\b(nghìn|ngàn)\b", "k", text)
    text = re.sub(r"\b(triệu|tr)\b", "m", text)
    text = re.sub(r"\b(tỷ|ty)\b", "b", text)
    text = text.replace(" ", "")

    match = re.search(r"(\d[\d.,]*)\s*([kmb]?)", text)
    if not match:
        return None
    num_str, unit = match.groups()
    num_str = num_str.strip(".,")
    if not num_str or not re.search(r"\d", num_str):
        return None

    try:
        if unit:
            clean_num = num_str.replace(",", ".")
            num = float(clean_num)
            if unit == "k":
                num *= 1_000
            elif unit == "m":
                num *= 1_000_000
            elif unit == "b":
                num *= 1_000_000_000
            return int(num)
        else:
            if "," in num_str and "." in num_str:
                if num_str.rfind(".") > num_str.rfind(","):
                    num_str = num_str.replace(",", "")
                else:
                    num_str = num_str.replace(".", "").replace(",", ".")
                return int(float(num_str))
            elif "," in num_str:
                parts = num_str.split(",")
                if len(parts[-1]) == 3 and all(len(p) <= 3 for p in parts[:-1]):
                    num_str = "".join(parts)
                    return int(num_str)
                else:
                    return int(float(num_str.replace(",", ".")))
            elif "." in num_str:
                parts = num_str.split(".")
                if len(parts[-1]) == 3 and all(len(p) <= 3 for p in parts[:-1]):
                    num_str = "".join(parts)
                    return int(num_str)
                else:
                    return int(float(num_str))
            else:
                return int(num_str)
    except (ValueError, TypeError):
        return None


def validate_or_normalize_sport(category):
    """Normalizes sport category to English; rejects non-sports topics (returns None).
    Supports multiple sports as a list or comma-separated string.
    """
    if not category:
        return None

    # If already a list or set of sports
    if isinstance(category, (list, tuple, set)):
        normalized = []
        for item in category:
            norm = validate_or_normalize_sport(item)
            if norm and norm not in normalized:
                normalized.append(norm)
        return ", ".join(normalized) if normalized else None

    text = str(category).strip()
    if "," in text:
        parts = [p.strip() for p in text.split(",") if p.strip()]
        normalized = []
        for p in parts:
            norm = validate_or_normalize_sport(p)
            if norm and norm not in normalized:
                normalized.append(norm)
        return ", ".join(normalized) if normalized else None

    # Single sport check
    lower = text.lower()
    if lower in SPORT_SYNONYMS:
        return SPORT_SYNONYMS[lower]
    for sport in VALID_SPORTS:
        if sport.lower() == lower:
            return sport
    for syn_key, canonical in SPORT_SYNONYMS.items():
        if syn_key in lower:
            return canonical

    return None


def extract_post_id(url):
    """Extracts unique post ID from Facebook URL (pfbid token, numeric ID, or permalink slug)."""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        path = parsed.path.rstrip("/")

        post_match = re.search(r"/posts/([^/?#]+)", path)
        if post_match:
            return post_match.group(1)

        share_match = re.search(r"/share/p/([^/?#]+)", path)
        if share_match:
            return share_match.group(1)

        query = parse_qs(parsed.query)
        if "story_fbid" in query and query["story_fbid"]:
            return query["story_fbid"][0]
        if "fbid" in query and query["fbid"]:
            return query["fbid"][0]

        token_match = re.search(r"(pfbid[A-Za-z0-9]+|\b\d{9,}\b)", url)
        if token_match:
            return token_match.group(1)

        return path.split("/")[-1] if "/" in path else None
    except Exception:
        return None


def extract_author_id(url, page=None):
    """Extracts author username or page ID from URL or DOM."""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        path_segments = [p for p in parsed.path.strip("/").split("/") if p]
        reserved = {
            "posts", "photos", "videos", "reel", "reels", "watch",
            "share", "groups", "story.php", "permalink.php", "profile.php",
        }
        if path_segments and path_segments[0].lower() not in reserved:
            return path_segments[0]
        query = parse_qs(parsed.query)
        if "id" in query and query["id"]:
            return query["id"][0]
    except Exception:
        pass
    return None


def detect_language(text):
    """Detects text language ('vi', 'en', or 'unknown')."""
    if not text:
        return "unknown"
    words = re.findall(r"\b\w+\b", text.lower())
    if not words:
        return "unknown"
    vn_word_count = sum(1 for w in words if w in COMMON_VN_WORDS)
    if vn_word_count >= 2:
        return "vi"
    vn_chars = len(VIETNAMESE_DISTINCT_RE.findall(text))
    if vn_chars >= 3 and (vn_word_count >= 1 or vn_chars / len(text) > 0.02):
        return "vi"
    if re.search(r"[A-Za-z]", text):
        return "en"
    return "unknown"


def extract_hashtags(text):
    """Extracts unique hashtags list from text."""
    if not text:
        return []
    tags = HASHTAG_RE.findall(text)
    return list(dict.fromkeys(tags))


def extract_mentions(text):
    """Extracts unique mentions list from text."""
    if not text:
        return []
    mentions = MENTION_RE.findall(text)
    return list(dict.fromkeys(mentions))


def build_dataset_record(
    post_url,
    text=None,
    published_at=None,
    collected_at=None,
    author_id=None,
    post_id=None,
    sport_category=None,
    like_count=None,
    comment_count=None,
    share_count=None,
    view_count=None,
    media_type="text",
    author_followers=None,
    language=None,
    hashtags=None,
    mentions=None,
):
    """Builds a standardized 17-column dataset record with exact schema compliance."""
    final_post_id = post_id or extract_post_id(post_url)
    final_author_id = author_id or extract_author_id(post_url)
    final_collected_at = collected_at or datetime.now(VIETNAM_TZ).isoformat()
    final_language = language or detect_language(text)
    final_hashtags = hashtags if hashtags is not None else extract_hashtags(text)
    final_mentions = mentions if mentions is not None else extract_mentions(text)
    final_media_type = media_type or "text"

    # View count represents total views/impressions across post types (if displayed)
    final_view_count = parse_numeric_count(view_count)

    record = {
        "post_id": final_post_id,
        "platform": "Facebook",
        "text": text,
        "published_at": published_at,
        "collected_at": final_collected_at,
        "author_id": final_author_id,
        "language": final_language,
        "hashtags": final_hashtags,
        "mentions": final_mentions,
        "like_count": parse_numeric_count(like_count),
        "comment_count": parse_numeric_count(comment_count),
        "share_count": parse_numeric_count(share_count),
        "view_count": final_view_count,
        "media_type": final_media_type,
        "author_followers": parse_numeric_count(author_followers),
        "post_url": post_url,
        "sport_category": validate_or_normalize_sport(sport_category),
    }

    assert list(record.keys()) == REQUIRED_COLUMNS, "Column mismatch in dataset record!"
    return record


def extract_page_metrics(page, anchors=None, anchor_text=None):
    """Executes DOM evaluation script scoped to the target post container."""
    target_anchors = anchors if anchors is not None else anchor_text
    try:
        return page.evaluate(DOM_EXTRACT_JS, target_anchors)
    except Exception as exc:
        return {
            "published_at": None,
            "like_count_raw": None,
            "comment_count_raw": None,
            "share_count_raw": None,
            "view_count_raw": None,
            "author_followers_raw": None,
            "media_type": "text",
            "error": str(exc),
        }


def convert_legacy_record(legacy, default_sport=None):
    """Converts a legacy record dictionary into the standard 17-column format."""
    url = legacy.get("post_url") or legacy.get("requested_url") or legacy.get("url")
    return build_dataset_record(
        post_url=url,
        text=legacy.get("text"),
        published_at=legacy.get("published_at"),
        collected_at=legacy.get("collected_at"),
        author_id=legacy.get("author_id"),
        post_id=legacy.get("post_id"),
        sport_category=legacy.get("sport_category", default_sport),
        like_count=legacy.get("like_count") or legacy.get("reaction_count"),
        comment_count=legacy.get("comment_count"),
        share_count=legacy.get("share_count"),
        view_count=legacy.get("view_count"),
        media_type=legacy.get("media_type", "text"),
        author_followers=legacy.get("author_followers"),
        language=legacy.get("language"),
        hashtags=legacy.get("hashtags"),
        mentions=legacy.get("mentions"),
    )


def convert_legacy_file(input_path, output_path=None):
    """Converts an entire legacy JSON file into the standard 17-column dataset format."""
    in_file = Path(input_path)
    if not in_file.exists():
        raise FileNotFoundError(f"File not found: {input_path}")
    data = json.loads(in_file.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        data = [data]
    converted = [convert_legacy_record(item) for item in data]
    out_file = Path(output_path) if output_path else in_file.with_name(f"{in_file.stem}_17cols.json")
    out_file.write_text(json.dumps(converted, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_file


def clean_profile_locks(profile_dir):
    """Removes stale Chromium singleton lock files left behind by interrupted sessions."""
    p = Path(profile_dir)
    if not p.is_dir():
        return
    for lock_name in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
        lock_file = p / lock_name
        try:
            if lock_file.is_symlink() or lock_file.exists():
                lock_file.unlink(missing_ok=True)
        except OSError:
            pass


def auto_discover_anchors(page):
    """Automatically derives clean text anchor keywords from page title for anchor-free scraping."""
    try:
        title = page.title()
        if not title:
            return []
        # Title format on permalinks: "Author - Snippet... | Facebook" or "Snippet | Facebook"
        clean = re.sub(r"\s*\|\s*Facebook.*$", "", title, flags=re.IGNORECASE).strip()
        if " - " in clean:
            parts = clean.split(" - ", 1)
            raw_snippet = parts[1].strip()
        else:
            raw_snippet = clean

        raw_snippet = re.sub(r"\.{2,}", "", raw_snippet).strip()
        anchors = []
        if raw_snippet:
            anchors.append(raw_snippet[:40].strip())
        clean_words = re.findall(r"[\w\u00C0-\u1EF9\uD835\uDC00-\uD835\uDC33]{3,}", raw_snippet)
        if clean_words:
            anchors.append(" ".join(clean_words[:4]))
        return [a for a in dict.fromkeys(anchors) if a]
    except Exception:
        return []


def fetch_author_followers(context, author_id, cache=None, timeout_ms=30000):
    """Resolves author's follower count into an integer by visiting their profile page.
    Results are cached in the provided dict to avoid duplicate visits.
    """
    if not author_id or author_id.lower() in ("profile.php", "story.php", "permalink.php"):
        return None
    if cache is not None and author_id in cache:
        return cache[author_id]

    followers_int = None
    page = None
    try:
        page = context.new_page()
        page.goto(f"https://www.facebook.com/{author_id}", wait_until="domcontentloaded", timeout=timeout_ms)
        page.wait_for_timeout(2500)
        raw_count = page.evaluate(r'''() => {
            const m = (document.body.innerText || "").match(/([\d.,]+[KkMmB]?|\d+)\s*(?:followers|người theo dõi)/i);
            return m ? m[1] : null;
        }''')
        followers_int = parse_numeric_count(raw_count)
    except Exception:
        followers_int = None
    finally:
        if page is not None:
            try:
                page.close()
            except Exception:
                pass

    if cache is not None:
        cache[author_id] = followers_int
    return followers_int


class PersistentBrowserSession:
    """Context manager for CloakBrowser with automatic cleanup, atexit hook, and stale lock clearing."""

    def __init__(self, profile_dir, fingerprint=12345, headless=False, viewport=None):
        self.profile_dir = Path(profile_dir)
        self.fingerprint = fingerprint
        self.headless = headless
        self.viewport = viewport or {"width": 1280, "height": 900}
        self.context = None
        self._cleaned_up = False

    def __enter__(self):
        clean_profile_locks(self.profile_dir)
        from cloakbrowser import launch_persistent_context
        self.context = launch_persistent_context(
            str(self.profile_dir),
            headless=self.headless,
            args=[f"--fingerprint={self.fingerprint}"],
            viewport=self.viewport,
        )
        atexit.register(self.close)
        return self.context

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def close(self):
        if not self._cleaned_up:
            self._cleaned_up = True
            if self.context is not None:
                try:
                    self.context.close()
                except Exception:
                    pass
                self.context = None
            clean_profile_locks(self.profile_dir)

