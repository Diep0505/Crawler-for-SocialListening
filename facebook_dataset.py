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
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

logger = logging.getLogger("facebook_dataset")
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
    "Cycling",
}

# Mapping common Vietnamese, Spanish, and English synonyms to canonical English sport names
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
    "marathon": "Athletics",
    "maratón": "Athletics",
    "maratona": "Athletics",
    "running": "Athletics",
    "chạy bộ": "Athletics",
    "atletismo": "Athletics",
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
    "cycling": "Cycling",
    "đua xe đạp": "Cycling",
    "xe đạp": "Cycling",
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
COMMON_ES_WORDS = {
    "el", "la", "de", "que", "y", "en", "un", "ser", "se", "no", "por",
    "con", "su", "para", "como", "estar", "tener", "le", "lo", "pero",
    "más", "este", "del", "las", "los", "una", "al", "es", "son", "fue",
    "sus", "así", "hoy", "año", "gran", "sobre", "entre", "también",
}
COMMON_EN_WORDS = {
    "the", "be", "to", "of", "and", "a", "in", "that", "have", "i",
    "it", "for", "not", "on", "with", "he", "as", "you", "do", "at",
    "this", "but", "his", "by", "from", "they", "we", "say", "her",
    "she", "or", "an", "will", "my", "one", "all", "would", "there",
    "their", "what", "is", "are", "was", "were", "has", "had",
}
COMMON_FR_WORDS = {
    "le", "la", "les", "de", "des", "du", "un", "une", "et", "est",
    "en", "que", "qui", "dans", "ce", "cette", "pour", "sur", "avec",
    "nous", "vous", "notre", "votre", "ils", "sont", "au", "aux", "par",
    "plus", "très", "bien", "faire", "tout", "ses", "son", "sa", "ont",
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


def parse_numeric_count_with_meta(value):
    """Converts counts like 20K, 1.5M, 20 nghìn, 1,200 into clean Python integers.
    Returns (integer_value, is_approximate).
    """
    if value is None:
        return None, False
    if isinstance(value, (int, float)):
        return int(value), False
    text = str(value).strip().lower()
    if not text or not re.search(r"\d", text):
        return None, False

    is_approximate = bool(re.search(r"[kmb]|nghìn|ngàn|triệu|tr|tỷ|ty", text))

    # Replace language-specific suffixes
    text = re.sub(r"\b(nghìn|ngàn)\b", "k", text)
    text = re.sub(r"\b(triệu|tr)\b", "m", text)
    text = re.sub(r"\b(tỷ|ty)\b", "b", text)
    text = text.replace(" ", "")

    match = re.search(r"(\d[\d.,]*)\s*([kmb]?)", text)
    if not match:
        return None, False
    num_str, unit = match.groups()
    num_str = num_str.strip(".,")
    if not num_str or not re.search(r"\d", num_str):
        return None, False

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
            return int(num), True
        else:
            if "," in num_str and "." in num_str:
                if num_str.rfind(".") > num_str.rfind(","):
                    num_str = num_str.replace(",", "")
                else:
                    num_str = num_str.replace(".", "").replace(",", ".")
                return int(float(num_str)), False
            elif "," in num_str:
                parts = num_str.split(",")
                if len(parts[-1]) == 3 and all(len(p) <= 3 for p in parts[:-1]):
                    num_str = "".join(parts)
                    return int(num_str), False
                else:
                    return int(float(num_str.replace(",", "."))), False
            elif "." in num_str:
                parts = num_str.split(".")
                if len(parts[-1]) == 3 and all(len(p) <= 3 for p in parts[:-1]):
                    num_str = "".join(parts)
                    return int(num_str), False
                else:
                    return int(float(num_str)), False
            else:
                return int(num_str), False
    except (ValueError, TypeError):
        return None, False


def parse_numeric_count(value):
    """Converts counts like 20K, 1.5M, 20 nghìn, 1,200 into clean Python integers."""
    res, _ = parse_numeric_count_with_meta(value)
    return res


def parse_facebook_timestamp(raw, reference_tz=VIETNAM_TZ):
    """Normalizes Facebook timestamp strings into ISO 8601 with timezone.
    Returns (iso_string, precision).
    When there is sufficient evidence (exact day, month, year, hour, minute), returns (iso_str, 'exact').
    When evidence is incomplete (e.g. '2 h', '3y', 'yesterday'), returns (None, 'insufficient_evidence')
    to preserve time integrity and avoid fabricating false precision.
    """
    if not raw or not isinstance(raw, str):
        return None, "insufficient_evidence"
    text = raw.strip()
    if not text:
        return None, "insufficient_evidence"

    # If already in valid ISO format
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=reference_tz)
        return dt.isoformat(), "exact"
    except Exception:
        pass

    # Clean leading day of week e.g. "Friday ", "Miércoles, "
    s_clean = re.sub(
        r"^(?:[A-Za-zÀ-ÿ]+day|[A-Za-zÀ-ÿ]+es|[A-Za-zÀ-ÿ]+do|[A-Za-zÀ-ÿ]+go|Thứ\s+\w+)[,\s]+",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()

    months_map = {
        "jan": 1, "january": 1, "ene": 1, "enero": 1, "tháng 1": 1, "tháng 01": 1,
        "feb": 2, "february": 2, "febrero": 2, "tháng 2": 2, "tháng 02": 2,
        "mar": 3, "march": 3, "marzo": 3, "tháng 3": 3, "tháng 03": 3,
        "apr": 4, "april": 4, "abr": 4, "abril": 4, "tháng 4": 4, "tháng 04": 4,
        "may": 5, "mayo": 5, "tháng 5": 5, "tháng 05": 5,
        "jun": 6, "june": 6, "junio": 6, "tháng 6": 6, "tháng 06": 6,
        "jul": 7, "july": 7, "julio": 7, "tháng 7": 7, "tháng 07": 7,
        "aug": 8, "august": 8, "ago": 8, "agosto": 8, "tháng 8": 8, "tháng 08": 8,
        "sep": 9, "september": 9, "set": 9, "septiembre": 9, "tháng 9": 9, "tháng 09": 9,
        "oct": 10, "october": 10, "octubre": 10, "tháng 10": 10,
        "nov": 11, "november": 11, "noviembre": 11, "tháng 11": 11,
        "dec": 12, "december": 12, "dic": 12, "diciembre": 12, "tháng 12": 12,
    }

    # 1. Pattern: 30 December 2022 at 01:52 or 30 de diciembre de 2022 a las 01:52
    m1 = re.search(
        r"(\d{1,2})\s*(?:de\s+)?([A-Za-zÀ-ÿ]+|\d+)\s*(?:de\s+)?(\d{4})\s*(?:at|a las|lúc|,)?\s*(\d{1,2}):(\d{2})(?::(\d{2}))?",
        s_clean,
        re.IGNORECASE,
    )
    if m1:
        day = int(m1.group(1))
        m_str = m1.group(2).lower()
        year = int(m1.group(3))
        hour = int(m1.group(4))
        minute = int(m1.group(5))
        sec = int(m1.group(6)) if m1.group(6) else 0
        month = months_map.get(m_str)
        if month:
            try:
                dt = datetime(year, month, day, hour, minute, sec, tzinfo=reference_tz)
                return dt.isoformat(), "exact"
            except ValueError:
                pass

    # 2. Pattern: December 30, 2022 at 1:52 PM
    m2 = re.search(
        r"([A-Za-zÀ-ÿ]+)\s+(\d{1,2}),?\s+(\d{4})\s*(?:at|a las|lúc|,)?\s*(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(am|pm)?",
        s_clean,
        re.IGNORECASE,
    )
    if m2:
        m_str = m2.group(1).lower()
        day = int(m2.group(2))
        year = int(m2.group(3))
        hour = int(m2.group(4))
        minute = int(m2.group(5))
        sec = int(m2.group(6)) if m2.group(6) else 0
        ampm = (m2.group(7) or "").lower()
        if ampm == "pm" and hour < 12:
            hour += 12
        elif ampm == "am" and hour == 12:
            hour = 0
        month = months_map.get(m_str)
        if month:
            try:
                dt = datetime(year, month, day, hour, minute, sec, tzinfo=reference_tz)
                return dt.isoformat(), "exact"
            except ValueError:
                pass

    return None, "insufficient_evidence"


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
    """Detects text language ('vi', 'es', 'en', or 'unknown').
    Falls back to 'unknown' rather than blindly defaulting Latin to 'en'.
    """
    if not text:
        return "unknown"
    words = [w.lower() for w in re.findall(r"\b\w+\b", text)]
    if not words:
        return "unknown"

    # Vietnamese check
    vn_word_count = sum(1 for w in words if w in COMMON_VN_WORDS)
    if vn_word_count >= 2:
        return "vi"
    vn_chars = len(VIETNAMESE_DISTINCT_RE.findall(text))
    if vn_chars >= 3 and (vn_word_count >= 1 or vn_chars / len(text) > 0.02):
        return "vi"

    # Multi-language scoring: Spanish vs French vs English
    es_score = sum(1 for w in words if w in COMMON_ES_WORDS)
    fr_score = sum(1 for w in words if w in COMMON_FR_WORDS)
    en_score = sum(1 for w in words if w in COMMON_EN_WORDS)

    scores = [("es", es_score), ("fr", fr_score), ("en", en_score)]
    scores.sort(key=lambda x: x[1], reverse=True)
    top_lang, top_score = scores[0]
    second_lang, second_score = scores[1]

    if top_score >= 2 and top_score > second_score:
        return top_lang

    # Secondary check with accent clues
    if fr_score >= 1 and re.search(r"[éèêëàâùûçœîï]", text.lower()):
        return "fr"
    if es_score >= 1 and re.search(r"[áíóúñ¿¡]", text.lower()):
        return "es"
    if en_score >= 2:
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
    """Removes stale Chromium singleton lock files left behind by interrupted sessions.
    Verifies process liveness to NEVER remove locks of an actively running session.
    """
    p = Path(profile_dir)
    if not p.is_dir():
        return
    for lock_name in ("SingletonLock", "SingletonSocket", "SingletonCookie"):
        lock_file = p / lock_name
        try:
            if lock_file.is_symlink():
                try:
                    target = os.readlink(lock_file)
                    pid_match = re.search(r"-(\d+)\Z", target)
                    if pid_match:
                        pid = int(pid_match.group(1))
                        try:
                            os.kill(pid, 0)
                            logger.info(f"Preserving active lock {lock_name} held by running PID {pid}")
                            continue
                        except OSError:
                            pass
                except OSError:
                    pass
                lock_file.unlink(missing_ok=True)
            elif lock_file.exists():
                lock_file.unlink(missing_ok=True)
        except OSError as exc:
            logger.warning(f"Failed to unlink lock file {lock_file}: {exc}")


def auto_discover_anchors(page):
    """Automatically derives clean text anchor keywords from page title for anchor-free scraping."""
    try:
        title = page.title()
        if not title:
            return []

        # 1. Remove notification counts: "(1) ", "(99+) "
        clean = re.sub(r"^\(\d+\+?\)\s*", "", title).strip()
        # 2. Remove leading symbols/emojis
        clean = re.sub(r"^[^\w\s]+", "", clean).strip()
        # 3. Remove trailing "| Facebook"
        clean = re.sub(r"\s*\|\s*Facebook.*$", "", clean, flags=re.IGNORECASE).strip()

        if not clean or clean.lower() == "facebook":
            return []

        candidates = []
        if " - " in clean:
            parts = clean.split(" - ", 1)
            for part in parts:
                p_clean = re.sub(r"\.{2,}", "", part).strip()
                if len(p_clean) >= 4:
                    candidates.append(p_clean)
        else:
            candidates.append(re.sub(r"\.{2,}", "", clean).strip())

        anchors = []
        for cand in candidates:
            if cand:
                anchors.append(cand[:40].strip())
            # Unicode math bold/italic (\U0001D400-\U0001D7FF) & all international word characters
            words = re.findall(r"[\w\U0001D400-\U0001D7FF]{3,}", cand, flags=re.UNICODE)
            if words:
                anchors.append(" ".join(words[:4]))

        return [a for a in dict.fromkeys(anchors) if a]
    except Exception as exc:
        logger.warning(f"Error in auto_discover_anchors: {exc}")
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


SCOPED_EXTRACTION_JS = r"""(postUrl) => {
    const res = {
        card_found: false,
        text: null,
        published_at_raw: null,
        like_count_raw: null,
        comment_count_raw: null,
        comment_is_zero: false,
        share_count_raw: null,
        view_count_raw: null,
        media_type: 'text',
        ts_coords: null,
        author_name: null,
        review_reasons: []
    };

    let card = null;
    let msg = null;

    // 1a. If postUrl is provided, match anchor matching the permalink/ID first
    if (postUrl) {
        const cleanPath = postUrl.replace(/^https?:\/\/(www\.)?facebook\.com\/?/, '').split('?')[0].replace(/\/$/, '');
        const allAnchors = Array.from(document.querySelectorAll('a[href]'));
        for (const a of allAnchors) {
            const h = a.href || '';
            if (cleanPath && h.includes(cleanPath)) {
                let curr = a;
                while (curr && curr !== document.body) {
                    if (curr.querySelector('[role="toolbar"], [aria-label*="reactions" i], [aria-label*="Like" i]')) {
                        card = curr;
                        break;
                    }
                    curr = curr.parentElement;
                }
                if (card) break;
            }
        }
    }

    // 1b. Fallback: match first message container with its toolbar ancestor
    if (!card) {
        msg = document.querySelector('div[data-ad-preview="message"], div[data-ad-comet-preview="message"]');
        if (msg) {
            let curr = msg;
            while (curr && curr !== document.body) {
                if (curr.querySelector('[role="toolbar"], [aria-label*="reactions" i], [aria-label*="Like" i]')) {
                    card = curr;
                    break;
                }
                curr = curr.parentElement;
            }
        }
    }

    // 1c. Fallback: find post card via post permalink or toolbar in main
    if (!card) {
        const main = document.querySelector('[role="main"]') || document.body;
        const postLink = Array.from(main.querySelectorAll('a')).find(a => {
            const h = a.href || '';
            if (h.includes('comment_id=')) return false;
            if (a.closest('[role="article"]') && a.closest('[role="article"]').getAttribute('aria-label')?.toLowerCase().includes('comment')) return false;
            return (h.includes('/posts/') || h.includes('permalink.php') || h.includes('story_fbid=') || h.includes('/share/p/')) && a.getBoundingClientRect().width > 15 && a.getBoundingClientRect().x < 500;
        });

        if (postLink) {
            let curr = postLink;
            while (curr && curr !== main) {
                if (curr.querySelector('[role="toolbar"], [aria-label*="reactions" i], [aria-label*="Like" i]')) {
                    card = curr;
                    break;
                }
                curr = curr.parentElement;
            }
        }
    }

    if (!card) {
        res.review_reasons.push("Could not isolate distinct post container from comments/feed.");
        return res;
    }

    res.card_found = true;

    // 2. Expand "See more" / "Xem thêm" / "Ver más"
    const moreBtns = Array.from(card.querySelectorAll('[role="button"], span[role="button"], div[role="button"]')).filter(b => {
        const t = (b.innerText || '').trim();
        return /^(?:see more|xem thêm|ver más|mehr anzeigen|voir plus)$/i.test(t);
    });
    moreBtns.forEach(b => { try { b.click(); } catch(e) {} });

    // 3. Extract text strictly from card
    const cardMsg = card.querySelector('div[data-ad-preview="message"], div[data-ad-comet-preview="message"]');
    if (cardMsg) {
        res.text = cardMsg.innerText.trim();
    } else {
        const textNodes = Array.from(card.querySelectorAll('div[dir="auto"], span[dir="auto"]')).filter(d => {
            if (d.closest('[role="toolbar"]') || d.closest('[role="article"]') || d.closest('form')) return false;
            const t = (d.innerText || '').trim();
            return t.length > 20;
        });
        textNodes.sort((a, b) => (b.innerText || '').length - (a.innerText || '').length);
        if (textNodes.length > 0) res.text = textNodes[0].innerText.trim();
    }

    // 4. Locate timestamp link in card
    const postLink = Array.from(card.querySelectorAll('a')).find(a => {
        const href = a.href || '';
        if (href.includes('comment_id=')) return false;
        if (a.closest('[role="article"]') && a.closest('[role="article"]').getAttribute('aria-label')?.toLowerCase().includes('comment')) return false;
        return (href.includes('/posts/') || href.includes('permalink.php') || href.includes('story_fbid=')) && a.getBoundingClientRect().width > 15;
    });

    if (postLink) {
        const aria = (postLink.getAttribute('aria-label') || '').trim();
        if (aria && /\d/.test(aria)) {
            res.published_at_raw = aria;
        }
        const r = postLink.getBoundingClientRect();
        if (r.width > 0 && r.height > 0) {
            res.ts_coords = { x: r.x + r.width / 2, y: r.y + r.height / 2 };
        }
    }

    // 5. Metrics
    // 5.0 Compact metric summary row: e.g. "44\n2\n6" or "44\n2"
    const metricDivs = Array.from(card.querySelectorAll('div')).filter(d => 
        /^\d[\d.,]*[KkMmB]?\n\d[\d.,]*[KkMmB]?(\n\d[\d.,]*[KkMmB]?)?$/.test((d.innerText || '').trim())
    );
    if (metricDivs.length > 0) {
        const parts = metricDivs[0].innerText.trim().split('\n').map(s => s.trim());
        if (parts.length >= 1 && !res.like_count_raw) res.like_count_raw = parts[0];
        if (parts.length >= 2 && !res.comment_count_raw) res.comment_count_raw = parts[1];
        if (parts.length >= 3 && !res.share_count_raw) res.share_count_raw = parts[2];
    }

    // 5.1 Reactions / Likes
    if (!res.like_count_raw) {
        const reactionEl = card.querySelector('[aria-label*="reacted to this" i], [aria-label*="reactions" i], [aria-label*="bày tỏ cảm xúc" i]');
        if (reactionEl) {
            const m = (reactionEl.getAttribute('aria-label') || '').match(/([\d.,]+[KkMmB]?|\d+)/);
            res.like_count_raw = m ? m[1] : reactionEl.innerText.trim();
        } else {
            const tbText = card.querySelector('[role="toolbar"]')?.innerText || '';
            const m = tbText.match(/\b(\d[\d.,]*[KkMmB]?)\b/);
            if (m) res.like_count_raw = m[1];
        }
    }

    // 5.2 Comments
    const cardText = card.innerText || '';
    if (!res.comment_count_raw) {
        const cMatch = cardText.match(/\b(\d[\d.,]*[KkMmB]?)\s+(?:comments?|bình luận|comentarios)\b/i);
        if (cMatch) {
            res.comment_count_raw = cMatch[1];
        } else if (/no comments yet|chưa có bình luận|sé el primero en comentar/i.test(cardText)) {
            res.comment_is_zero = true;
        }
    }

    // 5.3 Shares
    if (!res.share_count_raw) {
        const sMatch = cardText.match(/\b(\d[\d.,]*[KkMmB]?)\s+(?:shares?|lượt chia sẻ|veces compartido)\b/i);
        if (sMatch) res.share_count_raw = sMatch[1];
    }

    // 5.4 Views
    const vMatch = cardText.match(/\b(\d[\d.,]*[KkMmB]?)\s+(?:views?|lượt xem|reproducciones)\b/i);
    if (vMatch) res.view_count_raw = vMatch[1];

    // 6. Media type
    const vids = Array.from(card.querySelectorAll('video')).filter(v => v.clientWidth >= 150 && v.clientHeight >= 150);
    if (vids.length > 0 || card.querySelector('[data-video-id]')) {
        res.media_type = 'video';
    } else {
        const imgs = Array.from(card.querySelectorAll('img')).filter(img => {
            const w = img.naturalWidth || img.clientWidth || 0;
            const h = img.naturalHeight || img.clientHeight || 0;
            const src = img.src || '';
            const alt = img.alt || '';
            const isEmoji = src.includes('emoji.php') || src.includes('rsrc.php');
            const isAvatar = (w < 100 && h < 100);
            const isFbPhotoAlt = /^May be an image of/i.test(alt) || /^Có thể là hình ảnh về/i.test(alt) || /^No photo description/i.test(alt);
            return (!isEmoji && !isAvatar && (w >= 140 || h >= 140)) || isFbPhotoAlt;
        });
        if (imgs.length > 0 || card.querySelector('a[href*="/photo"]')) {
            res.media_type = 'image';
        } else if (card.querySelector('a[target="_blank"][rel*="nofollow"], a[href*="l.facebook.com"]')) {
            res.media_type = 'link';
        }
    }

    // 7. Author Name / Heading
    const authorEl = card.querySelector('h2, h3, h4, strong');
    if (authorEl) res.author_name = authorEl.innerText.trim();

    return res;
};"""


def extract_scoped_post_data(page, post_url=None, default_sport=None, reference_tz=VIETNAM_TZ):
    """Extracts post text, metrics, published timestamp, and media scoped strictly to the post container.
    Returns:
        (record_17_col, tech_metadata)
    """
    url = post_url or page.url
    page_text = ""
    try:
        page_text = (page.evaluate("() => document.body.innerText") or "").lower()
    except Exception:
        pass

    # Check for deleted/unavailable content
    if any(term in page_text for term in [
        "this content isn't available right now",
        "content not found",
        "bài viết này không còn hiển thị",
        "trang này không tồn tại",
        "este contenido no está disponible",
    ]):
        tech_meta = {
            "status": "unavailable",
            "review_reasons": ["Post is unavailable, deleted, or restricted by privacy settings."],
            "raw_published_at": None,
            "time_precision": "insufficient_evidence",
            "is_approximate": {},
            "missing_reasons": {"text": "post_unavailable"},
        }
        record = build_dataset_record(
            post_url=url,
            text=None,
            sport_category=default_sport,
        )
        return record, tech_meta

    # Execute scoped DOM evaluation
    try:
        dom_res = page.evaluate(SCOPED_EXTRACTION_JS, url)
    except Exception as exc:
        logger.error(f"DOM evaluation error on {url}: {exc}")
        dom_res = {
            "card_found": False,
            "text": None,
            "published_at_raw": None,
            "like_count_raw": None,
            "comment_count_raw": None,
            "comment_is_zero": False,
            "share_count_raw": None,
            "view_count_raw": None,
            "media_type": "text",
            "ts_coords": None,
            "review_reasons": [f"DOM evaluation failed: {exc}"],
        }

    raw_published = dom_res.get("published_at_raw")
    ts_coords = dom_res.get("ts_coords")

    # If timestamp was not directly available in aria-label, hover over coords to trigger tooltip
    if (not raw_published or not re.search(r"\d", raw_published)) and ts_coords:
        try:
            page.mouse.move(ts_coords["x"], ts_coords["y"])
            page.wait_for_timeout(700)
            tooltip_txt = page.evaluate(r'''() => {
                const tooltips = Array.from(document.querySelectorAll('[role="tooltip"]'));
                const found = tooltips.map(t => (t.innerText || '').trim()).find(t => t.length > 5 && /\d/.test(t));
                return found || null;
            }''')
            if tooltip_txt:
                raw_published = tooltip_txt
        except Exception as exc:
            logger.debug(f"Tooltip hover error: {exc}")

    # Standardize published timestamp
    iso_published, time_precision = parse_facebook_timestamp(raw_published, reference_tz=reference_tz)

    # Process counts with approximation metadata
    like_val, like_approx = parse_numeric_count_with_meta(dom_res.get("like_count_raw"))
    
    if dom_res.get("comment_is_zero"):
        comment_val, comment_approx = 0, False
    else:
        comment_val, comment_approx = parse_numeric_count_with_meta(dom_res.get("comment_count_raw"))

    share_val, share_approx = parse_numeric_count_with_meta(dom_res.get("share_count_raw"))
    view_val, view_approx = parse_numeric_count_with_meta(dom_res.get("view_count_raw"))

    # Author and post IDs
    final_post_id = extract_post_id(page.url) or extract_post_id(url)
    final_author_id = extract_author_id(page.url) or extract_author_id(url)

    # Sport category normalization
    extracted_text = dom_res.get("text")
    normalized_sport = validate_or_normalize_sport(default_sport)
    if not normalized_sport and extracted_text:
        normalized_sport = validate_or_normalize_sport(extracted_text)

    # Compile review reasons and missing field reasons
    review_reasons = list(dom_res.get("review_reasons") or [])
    missing_reasons = {}

    if not extracted_text:
        missing_reasons["text"] = "text_not_found"
        review_reasons.append("Post caption text could not be extracted.")
    if iso_published is None:
        missing_reasons["published_at"] = time_precision
    if like_val is None:
        missing_reasons["like_count"] = "no_reaction_badge_found"
    if comment_val is None:
        missing_reasons["comment_count"] = "no_comment_count_found"
    if share_val is None:
        missing_reasons["share_count"] = "no_share_count_found"
    if view_val is None:
        missing_reasons["view_count"] = "view_count_not_displayed"
    if not normalized_sport:
        missing_reasons["sport_category"] = "unidentified_sport"

    # Status classification
    if not dom_res.get("card_found"):
        status = "needs_review"
    elif extracted_text and not review_reasons:
        status = "success"
    elif extracted_text:
        status = "needs_review"
    else:
        status = "failed"

    tech_meta = {
        "status": status,
        "review_reasons": review_reasons,
        "raw_published_at": raw_published,
        "time_precision": time_precision,
        "is_approximate": {
            "like_count": like_approx,
            "comment_count": comment_approx,
            "share_count": share_approx,
            "view_count": view_approx,
        },
        "missing_reasons": missing_reasons,
    }

    record_17_col = build_dataset_record(
        post_url=page.url if page.url and "facebook.com" in page.url else url,
        text=extracted_text,
        published_at=iso_published,
        author_id=final_author_id,
        post_id=final_post_id,
        sport_category=normalized_sport,
        like_count=like_val,
        comment_count=comment_val,
        share_count=share_val,
        view_count=view_val,
        media_type=dom_res.get("media_type", "text"),
    )

    return record_17_col, tech_meta

