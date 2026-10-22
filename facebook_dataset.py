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
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

logger = logging.getLogger("facebook_dataset")
VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# Statistical language detector singleton using lingua-language-detector
_LINGUA_DETECTOR = None
_LINGUA_AVAILABLE = None


def _get_lingua_detector():
    global _LINGUA_DETECTOR, _LINGUA_AVAILABLE
    if _LINGUA_AVAILABLE is False:
        return None
    if _LINGUA_DETECTOR is not None:
        return _LINGUA_DETECTOR
    try:
        from lingua import Language, LanguageDetectorBuilder
        supported_langs = [
            Language.ENGLISH,
            Language.VIETNAMESE,
            Language.SPANISH,
            Language.FRENCH,
            Language.PORTUGUESE,
            Language.TAGALOG,
            Language.GERMAN,
            Language.ITALIAN,
            Language.JAPANESE,
            Language.CHINESE,
            Language.INDONESIAN,
            Language.THAI,
            Language.ARABIC,
        ]
        _LINGUA_DETECTOR = LanguageDetectorBuilder.from_languages(*supported_langs).build()
        _LINGUA_AVAILABLE = True
        return _LINGUA_DETECTOR
    except Exception as exc:
        logger.warning(f"lingua-language-detector initialization error: {exc}. Using heuristic fallback.")
        _LINGUA_AVAILABLE = False
        return None


LINGUA_LANG_MAP = {
    "ENGLISH": "en",
    "VIETNAMESE": "vi",
    "SPANISH": "es",
    "FRENCH": "fr",
    "PORTUGUESE": "pt",
    "TAGALOG": "tl",
    "GERMAN": "de",
    "ITALIAN": "it",
    "JAPANESE": "ja",
    "CHINESE": "zh",
    "INDONESIAN": "id",
    "THAI": "th",
    "ARABIC": "ar",
}

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

# Regex to extract mentions (@word, excluding email addresses)
MENTION_RE = re.compile(r"(?<![\w])@[\w\.]+", re.UNICODE)

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

    // 4.4. View count: Verified metric candidate only (default null, exclude command verbs & caption text)
    res.view_count_raw = null;
    res.view_status = "not_displayed";
    const isInvalidViewCmd = (str) => {
        if (!str) return true;
        const s = str.trim();
        return /^(?:view|xem|ver)\s+(?:profile|more|trang cá nhân|thêm|replies|câu trả lời|bình luận|comments|photos?|ảnh|videos?|history|insights|posts?)/i.test(s) ||
               /(?:more\s+comments|replies|câu trả lời|bình luận\s+khác)$/i.test(s) ||
               /target\s+is\s+\d+\s+views/i.test(s);
    };
    const viewMetricPat = /^(?:([\d.,]+[KkMmB]?|\d+\s*(?:nghìn|ngàn|triệu|tr|mil|millones))\s+(?:views?|lượt xem|reproducciones))$/i;
    const candEls = Array.from(postContainer.querySelectorAll('span, div, [role="button"], a')).filter(el => {
        if (el.closest('div[data-ad-preview="message"], div[data-ad-comet-preview="message"]')) return false;
        if (el.closest('ul, [role="article"] [role="article"]')) return false;
        const aria = (el.getAttribute('aria-label') || '').trim();
        const txt = (el.innerText || '').trim();
        if (isInvalidViewCmd(aria) || isInvalidViewCmd(txt)) return false;
        return viewMetricPat.test(aria) || viewMetricPat.test(txt);
    });
    for (const el of candEls) {
        const aria = (el.getAttribute('aria-label') || '').trim();
        const txt = (el.innerText || '').trim();
        const m = aria.match(viewMetricPat) || txt.match(viewMetricPat);
        if (m) {
            res.view_count_raw = m[1];
            res.view_status = "verified";
            break;
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
    text = str(value).replace("\u00a0", " ").replace("\u202f", " ").strip().lower()
    if not text or not re.search(r"\d", text):
        return None, False

    is_approximate = bool(re.search(r"[kmb]|nghìn|ngàn|triệu|tr|tỷ|ty|mil\b|millón|millones|million|billion", text))

    # Replace language-specific suffixes
    text = re.sub(r"\b(nghìn|ngàn|mil)\b", "k", text)
    text = re.sub(r"\b(triệu|tr|millón|millones|million|millions)\b", "m", text)
    text = re.sub(r"\b(tỷ|ty|mil millones|billion|billions)\b", "b", text)
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


def parse_facebook_timestamp(raw, reference_tz=VIETNAM_TZ, allow_relative=False, reference_dt=None):
    """Normalizes Facebook timestamp strings into ISO 8601 with timezone.
    Returns (iso_string, precision).
    When there is sufficient evidence (exact day, month, year, hour, minute), returns (iso_str, 'exact').
    When evidence is incomplete (e.g. '2 h', '3y', 'yesterday'), returns (None, 'insufficient_evidence')
    by default to preserve time integrity.
    If allow_relative=True, converts relative strings like 'an hour ago', '2 hours ago', '30m', 'just now'
    into approximate ISO timestamps with precision 'relative_approximate'.
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

    # 3. Vietnamese Pattern: 30 tháng 12, 2022 lúc 18:30 or 30 tháng 12 năm 2022 lúc 18:30
    mVn = re.search(
        r"(\d{1,2})\s+tháng\s+(\d{1,2})(?:[,\s]+(?:năm\s+)?)\s*(\d{4})\s*(?:lúc|,)?\s*(\d{1,2}):(\d{2})(?::(\d{2}))?",
        s_clean,
        re.IGNORECASE,
    )
    if mVn:
        day = int(mVn.group(1))
        month = int(mVn.group(2))
        year = int(mVn.group(3))
        hour = int(mVn.group(4))
        minute = int(mVn.group(5))
        sec = int(mVn.group(6)) if mVn.group(6) else 0
        try:
            dt = datetime(year, month, day, hour, minute, sec, tzinfo=reference_tz)
            return dt.isoformat(), "exact"
        except ValueError:
            pass

    # 4. Relative time fallback: "an hour ago", "X hours ago", "X mins ago", "just now", "X ngày trước"
    if allow_relative:
        from datetime import timedelta
        base_now = reference_dt or datetime.now(timezone.utc).astimezone(reference_tz)
        clean_lower = text.lower().strip()

        if re.search(r"\b(?:just now|vừa xong)\b", clean_lower):
            return base_now.isoformat(), "relative_approximate"

        if re.search(r"\b(?:an?\s+hour\s+ago|1\s*h(?:our)?\s+ago|1\s*giờ\s+trước)\b", clean_lower):
            dt = base_now - timedelta(hours=1)
            return dt.isoformat(), "relative_approximate"

        m_hours = re.search(r"\b(\d+)\s*(?:hours?|hrs?|h|giờ)\s*(?:ago|trước)?\b", clean_lower)
        if m_hours:
            dt = base_now - timedelta(hours=int(m_hours.group(1)))
            return dt.isoformat(), "relative_approximate"

        m_mins = re.search(r"\b(\d+)\s*(?:minutes?|mins?|m|phút)\s*(?:ago|trước)?\b", clean_lower)
        if m_mins:
            dt = base_now - timedelta(minutes=int(m_mins.group(1)))
            return dt.isoformat(), "relative_approximate"

        m_days = re.search(r"\b(\d+)\s*(?:days?|d|ngày)\s*(?:ago|trước)?\b", clean_lower)
        if m_days:
            dt = base_now - timedelta(days=int(m_days.group(1)))
            return dt.isoformat(), "relative_approximate"

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


SPORT_RULES = {
    "Football": re.compile(
        r"\b(football|soccer|bóng đá|cầu thủ|chuyển nhượng|transfer news|premier league|champions league|la liga|serie a|bundesliga|v-league|world cup|tiền đạo|thủ môn|hậu vệ|hlv|hat-trick|penalty|fifa|uefa|balón de oro|ballon d'or|here we go|fichaje|real madrid|barcelona|manchester united|liverpool|arsenal|chelsea|man city|bayern|juventus|psg)\b",
        re.IGNORECASE,
    ),
    "Basketball": re.compile(
        r"\b(basketball|bóng rổ|nba|wnba|euroleague|vba|fiba|slam dunk|dunk|dunks|three-pointer|3-pointer|3pt|3pts|ném 3 điểm|buzzer beater|triple-double|double-double|alley-oop|layup|free throw|field goal|pts|reb|ast|rebounds?|assists?|point guard|shooting guard|small forward|power forward|lakers|celtics|bulls|bucks|nuggets|76ers|sixers|knicks|mavericks|mavs|clippers|timberwolves|cavaliers|cavs|pacers|grizzlies|pelicans|trail blazers|raptors|hornets|pistons|san antonio spurs|miami heat|utah jazz|orlando magic|okc thunder|oklahoma city thunder|washington wizards|phoenix suns|sacramento kings|brooklyn nets|atlanta hawks|houston rockets|lebron|steph curry|stephen curry|giannis|tatum|jokic|jokić|doncic|dončić|wembanyama|durant|draymond green|dubs|gsw|heat nation|raptors nation|bad boys)\b|🏀|(?<!\bue\s)(?<!\bred\s)\bwarriors\b",
        re.IGNORECASE,
    ),
    "Tennis": re.compile(
        r"\b(tennis|quần vợt|grand slam|wimbledon|roland garros|us open|australian open|atp|wta|tay vợt|vợt tennis)\b",
        re.IGNORECASE,
    ),
    "Athletics": re.compile(
        r"\b(athletics|điền kinh|marathon|maratón|maratona|half marathon|bán marathon|chạy bộ|running|runner|10k|21k|42k|chạy 100m|chạy 200m|chạy 400m|atletismo|world athletics|rfea)\b",
        re.IGNORECASE,
    ),
    "Motorsport": re.compile(
        r"\b(motorsport|đua xe|f1|formula 1|formula one|motogp|tay đua|grand prix)\b",
        re.IGNORECASE,
    ),
    "Volleyball": re.compile(
        r"\b(volleyball|bóng chuyền|fivb)\b",
        re.IGNORECASE,
    ),
    "Badminton": re.compile(
        r"\b(badminton|cầu lông|bwf)\b",
        re.IGNORECASE,
    ),
    "Swimming": re.compile(
        r"\b(swimming|bơi lội|bơi lội việt nam|kỷ lục bơi|world aquatics)\b",
        re.IGNORECASE,
    ),
    "Martial Arts": re.compile(
        r"\b(martial arts|võ thuật|ufc|mma|boxing|quyền anh|taekwondo|karate|judo|bjj|one championship)\b",
        re.IGNORECASE,
    ),
    "Table Tennis": re.compile(
        r"\b(table tennis|bóng bàn|ping pong|ittf)\b",
        re.IGNORECASE,
    ),
    "Golf": re.compile(
        r"\b(golf|pga tour|liv golf|golfer|green fees)\b",
        re.IGNORECASE,
    ),
    "Rugby": re.compile(
        r"\b(rugby|bóng bầu dục)\b",
        re.IGNORECASE,
    ),
    "Baseball": re.compile(
        r"\b(baseball|bóng chày|mlb)\b",
        re.IGNORECASE,
    ),
    "Gymnastics": re.compile(
        r"\b(gymnastics|thể dục dụng cụ)\b",
        re.IGNORECASE,
    ),
    "Chess": re.compile(
        r"\b(chess|cờ vua|fide|grandmaster|đại kiện tướng)\b",
        re.IGNORECASE,
    ),
    "Shooting": re.compile(
        r"\b(target shooting|sport shooting|bắn súng thể thao|bắn súng|xạ thủ|issf|bắn súng đĩa bay|10m air rifle|50m rifle)\b",
        re.IGNORECASE,
    ),
    "Weightlifting": re.compile(
        r"\b(weightlifting|cử tạ)\b",
        re.IGNORECASE,
    ),
    "Cycling": re.compile(
        r"\b(cycling|đua xe đạp|tour de france|giro d'italia|xe đạp thể thao)\b",
        re.IGNORECASE,
    ),
}

NON_SPORT_PATTERN = re.compile(
    r"\b(khai trương|quán cà phê|quán cafe|coffee shop|grand opening|nhà hàng|restaurant|thực đơn|menu đồ uống|buffet|ca nhạc|concert|liveshow|album mới|bất động sản|real estate|khuyến mãi khủng|sale off)\b",
    re.IGNORECASE,
)

# -------------------------------------------------------------
# NBA COMPETITION & EVIDENCE CLASSIFIER RULES
# -------------------------------------------------------------
NON_NBA_COMPETITION_RULES = {
    "WNBA": re.compile(
        r"\b(wnba|women's nba|caitlin clark|angel reese|new york liberty|las vegas aces|seattle storm|minnesota lynx|phoenix mercury|chicago sky|indiana fever|dallas wings|atlanta dream|connecticut sun|los angeles sparks|washington mystics)\b",
        re.IGNORECASE,
    ),
    "UAAP": re.compile(
        r"\b(uaap|university athletic association of the philippines|ue red warriors|up fighting maroons|ust growling tigers|ateneo blue eagles|de la salle green archers|adamson soaring falcons|nu bulldogs|feu tamaraws)\b",
        re.IGNORECASE,
    ),
    "EuroLeague": re.compile(
        r"\b(euroleague|euro league|panathinaikos|olympiacos|fenerbahce|anadolu efes|partizan|crvena zvezda|monaco basket|baskonia|virtus bologna|eurocup)\b",
        re.IGNORECASE,
    ),
    "VBA": re.compile(
        r"\b(vba|vietnam basketball association|saigon heat|thang long warriors|danang dragons|cantho catfish|hanoi buffaloes|nha trang dolphins|ho chi minh city wings)\b",
        re.IGNORECASE,
    ),
    "FIBA": re.compile(
        r"\b(fiba|fiba world cup|eurobasket|afrobasket|americup|fiba asia cup|olympic basketball)\b",
        re.IGNORECASE,
    ),
    "NCAA": re.compile(
        r"\b(ncaa|march madness|college basketball|final four|ncaa tournament|acc basketball|big ten basketball|sec basketball)\b",
        re.IGNORECASE,
    ),
    "NBA2K": re.compile(
        r"\b(nba 2k|nba2k|nba 2k2[0-9]|2k2[0-9]|mycareer|myteam|nba 2k league|ronnie 2k)\b",
        re.IGNORECASE,
    ),
    "GLeague": re.compile(
        r"\b(g league|g-league|nba g league|gleague|rip city remix|motor city cruise|delaware blue coats|austin spurs|south bay lakers|capital city go-go|santa cruz warriors|fort wayne mad ants|indiana mad ants|sioux falls skyforce|texas legends|birmingham squadron|college park skyhawks|greenboro swarm|iowa wolves|long island nets|maine celtics|memphis hustle|ontario clippers|osceola magic|raptors 905|salt lake city stars|stockton kings|westchester knicks|windy city bulls|wisconsin herd)\b",
        re.IGNORECASE,
    ),
}

NBA_EXPLICIT_RULE = re.compile(
    r"\b(nba finals|nba playoffs|nba all-star|nba in-season tournament|in-season tournament|nba cup|nba draft|nba champion|nba championship|kia nba|nba on espn|nba on tnt|#nbafinals|#nbaplayoffs|#nbaallstar|#nba|(?<![Ww])nba|@nba)\b",
    re.IGNORECASE,
)

NBA_TEAMS_RULE = re.compile(
    r"\b(lakers|los angeles lakers|la lakers|celtics|boston celtics|golden state warriors|gsw|dubs|chicago bulls|bulls|milwaukee bucks|bucks|denver nuggets|nuggets|philadelphia 76ers|76ers|sixers|phila 76ers|dallas mavericks|mavericks|mavs|clippers|la clippers|los angeles clippers|timberwolves|minnesota timberwolves|wolves|cleveland cavaliers|cavaliers|cavs|indiana pacers|memphis grizzlies|grizzlies|new orleans pelicans|pelicans|portland trail blazers|trail blazers|blazers|toronto raptors|raptors|charlotte hornets|hornets|detroit pistons|pistons|san antonio spurs|spurs|miami heat|utah jazz|orlando magic|okc thunder|oklahoma city thunder|thunder|washington wizards|wizards|phoenix suns|sacramento kings|brooklyn nets|atlanta hawks|hawks|houston rockets|rockets|new york knicks|knicks)\b|(?<!\bue\s)(?<!\bred\s)\bwarriors\b",
    re.IGNORECASE,
)

NBA_PERSONS_RULE = re.compile(
    r"\b(lebron james|lebron|steph curry|stephen curry|kevin durant|giannis antetokounmpo|giannis|nikola jokic|nikola jokić|jokic|jokić|luka doncic|luka dončić|doncic|dončić|jayson tatum|tatum|victor wembanyama|wembanyama|wemby|anthony davis|jimmy butler|joel embiid|embiid|anthony edwards|shai gilgeous-alexander|sga|devin booker|booker|donovan mitchell|kyrie irving|kyrie|kawhi leonard|kawhi|paul george|damian lillard|dame lillard|jalen brunson|brunson|bam adebayo|tyrese haliburton|chet holmgren|ja morant|zion williamson|russell westbrook|james harden|chris paul|klay thompson|draymond green|steve kerr|erik spoelstra|gregg popovich|doc rivers|tyronn lue|kristaps porzingis|kristaps porziņģis|dennis schroder|dennis schröder|schroder|schröder|jonas valanciunas|jonas valančiūnas|valanciunas|valančiūnas|bogdan bogdanovic|bogdan bogdanović|bojan bogdanovic|bojan bogdanović|deni avdija|alperen sengun|alperen şengün|sengun|şengün)\b",
    re.IGNORECASE,
)

BASKETBALL_ACTION_RULE = re.compile(
    r"\b(three-pointer|3-pointer|3pt|3pts|triple-double|double-double|slam dunk|dunk|buzzer beater|alley-oop|free throw|field goal|fg%|rebounds?|assists?|steals?|blocks?|turnovers?|plus-minus|\+\/-|tip-off|half-time|overtime|box score|roster|trade deadline|head coach|point guard|shooting guard|small forward|power forward|center)\b",
    re.IGNORECASE,
)

COMMERCIAL_PROMO_RULE = re.compile(
    r"\b(we are hiring|apply now|job vacancy|job opening|tuyển dụng|store discount|promo code|coupon code|buy tickets now|ticket presale|shop now|merchandise sale|sweepstakes|enter to win|giveaway alert)\b",
    re.IGNORECASE,
)

# -------------------------------------------------------------
# NBA SOURCE REGISTRY FOR CONTEXTUAL CLASSIFICATION
# -------------------------------------------------------------
NBA_SOURCE_REGISTRY = {
    # Official NBA
    "nba": {"category": "official_nba", "franchise": None, "competition": "NBA"},
    "nbacom": {"category": "official_nba", "franchise": None, "competition": "NBA"},

    # Official 30 NBA Franchises (handles & canonical names)
    "warriors": {"category": "official_nba_team", "franchise": "Golden State Warriors", "competition": "NBA"},
    "losangeleslakers": {"category": "official_nba_team", "franchise": "Los Angeles Lakers", "competition": "NBA"},
    "lakers": {"category": "official_nba_team", "franchise": "Los Angeles Lakers", "competition": "NBA"},
    "celtics": {"category": "official_nba_team", "franchise": "Boston Celtics", "competition": "NBA"},
    "chicagobulls": {"category": "official_nba_team", "franchise": "Chicago Bulls", "competition": "NBA"},
    "bulls": {"category": "official_nba_team", "franchise": "Chicago Bulls", "competition": "NBA"},
    "miamiheat": {"category": "official_nba_team", "franchise": "Miami Heat", "competition": "NBA"},
    "heat": {"category": "official_nba_team", "franchise": "Miami Heat", "competition": "NBA"},
    "raptors": {"category": "official_nba_team", "franchise": "Toronto Raptors", "competition": "NBA"},
    "spurs": {"category": "official_nba_team", "franchise": "San Antonio Spurs", "competition": "NBA"},
    "pacers": {"category": "official_nba_team", "franchise": "Indiana Pacers", "competition": "NBA"},
    "detroitpistons": {"category": "official_nba_team", "franchise": "Detroit Pistons", "competition": "NBA"},
    "trailblazers": {"category": "official_nba_team", "franchise": "Portland Trail Blazers", "competition": "NBA"},
    "cavs": {"category": "official_nba_team", "franchise": "Cleveland Cavaliers", "competition": "NBA"},
    "cavaliers": {"category": "official_nba_team", "franchise": "Cleveland Cavaliers", "competition": "NBA"},
    "timberwolves": {"category": "official_nba_team", "franchise": "Minnesota Timberwolves", "competition": "NBA"},
    "denvernuggets": {"category": "official_nba_team", "franchise": "Denver Nuggets", "competition": "NBA"},
    "nuggets": {"category": "official_nba_team", "franchise": "Denver Nuggets", "competition": "NBA"},
    "philadelphia76ers": {"category": "official_nba_team", "franchise": "Philadelphia 76ers", "competition": "NBA"},
    "sixers": {"category": "official_nba_team", "franchise": "Philadelphia 76ers", "competition": "NBA"},
    "dallasmavericks": {"category": "official_nba_team", "franchise": "Dallas Mavericks", "competition": "NBA"},
    "mavs": {"category": "official_nba_team", "franchise": "Dallas Mavericks", "competition": "NBA"},
    "laclippers": {"category": "official_nba_team", "franchise": "Los Angeles Clippers", "competition": "NBA"},
    "clippers": {"category": "official_nba_team", "franchise": "Los Angeles Clippers", "competition": "NBA"},
    "memphisgrizzlies": {"category": "official_nba_team", "franchise": "Memphis Grizzlies", "competition": "NBA"},
    "grizzlies": {"category": "official_nba_team", "franchise": "Memphis Grizzlies", "competition": "NBA"},
    "pelicans": {"category": "official_nba_team", "franchise": "New Orleans Pelicans", "competition": "NBA"},
    "hornets": {"category": "official_nba_team", "franchise": "Charlotte Hornets", "competition": "NBA"},
    "utahjazz": {"category": "official_nba_team", "franchise": "Utah Jazz", "competition": "NBA"},
    "orlandomagic": {"category": "official_nba_team", "franchise": "Orlando Magic", "competition": "NBA"},
    "okcthunder": {"category": "official_nba_team", "franchise": "Oklahoma City Thunder", "competition": "NBA"},
    "wizards": {"category": "official_nba_team", "franchise": "Washington Wizards", "competition": "NBA"},
    "suns": {"category": "official_nba_team", "franchise": "Phoenix Suns", "competition": "NBA"},
    "kings": {"category": "official_nba_team", "franchise": "Sacramento Kings", "competition": "NBA"},
    "brooklynnets": {"category": "official_nba_team", "franchise": "Brooklyn Nets", "competition": "NBA"},
    "hawks": {"category": "official_nba_team", "franchise": "Atlanta Hawks", "competition": "NBA"},
    "houstonrockets": {"category": "official_nba_team", "franchise": "Houston Rockets", "competition": "NBA"},
    "rockets": {"category": "official_nba_team", "franchise": "Houston Rockets", "competition": "NBA"},
    "nyknicks": {"category": "official_nba_team", "franchise": "New York Knicks", "competition": "NBA"},
    "bucks": {"category": "official_nba_team", "franchise": "Milwaukee Bucks", "competition": "NBA"},

    # Verified NBA-focused media & fan communities
    "heatnation": {"category": "nba_focused_media", "franchise": "Miami Heat", "competition": "NBA"},
    "8points9seconds": {"category": "nba_focused_media", "franchise": "Indiana Pacers", "competition": "NBA"},
    "raptorsnation": {"category": "nba_focused_media", "franchise": "Toronto Raptors", "competition": "NBA"},
    "raptorsrepublic": {"category": "nba_focused_media", "franchise": "Toronto Raptors", "competition": "NBA"},
    "airalamo": {"category": "nba_focused_media", "franchise": "San Antonio Spurs", "competition": "NBA"},
    "detroitbadboys": {"category": "nba_focused_media", "franchise": "Detroit Pistons", "competition": "NBA"},
    "fearthesword": {"category": "nba_focused_media", "franchise": "Cleveland Cavaliers", "competition": "NBA"},
    "blazersedge": {"category": "nba_focused_media", "franchise": "Portland Trail Blazers", "competition": "NBA"},
    "libertyballers": {"category": "nba_focused_media", "franchise": "Philadelphia 76ers", "competition": "NBA"},
    "celticsblog": {"category": "nba_focused_media", "franchise": "Boston Celtics", "competition": "NBA"},
    "goldenstateofmind": {"category": "nba_focused_media", "franchise": "Golden State Warriors", "competition": "NBA"},
    "bluemanhoop": {"category": "nba_focused_media", "franchise": "Golden State Warriors", "competition": "NBA"},
    "clutchpoints": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "basketballforever": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "hoopsrumors": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "hoopshype": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "nbamaniacs": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "slammagazine": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "lakersnation": {"category": "nba_focused_media", "franchise": "Los Angeles Lakers", "competition": "NBA"},
    "lakeshowlife": {"category": "nba_focused_media", "franchise": "Los Angeles Lakers", "competition": "NBA"},
    "hardwoodhoudini33": {"category": "nba_focused_media", "franchise": "Boston Celtics", "competition": "NBA"},
    "thesmokingcuban": {"category": "nba_focused_media", "franchise": "Dallas Mavericks", "competition": "NBA"},
    "thunderousintentions": {"category": "nba_focused_media", "franchise": "Oklahoma City Thunder", "competition": "NBA"},
    "nuggetslove": {"category": "nba_focused_media", "franchise": "Denver Nuggets", "competition": "NBA"},
    "denverstiffs": {"category": "nba_focused_media", "franchise": "Denver Nuggets", "competition": "NBA"},
    "pippenainteasyfs": {"category": "nba_focused_media", "franchise": "Chicago Bulls", "competition": "NBA"},
    "dailyknicks": {"category": "nba_focused_media", "franchise": "New York Knicks", "competition": "NBA"},
    "fansidedkjg": {"category": "nba_focused_media", "franchise": "Cleveland Cavaliers", "competition": "NBA"},
    "sixersense": {"category": "nba_focused_media", "franchise": "Philadelphia 76ers", "competition": "NBA"},
    "ripcityproject": {"category": "nba_focused_media", "franchise": "Portland Trail Blazers", "competition": "NBA"},
    "thejnotes": {"category": "nba_focused_media", "franchise": "Utah Jazz", "competition": "NBA"},
    "slcdunkfb": {"category": "nba_focused_media", "franchise": "Utah Jazz", "competition": "NBA"},
    "valleyofthesuns": {"category": "nba_focused_media", "franchise": "Phoenix Suns", "competition": "NBA"},
    "behindthebuckpass": {"category": "nba_focused_media", "franchise": "Milwaukee Bucks", "competition": "NBA"},
    "orlandomagicdaily": {"category": "nba_focused_media", "franchise": "Orlando Magic", "competition": "NBA"},
    "peachtreehoops": {"category": "nba_focused_media", "franchise": "Atlanta Hawks", "competition": "NBA"},
    "canishoopussbn": {"category": "nba_focused_media", "franchise": "Minnesota Timberwolves", "competition": "NBA"},
    "nbavietnam": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "basketusa": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "trashtalk.fr": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "basketsession": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "parlonsbasketnba": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "bebasket.nba": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "insidebasket": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "nbamexico": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "nbalatam": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "hispanosnba": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "gigantesdelbasket": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "nbabrasil": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "bolapresa": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "jumperbrasil": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "nbapassioncom": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "nbareligion": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "dunkestfantasybasketball": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "nbajapan": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "nba.taiwan": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "nbaindonesia": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "nbaliveinthailand": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},
    "nbaarabic": {"category": "nba_focused_media", "franchise": None, "competition": "NBA"},

    # General Basketball / Non-NBA
    "onesportsphl": {"category": "general_sports_philippines", "default_competition": "UAAP/PBA"},
    "fiba": {"category": "general_basketball", "default_competition": "FIBA"},
    "vba": {"category": "general_basketball", "default_competition": "VBA"},
    "euroleague": {"category": "general_basketball", "default_competition": "EuroLeague"},
}


def classify_nba_post(text, sport_hint=None, author_or_source=None, policy="contextual"):
    """Classifies whether a post caption belongs strictly to the NBA competition scope.

    Supports two policy modes:
      - 'contextual' (recommended): leverages verified official NBA teams/leagues & dedicated NBA media
        sources to classify genuine team content (e.g. 'Back to work 🏀🔥', 'Half.', 'Onto the regular season we go').
      - 'strict': requires explicit NBA franchise, player, or league keywords directly in caption text.

    Returns:
        dict with:
            matched (bool): True if verified NBA evidence exists and satisfies policies.
            status (str): 'matched' | 'unmatched' | 'uncertain'.
            sport_category (str or None): 'Basketball' or None.
            competition (str or None): 'NBA', detected other league, or None.
            rule_score (float): rule-based score (1.0 = direct evidence, 0.5 = uncertain, 0.0 = unmatched).
            confidence (str): 'high' | 'medium' | 'low'.
            evidence (list[str]): matched tokens supporting classification.
            reason (str): human-readable rationale.
            policy_note (str): policy explanation.
            classification_basis (str): 'text_evidence' | 'verified_source_context' | 'verified_media_context' | 'unidentified'
    """
    if not text or not str(text).strip():
        return {
            "matched": False,
            "status": "uncertain",
            "sport_category": None,
            "competition": None,
            "rule_score": 0.0,
            "confidence": "low",
            "evidence": [],
            "reason": "no_caption_or_media_only",
            "policy_note": "Post caption is required for research dataset inclusion",
            "classification_basis": "unidentified",
        }

    clean_text = str(text).strip()
    alphanumeric = re.sub(r"[\s\W\d_]+", "", clean_text)
    if len(alphanumeric) < 4:
        return {
            "matched": False,
            "status": "uncertain",
            "sport_category": "Basketball" if (sport_hint and "basket" in str(sport_hint).lower()) else None,
            "competition": None,
            "rule_score": 0.0,
            "confidence": "low",
            "evidence": [],
            "reason": "insufficient_text_content_or_emoji_only",
            "policy_note": "Minimal or emoji-only text lacks sufficient substantive evidence",
            "classification_basis": "unidentified",
        }

    # Resolve canonical author / source context from registry
    canonical_source = None
    source_info = None
    if author_or_source:
        s_clean = str(author_or_source).strip()
        m_profile = re.search(r"facebook\.com/(?:pages/[^/]+/|profile\.php\?id=)?([^/?#]+)", s_clean, re.IGNORECASE)
        candidate_handle = m_profile.group(1).lower() if m_profile else s_clean.lower()
        candidate_handle = candidate_handle.rstrip("/")
        if candidate_handle in NBA_SOURCE_REGISTRY:
            canonical_source = candidate_handle
            source_info = NBA_SOURCE_REGISTRY[candidate_handle]
        else:
            simp = re.sub(r"[\s\W_]+", "", candidate_handle)
            if simp in NBA_SOURCE_REGISTRY:
                canonical_source = simp
                source_info = NBA_SOURCE_REGISTRY[simp]

    # 1. Non-NBA competition detection
    detected_non_nba = []
    for comp_name, comp_pat in NON_NBA_COMPETITION_RULES.items():
        matches = comp_pat.findall(clean_text)
        if matches:
            detected_non_nba.append((comp_name, list(dict.fromkeys(m.lower() for m in matches))))

    # 2. NBA evidence detection
    nba_explicit = NBA_EXPLICIT_RULE.findall(clean_text)
    nba_teams = NBA_TEAMS_RULE.findall(clean_text)
    nba_persons = NBA_PERSONS_RULE.findall(clean_text)
    bball_actions = BASKETBALL_ACTION_RULE.findall(clean_text)

    # Clean evidence lists
    exp_tokens = list(dict.fromkeys(m.lower() for m in nba_explicit))
    team_tokens = list(dict.fromkeys(m.lower() for m in nba_teams))
    person_tokens = list(dict.fromkeys(m.lower() for m in nba_persons))
    action_tokens = list(dict.fromkeys(m.lower() for m in bball_actions))

    all_nba_evidence = list(dict.fromkeys(exp_tokens + team_tokens + person_tokens))

    # Evaluate non-NBA competition conflicts
    if detected_non_nba:
        primary_comp, comp_tokens = detected_non_nba[0]
        if primary_comp == "UAAP":
            return {
                "matched": False,
                "status": "unmatched",
                "sport_category": "Basketball",
                "competition": "UAAP",
                "rule_score": 0.0,
                "confidence": "high",
                "evidence": comp_tokens,
                "reason": "uaap_collegiate_basketball_excluded",
                "policy_note": "Philippine UAAP collegiate basketball is excluded from NBA dataset scope",
                "classification_basis": "text_evidence",
            }
        if primary_comp == "NBA2K":
            return {
                "matched": False,
                "status": "unmatched",
                "sport_category": "Basketball",
                "competition": "NBA2K",
                "rule_score": 0.0,
                "confidence": "high",
                "evidence": comp_tokens,
                "reason": "esports_video_game_excluded",
                "policy_note": "NBA 2K esports/video game content is excluded by project policy",
                "classification_basis": "text_evidence",
            }
        if primary_comp == "GLeague":
            return {
                "matched": False,
                "status": "unmatched",
                "sport_category": "Basketball",
                "competition": "GLeague",
                "rule_score": 0.0,
                "confidence": "high",
                "evidence": comp_tokens,
                "reason": "development_league_excluded",
                "policy_note": "NBA G-League development content is excluded from main NBA scope",
                "classification_basis": "text_evidence",
            }
        if not all_nba_evidence:
            return {
                "matched": False,
                "status": "unmatched",
                "sport_category": "Basketball",
                "competition": primary_comp,
                "rule_score": 0.0,
                "confidence": "high",
                "evidence": comp_tokens,
                "reason": f"non_nba_competition_detected ({primary_comp})",
                "policy_note": f"Content focuses exclusively on {primary_comp}, not NBA",
                "classification_basis": "text_evidence",
            }
        # Both NBA evidence and other league mentioned (e.g. NBA player in FIBA/Olympics or NCAA draft)
        if person_tokens or team_tokens:
            return {
                "matched": True,
                "status": "matched",
                "sport_category": "Basketball",
                "competition": "NBA",
                "secondary_competition": primary_comp,
                "rule_score": 0.8,
                "confidence": "medium",
                "evidence": all_nba_evidence[:5] + comp_tokens[:3],
                "reason": f"nba_player_or_team_in_international_or_draft_context ({primary_comp})",
                "policy_note": f"NBA entity is subject of post mentioning {primary_comp}",
                "classification_basis": "text_evidence",
            }
        mixed_ev = list(dict.fromkeys(all_nba_evidence + comp_tokens))
        return {
            "matched": False,
            "status": "uncertain",
            "sport_category": "Basketball",
            "competition": f"Mixed ({primary_comp} + NBA)",
            "rule_score": 0.5,
            "confidence": "medium",
            "evidence": mixed_ev[:10],
            "reason": "mixed_competitions_insufficient_evidence",
            "policy_note": "Post contains mixed league references without unambiguous single-competition focus",
            "classification_basis": "text_evidence",
        }

    # 3. Validated NBA evidence in text
    if all_nba_evidence:
        promo_matches = COMMERCIAL_PROMO_RULE.findall(clean_text)
        has_game_context = bool(action_tokens) or any(
            t in clean_text.lower() for t in ["vs", "game", "score", "pts", "highlights", "win", "loss", "points", "season", "trade", "playoffs"]
        )
        hiring_matches = [p for p in promo_matches if p.lower() in {"we are hiring", "apply now", "job vacancy", "job opening", "tuyển dụng"}]
        if hiring_matches and not has_game_context:
            return {
                "matched": False,
                "status": "uncertain",
                "sport_category": "Basketball",
                "competition": "NBA",
                "rule_score": 0.5,
                "confidence": "low",
                "content_type": "recruitment",
                "evidence": all_nba_evidence[:5] + [h.lower() for h in hiring_matches],
                "reason": "recruitment_or_administrative_content",
                "policy_note": "Administrative hiring without game context is flagged for review",
                "classification_basis": "text_evidence",
            }

        if promo_matches and not has_game_context:
            return {
                "matched": True,
                "status": "matched",
                "sport_category": "Basketball",
                "competition": "NBA",
                "rule_score": 0.8,
                "confidence": "medium",
                "content_type": "commercial_or_promo",
                "evidence": all_nba_evidence[:5] + [p.lower() for p in promo_matches[:3]],
                "reason": "nba_commercial_or_merchandise_content",
                "policy_note": "Commercial or ticket announcement related to NBA team/franchise",
                "classification_basis": "text_evidence",
            }

        return {
            "matched": True,
            "status": "matched",
            "sport_category": "Basketball",
            "competition": "NBA",
            "rule_score": 1.0,
            "confidence": "high",
            "evidence": all_nba_evidence[:10],
            "reason": "matched_nba_evidence",
            "policy_note": "Direct verified NBA franchise, player, or league token matched",
            "classification_basis": "text_evidence",
        }

    # 4. Contextual classification via verified NBA source registry
    if policy == "contextual" and source_info:
        cat = source_info.get("category")
        if cat in ("official_nba", "official_nba_team"):
            return {
                "matched": True,
                "status": "matched",
                "sport_category": "Basketball",
                "competition": "NBA",
                "rule_score": 1.0,
                "confidence": "high",
                "evidence": [f"source:{canonical_source}"] + action_tokens[:5],
                "reason": f"verified_official_nba_team_source ({canonical_source})",
                "policy_note": "Post published by verified official NBA team / league with no conflicting competition evidence",
                "classification_basis": "verified_source_context",
            }
        elif cat == "nba_focused_media":
            if action_tokens or (sport_hint and "basket" in str(sport_hint).lower()):
                return {
                    "matched": True,
                    "status": "matched",
                    "sport_category": "Basketball",
                    "competition": "NBA",
                    "rule_score": 0.9,
                    "confidence": "high",
                    "evidence": action_tokens[:5] or [f"source:{canonical_source}"],
                    "reason": f"verified_nba_focused_media ({canonical_source}) with basketball content",
                    "policy_note": "Post from verified NBA-focused media outlet covering basketball topics",
                    "classification_basis": "verified_media_context",
                }
            else:
                return {
                    "matched": False,
                    "status": "uncertain",
                    "sport_category": "Basketball",
                    "competition": "NBA",
                    "rule_score": 0.5,
                    "confidence": "medium",
                    "evidence": [f"source:{canonical_source}"],
                    "reason": "insufficient_nba_evidence_needs_review",
                    "policy_note": "Post from NBA-focused media but caption lacks distinct game/basketball tokens",
                    "classification_basis": "verified_media_context",
                }

    # 5. Generic Basketball terms without specific NBA evidence
    if action_tokens or re.search(r"(\b(basketball|bóng rổ)\b|🏀)", clean_text, re.IGNORECASE):
        bball_ev = action_tokens or (["🏀"] if "🏀" in clean_text else ["basketball"])
        if sport_hint and "basket" in str(sport_hint).lower():
            return {
                "matched": False,
                "status": "uncertain",
                "sport_category": "Basketball",
                "competition": None,
                "rule_score": 0.5,
                "confidence": "medium",
                "evidence": bball_ev[:5],
                "reason": "basketball_detected_but_lacks_distinct_nba_evidence",
                "policy_note": "Basketball action detected but specific NBA franchise/player evidence is missing",
                "classification_basis": "text_evidence",
            }
        return {
            "matched": False,
            "status": "unmatched",
            "sport_category": "Basketball",
            "competition": None,
            "rule_score": 0.0,
            "confidence": "low",
            "evidence": bball_ev[:5],
            "reason": "basketball_detected_outside_nba_scope",
            "policy_note": "Generic basketball without source hint or NBA evidence",
            "classification_basis": "text_evidence",
        }

    # 6. Non-sport content
    return {
        "matched": False,
        "status": "unmatched",
        "sport_category": None,
        "competition": None,
        "rule_score": 0.0,
        "confidence": "high",
        "evidence": [],
        "reason": "no_basketball_or_nba_evidence",
        "policy_note": "Content contains no recognized basketball or NBA tokens",
        "classification_basis": "unidentified",
    }


def strip_vietnamese_accents(text: str) -> str:
    """Strips Vietnamese diacritical marks while preserving base characters."""
    if not text:
        return ""
    text = text.replace("đ", "d").replace("Đ", "D")
    nfd = unicodedata.normalize("NFD", text)
    return "".join(c for c in nfd if unicodedata.category(c) != "Mn")


def _keyword_to_regex(keyword: str, strip_accents_flag: bool = False) -> re.Pattern:
    clean = keyword.strip()
    if strip_accents_flag:
        clean = strip_vietnamese_accents(clean)
    else:
        clean = unicodedata.normalize("NFC", clean)
    clean = clean.lower()
    # Boundary (?<!\w) allows start of text, whitespace, punctuation, or '#'
    # Boundary (?!\w) allows end of text, whitespace, punctuation
    pattern = rf"(?<!\w){re.escape(clean)}(?!\w)"
    return re.compile(pattern, re.IGNORECASE)


def match_keywords(
    text: str,
    include_keywords: list,
    exclude_keywords: list = None,
    match_mode: str = "any",
    strip_accents: bool = False,
):
    """Evaluates whether text matches configured keyword criteria.

    Principles:
    - Unicode normalization (NFC) and case-insensitive.
    - Word boundary rules prevent substring false positives (e.g. 'AI' in 'Thailand' does not match).
    - Hashtags like '#AI' or '#xe_dien' match cleanly.
    - match_mode 'any' requires at least one include keyword to match.
    - match_mode 'all' requires all include keywords to match.
    - Any matching exclude_keyword causes rejection.

    Returns:
        (is_accepted: bool, matched_keywords: list[str], rejection_reasons: list[str])
    """
    if not text or not str(text).strip():
        return False, [], ["missing_caption_text"]

    norm_text = strip_vietnamese_accents(text) if strip_accents else unicodedata.normalize("NFC", text)
    norm_text_lower = norm_text.lower()

    # 1. Check exclude keywords
    matched_excludes = []
    if exclude_keywords:
        for exc in exclude_keywords:
            if not exc or not str(exc).strip():
                continue
            pat = _keyword_to_regex(str(exc), strip_accents_flag=strip_accents)
            if pat.search(norm_text_lower):
                matched_excludes.append(str(exc).strip())

    if matched_excludes:
        return False, [], [f"excluded_keyword_matched: {', '.join(matched_excludes)}"]

    # 2. Check include keywords
    clean_includes = [str(k).strip() for k in (include_keywords or []) if str(k).strip()]
    if not clean_includes:
        return True, [], []

    matched_includes = []
    for inc in clean_includes:
        pat = _keyword_to_regex(inc, strip_accents_flag=strip_accents)
        if pat.search(norm_text_lower):
            matched_includes.append(inc)

    if match_mode.lower() == "all":
        if len(matched_includes) == len(clean_includes):
            return True, matched_includes, []
        else:
            missing = [k for k in clean_includes if k not in matched_includes]
            return False, matched_includes, [f"missing_required_keywords: {', '.join(missing)}"]
    else:  # "any"
        if len(matched_includes) > 0:
            return True, matched_includes, []
        else:
            return False, [], ["keyword_not_matched"]


def evaluate_record_acceptance(
    record,
    tech_meta=None,
    target_sport="Basketball",
    target_competition="NBA",
    allow_video=False,
    require_like_count=True,
):
    """Evaluates whether a dataset record meets quality criteria for inclusion
    in the research dataset.

    Returns:
        (is_accepted: bool, rejection_reasons: list[str])
    """
    reasons = []
    if not record.get("post_id"):
        reasons.append("missing_post_id")
    if not record.get("post_url"):
        reasons.append("missing_post_url")
    if record.get("media_type") == "video" and not allow_video:
        reasons.append("video_post_omitted_by_policy")
    if not record.get("text") or not str(record.get("text")).strip():
        reasons.append("missing_caption_text")
    if not record.get("published_at"):
        reasons.append("missing_published_at")
    if not record.get("collected_at"):
        reasons.append("missing_collected_at")

    # Target competition evaluation
    nba_meta = {}
    if tech_meta and "nba_classification" in tech_meta:
        nba_meta = tech_meta["nba_classification"]
    elif tech_meta and "sport_classification" in tech_meta and isinstance(tech_meta["sport_classification"], dict):
        nba_meta = tech_meta["sport_classification"].get("nba_classification") or {}

    if target_competition == "NBA":
        status = nba_meta.get("status")
        if status != "matched":
            reasons.append(f"nba_competition_not_matched (status: {status}, reason: {nba_meta.get('reason')})")

    # Sport category evaluation
    sport = record.get("sport_category") or ""
    # If NBA matched, ensure sport category is recognized as Basketball
    if nba_meta.get("matched") and not sport:
        sport = "Basketball"
        record["sport_category"] = "Basketball"

    if target_sport:
        clean_target = target_sport.strip().lower()
        sports_in_rec = [s.strip().lower() for s in sport.split(",") if s.strip()]
        if clean_target not in sports_in_rec:
            reasons.append(f"sport_category_mismatch (got '{sport}', expected '{target_sport}')")

    like_count = record.get("like_count")
    if require_like_count:
        if like_count is None or not isinstance(like_count, int) or like_count < 0:
            reasons.append("missing_or_invalid_like_count")
    else:
        if like_count is not None and (not isinstance(like_count, int) or like_count < 0):
            reasons.append("invalid_like_count")

    is_accepted = (len(reasons) == 0)
    return is_accepted, reasons


def classify_post_sport(text, sport_hint=None, media_type="text"):
    """Classifies the sport category of an individual post strictly based on its own evidence.
    
    Returns:
        (canonical_sport_string_or_None, metadata_dict)
    """
    clean_hint = validate_or_normalize_sport(sport_hint) if sport_hint else None
    
    if not text or not str(text).strip():
        return None, {
            "status": "unknown",
            "classification_method": "no_caption_or_media_only",
            "evidence": [],
            "sport_hint": clean_hint,
            "detected_sports": [],
            "rule_score": 0.0,
            "confidence": "low",
            "reason": "No text content available to infer sport",
        }
    
    clean_text = str(text).strip()
    
    # Check if text contains only emojis, whitespace, or punctuation
    alphanumeric = re.sub(r"[\s\W\d_]+", "", clean_text)
    if not alphanumeric:
        return None, {
            "status": "unknown",
            "classification_method": "emoji_or_symbols_only",
            "evidence": [],
            "sport_hint": clean_hint,
            "detected_sports": [],
            "rule_score": 0.0,
            "confidence": "low",
            "reason": "Caption contains only emojis/symbols without text evidence",
        }

    detected_sports = []
    evidence = []

    for sport_name, pattern in SPORT_RULES.items():
        matches = pattern.findall(clean_text)
        if matches:
            detected_sports.append(sport_name)
            evidence.extend([m.lower() for m in matches if m.lower() not in evidence])

    # Filter spurious multi-sport cross-talk:
    # If Basketball is detected, remove Athletics if only generic running/runner matched,
    # and remove Shooting if only target shooting pattern without target sport context matched.
    if "Basketball" in detected_sports and len(detected_sports) > 1:
        if "Athletics" in detected_sports:
            ath_words = {"running", "runner", "chạy bộ"}
            ath_ev = [e for e in evidence if e in ath_words]
            distinct_ath = [e for e in evidence if e not in ath_words and e in {"athletics", "điền kinh", "marathon", "maratón", "maratona", "half marathon", "bán marathon", "10k", "21k", "42k", "atletismo", "world athletics", "rfea"}]
            if ath_ev and not distinct_ath:
                detected_sports.remove("Athletics")
        if "Shooting" in detected_sports:
            distinct_shooting = [e for e in evidence if e in {"target shooting", "sport shooting", "bắn súng thể thao", "xạ thủ", "issf", "10m air rifle", "50m rifle"}]
            if not distinct_shooting:
                detected_sports.remove("Shooting")

    if detected_sports:
        canonical_str = ", ".join(detected_sports)
        method = "multi_sport_keyword_match" if len(detected_sports) > 1 else "rule_keyword_match"
        rule_score = 1.0 if len(detected_sports) == 1 else 0.8
        return canonical_str, {
            "status": "classified",
            "classification_method": method,
            "evidence": evidence[:10],
            "sport_hint": clean_hint,
            "detected_sports": detected_sports,
            "rule_score": rule_score,
            "confidence": "rule_based_high" if rule_score >= 1.0 else "rule_based_medium",
            "reason": f"Matched rules for {canonical_str}",
        }

    # If no sport detected, check for non-sport announcement
    non_sport_matches = NON_SPORT_PATTERN.findall(clean_text)
    if non_sport_matches:
        return None, {
            "status": "non_sport",
            "classification_method": "non_sport_content",
            "evidence": [m.lower() for m in non_sport_matches[:5]],
            "sport_hint": clean_hint,
            "detected_sports": [],
            "rule_score": 0.9,
            "confidence": "rule_based_high",
            "reason": "Content indicates non-sports announcement",
        }

    # Ambiguous or unknown content without clear evidence
    return None, {
        "status": "unknown",
        "classification_method": "unidentified",
        "evidence": [],
        "sport_hint": clean_hint,
        "detected_sports": [],
        "rule_score": 0.0,
        "confidence": "low",
        "reason": "Content has no clear sport-related keywords or evidence",
    }


def normalize_facebook_post_url(url):
    """Normalizes Facebook post URL by removing tracking query parameters
    and fragments while preserving essential post identity parameters (story_fbid, id).
    """
    if not url:
        return ""
    try:
        parsed = urlparse(url.strip())
        if not parsed.netloc:
            return url.strip()

        netloc = parsed.netloc.lower()
        if netloc in ("m.facebook.com", "mobile.facebook.com", "web.facebook.com"):
            netloc = "www.facebook.com"

        path = parsed.path.rstrip("/")
        if netloc in ("l.facebook.com", "lm.facebook.com") or path.endswith(("/l.php", "/ad_break")):
            return ""

        if any(p in path for p in ("/posts/", "/permalink/", "/share/p/", "/videos/", "/reel/", "/photos/")):
            return f"{parsed.scheme}://{netloc}{path}/"

        if "/watch" in path:
            query = parse_qs(parsed.query)
            if "v" in query and query["v"]:
                return f"{parsed.scheme}://{netloc}{path}?v={query['v'][0]}"

        if "photo" in path:
            query = parse_qs(parsed.query)
            if "fbid" in query and query["fbid"]:
                return f"{parsed.scheme}://{netloc}{path}?fbid={query['fbid'][0]}"

        if path.endswith(("/permalink.php", "/story.php", "/profile.php")):
            query = parse_qs(parsed.query)
            if "story_fbid" in query or "fbid" in query:
                keep_params = {}
                for k in ("story_fbid", "fbid", "id"):
                    if k in query and query[k]:
                        keep_params[k] = query[k][0]
                if keep_params:
                    from urllib.parse import urlencode
                    return f"{parsed.scheme}://{netloc}{path}?" + urlencode(sorted(keep_params.items()))

        query = parse_qs(parsed.query)
        tracking_keys = {
            "fbclid", "ref", "__cft__", "__tn__", "source", "paipv", "locale",
            "comment_id", "reply_comment_id", "notif_id", "notif_t", "extid"
        }
        filtered_query = {k: v[0] for k, v in query.items() if k.lower() not in tracking_keys and v}
        from urllib.parse import urlencode
        q_str = ("?" + urlencode(sorted(filtered_query.items()))) if filtered_query else ""
        return f"{parsed.scheme}://{netloc}{path}{q_str}"
    except Exception:
        return url.strip()


def parse_author_followers_with_meta(raw_value):
    """Parses raw follower count string (e.g. '3M', '1.2K', '50,000') into integer and approximation flag.
    Returns (int_value, is_approximate).
    """
    if raw_value is None:
        return None, False
    s = str(raw_value).strip()
    if not s:
        return None, False
    val = parse_numeric_count(s)
    if val is None:
        return None, False
    is_approx = bool(re.search(r"[KkMmBb]|mil|triệu", s, re.IGNORECASE))
    return val, is_approx


def extract_post_id(url):
    """Extracts unique post ID from Facebook URL (pfbid token, numeric ID, or permalink slug)."""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        path = parsed.path.rstrip("/")

        # Check group post / permalink format first
        group_match = re.search(r"/groups/[^/?#]+/(?:posts|permalink)/([^/?#]+)", path)
        if group_match:
            return group_match.group(1)

        post_match = re.search(r"/posts/([^/?#]+)", path)
        if post_match:
            return post_match.group(1)

        permalink_match = re.search(r"/permalink/([^/?#]+)", path)
        if permalink_match:
            return permalink_match.group(1)

        share_match = re.search(r"/share/p/([^/?#]+)", path)
        if share_match:
            return share_match.group(1)

        video_match = re.search(r"/(?:videos|reel|reels)/([^/?#]+)", path)
        if video_match:
            return video_match.group(1)

        query = parse_qs(parsed.query)
        if "/watch" in path and "v" in query and query["v"]:
            return query["v"][0]
        if "story_fbid" in query and query["story_fbid"]:
            return query["story_fbid"][0]
        if "fbid" in query and query["fbid"]:
            return query["fbid"][0]

        token_match = re.search(r"(pfbid[A-Za-z0-9]+|\b\d{9,}\b)", url)
        if token_match:
            return token_match.group(1)

        return None
    except Exception:
        return None


def extract_author_id(url, page=None):
    """Extracts author username or page ID from URL or DOM."""
    if not url and not page:
        return None
    try:
        if url:
            parsed = urlparse(url)
            path_segments = [p for p in parsed.path.strip("/").split("/") if p]
            reserved = {
                "posts", "photos", "videos", "reel", "reels", "watch",
                "share", "groups", "story.php", "permalink.php", "profile.php",
            }
            if path_segments:
                first_seg = path_segments[0]
                if first_seg.lower() not in reserved:
                    # Top-level numeric slug like facebook.com/3211971012344948 is an object ID, not an author handle
                    if not (first_seg.isdigit() and len(path_segments) == 1):
                        return first_seg
            query = parse_qs(parsed.query)
            if "id" in query and query["id"] and query["id"][0].isdigit():
                return query["id"][0]

        # If URL didn't yield an author and page is provided, extract from DOM
        if page:
            author_from_dom = page.evaluate(r'''() => {
                const authorAnchor = document.querySelector('h2 a[href], h3 a[href], h4 a[href], strong a[href], a[role="link"][tabindex="0"]');
                if (authorAnchor) {
                    const href = authorAnchor.getAttribute('href') || '';
                    return href;
                }
                return null;
            }''')
            if author_from_dom:
                return extract_author_id(author_from_dom)
    except Exception:
        pass
    return None


def detect_language_with_meta(text):
    """Statistical offline language detector using lingua-language-detector.
    Returns:
        (lang_code: str, metadata: dict)
    """
    if not text:
        return "unknown", {
            "status": "unknown",
            "method": "lingua_statistical",
            "version": "2.2.0",
            "reason": "empty_text",
            "confidence": 0.0,
            "candidates": []
        }

    clean = re.sub(r"https?://\S+", "", str(text))
    clean = re.sub(r"@\w+", "", clean)
    clean = re.sub(r"#(\w+)", r"\1", clean)
    alpha = re.sub(r"[\s\W\d_]+", "", clean)
    if len(alpha) < 4:
        return "unknown", {
            "status": "unknown",
            "method": "lingua_statistical",
            "version": "2.2.0",
            "reason": "text_too_short_or_emoji_only",
            "confidence": 0.0,
            "candidates": []
        }

    detector = _get_lingua_detector()
    if detector is not None:
        try:
            confs = detector.compute_language_confidence_values(clean)
            if confs:
                c0 = confs[0]
                c1 = confs[1] if len(confs) > 1 else None
                margin = c0.value - (c1.value if c1 else 0.0)
                top_code = LINGUA_LANG_MAP.get(c0.language.name, "unknown")
                candidates = [[LINGUA_LANG_MAP.get(c.language.name, c.language.name.lower()), round(c.value, 3)] for c in confs[:3]]
                # Calibrated thresholds based on text length and candidate margin
                words = clean.split()
                is_short = len(words) < 5 or len(clean) < 30
                if is_short:
                    is_confident = (c0.value >= 0.65 and margin >= 0.20)
                else:
                    is_confident = (c0.value >= 0.50 and margin >= 0.10)

                if is_confident and top_code != "unknown":
                    return top_code, {
                        "status": "detected",
                        "method": "lingua_statistical",
                        "version": "2.2.0",
                        "language": top_code,
                        "confidence": round(c0.value, 3),
                        "margin": round(margin, 3),
                        "candidates": candidates,
                        "reason": "high_confidence"
                    }
                else:
                    return "unknown", {
                        "status": "unknown",
                        "method": "lingua_statistical",
                        "version": "2.2.0",
                        "reason": "low_confidence_or_ambiguous",
                        "confidence": round(c0.value, 3),
                        "margin": round(margin, 3),
                        "candidates": candidates
                    }
        except Exception as exc:
            logger.debug(f"lingua detection error: {exc}")

    # Fallback to heuristic
    words = [w.lower() for w in re.findall(r"\b\w+\b", text)]
    if not words:
        return "unknown", {"status": "unknown", "method": "heuristic_fallback", "reason": "no_words"}
    vn_word_count = sum(1 for w in words if w in COMMON_VN_WORDS)
    if vn_word_count >= 2:
        return "vi", {"status": "detected", "method": "heuristic_fallback", "language": "vi"}
    vn_chars = len(VIETNAMESE_DISTINCT_RE.findall(text))
    if vn_chars >= 3 and (vn_word_count >= 1 or vn_chars / len(text) > 0.02):
        return "vi", {"status": "detected", "method": "heuristic_fallback", "language": "vi"}

    es_score = sum(1 for w in words if w in COMMON_ES_WORDS)
    fr_score = sum(1 for w in words if w in COMMON_FR_WORDS)
    en_score = sum(1 for w in words if w in COMMON_EN_WORDS)

    scores = [("es", es_score), ("fr", fr_score), ("en", en_score)]
    scores.sort(key=lambda x: x[1], reverse=True)
    top_lang, top_score = scores[0]
    second_lang, second_score = scores[1]

    if top_score >= 2 and top_score > second_score:
        return top_lang, {"status": "detected", "method": "heuristic_fallback", "language": top_lang}
    if fr_score >= 1 and re.search(r"[éèêëàâùûçœîï]", text.lower()):
        return "fr", {"status": "detected", "method": "heuristic_fallback", "language": "fr"}
    if es_score >= 1 and re.search(r"[áíóúñ¿¡]", text.lower()):
        return "es", {"status": "detected", "method": "heuristic_fallback", "language": "es"}
    if en_score >= 2:
        return "en", {"status": "detected", "method": "heuristic_fallback", "language": "en"}

    return "unknown", {"status": "unknown", "method": "heuristic_fallback", "reason": "insufficient_word_score"}


def detect_language(text):
    """Detects text language ('en', 'vi', 'es', 'fr', 'pt', 'tl', or 'unknown')."""
    lang_code, _ = detect_language_with_meta(text)
    return lang_code


def extract_hashtags(text):
    """Extracts unique hashtags list from text."""
    if not text:
        return []
    tags = HASHTAG_RE.findall(text)
    return list(dict.fromkeys(tags))


def extract_mentions(text, additional_mentions=None):
    """Extracts unique mentions list from text and optional additional mentions list, stripping trailing punctuation."""
    mentions = []
    if text:
        raw_mentions = MENTION_RE.findall(text)
        clean = [m.rstrip(".,;:!?") for m in raw_mentions if len(m.rstrip(".,;:!?")) > 1]
        mentions.extend(clean)
    if additional_mentions:
        for m in additional_mentions:
            if not m:
                continue
            m_str = str(m).strip()
            if not m_str.startswith("@"):
                m_str = "@" + m_str
            cleaned = m_str.rstrip(".,;:!?")
            if len(cleaned) > 1 and cleaned not in mentions:
                mentions.append(cleaned)
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
    platform="Facebook",
    allow_video_caption=False,
):
    """Builds a standardized 17-column dataset record with exact schema compliance."""
    final_post_id = post_id or extract_post_id(post_url)
    final_author_id = author_id or extract_author_id(post_url)

    # Standardize collected_at to Asia/Ho_Chi_Minh (+07:00)
    if collected_at:
        try:
            dt = datetime.fromisoformat(str(collected_at).replace("Z", "+00:00"))
            final_collected_at = dt.astimezone(VIETNAM_TZ).isoformat()
        except Exception:
            final_collected_at = str(collected_at)
    else:
        final_collected_at = datetime.now(VIETNAM_TZ).isoformat()

    final_media_type = media_type or "text"

    # For Facebook video posts, omit caption text only when allow_video_caption is False (NBA policy)
    if final_media_type == "video" and str(platform or "Facebook").lower() == "facebook" and not allow_video_caption:
        final_text = None
        final_language = None
        final_hashtags = []
        final_mentions = []
        final_sport_category = None
    else:
        final_text = text
        final_language = language or detect_language(text)
        final_hashtags = hashtags if hashtags is not None else extract_hashtags(text)
        if mentions is not None:
            cleaned_list = []
            for m in mentions:
                if not m:
                    continue
                m_str = str(m).strip()
                if not m_str.startswith("@"):
                    m_str = "@" + m_str
                cleaned = m_str.rstrip(".,;:!?")
                if len(cleaned) > 1 and cleaned not in cleaned_list:
                    cleaned_list.append(cleaned)
            final_mentions = cleaned_list
        else:
            final_mentions = extract_mentions(text)
        final_sport_category = validate_or_normalize_sport(sport_category)

    # View count represents total views/impressions across post types (if displayed)
    final_view_count = parse_numeric_count(view_count)

    record = {
        "post_id": str(final_post_id) if final_post_id is not None else None,
        "platform": str(platform or "Facebook"),
        "text": final_text,
        "published_at": published_at,
        "collected_at": final_collected_at,
        "author_id": str(final_author_id) if final_author_id is not None else None,
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
        "sport_category": final_sport_category,
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
        # 3. Remove trailing "| Facebook", "· Facebook", "- Facebook"
        clean = re.sub(r"\s*[|·•–—\-]\s*Facebook.*$", "", clean, flags=re.IGNORECASE).strip()

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
            const m = (document.body.innerText || "").match(/([\d.,]+[KkMmB]?|\d+)\s*(?:followers?|người theo dõi|seguidores?|abonnés?|seguaci)/i);
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
            try:
                atexit.unregister(self.close)
            except Exception:
                pass
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
        dom_mentions: [],
        review_reasons: []
    };

    let card = null;
    let msg = null;

    // Dismiss floating chat tabs or notification toasts if present
    try {
        const closeBtns = Array.from(document.querySelectorAll('[aria-label="Close chat" i], [aria-label="Đóng đoạn chat" i], [aria-label="Close" i][role="button"], [aria-label="Đóng" i][role="button"]'));
        for (const b of closeBtns) {
            const dlg = b.closest('[role="dialog"]');
            if (dlg && /chat|tin nhắn|trò chuyện|messenger|thông báo|notification/i.test((dlg.getAttribute('aria-label') || '') + ' ' + (dlg.innerText || ''))) {
                try { b.click(); } catch(e) {}
            }
        }
    } catch(e) {}

    // Helper: test if an element looks like a notification / chat / toast popup to be rejected
    const isNotificationOrChat = (el) => {
        if (!el) return false;
        const ariaLabel = (el.getAttribute('aria-label') || '').toLowerCase();
        const ariaLabelledby = (el.getAttribute('aria-labelledby') || '').toLowerCase();
        const combined = ariaLabel + ' ' + ariaLabelledby;
        if (/notifications|thông báo|chat|tin nhắn|messenger|trò chuyện/i.test(combined)) {
            return true;
        }
        const text = (el.innerText || '').toLowerCase();
        if (/reacted to your|bày tỏ cảm xúc về|commented on your|đã bình luận về|shared your|chia sẻ bài viết của bạn|nhắc đến bạn trong|mentioned you in/i.test(text)) {
            return true;
        }
        if (el.closest('[role="alert"], [data-pagelet*="Toast"], [aria-live="polite"], [aria-live="assertive"]')) {
            return true;
        }
        return false;
    };

    // 0. Priority: If post is displayed inside a modal dialog [role="dialog"], use the dialog container!
    const postDialogs = Array.from(document.querySelectorAll('[role="dialog"]')).filter(d => {
        if (isNotificationOrChat(d)) return false;
        const hasToolbar = Boolean(d.querySelector('[role="toolbar"], [aria-label*="reactions" i], [aria-label*="Like" i], [aria-label*="Thích" i]'));
        if (!hasToolbar) return false;
        const hasPostEvidence = d.querySelector('div[data-ad-preview="message"], div[data-ad-comet-preview="message"]') ||
                                d.querySelector('a[href*="/posts/"], a[href*="permalink.php"], a[href*="story_fbid="]');
        return Boolean(hasPostEvidence);
    });

    if (postDialogs.length > 0) {
        if (postUrl) {
            const cleanPath = postUrl.replace(/^https?:\/\/(www\.)?facebook\.com\/?/, '').split('?')[0].replace(/\/$/, '');
            const matchingDlg = postDialogs.find(d => {
                const anchors = Array.from(d.querySelectorAll('a[href]'));
                return anchors.some(a => (a.href || '').includes(cleanPath));
            });
            if (matchingDlg) card = matchingDlg;
        }
        if (!card) {
            const withMsg = postDialogs.find(d => d.querySelector('div[data-ad-preview="message"], div[data-ad-comet-preview="message"]'));
            card = withMsg || postDialogs[0];
        }
    }

    // 1a. If postUrl is provided and card not found in dialog, match anchor matching the permalink/ID
    if (!card && postUrl) {
        const cleanPath = postUrl.replace(/^https?:\/\/(www\.)?facebook\.com\/?/, '').split('?')[0].replace(/\/$/, '');
        const allAnchors = Array.from(document.querySelectorAll('a[href]'));
        for (const a of allAnchors) {
            const h = a.href || '';
            if (cleanPath && h.includes(cleanPath)) {
                let curr = a;
                while (curr && curr !== document.body) {
                    if (isNotificationOrChat(curr)) {
                        curr = null;
                        break;
                    }
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

    // 1b. Match first message container with its toolbar ancestor
    if (!card) {
        const candidateMsgs = Array.from(document.querySelectorAll('div[data-ad-preview="message"], div[data-ad-comet-preview="message"]')).filter(m => !isNotificationOrChat(m));
        if (candidateMsgs.length > 0) {
            let curr = candidateMsgs[0];
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
            if (isNotificationOrChat(a)) return false;
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
    let mentionContainer = null;
    const cardMsg = card.querySelector('div[data-ad-preview="message"], div[data-ad-comet-preview="message"]');
    if (cardMsg) {
        res.text = cardMsg.innerText.trim();
        mentionContainer = cardMsg;
    } else {
        const textNodes = Array.from(card.querySelectorAll('div[dir="auto"], span[dir="auto"]')).filter(d => {
            if (d.closest('[role="toolbar"]') || d.closest('[role="article"]') || d.closest('form')) return false;
            if (isNotificationOrChat(d)) return false;
            const t = (d.innerText || '').trim();
            if (/reacted to your|bày tỏ cảm xúc|commented on your|đã bình luận|shared your|nhắc đến bạn/i.test(t)) return false;
            return t.length > 20;
        });
        textNodes.sort((a, b) => (b.innerText || '').length - (a.innerText || '').length);
        if (textNodes.length > 0) {
            res.text = textNodes[0].innerText.trim();
            mentionContainer = textNodes[0];
        }
    }

    // 3b. Extract DOM mentions from tagged profile / page links inside the post text
    if (mentionContainer) {
        const anchors = Array.from(mentionContainer.querySelectorAll('a[href]'));
        for (const a of anchors) {
            const href = a.getAttribute('href') || a.href || '';
            if (!href) continue;
            if (href.includes('/hashtag/') || href.includes('l.facebook.com') || href.includes('/photo') || href.includes('/video')) continue;
            try {
                const baseOrigin = (window.location.origin && window.location.origin !== 'null') ? window.location.origin : 'https://www.facebook.com';
                const u = new URL(href, baseOrigin);
                if (!u.hostname.includes('facebook.com')) continue;
                let path = u.pathname.replace(/^\/+|\/+$/g, '');
                if (path.includes('profile.php')) {
                    const id = u.searchParams.get('id');
                    if (id) res.dom_mentions.push('@' + id);
                } else if (path && !path.includes('/')) {
                    const reserved = ['groups', 'pages', 'watch', 'events', 'share', 'reel', 'reels', 'stories', 'marketplace', 'gaming', 'friends', 'settings', 'help', 'privacy', 'photo.php', 'permalink.php'];
                    if (!reserved.includes(path.toLowerCase())) {
                        res.dom_mentions.push('@' + path);
                    }
                } else if (path.startsWith('people/')) {
                    const parts = path.split('/');
                    if (parts.length >= 3 && /^\d+$/.test(parts[2])) {
                        res.dom_mentions.push('@' + parts[2]);
                    } else if (parts.length >= 2) {
                        res.dom_mentions.push('@' + parts[1]);
                    }
                }
            } catch(e) {}
        }
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
    // 5.0 Compact metric summary row: e.g. "44\n2\n6" or "44\n2" (only outside caption/comments)
    const metricDivs = Array.from(card.querySelectorAll('div')).filter(d => {
        if (d.closest('div[data-ad-preview="message"], div[data-ad-comet-preview="message"]')) return false;
        if (d.closest('ul, [role="article"] [role="article"]')) return false;
        return /^\d[\d.,]*[KkMmB]?\n\d[\d.,]*[KkMmB]?(\n\d[\d.,]*[KkMmB]?)?$/.test((d.innerText || '').trim());
    });
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
    if (!res.like_count_raw) {
        const likeBtn = card.querySelector('[role="button"][aria-label*="Like" i], [role="button"][aria-label*="Thích" i]');
        if (likeBtn) {
            const m = (likeBtn.innerText || '').match(/\b([\d.,]+[KkMmB]?|\d+)\b/);
            if (m) res.like_count_raw = m[1];
        }
    }

    // 5.2 Comments
    if (!res.comment_count_raw) {
        const commentEl = card.querySelector('[aria-label*="comment" i], [aria-label*="bình luận" i], [aria-label*="comentario" i]');
        if (commentEl) {
            const m = (commentEl.getAttribute('aria-label') || '').match(/([\d.,]+[KkMmB]?|\d+)/);
            if (m) res.comment_count_raw = m[1];
        }
    }
    if (!res.comment_count_raw) {
        const commentBtn = card.querySelector('[role="button"][aria-label*="comment" i], [role="button"][aria-label*="bình luận" i]');
        if (commentBtn) {
            const m = (commentBtn.innerText || '').match(/\b([\d.,]+[KkMmB]?|\d+)\b/);
            if (m) res.comment_count_raw = m[1];
        }
    }
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
        const shareEl = card.querySelector('[aria-label*="share" i], [aria-label*="lượt chia sẻ" i], [aria-label*="compartido" i]');
        if (shareEl) {
            const m = (shareEl.getAttribute('aria-label') || '').match(/([\d.,]+[KkMmB]?|\d+)/);
            if (m) res.share_count_raw = m[1];
        }
    }
    if (!res.share_count_raw) {
        const shareBtn = card.querySelector('[role="button"][aria-label*="share" i], [role="button"][aria-label*="chia sẻ" i]');
        if (shareBtn) {
            const m = (shareBtn.innerText || '').match(/\b([\d.,]+[KkMmB]?|\d+)\b/);
            if (m) res.share_count_raw = m[1];
        }
    }
    if (!res.share_count_raw) {
        const sMatch = cardText.match(/\b(\d[\d.,]*[KkMmB]?)\s+(?:shares?|lượt chia sẻ|veces compartido)\b/i);
        if (sMatch) res.share_count_raw = sMatch[1];
    }

    // 5.4 Views: Verified Metric Candidates Only (default null, exclude command verbs & caption text)
    res.view_count_raw = null;
    res.view_candidates = [];
    res.view_status = "not_displayed";

    const isInvalidViewCommand = (str) => {
        if (!str) return true;
        const s = str.trim();
        return /^(?:view|xem|ver)\s+(?:profile|more|trang cá nhân|thêm|replies|câu trả lời|bình luận|comments|photos?|ảnh|videos?|history|insights|posts?)/i.test(s) ||
               /(?:more\s+comments|replies|câu trả lời|bình luận\s+khác)$/i.test(s) ||
               /target\s+is\s+\d+\s+views/i.test(s);
    };

    const viewMetricRegex = /^(?:([\d.,]+[KkMmB]?|\d+\s*(?:nghìn|ngàn|triệu|tr|mil|millones))\s+(?:views?|lượt xem|reproducciones))$/i;

    const candidateEls = Array.from(card.querySelectorAll('span, div, [role="button"], a')).filter(el => {
        if (el.closest('div[data-ad-preview="message"], div[data-ad-comet-preview="message"]')) return false;
        if (el.closest('ul, [role="article"] [role="article"]')) return false;
        const aria = (el.getAttribute('aria-label') || '').trim();
        const txt = (el.innerText || '').trim();
        if (isInvalidViewCommand(aria) || isInvalidViewCommand(txt)) return false;
        return viewMetricRegex.test(aria) || viewMetricRegex.test(txt);
    });

    for (const el of candidateEls) {
        const aria = (el.getAttribute('aria-label') || '').trim();
        const txt = (el.innerText || '').trim();
        const m = aria.match(viewMetricRegex) || txt.match(viewMetricRegex);
        if (m) {
            res.view_candidates.push({
                raw: m[1],
                label: aria || txt,
                tag: el.tagName.toLowerCase()
            });
        }
    }

    if (res.view_candidates.length > 0) {
        const uniqueCounts = Array.from(new Set(res.view_candidates.map(c => c.raw)));
        if (uniqueCounts.length === 1) {
            res.view_count_raw = uniqueCounts[0];
            res.view_status = "verified";
        } else {
            res.view_status = "conflicting_candidates";
            res.view_count_raw = null;
        }
    }

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


def extract_scoped_post_data(
    page,
    post_url=None,
    default_sport=None,
    reference_tz=VIETNAM_TZ,
    nba_policy="contextual",
    view_policy="verified_only",
    allow_video_caption=False,
):
    """Extracts post text, metrics, published timestamp, and media scoped strictly to the post container.
    Returns:
        (record_17_col, tech_metadata)
    """
    url = post_url or page.url

    # Check for deleted/unavailable content scoped to main content or heading
    is_unavailable = False
    if page is not None and hasattr(page, "evaluate"):
        try:
            unavailable_eval = page.evaluate(r'''() => {
                const errHeading = document.querySelector('h2, h3, [role="heading"]');
                const hText = errHeading ? (errHeading.innerText || '').toLowerCase() : '';
                const main = document.querySelector('[role="main"]') || document.body;
                const mainText = (main.innerText || '').toLowerCase();
                const phrases = [
                    "this content isn't available right now",
                    "content not found",
                    "bài viết này không còn hiển thị",
                    "trang này không tồn tại",
                    "este contenido no está disponible",
                    "sorry, this content isn't available",
                    "link you followed may be broken"
                ];
                const hasHeadingError = phrases.some(p => hText.includes(p));
                const hasCard = Boolean(main.querySelector('[role="article"], div[data-ad-preview="message"]'));
                const hasMainError = !hasCard && phrases.some(p => mainText.includes(p));
                return hasHeadingError || hasMainError;
            }''')
            is_unavailable = bool(unavailable_eval)
        except Exception:
            pass

    if is_unavailable:
        tech_meta = {
            "status": "unavailable",
            "review_reasons": ["Post is unavailable, deleted, or restricted by privacy settings."],
            "raw_published_at": None,
            "time_precision": "insufficient_evidence",
            "is_approximate": {},
            "missing_reasons": {"text": "post_unavailable"},
            "sport_classification": {"status": "unavailable"},
            "nba_classification": {"status": "unmatched", "reason": "post_unavailable"},
        }
        record = build_dataset_record(
            post_url=url,
            text=None,
            sport_category=default_sport,
        )
        return record, tech_meta

    # Execute scoped DOM evaluation (handles button expansion inside target card)
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

    # --- like_count retry: Facebook sometimes lazy-renders the reaction badge ---
    # If like_count_raw is still None after initial DOM eval, wait progressively
    # and re-probe the reaction badge only (lightweight JS, no full re-eval).
    # Up to 3 retries with increasing wait: 1.2s → 2.0s → 3.0s.
    _LIKE_RETRY_WAITS_MS = [1200, 2000, 3000]
    _LIKE_RETRY_JS = r"""
    (function() {
        // Lightweight re-probe for reaction badge only
        const reactionSelectors = [
            '[aria-label*="reaction"][aria-label*=" "]',
            'span[class*="x1e558r4"]',
            'span[class*="xt0psk2"]',
        ];
        for (const sel of reactionSelectors) {
            const els = Array.from(document.querySelectorAll(sel));
            for (const el of els) {
                if (el.closest('ul, [role="article"] [role="article"]')) continue;
                const aria = (el.getAttribute('aria-label') || '').trim();
                const txt = (el.innerText || '').trim();
                const m = aria.match(/^([\d.,]+[KkMmBb]?)\s+/i) || txt.match(/^([\d.,]+[KkMmBb]?)$/i);
                if (m) return m[1];
                // reaction badge pattern: "1.2K reactions" / "708 reactions"
                const m2 = (aria + ' ' + txt).match(/([\d.,]+[KkMmBb]?)\s*(?:reaction|reac|lượt bày tỏ)/i);
                if (m2) return m2[1];
            }
        }
        // Broader fallback: find any span near a reaction emoji group
        const emojiGroups = document.querySelectorAll(
            '[aria-label*="Like"], [aria-label*="Love"], [aria-label*="Haha"], [aria-label*="Wow"]'
        );
        for (const eg of emojiGroups) {
            const parent = eg.closest('[role="group"], div') || eg.parentElement;
            if (!parent) continue;
            const spans = parent.querySelectorAll('span');
            for (const sp of spans) {
                const t = (sp.innerText || '').trim();
                if (/^[\d.,]+[KkMmBb]?$/.test(t)) return t;
            }
        }
        return null;
    })()
    """
    if dom_res.get("card_found") and not dom_res.get("like_count_raw"):
        for _retry_idx, _wait_ms in enumerate(_LIKE_RETRY_WAITS_MS, start=1):
            try:
                page.wait_for_timeout(_wait_ms)
                _retried_like = page.evaluate(_LIKE_RETRY_JS)
                if _retried_like:
                    dom_res["like_count_raw"] = _retried_like
                    logger.debug(
                        f"like_count retry #{_retry_idx} succeeded for {url}: raw='{_retried_like}'"
                    )
                    break
                logger.debug(f"like_count retry #{_retry_idx} — badge still not rendered for {url}")
            except Exception as _exc:
                logger.debug(f"like_count retry #{_retry_idx} error for {url}: {_exc}")
                break

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
    iso_published, time_precision = parse_facebook_timestamp(raw_published, reference_tz=reference_tz, allow_relative=True)

    # Process counts with approximation metadata
    like_val, like_approx = parse_numeric_count_with_meta(dom_res.get("like_count_raw"))
    
    if dom_res.get("comment_is_zero"):
        comment_val, comment_approx = 0, False
    else:
        comment_val, comment_approx = parse_numeric_count_with_meta(dom_res.get("comment_count_raw"))

    share_val, share_approx = parse_numeric_count_with_meta(dom_res.get("share_count_raw"))

    # View count: default to None unless verified under verified_only policy
    view_raw = dom_res.get("view_count_raw")
    view_status = dom_res.get("view_status", "not_displayed")
    view_candidates = dom_res.get("view_candidates") or []
    view_val = None
    view_approx = False

    if view_policy == "off":
        view_val = None
        view_status = "off"
    elif view_policy == "verified_only" and view_raw and view_status == "verified":
        view_val, view_approx = parse_numeric_count_with_meta(view_raw)

    # Author and post IDs
    page_url = page.url if (page is not None and hasattr(page, "url")) else None
    final_post_id = (extract_post_id(page_url) if page_url else None) or extract_post_id(url)
    final_author_id = (extract_author_id(page_url) if page_url else None) or extract_author_id(url)

    media_type = dom_res.get("media_type") or "text"
    extracted_text = None if (media_type == "video" and not allow_video_caption) else dom_res.get("text")

    if extracted_text:
        extracted_text = re.sub(
            r"[\s\n]*…?\s*(?:See more|Xem thêm|Ver más|Mehr anzeigen|Voir plus)\s*$",
            "",
            extracted_text,
            flags=re.IGNORECASE,
        ).strip()

        # Quality guard: reject notification toast phrases leaking into text
        if re.search(r"(?:reacted to your|bày tỏ cảm xúc về|commented on your|đã bình luận về|shared your reel|shared your post)", extracted_text, re.IGNORECASE):
            logger.warning(f"Notification toast text detected and discarded for {url}: {extracted_text[:60]}...")
            extracted_text = None
            dom_res["text"] = None
            dom_res["review_reasons"].append("Extracted text matched notification toast pattern; discarded.")

    normalized_sport, sport_meta = classify_post_sport(
        extracted_text,
        sport_hint=default_sport,
        media_type=media_type,
    )
    nba_meta = classify_nba_post(
        extracted_text,
        sport_hint=default_sport,
        author_or_source=final_author_id or url,
        policy=nba_policy,
    )
    if nba_meta.get("matched"):
        normalized_sport = "Basketball"
    sport_meta["nba_classification"] = nba_meta

    detected_lang, lang_meta = detect_language_with_meta(extracted_text)

    # Compile review reasons and missing field reasons
    review_reasons = list(dom_res.get("review_reasons") or [])
    missing_reasons = {}

    if not extracted_text:
        if media_type == "video":
            missing_reasons["text"] = "video_text_omitted"
        else:
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
    if not normalized_sport:
        missing_reasons["sport_category"] = sport_meta.get("status", "unidentified_sport")

    # Status classification
    if not dom_res.get("card_found"):
        status = "needs_review"
    elif (extracted_text or media_type == "video") and not review_reasons:
        status = "success"
    elif extracted_text or media_type == "video":
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
        "sport_classification": sport_meta,
        "nba_classification": nba_meta,
        "language_meta": lang_meta,
        "view_extraction": {
            "status": view_status,
            "raw_value": view_raw if view_status == "verified" else None,
            "normalized_value": view_val,
            "is_approximate": view_approx,
            "candidates": view_candidates,
            "policy": view_policy,
            "reason": "verified_post_metric" if view_status == "verified" else f"view_{view_status}",
        },
    }

    # Mentions extraction combining regex and DOM tagged anchors
    dom_mentions = dom_res.get("dom_mentions") or []
    combined_mentions = extract_mentions(extracted_text, additional_mentions=dom_mentions)

    record_17_col = build_dataset_record(
        post_url=page_url if page_url and "facebook.com" in page_url else url,
        text=extracted_text,
        published_at=iso_published,
        author_id=final_author_id,
        post_id=final_post_id,
        sport_category=normalized_sport,
        language=detected_lang,
        mentions=combined_mentions,
        like_count=like_val,
        comment_count=comment_val,
        share_count=share_val,
        view_count=view_val,
        media_type=dom_res.get("media_type", "text"),
        allow_video_caption=allow_video_caption,
    )

    return record_17_col, tech_meta


def extract_feed_card_data(
    page,
    card_locator,
    sport_hint=None,
    reference_tz=VIETNAM_TZ,
    secondary_context=None,
    source_author_id=None,
    source_url=None,
    max_subtab_retries=1,
):
    """Extracts a post's 17-column dataset record directly from an identified feed card.
    Uses Playwright locators for See more expansion and hover tooltip timestamp.
    Falls back to opening the post permalink in a secondary tab if critical fields are missing.
    """
    try:
        card_locator.scroll_into_view_if_needed(timeout=3000)
    except Exception:
        pass

    # 1. Discover post permalink anchor within this specific card
    card_links = []
    try:
        card_links = card_locator.evaluate(r'''el => {
            let card = el;
            if (el.tagName === 'A' || !el.querySelector('[role="toolbar"], [aria-label*="Like" i], [aria-label*="Thích" i]')) {
                let curr = el;
                for (let i = 0; i < 25 && curr && curr !== document.body; i++) {
                    const hasAction = curr.querySelector('[aria-label*="Like" i], [aria-label*="Thích" i], [aria-label*="reactions" i], [role="toolbar"]');
                    if (hasAction && (curr.innerText || '').length > 30) {
                        card = curr;
                        break;
                    }
                    curr = curr.parentElement;
                }
            }
            const anchors = Array.from(card.querySelectorAll('a[href]'));
            if (card.tagName === 'A') anchors.push(card);
            return anchors.map(a => ({
                href: a.href || '',
                aria: (a.getAttribute('aria-label') || '').trim(),
                text: (a.innerText || '').trim(),
                is_comment: a.closest('[aria-label*="comment" i]') !== null || (a.href || '').includes('comment_id=')
            })).filter(l => !l.is_comment && (
                l.href.includes('/posts/') || l.href.includes('/share/p/') ||
                l.href.includes('permalink.php') || l.href.includes('story_fbid=') ||
                l.href.includes('/videos/') || l.href.includes('/reel/') ||
                l.href.includes('/watch') || l.href.includes('/photo')
            ));
        }''')
    except Exception as exc:
        logger.debug(f"Card link evaluation error: {exc}")

    post_url = None
    post_id = None
    is_share_alias = False
    raw_ts_candidate = None
    matched_link_href = None

    for cl in card_links:
        parsed = extract_post_id(cl["href"])
        if parsed:
            post_id = parsed
            post_url = normalize_facebook_post_url(cl["href"])
            is_share_alias = "/share/p/" in cl["href"]
            raw_ts_candidate = cl["aria"] or cl["text"]
            matched_link_href = cl["href"]
            break

    # 2. Extract author handle from heading inside this card
    author_id = None
    try:
        author_href = card_locator.evaluate(r'''el => {
            let card = el;
            if (el.tagName === 'A' || !el.querySelector('h2, h3, h4, strong')) {
                let curr = el;
                for (let i = 0; i < 25 && curr && curr !== document.body; i++) {
                    if (curr.querySelector('h2, h3, h4, strong')) {
                        card = curr;
                        break;
                    }
                    curr = curr.parentElement;
                }
            }
            const a = card.querySelector('h2 a[href], h3 a[href], h4 a[href], strong a[href]');
            return a ? a.href : null;
        }''')
        if author_href:
            author_id = extract_author_id(author_href)
    except Exception:
        pass

    # 3. Expand 'See more' using locator
    try:
        see_more_btn = card_locator.locator(
            'role=button[name=/^(?:see more|xem thêm|ver más|mehr anzeigen|voir plus)$/i]'
        ).or_(card_locator.locator('text=/^(?:see more|xem thêm|ver más)$/i')).first
        if see_more_btn.count() > 0 and see_more_btn.is_visible():
            see_more_btn.click(timeout=1000)
            page.wait_for_timeout(350)
    except Exception:
        pass

    # 4. Extract message text strictly from card
    extracted_text = None
    feed_dom_mentions = []
    try:
        msg_eval = card_locator.evaluate(r'''el => {
            let card = el;
            if (el.tagName === 'A' || !el.querySelector('div[data-ad-preview="message"], div[data-ad-comet-preview="message"]')) {
                let curr = el;
                for (let i = 0; i < 25 && curr && curr !== document.body; i++) {
                    const hasAction = curr.querySelector('[aria-label*="Like" i], [aria-label*="Thích" i], [aria-label*="reactions" i], [role="toolbar"]');
                    if (hasAction && (curr.innerText || '').length > 30) {
                        card = curr;
                        break;
                    }
                    curr = curr.parentElement;
                }
            }
            let mentionContainer = null;
            let text = null;
            const msg = card.querySelector('div[data-ad-preview="message"], div[data-ad-comet-preview="message"]');
            if (msg) {
                text = (msg.innerText || '').trim();
                mentionContainer = msg;
            } else {
                const nodes = Array.from(card.querySelectorAll('div[dir="auto"], span[dir="auto"]')).filter(d => {
                    if (d.closest('[role="toolbar"]') || d.closest('form')) return false;
                    if (d.closest('h2, h3, h4, [role="button"], button')) return false;
                    const t = (d.innerText || '').trim();
                    if (t.length < 2) return false;
                    if (/^(?:like|comment|share|thích|bình luận|chia sẻ|follow|theo dõi|public|chỉ mình tôi|bạn bè|\d+[\d.,]*[KkMmB]?)$/i.test(t)) return false;
                    return true;
                });
                nodes.sort((a,b) => (b.innerText || '').length - (a.innerText || '').length);
                if (nodes.length > 0) {
                    text = (nodes[0].innerText || '').trim();
                    mentionContainer = nodes[0];
                }
            }
            const dom_mentions = [];
            if (mentionContainer) {
                const anchors = Array.from(mentionContainer.querySelectorAll('a[href]'));
                for (const a of anchors) {
                    const href = a.getAttribute('href') || a.href || '';
                    if (!href) continue;
                    if (href.includes('/hashtag/') || href.includes('l.facebook.com') || href.includes('/photo') || href.includes('/video')) continue;
                    try {
                        const baseOrigin = (window.location.origin && window.location.origin !== 'null') ? window.location.origin : 'https://www.facebook.com';
                        const u = new URL(href, baseOrigin);
                        if (!u.hostname.includes('facebook.com')) continue;
                        let path = u.pathname.replace(/^\/+|\/+$/g, '');
                        if (path.includes('profile.php')) {
                            const id = u.searchParams.get('id');
                            if (id) dom_mentions.push('@' + id);
                        } else if (path && !path.includes('/')) {
                            const reserved = ['groups', 'pages', 'watch', 'events', 'share', 'reel', 'reels', 'stories', 'marketplace', 'gaming', 'friends', 'settings', 'help', 'privacy', 'photo.php', 'permalink.php'];
                            if (!reserved.includes(path.toLowerCase())) {
                                dom_mentions.push('@' + path);
                            }
                        } else if (path.startsWith('people/')) {
                            const parts = path.split('/');
                            if (parts.length >= 3 && /^\d+$/.test(parts[2])) {
                                dom_mentions.push('@' + parts[2]);
                            } else if (parts.length >= 2) {
                                dom_mentions.push('@' + parts[1]);
                            }
                        }
                    } catch(e) {}
                }
            }
            return { text: text, dom_mentions: dom_mentions };
        }''')
        if msg_eval:
            extracted_text = msg_eval.get("text")
            feed_dom_mentions = msg_eval.get("dom_mentions") or []
    except Exception:
        pass

    # 5. Timestamp and tooltip hover
    raw_published = raw_ts_candidate
    if matched_link_href:
        is_rel = bool(re.search(r"^\d+\s*[hmsd]|giờ|phút|ngày|hôm qua|yesterday|just now", raw_published or "", re.IGNORECASE))
        if not raw_published or not re.search(r"\d", raw_published) or is_rel:
            try:
                ts_loc = card_locator.locator(f'a[href="{matched_link_href}"]').first
                if ts_loc.count() > 0:
                    ts_loc.hover(timeout=1000)
                    page.wait_for_timeout(600)
                    tooltip_txt = page.evaluate(r'''() => {
                        const tooltips = Array.from(document.querySelectorAll('[role="tooltip"]'));
                        const found = tooltips.map(t => (t.innerText || '').trim()).find(t => t.length > 4 && /\d/.test(t));
                        return found || null;
                    }''')
                    if tooltip_txt:
                        raw_published = tooltip_txt
            except Exception:
                pass

    iso_published, time_precision = parse_facebook_timestamp(raw_published, reference_tz=reference_tz, allow_relative=True)

    # 6. Fallback in secondary tab only if strictly necessary
    needs_fallback = False
    if post_url and secondary_context is not None and max_subtab_retries > 0:
        if is_share_alias:
            needs_fallback = True
        elif not post_id:
            needs_fallback = True
        elif not iso_published and not raw_published:
            needs_fallback = True
        elif not extracted_text and not (post_id and iso_published):
            needs_fallback = True

    if needs_fallback:
        sub_page = None
        try:
            sub_page = secondary_context.new_page()
            sub_page.goto(post_url, wait_until="domcontentloaded", timeout=12000)
            sub_page.wait_for_timeout(1500)
            sub_rec, sub_meta = extract_scoped_post_data(sub_page, post_url=post_url, default_sport=sport_hint, reference_tz=reference_tz)
            if is_share_alias:
                sub_meta["alias_url"] = post_url
            return sub_rec, sub_meta
        except Exception as exc:
            logger.debug(f"Secondary tab fallback failed for {post_url}: {exc}")
        finally:
            if sub_page is not None:
                try:
                    sub_page.close()
                except Exception:
                    pass

    # 7. Metrics from card summary area
    dom_metrics = {}
    try:
        dom_metrics = card_locator.evaluate(r'''el => {
            const res = {
                like_raw: null, comment_raw: null, comment_zero: false,
                share_raw: null, view_raw: null, media_type: 'text'
            };
            const metricDivs = Array.from(el.querySelectorAll('div')).filter(d => 
                /^\d[\d.,]*[KkMmB]?\n\d[\d.,]*[KkMmB]?(\n\d[\d.,]*[KkMmB]?)?$/.test((d.innerText || '').trim())
            );
            if (metricDivs.length > 0) {
                const parts = metricDivs[0].innerText.trim().split('\n').map(s => s.trim());
                if (parts.length >= 1) res.like_raw = parts[0];
                if (parts.length >= 2) res.comment_raw = parts[1];
                if (parts.length >= 3) res.share_raw = parts[2];
            }
            if (!res.like_raw) {
                const rx = el.querySelector('[aria-label*="reactions" i], [aria-label*="bày tỏ cảm xúc" i], [aria-label*="reacted to this" i]');
                if (rx) {
                    const m = (rx.getAttribute('aria-label') || '').match(/([\d.,]+[KkMmB]?|\d+)/);
                    res.like_raw = m ? m[1] : rx.innerText.trim();
                } else {
                    const tb = el.querySelector('[role="toolbar"]');
                    if (tb) {
                        const m = (tb.innerText || '').match(/\b(\d[\d.,]*[KkMmB]?)\b/);
                        if (m) res.like_raw = m[1];
                    }
                }
            }
            const text = el.innerText || '';
            if (!res.comment_raw) {
                const cMatch = text.match(/\b(\d[\d.,]*[KkMmB]?)\s+(?:comments?|bình luận|comentarios)\b/i);
                if (cMatch) res.comment_raw = cMatch[1];
                else if (/no comments yet|chưa có bình luận|sé el primero en comentar/i.test(text)) res.comment_zero = true;
            }
            // Views: Verified Metric Candidates Only (default null)
            res.view_raw = null;
            const isInvalidFeedViewCmd = (str) => {
                if (!str) return true;
                const s = str.trim();
                return /^(?:view|xem|ver)\s+(?:profile|more|trang cá nhân|thêm|replies|câu trả lời|bình luận|comments|photos?|ảnh|videos?|history|insights|posts?)/i.test(s) ||
                       /(?:more\s+comments|replies|câu trả lời|bình luận\s+khác)$/i.test(s) ||
                       /target\s+is\s+\d+\s+views/i.test(s);
            };
            const feedViewPat = /^(?:([\d.,]+[KkMmB]?|\d+\s*(?:nghìn|ngàn|triệu|tr|mil|millones))\s+(?:views?|lượt xem|reproducciones))$/i;
            const feedCandEls = Array.from(el.querySelectorAll('span, div, [role="button"], a')).filter(item => {
                if (item.closest('div[data-ad-preview="message"], div[data-ad-comet-preview="message"]')) return false;
                if (item.closest('ul, [role="article"] [role="article"]')) return false;
                const aria = (item.getAttribute('aria-label') || '').trim();
                const txt = (item.innerText || '').trim();
                if (isInvalidFeedViewCmd(aria) || isInvalidFeedViewCmd(txt)) return false;
                return feedViewPat.test(aria) || feedViewPat.test(txt);
            });
            for (const item of feedCandEls) {
                const aria = (item.getAttribute('aria-label') || '').trim();
                const txt = (item.innerText || '').trim();
                const m = aria.match(feedViewPat) || txt.match(feedViewPat);
                if (m) {
                    res.view_raw = m[1];
                    break;
                }
            }

            if (el.querySelector('video') || el.querySelector('[data-video-id]')) {
                res.media_type = 'video';
            } else if (el.querySelector('img') || el.querySelector('a[href*="/photo"]')) {
                res.media_type = 'image';
            } else if (el.querySelector('a[target="_blank"][rel*="nofollow"], a[href*="l.facebook.com"]')) {
                res.media_type = 'link';
            }
            return res;
        }''')
    except Exception:
        pass

    like_val, like_approx = parse_numeric_count_with_meta(dom_metrics.get("like_raw"))
    if dom_metrics.get("comment_zero"):
        comment_val, comment_approx = 0, False
    else:
        comment_val, comment_approx = parse_numeric_count_with_meta(dom_metrics.get("comment_raw"))
    share_val, share_approx = parse_numeric_count_with_meta(dom_metrics.get("share_raw"))
    view_val, view_approx = parse_numeric_count_with_meta(dom_metrics.get("view_raw"))

    feed_media_type = dom_metrics.get("media_type", "text")
    if feed_media_type == "video":
        extracted_text = None

    # 8. Sport classification strictly from caption evidence
    sport_category, sport_meta = classify_post_sport(
        extracted_text,
        sport_hint=sport_hint,
        media_type=feed_media_type,
    )

    review_reasons = []
    missing_reasons = {}

    if not post_url or not post_id:
        review_reasons.append("Card post permalink or ID could not be identified.")
    if not extracted_text:
        missing_reasons["text"] = "video_text_omitted" if feed_media_type == "video" else "text_not_found"
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
    if not sport_category:
        missing_reasons["sport_category"] = sport_meta.get("status", "unidentified_sport")

    status = "success" if ((extracted_text or feed_media_type == "video") and post_id and not review_reasons) else "needs_review"

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
        "sport_classification": sport_meta,
    }

    combined_mentions = extract_mentions(extracted_text, additional_mentions=feed_dom_mentions)

    record_17_col = build_dataset_record(
        post_url=post_url or (source_url or "https://www.facebook.com"),
        text=extracted_text,
        published_at=iso_published,
        author_id=author_id or source_author_id,
        post_id=post_id,
        sport_category=sport_category,
        mentions=combined_mentions,
        like_count=like_val,
        comment_count=comment_val,
        share_count=share_val,
        view_count=view_val,
        media_type=dom_metrics.get("media_type", "text"),
    )

    return record_17_col, tech_meta

