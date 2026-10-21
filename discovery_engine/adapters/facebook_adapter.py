"""Facebook Discovery Adapter.

Discovers posts across:
1. Keyword Search (Facebook Search Posts: fetches individual user posts, group posts, fanpages)
2. Public Groups (Community discussions and individual posts)
3. Hashtags
4. Verified & Active Fanpages
"""

import re
from urllib.parse import quote_plus
from typing import List, Optional, Any
from .base import BaseDiscoveryAdapter
from ..models import DiscoveredPost
import facebook_dataset as fd

GROUP_POST_RE = re.compile(r"/groups/([^/]+)/(?:posts|permalink)/([^/?]+)", re.IGNORECASE)
PROFILE_POST_RE = re.compile(r"/profile\.php\?id=(\d+).*?[?&]story_fbid=([^&]+)", re.IGNORECASE)


class FacebookDiscoveryAdapter(BaseDiscoveryAdapter):
    platform_name: str = "Facebook"

    def build_search_targets(
        self,
        queries: List[str],
        sport: Optional[str] = None,
        competition: Optional[str] = None,
    ) -> List[str]:
        """Builds Facebook Search URLs targeting public posts."""
        targets = []
        for q in queries:
            clean = q.strip()
            if not clean:
                continue
            encoded = quote_plus(clean)
            # Facebook post search endpoint
            targets.append(f"https://www.facebook.com/search/posts/?q={encoded}")
        return targets

    def build_hashtag_targets(
        self,
        hashtags: List[str],
        sport: Optional[str] = None,
        competition: Optional[str] = None,
    ) -> List[str]:
        """Builds Facebook hashtag feed URLs."""
        targets = []
        for tag in hashtags:
            clean = tag.strip().lstrip("#")
            if clean:
                targets.append(f"https://www.facebook.com/hashtag/{clean}")
        return targets

    def build_group_targets(self, groups: List[str]) -> List[str]:
        """Builds Facebook public group URLs."""
        targets = []
        for g in groups:
            clean = g.strip()
            if not clean:
                continue
            if clean.startswith("http"):
                targets.append(clean)
            else:
                targets.append(f"https://www.facebook.com/groups/{clean}/")
        return targets

    def build_page_targets(self, pages: List[str]) -> List[str]:
        """Builds Facebook Fanpage URLs."""
        targets = []
        for p in pages:
            clean = p.strip()
            if not clean:
                continue
            if clean.startswith("http"):
                targets.append(clean)
            else:
                targets.append(f"https://www.facebook.com/{clean}/")
        return targets

    def normalize_post_url(self, raw_url: str) -> Optional[str]:
        if not raw_url or "facebook.com" not in raw_url:
            return None
        return fd.normalize_facebook_post_url(raw_url)

    def extract_post_id(self, url: str) -> Optional[str]:
        if not url:
            return None
        # Check standard extractor
        p_id = fd.extract_post_id(url)
        if p_id:
            return p_id
        # Check group post
        m_group = GROUP_POST_RE.search(url)
        if m_group:
            return m_group.group(2)
        # Check profile story fbid
        m_prof = PROFILE_POST_RE.search(url)
        if m_prof:
            return m_prof.group(2)
        return None

    def determine_source_type(self, url: str, default: str = "search_result") -> str:
        """Determines whether a post is from an individual user, public group, or fanpage."""
        if not url:
            return default
        lower = url.lower()
        if "/groups/" in lower:
            return "group"
        if "profile.php" in lower or "/people/" in lower:
            return "individual"
        if any(token in lower for token in ["/posts/", "/photos/", "/videos/"]):
            # If not group and not numeric user profile, it could be fanpage or user vanity url
            return "fanpage"
        return default

    def parse_discovered_links(
        self,
        links: List[str],
        sport: Optional[str] = None,
        competition: Optional[str] = None,
        source_type: str = "search_result",
        discovery_method: str = "keyword_search",
    ) -> List[DiscoveredPost]:
        items: List[DiscoveredPost] = []
        seen = set()

        for link in links:
            if not link:
                continue
            clean_url = self.normalize_post_url(link.strip())
            if not clean_url or clean_url in seen:
                continue
            seen.add(clean_url)

            post_id = self.extract_post_id(clean_url)
            author_id = fd.extract_author_id(clean_url)
            inferred_type = self.determine_source_type(clean_url, default=source_type)

            items.append(
                DiscoveredPost(
                    url=clean_url,
                    platform=self.platform_name,
                    post_id=post_id,
                    author_id=author_id,
                    source_type=inferred_type,
                    sport=sport,
                    competition=competition,
                    discovery_method=discovery_method,
                )
            )
        return items

    def extract_mention_profiles(
        self,
        records_or_mentions: List[Any],
        max_per_post: int = 3,
        max_total_sources: int = 50,
    ) -> List[str]:
        """Extracts unique verified Facebook profile/Page URLs for snowball discovery.
        
        Prioritizes actual DOM hrefs and verified structured profile links rather than
        blindly concatenating arbitrary '@...' text tokens into unverified Facebook URLs.
        Applies depth/fan-out limits and deduplication.
        """
        extracted = []
        seen = set()
        reserved_paths = {
            "events", "groups", "marketplace", "watch", "help", "gaming", "stories",
            "reels", "settings", "privacy", "friends", "share", "photo.php", "permalink.php",
            "here", "everyone", "highlight", "followers", "channel"
        }

        for item in records_or_mentions:
            candidate_urls = []
            
            if isinstance(item, dict):
                # 1. Prioritize explicit verified DOM mention hrefs if available
                dom_hrefs = item.get("dom_mention_urls") or item.get("mention_urls") or []
                for href in dom_hrefs:
                    if href and "facebook.com" in href:
                        candidate_urls.append(href)

                # 2. Structured dom_mentions from page link extraction (e.g., @12345678 or @celtics)
                dom_mentions = item.get("dom_mentions") or []
                for dm in dom_mentions:
                    clean = str(dm).strip().lstrip("@").rstrip(".,;:!?")
                    if clean and clean.lower() not in reserved_paths:
                        if clean.isdigit():
                            candidate_urls.append(f"https://www.facebook.com/profile.php?id={clean}")
                        elif not re.search(r"[\s\<\>\'\"\&]", clean):
                            candidate_urls.append(f"https://www.facebook.com/{clean}/")

                # If no DOM links exist, inspect raw mentions with strict slug validation
                if not candidate_urls and "mentions" in item:
                    raw_mentions = item.get("mentions") or []
                    for m in raw_mentions:
                        clean = str(m).strip().lstrip("@").rstrip(".,;:!?")
                        # Only accept clean single-word slugs that look like valid Facebook page usernames
                        if clean and len(clean) >= 3 and not re.search(r"[\s\<\>\'\"\&]", clean) and clean.lower() not in reserved_paths:
                            candidate_urls.append(f"https://www.facebook.com/{clean}/")

            elif isinstance(item, str):
                # If a direct Facebook URL was passed
                if "facebook.com" in item:
                    candidate_urls.append(item)
                else:
                    clean = item.strip().lstrip("@").rstrip(".,;:!?")
                    if clean and len(clean) >= 3 and not re.search(r"[\s\<\>\'\"\&]", clean) and clean.lower() not in reserved_paths:
                        candidate_urls.append(f"https://www.facebook.com/{clean}/")

            # Per-post limit (fan-out protection)
            post_count = 0
            for u in candidate_urls:
                if post_count >= max_per_post:
                    break
                norm = (self.normalize_post_url(u) or u).rstrip("/") + "/"
                slug = norm.lower()
                if slug in seen:
                    continue
                seen.add(slug)
                extracted.append(norm)
                post_count += 1
                if len(extracted) >= max_total_sources:
                    return extracted

        return extracted

