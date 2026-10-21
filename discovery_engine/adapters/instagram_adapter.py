"""Instagram Discovery Adapter.

Discovers posts across:
1. Hashtag Feeds (/explore/tags/{hashtag}/)
2. Profiles (Athletes, Teams, Influencers, Fan Accounts)
3. Reels and Media Posts
"""

import re
from typing import List, Optional
from .base import BaseDiscoveryAdapter
from ..models import DiscoveredPost

IG_POST_RE = re.compile(
    r"(?:https?://)?(?:www\.)?instagram\.com/(?:p|reel)/([A-Za-z0-9_-]+)",
    re.IGNORECASE,
)
IG_PROFILE_RE = re.compile(
    r"(?:https?://)?(?:www\.)?instagram\.com/([A-Za-z0-9_.-]+)/?",
    re.IGNORECASE,
)
IG_RESERVED = {"explore", "reels", "direct", "stories", "accounts", "legal", "about"}


class InstagramDiscoveryAdapter(BaseDiscoveryAdapter):
    platform_name: str = "Instagram"

    def build_search_targets(
        self,
        queries: List[str],
        sport: Optional[str] = None,
        competition: Optional[str] = None,
    ) -> List[str]:
        """Maps queries into Instagram search or tag entry points."""
        targets = []
        for q in queries:
            clean = re.sub(r"[^\w]", "", q.lower().strip())
            if clean:
                targets.append(f"https://www.instagram.com/explore/tags/{clean}/")
        return targets

    def build_hashtag_targets(
        self,
        hashtags: List[str],
        sport: Optional[str] = None,
        competition: Optional[str] = None,
    ) -> List[str]:
        """Builds Instagram hashtag exploration URLs."""
        targets = []
        for tag in hashtags:
            clean = tag.strip().lstrip("#")
            if clean:
                targets.append(f"https://www.instagram.com/explore/tags/{clean}/")
        return targets

    def build_profile_targets(self, profiles: List[str]) -> List[str]:
        """Builds Instagram profile target URLs."""
        targets = []
        for p in profiles:
            clean = p.strip().lstrip("@")
            if clean and clean.lower() not in IG_RESERVED:
                targets.append(f"https://www.instagram.com/{clean}/")
        return targets

    def normalize_post_url(self, raw_url: str) -> Optional[str]:
        if not raw_url:
            return None
        m = IG_POST_RE.search(raw_url)
        if m:
            shortcode = m.group(1)
            is_reel = "/reel/" in raw_url.lower()
            kind = "reel" if is_reel else "p"
            return f"https://www.instagram.com/{kind}/{shortcode}/"
        return None

    def extract_post_id(self, url: str) -> Optional[str]:
        if not url:
            return None
        m = IG_POST_RE.search(url)
        return m.group(1) if m else None

    def parse_discovered_links(
        self,
        links: List[str],
        sport: Optional[str] = None,
        competition: Optional[str] = None,
        source_type: str = "search_result",
        discovery_method: str = "hashtag_crawl",
    ) -> List[DiscoveredPost]:
        items: List[DiscoveredPost] = []
        seen = set()

        for link in links:
            if not link:
                continue
            normalized = self.normalize_post_url(link.strip())
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)

            shortcode = self.extract_post_id(normalized)

            items.append(
                DiscoveredPost(
                    url=normalized,
                    platform=self.platform_name,
                    post_id=shortcode,
                    source_type=source_type,
                    sport=sport,
                    competition=competition,
                    discovery_method=discovery_method,
                )
            )
        return items
