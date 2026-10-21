"""Threads Discovery Adapter.

Discovers posts across:
1. Search queries (threads.net/search?q={query})
2. Hashtags / Topics
3. User feeds (threads.net/@{username})
"""

import re
from urllib.parse import quote_plus
from typing import List, Optional
from .base import BaseDiscoveryAdapter
from ..models import DiscoveredPost

THREADS_POST_RE = re.compile(
    r"(?:https?://)?(?:www\.)?threads\.net/@([A-Za-z0-9_.-]+)/post/([A-Za-z0-9_-]+)",
    re.IGNORECASE,
)


class ThreadsDiscoveryAdapter(BaseDiscoveryAdapter):
    platform_name: str = "Threads"

    def build_search_targets(
        self,
        queries: List[str],
        sport: Optional[str] = None,
        competition: Optional[str] = None,
    ) -> List[str]:
        """Builds Threads search URLs."""
        targets = []
        for q in queries:
            clean = q.strip()
            if clean:
                encoded = quote_plus(clean)
                targets.append(f"https://www.threads.net/search?q={encoded}")
        return targets

    def build_hashtag_targets(
        self,
        hashtags: List[str],
        sport: Optional[str] = None,
        competition: Optional[str] = None,
    ) -> List[str]:
        """Builds Threads search URLs for hashtags."""
        targets = []
        for tag in hashtags:
            clean = tag.strip().lstrip("#")
            if clean:
                encoded = quote_plus(f"#{clean}")
                targets.append(f"https://www.threads.net/search?q={encoded}")
        return targets

    def build_user_targets(self, handles: List[str]) -> List[str]:
        """Builds Threads user profile target URLs."""
        targets = []
        for h in handles:
            clean = h.strip().lstrip("@")
            if clean:
                targets.append(f"https://www.threads.net/@{clean}")
        return targets

    def normalize_post_url(self, raw_url: str) -> Optional[str]:
        if not raw_url:
            return None
        m = THREADS_POST_RE.search(raw_url)
        if m:
            user, post_id = m.groups()
            return f"https://www.threads.net/@{user}/post/{post_id}"
        return None

    def extract_post_id(self, url: str) -> Optional[str]:
        if not url:
            return None
        m = THREADS_POST_RE.search(url)
        return m.group(2) if m else None

    def extract_author_id(self, url: str) -> Optional[str]:
        if not url:
            return None
        m = THREADS_POST_RE.search(url)
        return m.group(1) if m else None

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
            normalized = self.normalize_post_url(link.strip())
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)

            post_id = self.extract_post_id(normalized)
            author_id = self.extract_author_id(normalized)

            items.append(
                DiscoveredPost(
                    url=normalized,
                    platform=self.platform_name,
                    post_id=post_id,
                    author_id=author_id,
                    source_type=source_type,
                    sport=sport,
                    competition=competition,
                    discovery_method=discovery_method,
                )
            )
        return items
