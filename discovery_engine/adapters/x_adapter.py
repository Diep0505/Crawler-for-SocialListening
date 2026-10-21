"""X / Twitter Discovery Adapter.

Discovers posts across:
1. Keyword Search & Advanced Operators (-is:retweet, latest tweets)
2. Hashtags
3. User Timelines (athletes, reporters, fans)
4. Mention graph
"""

import re
from urllib.parse import quote
from typing import List, Optional
from .base import BaseDiscoveryAdapter
from ..models import DiscoveredPost

TWEET_URL_RE = re.compile(
    r"(?:https?://)?(?:www\.)?(?:twitter\.com|x\.com)/([A-Za-z0-9_]{1,50})/status/(\d+)",
    re.IGNORECASE,
)


class XDiscoveryAdapter(BaseDiscoveryAdapter):
    platform_name: str = "X"

    def build_search_targets(
        self,
        queries: List[str],
        sport: Optional[str] = None,
        competition: Optional[str] = None,
    ) -> List[str]:
        """Builds X live search URLs with boolean filters."""
        targets = []
        for q in queries:
            clean = q.strip()
            if not clean:
                continue
            # Add -is:retweet filter for original content if not specified
            query_str = clean
            if "-is:retweet" not in query_str.lower() and "filter:" not in query_str.lower():
                query_str = f"{clean} -is:retweet"
            encoded = quote(query_str)
            # f=live sorts by latest chronological tweets
            targets.append(f"https://x.com/search?q={encoded}&f=live")
        return targets

    def build_hashtag_targets(
        self,
        hashtags: List[str],
        sport: Optional[str] = None,
        competition: Optional[str] = None,
    ) -> List[str]:
        """Builds X hashtag live search URLs."""
        targets = []
        for tag in hashtags:
            clean = tag.strip().lstrip("#")
            if clean:
                encoded = quote(f"#{clean} -is:retweet")
                targets.append(f"https://x.com/search?q={encoded}&f=live")
        return targets

    def build_user_targets(self, handles: List[str]) -> List[str]:
        """Builds user profile target URLs."""
        targets = []
        for h in handles:
            clean = h.strip().lstrip("@")
            if clean:
                targets.append(f"https://x.com/{clean}")
        return targets

    def normalize_post_url(self, raw_url: str) -> Optional[str]:
        if not raw_url:
            return None
        m = TWEET_URL_RE.search(raw_url)
        if m:
            user, tweet_id = m.groups()
            return f"https://x.com/{user}/status/{tweet_id}"
        return None

    def extract_post_id(self, url: str) -> Optional[str]:
        if not url:
            return None
        m = TWEET_URL_RE.search(url)
        return m.group(2) if m else None

    def extract_author_id(self, url: str) -> Optional[str]:
        if not url:
            return None
        m = TWEET_URL_RE.search(url)
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
