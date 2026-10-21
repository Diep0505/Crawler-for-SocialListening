"""Data Models for Multi-Platform Discovered Content."""

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, Dict, Any


@dataclass
class DiscoveredPost:
    url: str
    platform: str = "Facebook"  # "Facebook", "X", "Instagram", "Threads"
    post_id: Optional[str] = None
    author_id: Optional[str] = None
    source_type: str = "fanpage"  # "fanpage", "individual", "group", "hashtag", "search_result"
    sport: Optional[str] = None
    competition: Optional[str] = None
    discovery_method: str = "keyword_search"  # "keyword_search", "hashtag_crawl", "group_feed", "page_feed", "mention_graph"
    preview_text: Optional[str] = None
    discovered_at: Optional[str] = None

    def __post_init__(self):
        if not self.discovered_at:
            self.discovered_at = datetime.now(timezone.utc).isoformat()
        if self.platform:
            # Capitalize platform properly (Facebook, X, Instagram, Threads)
            p_map = {"facebook": "Facebook", "x": "X", "twitter": "X", "instagram": "Instagram", "threads": "Threads"}
            self.platform = p_map.get(self.platform.lower(), self.platform)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DiscoveredPost":
        # Handle field aliases
        url = data.get("url") or data.get("post_url") or data.get("canonical_url")
        if not url:
            raise ValueError("URL is required for DiscoveredPost")
        
        return cls(
            url=url,
            platform=data.get("platform", "Facebook"),
            post_id=data.get("post_id"),
            author_id=data.get("author_id"),
            source_type=data.get("source_type", "fanpage"),
            sport=data.get("sport") or data.get("sport_category"),
            competition=data.get("competition"),
            discovery_method=data.get("discovery_method", "keyword_search"),
            preview_text=data.get("preview_text") or data.get("text"),
            discovered_at=data.get("discovered_at"),
        )

    def to_pipeline_scope(self) -> str:
        """Returns standard scope string: e.g. 'facebook:basketball:nba'."""
        plat = self.platform.lower()
        sp = (self.sport or "sport").lower()
        comp = (self.competition or "general").lower()
        return f"{plat}:{sp}:{comp}"
