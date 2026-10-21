"""Abstract Base Discovery Adapter for all platforms."""

from abc import ABC, abstractmethod
from typing import List, Optional
from ..models import DiscoveredPost


class BaseDiscoveryAdapter(ABC):
    """Base interface for discovering posts on a given social listening platform."""

    platform_name: str = "Base"

    @abstractmethod
    def build_search_targets(
        self,
        queries: List[str],
        sport: Optional[str] = None,
        competition: Optional[str] = None,
    ) -> List[str]:
        """Builds search entry URLs or endpoint definitions from queries."""
        pass

    @abstractmethod
    def build_hashtag_targets(
        self,
        hashtags: List[str],
        sport: Optional[str] = None,
        competition: Optional[str] = None,
    ) -> List[str]:
        """Builds hashtag exploration entry URLs."""
        pass

    @abstractmethod
    def normalize_post_url(self, raw_url: str) -> Optional[str]:
        """Normalizes a raw post URL into canonical platform format."""
        pass

    @abstractmethod
    def extract_post_id(self, url: str) -> Optional[str]:
        """Extracts unique post ID from a platform post URL."""
        pass

    @abstractmethod
    def parse_discovered_links(
        self,
        links: List[str],
        sport: Optional[str] = None,
        competition: Optional[str] = None,
        source_type: str = "search_result",
        discovery_method: str = "keyword_search",
    ) -> List[DiscoveredPost]:
        """Converts raw link strings into structured DiscoveredPost items."""
        pass
