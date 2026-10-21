"""Multi-Platform Sport Social Listening Discovery Engine.

Supports automated post discovery across Facebook, X (Twitter), Instagram, and Threads
covering Basketball (NBA, etc.), Football (EPL, UCL, etc.), F1, and Athletics.
Covers both official fanpages and individual user/group posts.
"""

from .taxonomy import (
    SPORT_TAXONOMY,
    get_sport_queries,
    get_sport_hashtags,
    classify_sport_and_competition,
)
from .models import DiscoveredPost
from .engine import DiscoveryEngine

__all__ = [
    "SPORT_TAXONOMY",
    "get_sport_queries",
    "get_sport_hashtags",
    "classify_sport_and_competition",
    "DiscoveredPost",
    "DiscoveryEngine",
]
