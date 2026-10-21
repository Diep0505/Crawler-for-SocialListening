"""Adapters for platform discovery engines."""

from .base import BaseDiscoveryAdapter
from .facebook_adapter import FacebookDiscoveryAdapter
from .x_adapter import XDiscoveryAdapter
from .instagram_adapter import InstagramDiscoveryAdapter
from .threads_adapter import ThreadsDiscoveryAdapter

__all__ = [
    "BaseDiscoveryAdapter",
    "FacebookDiscoveryAdapter",
    "XDiscoveryAdapter",
    "InstagramDiscoveryAdapter",
    "ThreadsDiscoveryAdapter",
]
