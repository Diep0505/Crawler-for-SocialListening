"""Discovery Engine Orchestrator for Multi-Platform Sport Social Listening.

Combines multiple discovery techniques:
- Keyword Search (Facebook Search, X Live Search, Threads Search)
- Hashtag Crawl (Instagram, X, Facebook, Threads)
- Public Groups (Facebook Community Discussions & Fan Discussions)
- Seed & Fanpage Feeds
- Deduplication against SQLite PipelineDB
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from datetime import datetime, timezone

from .models import DiscoveredPost
from .taxonomy import (
    SPORT_TAXONOMY,
    get_sport_queries,
    get_sport_hashtags,
    classify_sport_and_competition,
)
from .adapters.base import BaseDiscoveryAdapter
from .adapters.facebook_adapter import FacebookDiscoveryAdapter
from .adapters.x_adapter import XDiscoveryAdapter
from .adapters.instagram_adapter import InstagramDiscoveryAdapter
from .adapters.threads_adapter import ThreadsDiscoveryAdapter

logger = logging.getLogger("DiscoveryEngine")


class DiscoveryEngine:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = Path(db_path) if db_path else Path("output/pipeline.db")
        self.adapters: Dict[str, BaseDiscoveryAdapter] = {
            "facebook": FacebookDiscoveryAdapter(),
            "x": XDiscoveryAdapter(),
            "instagram": InstagramDiscoveryAdapter(),
            "threads": ThreadsDiscoveryAdapter(),
        }

    def get_adapter(self, platform: str) -> Optional[BaseDiscoveryAdapter]:
        return self.adapters.get(str(platform).lower())

    def plan_discovery_targets(
        self,
        platforms: Optional[List[str]] = None,
        sports: Optional[List[str]] = None,
        competitions: Optional[List[str]] = None,
        techniques: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Generates target URLs across selected platforms and techniques.
        
        Techniques can include:
        - 'keyword_search'
        - 'hashtag_crawl'
        - 'group_feed' (Facebook public groups)
        - 'page_feed' (Official media & fanpages)
        """
        active_platforms = [p.lower() for p in (platforms or list(self.adapters.keys()))]
        active_sports = sports or list(SPORT_TAXONOMY.keys())
        active_techniques = set(techniques or ["keyword_search", "hashtag_crawl", "page_feed", "group_feed"])

        plan: Dict[str, Any] = {
            "platforms": active_platforms,
            "sports": active_sports,
            "competitions": competitions,
            "targets": {},
            "total_targets": 0,
        }

        for plat_key in active_platforms:
            adapter = self.get_adapter(plat_key)
            if not adapter:
                continue

            plat_targets: Dict[str, List[str]] = {}

            for s_name in active_sports:
                if s_name not in SPORT_TAXONOMY:
                    continue

                comps = [competitions] if isinstance(competitions, str) else competitions
                queries = get_sport_queries(sport=s_name, competition=comps[0] if comps and len(comps) == 1 else None)
                hashtags = get_sport_hashtags(sport=s_name, competition=comps[0] if comps and len(comps) == 1 else None)

                # 1. Keyword search targets
                if "keyword_search" in active_techniques:
                    s_key = f"{s_name}:keywords"
                    plat_targets[s_key] = adapter.build_search_targets(queries, sport=s_name)

                # 2. Hashtag targets
                if "hashtag_crawl" in active_techniques:
                    h_key = f"{s_name}:hashtags"
                    plat_targets[h_key] = adapter.build_hashtag_targets(hashtags, sport=s_name)

            total_for_plat = sum(len(urls) for urls in plat_targets.values())
            plan["targets"][plat_key] = {
                "categories": plat_targets,
                "count": total_for_plat,
            }
            plan["total_targets"] += total_for_plat

        return plan

    def process_discovered_links(
        self,
        raw_links: List[str],
        platform: str,
        sport: Optional[str] = None,
        competition: Optional[str] = None,
        source_type: str = "search_result",
        discovery_method: str = "keyword_search",
    ) -> List[DiscoveredPost]:
        """Parses, normalizes, and classifies raw link strings for a platform."""
        adapter = self.get_adapter(platform)
        if not adapter:
            logger.warning(f"No adapter registered for platform: {platform}")
            return []

        posts = adapter.parse_discovered_links(
            links=raw_links,
            sport=sport,
            competition=competition,
            source_type=source_type,
            discovery_method=discovery_method,
        )
        return posts

    def enqueue_posts(
        self,
        posts: List[DiscoveredPost],
        db_path: Optional[str] = None,
    ) -> Dict[str, int]:
        """Enqueues DiscoveredPost objects into SQLite pipeline_tasks table."""
        from facebook_pipeline import PipelineDB

        target_db = Path(db_path) if db_path else self.db_path
        target_db.parent.mkdir(parents=True, exist_ok=True)

        db = PipelineDB(str(target_db))
        added_count = 0
        skipped_count = 0

        for post in posts:
            was_added = db.enqueue_discovered_post(post)
            if was_added:
                added_count += 1
            else:
                skipped_count += 1

        db.close()
        return {
            "total_submitted": len(posts),
            "enqueued": added_count,
            "skipped_or_existing": skipped_count,
        }

    def export_discovery_batch(
        self,
        posts: List[DiscoveredPost],
        output_dir: str = "output/discovery",
    ) -> Dict[str, str]:
        """Saves discovered posts into JSON and JSONL artifacts."""
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        now_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        jsonl_file = out_path / f"discovered_posts_{now_str}.jsonl"
        summary_file = out_path / f"discovery_summary_{now_str}.json"

        # Write JSONL
        with jsonl_file.open("w", encoding="utf-8") as f:
            for p in posts:
                f.write(json.dumps(p.to_dict(), ensure_ascii=False) + "\n")

        # Summary statistics
        by_platform: Dict[str, int] = {}
        by_sport: Dict[str, int] = {}
        by_source_type: Dict[str, int] = {}

        for p in posts:
            by_platform[p.platform] = by_platform.get(p.platform, 0) + 1
            sp = p.sport or "unknown"
            by_sport[sp] = by_sport.get(sp, 0) + 1
            by_source_type[p.source_type] = by_source_type.get(p.source_type, 0) + 1

        summary = {
            "total_discovered": len(posts),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "by_platform": by_platform,
            "by_sport": by_sport,
            "by_source_type": by_source_type,
            "jsonl_path": str(jsonl_file),
        }

        with summary_file.open("w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        return {
            "jsonl": str(jsonl_file),
            "summary": str(summary_file),
        }
