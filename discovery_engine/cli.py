"""CLI Interface for Multi-Platform Sport Social Listening Discovery Engine."""

import argparse
import json
import sys
from pathlib import Path

from .engine import DiscoveryEngine
from .taxonomy import SPORT_TAXONOMY


def parse_args():
    parser = argparse.ArgumentParser(
        description="Multi-Platform Discovery Engine for Sport Social Listening.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--platforms",
        type=str,
        default="facebook,x,instagram,threads",
        help="Comma-separated platforms: facebook, x, instagram, threads",
    )
    parser.add_argument(
        "--sports",
        type=str,
        default="Basketball,Football,F1,Athletics",
        help="Comma-separated sports: Basketball, Football, F1, Athletics",
    )
    parser.add_argument(
        "--competitions",
        type=str,
        default=None,
        help="Optional competition filters, e.g. NBA, EPL, F1",
    )
    parser.add_argument(
        "--techniques",
        type=str,
        default="keyword_search,hashtag_crawl,page_feed,group_feed",
        help="Techniques: keyword_search, hashtag_crawl, page_feed, group_feed",
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Generate and print discovery search plan targets without executing crawl.",
    )
    parser.add_argument(
        "--import-file",
        type=str,
        default=None,
        help="Import URLs from a text or jsonl file into discovery pipeline.",
    )
    parser.add_argument(
        "--import-platform",
        type=str,
        default="Facebook",
        help="Platform associated with imported file.",
    )
    parser.add_argument(
        "--import-sport",
        type=str,
        default="Basketball",
        help="Sport associated with imported file.",
    )
    parser.add_argument(
        "--import-competition",
        type=str,
        default="NBA",
        help="Competition associated with imported file.",
    )
    parser.add_argument(
        "--enqueue",
        action="store_true",
        help="Automatically enqueue discovered posts into SQLite pipeline queue.",
    )
    parser.add_argument(
        "--db",
        type=str,
        default="output/pipeline.db",
        help="Path to SQLite pipeline database.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="output/discovery",
        help="Directory to save discovery artifacts.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    platforms = [p.strip().lower() for p in args.platforms.split(",") if p.strip()]
    sports = [s.strip() for s in args.sports.split(",") if s.strip()]
    competitions = [c.strip() for c in args.competitions.split(",") if c.strip()] if args.competitions else None
    techniques = [t.strip() for t in args.techniques.split(",") if t.strip()]

    engine = DiscoveryEngine(db_path=args.db)

    print("=================================================================")
    print("🚀 MULTI-PLATFORM SPORT DISCOVERY ENGINE")
    print("=================================================================")
    print(f"Platforms    : {', '.join(platforms)}")
    print(f"Sports       : {', '.join(sports)}")
    print(f"Competitions : {', '.join(competitions) if competitions else 'All taxonomy competitions'}")
    print(f"Techniques   : {', '.join(techniques)}")
    print(f"Database     : {args.db}")
    print("-----------------------------------------------------------------")

    # 1. Plan mode
    plan = engine.plan_discovery_targets(
        platforms=platforms,
        sports=sports,
        competitions=competitions,
        techniques=techniques,
    )

    print(f"Generated {plan['total_targets']} search & crawl targets across platforms:")
    for plat, pdata in plan["targets"].items():
        print(f"  • [{plat.upper()}]: {pdata['count']} targets")
        for cat, urls in pdata["categories"].items():
            print(f"      - {cat}: {len(urls)} targets (sample: {urls[0] if urls else 'N/A'})")

    if args.plan_only:
        out_plan = Path(args.output_dir) / "discovery_plan.json"
        out_plan.parent.mkdir(parents=True, exist_ok=True)
        with out_plan.open("w", encoding="utf-8") as f:
            json.dump(plan, f, indent=2, ensure_ascii=False)
        print(f"\n[OK] Plan saved to: {out_plan}")
        return

    # 2. File Import mode
    if args.import_file:
        import_path = Path(args.import_file)
        if not import_path.is_file():
            print(f"[Error] Import file not found: {args.import_file}", file=sys.stderr)
            sys.exit(1)

        raw_lines = [line.strip() for line in import_path.read_text(encoding="utf-8-sig").splitlines() if line.strip() and not line.strip().startswith("#")]
        print(f"\nImporting {len(raw_lines)} raw links from {import_path.name}...")

        discovered = engine.process_discovered_links(
            raw_links=raw_lines,
            platform=args.import_platform,
            sport=args.import_sport,
            competition=args.import_competition,
            source_type="imported_source",
            discovery_method="file_import",
        )
        print(f"[OK] Parsed and normalized {len(discovered)} valid posts.")

        # Export batch
        artifacts = engine.export_discovery_batch(discovered, output_dir=args.output_dir)
        print(f"Saved artifacts:")
        print(f"  JSONL   : {artifacts['jsonl']}")
        print(f"  Summary : {artifacts['summary']}")

        # Enqueue if requested
        if args.enqueue:
            stats = engine.enqueue_posts(discovered, db_path=args.db)
            print(f"\n[Database Ingestion Results]")
            print(f"  Total submitted      : {stats['total_submitted']}")
            print(f"  Successfully enqueued: {stats['enqueued']}")
            print(f"  Skipped (duplicates) : {stats['skipped_or_existing']}")

    print("\n[OK] Discovery Engine completed successfully.")


if __name__ == "__main__":
    main()
