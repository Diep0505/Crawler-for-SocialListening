"""X / Twitter Discovery Adapter.

Discovers posts across:
1. Keyword Search & Advanced Operators (-is:retweet, latest tweets)
2. Hashtags
3. User Timelines (athletes, reporters, fans)
4. Mention graph
"""

import re
from datetime import datetime
from urllib.parse import quote
from typing import List, Optional, Any, Dict, Tuple
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

    def determine_source_type(self, author_id: Optional[str], profile_info: Optional[Dict[str, Any]] = None) -> str:
        """Determines whether an X account is an organization/media outlet, an individual, or unclassified.
        
        Uses known sports entities, verified account status, and semantic handle patterns.
        """
        if not author_id:
            return "unclassified_account"
        handle = str(author_id).lower().lstrip("@")

        known_orgs = {
            # NBA & Basketball
            "nba", "wnba", "celtics", "warriors", "lakers", "chicagobulls", "bucks",
            "dallasmavs", "spurs", "miamiheat", "nyknicks", "okcthunder", "houstonrockets",
            "cavs", "nuggets", "sixers", "timberwolves", "suns", "clippers", "pacers",
            "grizzlies", "pelicans", "trailblazers", "raptors", "hornets", "detroitpistons",
            "utahjazz", "orlandomagic", "washwizards", "sacramentokings", "brooklynnets", "atlhawks",
            # Media & Broadcasters
            "bleacherreport", "sportscenter", "espn", "espnnba", "nbaontnt", "theathletic",
            "clutchpoints", "hoopcentral", "slamonline", "slam", "skysports", "skysportsnba",
            "cbssports", "yahoosports", "actionnetworkhq", "overtime",
            # Football & European clubs
            "premierleague", "championsleague", "mancity", "manutd", "liverpoolfc", "chelseafc",
            "arsenal", "spursofficial", "realmadrid", "fcbarcelona", "bayernmunich", "juventusfc",
            # F1 & Athletics
            "f1", "scuderiaferrari", "redbullracing", "mercedesamgf1", "mclarenf1", "worldathletics",
        }
        
        # Check known orgs or org semantic tokens
        org_tokens = [
            "official", "sports", "news", "tv", "media", "fc", "nation",
            "press", "insider", "network", "report", "daily", "digest", "feed",
            "hq", "team", "club", "association", "league", "hoops"
        ]
        
        if handle in known_orgs or any(tok in handle for tok in org_tokens):
            return "organization"

        # Check if user profile explicitly indicates organization vs person
        if profile_info:
            verified_type = profile_info.get("verified_type")  # "business", "government"
            if verified_type in ("business", "government"):
                return "organization"

        # Known individual athlete/journalist patterns
        individual_tokens = ["coach", "player", "wrtr", "analyst", "reporter", "fan", "guy"]
        if any(tok in handle for tok in individual_tokens):
            return "individual"

        # By default, personal usernames without org markers are classified as individual accounts
        return "individual"

    def extract_mention_profiles(
        self,
        tweets_or_records: List[Any],
        max_per_post: int = 3,
        relevance_sport: Optional[str] = None,
    ) -> List[str]:
        """Extracts verified X profile URLs from tweet entity mentionedUsers (platform-native).
        
        Does NOT convert arbitrary text handles blindly. Retains platform-native X usernames,
        limits fan-out per post, and applies anti-spam deduplication.
        """
        seen = set()
        profiles = []
        for item in tweets_or_records:
            handles = []
            if hasattr(item, "mentionedUsers") and item.mentionedUsers:
                for u in item.mentionedUsers:
                    h = getattr(u, "username", None)
                    if h:
                        handles.append(h)
            elif isinstance(item, dict):
                # Only structured mentions list
                m_list = item.get("mentions") or []
                for m in m_list:
                    clean = str(m).strip().lstrip("@").rstrip(".,;:!?")
                    if clean and not re.search(r"\s", clean):  # X handles cannot contain whitespace
                        handles.append(clean)

            # Limit depth/snowball fan-out per post
            count_for_post = 0
            for h in handles:
                if count_for_post >= max_per_post:
                    break
                h_clean = h.strip().lstrip("@").lower()
                # Ignore system tokens and generic broadcasts
                if not h_clean or h_clean in seen or h_clean in {"everyone", "here", "followers", "channel"}:
                    continue
                seen.add(h_clean)
                profiles.append(f"https://x.com/{h_clean}")
                count_for_post += 1

        return profiles

    def map_tweet_to_record(
        self,
        tweet,
        sport_hint: Optional[str] = None,
        competition: Optional[str] = None,
        query_source: Optional[str] = None,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Maps a twscrape Tweet object or dictionary into the standardized 17-column record.
        Strictly validates research gates without blindly assigning is_accepted=1.
        
        Rules implemented:
        1. view_count: preserves exact int | None, never falls back to video views.
        2. quote tweet: outer tweet metrics are strictly preserved, quote metrics isolated in metadata.
        3. author_followers: placeholder accounts with 0 counters are set to None.
        4. scope validation: ambiguous keywords like 'warriors' without corroborating sports context
           are routed to 'needs_review' with clear review reasons.
        """
        import facebook_dataset as fd
        from ..taxonomy import classify_sport_and_competition

        video_views = None
        is_quote = False
        quote_count = None
        quoted_tweet_info = None
        is_placeholder_user = False
        in_reply_to_id = None

        # 1. Structured extraction
        if hasattr(tweet, "id"):
            post_id = str(tweet.id)
            text = tweet.rawContent or ""
            published_at = tweet.date.isoformat() if hasattr(tweet.date, "isoformat") else str(tweet.date)
            
            # Author & followers: detect placeholder / unavailable users
            u = getattr(tweet, "user", None)
            if not u or getattr(u, "_type", "") == "UserUnavailable" or not getattr(u, "username", None):
                is_placeholder_user = True
                author_id = getattr(u, "username", None) or "unavailable_user"
                author_followers = None
            else:
                author_id = u.username
                # twscrape placeholder check: if rawDescription is None and all counts are 0, user is placeholder
                is_zero_placeholder = (
                    getattr(u, "rawDescription", None) is None
                    and getattr(u, "followersCount", 0) == 0
                    and getattr(u, "statusesCount", 0) == 0
                    and getattr(u, "created", None) is None
                )
                if is_zero_placeholder:
                    is_placeholder_user = True
                    author_followers = None
                else:
                    author_followers = u.followersCount

            like_count = getattr(tweet, "likeCount", None)
            comment_count = getattr(tweet, "replyCount", None)
            share_count = getattr(tweet, "retweetCount", None)
            
            # view_count: preserve exact None when missing (twscrape: viewCount: int | None)
            raw_view = getattr(tweet, "viewCount", None)
            view_count = int(raw_view) if raw_view is not None else None

            # Quotes & Video metrics (kept separate from tweet metrics)
            quote_count = getattr(tweet, "quoteCount", None)
            quoted = getattr(tweet, "quotedTweet", None)
            is_quote = bool(getattr(tweet, "isQuoteStatus", False) or quoted)
            if quoted:
                quoted_tweet_info = {
                    "id": str(getattr(quoted, "id", "")),
                    "author": getattr(quoted.user, "username", None) if getattr(quoted, "user", None) else None,
                    "text": getattr(quoted, "rawContent", None),
                    "like_count": getattr(quoted, "likeCount", None),
                    "view_count": getattr(quoted, "viewCount", None),
                }

            in_reply_to_id = getattr(tweet, "inReplyToTweetId", None)

            media_type = "text"
            media = getattr(tweet, "media", None)
            if media:
                if getattr(media, "videos", None):
                    media_type = "video"
                    first_video = media.videos[0]
                    if hasattr(first_video, "views") and first_video.views is not None:
                        video_views = first_video.views
                elif getattr(media, "photos", None):
                    media_type = "image"

            hashtags = [h.strip().lstrip("#") for h in (getattr(tweet, "hashtags", []) or [])]
            mentions = [f"@{m.username}" if hasattr(m, "username") else str(m) for m in (getattr(tweet, "mentionedUsers", []) or [])]
            lang = getattr(tweet, "lang", None)
            post_url = getattr(tweet, "url", None) or f"https://x.com/{author_id}/status/{post_id}"

        elif isinstance(tweet, dict):
            post_id = str(tweet.get("id") or tweet.get("post_id") or "")
            text = tweet.get("rawContent") or tweet.get("text") or ""
            published_at = str(tweet.get("date") or tweet.get("published_at") or "")
            author_id = tweet.get("username") or tweet.get("author_id")
            
            if tweet.get("is_placeholder_user") or not author_id:
                is_placeholder_user = True
                author_followers = None
            else:
                author_followers = tweet.get("followersCount") if "followersCount" in tweet else tweet.get("author_followers")

            like_count = tweet.get("likeCount") if "likeCount" in tweet else tweet.get("like_count")
            comment_count = tweet.get("replyCount") if "replyCount" in tweet else tweet.get("comment_count")
            share_count = tweet.get("retweetCount") if "retweetCount" in tweet else tweet.get("share_count")
            
            raw_view = tweet.get("viewCount") if "viewCount" in tweet else tweet.get("view_count")
            view_count = int(raw_view) if raw_view is not None else None
            
            quote_count = tweet.get("quoteCount") if "quoteCount" in tweet else tweet.get("quote_count")
            video_views = tweet.get("video_views")
            is_quote = bool(tweet.get("is_quote") or tweet.get("quotedTweet"))
            if tweet.get("quotedTweet") and isinstance(tweet["quotedTweet"], dict):
                quoted_tweet_info = {
                    "id": str(tweet["quotedTweet"].get("id")),
                    "author": tweet["quotedTweet"].get("username"),
                    "text": tweet["quotedTweet"].get("text"),
                    "like_count": tweet["quotedTweet"].get("likeCount"),
                    "view_count": tweet["quotedTweet"].get("viewCount"),
                }

            in_reply_to_id = tweet.get("inReplyToTweetId")
            hashtags = tweet.get("hashtags")
            mentions = tweet.get("mentions")
            lang = tweet.get("lang") or tweet.get("language")
            post_url = tweet.get("url") or tweet.get("post_url") or f"https://x.com/{author_id}/status/{post_id}"
            media_type = tweet.get("media_type", "text")
        else:
            raise ValueError(f"Unsupported tweet object type: {type(tweet)}")

        # Post type determination
        if in_reply_to_id:
            post_type = "reply"
        elif is_quote:
            post_type = "quote"
        else:
            post_type = "original"

        # 2. Scope & Sport/Competition Classification
        cls_sport, cls_comp, conf, meta = classify_sport_and_competition(text, context=author_id)
        final_sport = sport_hint or cls_sport or "Basketball"
        final_comp = competition or cls_comp or "NBA"

        # 3. Research Validation Gate
        review_reasons = []
        if not post_id or post_id == "None":
            review_reasons.append("missing_post_id")
        if not post_url or not post_url.startswith("http"):
            review_reasons.append("missing_or_invalid_post_url")
        if not published_at or published_at == "None":
            review_reasons.append("missing_published_at")
        else:
            # Validate timestamp format
            try:
                dt_clean = published_at.replace("Z", "+00:00")
                parsed_dt = datetime.fromisoformat(dt_clean)
                if parsed_dt.year < 2006 or parsed_dt.year > 2030:
                    review_reasons.append("timestamp_out_of_valid_range")
            except Exception:
                review_reasons.append("invalid_timestamp_format")

        if not text or not str(text).strip():
            review_reasons.append("empty_caption_text")

        # 4. Scope Assessment (Prevent ambiguous false positives like "Warriors" movies/cats)
        ambiguous_tokens = {"warriors", "kings", "magic", "thunder", "spurs", "nets", "heat", "suns", "wizards", "bulls", "bucks", "pacers", "hawks", "rockets", "jazz"}
        text_lower = text.lower() if text else ""
        
        if final_comp == "NBA":
            # Strong corroborating NBA signals
            strong_signals = [
                "nba", "basketball", "bóng rổ", "lakers", "celtics", "steph curry", "curry", "lebron",
                "triple-double", "slam dunk", "all-star", "dunk", "three-pointer", "buzzer beater",
                "playoffs", "gsw", "dubnation", "lakehow", "mffl", "rebound", "free throw", "court",
                "halftime", "coach", "rookie", "giannis", "tatum", "doncic", "jokic", "wembanyama",
                "anthony edwards", "shai", "brunson", "trade deadline"
            ]
            has_strong_signal = any(sig in text_lower for sig in strong_signals)
            is_sports_author = bool(author_id and self.determine_source_type(author_id) == "organization")

            # Check if text only matched an ambiguous token without corroborating context
            matched_evidence = meta.get("evidence", [])
            only_ambiguous = (
                len(matched_evidence) > 0
                and all(tok.lower().strip("#") in ambiguous_tokens for tok in matched_evidence)
            )

            if only_ambiguous and not has_strong_signal and not is_sports_author:
                review_reasons.append(
                    f"ambiguous_scope_keyword ({matched_evidence}) lacks corroborating NBA/basketball context"
                )
            elif not has_strong_signal and not is_sports_author and meta.get("status") != "matched":
                review_reasons.append(f"scope_unmatched_or_insufficient_evidence (target: {final_comp})")

        is_accepted = (len(review_reasons) == 0)
        status = "success" if is_accepted else "needs_review"
        source_type = self.determine_source_type(author_id)

        # 5. Standard 17-column Record
        record = fd.build_dataset_record(
            post_url=post_url,
            text=text,
            published_at=published_at,
            author_id=author_id,
            post_id=post_id,
            sport_category=final_sport,
            like_count=like_count,
            comment_count=comment_count,
            share_count=share_count,
            view_count=view_count,
            media_type=media_type,
            author_followers=author_followers,
            language=lang,
            hashtags=hashtags,
            mentions=mentions,
            platform="X",
        )

        tech_meta = {
            "platform": "X",
            "status": status,
            "is_accepted": is_accepted,
            "review_reasons": review_reasons,
            "source_type": source_type,
            "post_type": post_type,
            "sport_classification": meta,
            "confidence": conf,
            "competition": final_comp,
            "discovery_query": query_source,
            "time_precision": "exact",
            "metric_definitions": {
                "like_count": "Likes on X (twscrape: likeCount)",
                "comment_count": "Replies on X (twscrape: replyCount)",
                "share_count": "Retweets / Reposts on X (twscrape: retweetCount; excludes Quotes)",
                "view_count": "Tweet Impressions/Views (twscrape: viewCount; null when omitted by platform)",
            },
            "metrics_details": {
                "quote_count": quote_count,
                "is_quote": is_quote,
                "video_views": video_views,
                "quoted_tweet": quoted_tweet_info,
            },
            "author_metadata": {
                "is_placeholder_user": is_placeholder_user,
                "author_followers": author_followers,
                "author_followers_note": "Null indicates missing or placeholder profile object, not confirmed zero" if is_placeholder_user else "Observed count",
            },
        }

        return record, tech_meta
