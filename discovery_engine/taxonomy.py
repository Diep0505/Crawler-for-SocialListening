"""Taxonomy, Keyword Rules, and Classification Engine for Multi-Sport Social Listening.

Scope:
- Basketball: NBA (primary), WNBA, EuroLeague, VBA, NCAA, FIBA, G-League
- Football: EPL, Champions League, La Liga, Serie A, Bundesliga, Ligue 1, V-League, World Cup
- F1: Formula 1, Grand Prix, Teams, Drivers
- Athletics: World Athletics, Olympics, Major Marathons
"""

import re
from typing import Dict, List, Optional, Tuple, Any

SPORT_TAXONOMY: Dict[str, Dict[str, Any]] = {
    "Basketball": {
        "competitions": {
            "NBA": {
                "keywords": [
                    "nba", "national basketball association", "nba finals", "nba playoffs",
                    "all-star game", "slam dunk", "buzzer beater", "triple-double",
                    "lakers", "celtics", "warriors", "bulls", "bucks", "nuggets", "76ers",
                    "sixers", "knicks", "mavericks", "mavs", "clippers", "timberwolves",
                    "cavaliers", "cavs", "pacers", "grizzlies", "pelicans", "trail blazers",
                    "raptors", "hornets", "pistons", "spurs", "miami heat", "utah jazz",
                    "magic", "thunder", "wizards", "suns", "kings", "nets", "hawks", "rockets",
                    "lebron", "stephen curry", "steph curry", "giannis", "tatum", "jokic",
                    "doncic", "wembanyama", "kevin durant", "draymond green", "anthony edwards",
                    "shai gilgeous-alexander", "bóng rổ nba", "giải bóng rổ nba"
                ],
                "hashtags": [
                    "nba", "nbatwitter", "nbaplayoffs", "nbafinals", "lakers", "warriors",
                    "celtics", "dubnation", "lakehow", "nbaallstar", "hoops"
                ],
                "official_handles": ["nba", "bleacherreport", "espn", "clutchpoints"],
            },
            "WNBA": {
                "keywords": ["wnba", "women's nba", "caitlin clark", "angel reese", "sabrina ionescu", "las vegas aces", "new york liberty"],
                "hashtags": ["wnba", "wnbatwitter", "caitlinclark"],
                "official_handles": ["wnba"],
            },
            "EuroLeague": {
                "keywords": ["euroleague", "euro league", "panathinaikos", "olympiacos", "fenerbahce", "real madrid basket", "barca basket"],
                "hashtags": ["euroleague", "devotion"],
                "official_handles": ["euroleague"],
            },
            "VBA": {
                "keywords": ["vba", "vietnam basketball association", "saigon heat", "thang long warriors", "danang dragons", "cantho catfish", "hanoi buffaloes"],
                "hashtags": ["vba", "bongrovn", "saigonheat"],
                "official_handles": ["vba.vn"],
            },
            "NCAA": {
                "keywords": ["march madness", "ncaa basketball", "college basketball", "final four"],
                "hashtags": ["marchmadness", "collegehoops"],
                "official_handles": ["ncaahoops"],
            },
            "FIBA": {
                "keywords": ["fiba", "fiba world cup", "eurobasket", "fiba asia cup", "olympic basketball"],
                "hashtags": ["fiba", "fibaworldcup"],
                "official_handles": ["fiba"],
            },
        },
        "general_keywords": ["basketball", "bóng rổ", "three-pointer", "3-pointer", "slam dunk", "layup", "alley-oop"],
    },
    "Football": {
        "competitions": {
            "EPL": {
                "keywords": [
                    "premier league", "epl", "ngoại hạng anh", "manchester united", "man utd",
                    "liverpool", "arsenal", "man city", "chelsea", "tottenham", "spurs",
                    "newcastle", "aston villa", "haaland", "salah", "saka", "palmer",
                    "guardiola", "arteta", "slot", "amorim", "bóng đá anh"
                ],
                "hashtags": ["premierleague", "epl", "mufc", "lfc", "afc", "cfc", "mancity", "pl"],
                "official_handles": ["premierleague", "skysportsfootball", "fabrizioromano"],
            },
            "Champions League": {
                "keywords": [
                    "champions league", "ucl", "cúp c1", "uefa champions league", "c1 châu âu",
                    "championsleague"
                ],
                "hashtags": ["ucl", "championsleague", "uefachampionsleague"],
                "official_handles": ["championsleague"],
            },
            "La Liga": {
                "keywords": [
                    "la liga", "laliga", "giải tây ban nha", "real madrid", "barcelona",
                    "barca", "atletico madrid", "el clasico", "bellingham", "vinicius",
                    "mbappe", "yamal"
                ],
                "hashtags": ["laliga", "elclasico", "realmadrid", "barca"],
                "official_handles": ["laliga"],
            },
            "Serie A": {
                "keywords": ["serie a", "giải ý", "juventus", "inter milan", "ac milan", "as roma", "napoli"],
                "hashtags": ["seriea", "calcio", "juve", "inter", "acmilan"],
                "official_handles": ["seriea"],
            },
            "Bundesliga": {
                "keywords": ["bundesliga", "giải đức", "bayern munich", "borussia dortmund", "bayer leverkusen", "kane"],
                "hashtags": ["bundesliga", "fcbayern", "bvb"],
                "official_handles": ["bundesliga_en"],
            },
            "Ligue 1": {
                "keywords": ["ligue 1", "giải pháp", "psg", "paris saint-germain", "marseille", "monaco"],
                "hashtags": ["ligue1", "psg"],
                "official_handles": ["ligue1"],
            },
            "V-League": {
                "keywords": [
                    "v-league", "vleague", "bóng đá việt nam", "đội tuyển việt nam", "hà nội fc",
                    "hagl", "công an hà nội", "thể công viettel", "nam định", "bình dương"
                ],
                "hashtags": ["vleague", "bongdavietnam", "dtvn"],
                "official_handles": ["vpf"],
            },
            "World Cup": {
                "keywords": ["world cup", "fifa world cup", "fifa", "vòng chung kết thế giới"],
                "hashtags": ["worldcup", "fifaworldcup"],
                "official_handles": ["fifaworldcup"],
            },
        },
        "general_keywords": ["football", "soccer", "bóng đá", "cầu thủ", "chuyển nhượng", "hat-trick", "penalty", "ballon d'or"],
    },
    "F1": {
        "competitions": {
            "F1": {
                "keywords": [
                    "f1", "formula 1", "formula one", "đua xe f1", "grand prix", "fia formula 1",
                    "verstappen", "hamilton", "leclerc", "norris", "piastri", "russell", "alonso",
                    "red bull racing", "scuderia ferrari", "mercedes amg f1", "mclaren f1",
                    "pole position", "pit stop", "safety car", "paddock"
                ],
                "hashtags": ["f1", "formula1", "f12026", "ferrari", "redbullracing", "mclaren", "lewishamilton", "maxverstappen"],
                "official_handles": ["f1", "fia", "skysportsf1"],
            },
        },
        "general_keywords": ["f1", "formula 1", "motorsport", "đua xe công thức 1", "tay đua f1"],
    },
    "Athletics": {
        "competitions": {
            "World Athletics": {
                "keywords": [
                    "world athletics", "điền kinh thế giới", "chạy 100m", "chạy 200m", "chạy tiếp sức",
                    "nhảy cao", "nhảy xa", "kỷ lục điền kinh", "world athletics championship"
                ],
                "hashtags": ["worldathletics", "athletics", "trackandfield"],
                "official_handles": ["worldathletics"],
            },
            "Olympics": {
                "keywords": ["olympic athletics", "điền kinh olympic", "thế vận hội", "huy chương vàng điền kinh"],
                "hashtags": ["olympics", "olympicathletics"],
                "official_handles": ["olympics"],
            },
            "Marathon": {
                "keywords": [
                    "marathon", "bán marathon", "half marathon", "boston marathon", "london marathon",
                    "berlin marathon", "tokyo marathon", "chicago marathon", "chạy marathon", "kipchoge"
                ],
                "hashtags": ["marathon", "halfmarathon", "runner", "running", "chaybo"],
                "official_handles": ["wmmajors"],
            },
        },
        "general_keywords": ["athletics", "điền kinh", "marathon", "chạy bộ", "running", "runner", "track and field"],
    },
}


def get_sport_queries(
    sport: Optional[str] = None,
    competition: Optional[str] = None,
    limit_per_comp: Optional[int] = None,
) -> List[str]:
    """Generates search query strings for a given sport and/or competition."""
    queries: List[str] = []
    
    target_sports = [sport] if sport and sport in SPORT_TAXONOMY else list(SPORT_TAXONOMY.keys())
    
    for s_name in target_sports:
        s_data = SPORT_TAXONOMY[s_name]
        comps = s_data["competitions"]
        
        target_comps = [competition] if competition and competition in comps else list(comps.keys())
        
        for c_name in target_comps:
            c_data = comps[c_name]
            kws = c_data["keywords"]
            if limit_per_comp:
                kws = kws[:limit_per_comp]
            for kw in kws:
                queries.append(kw)
    return list(dict.fromkeys(queries))


def get_sport_hashtags(sport: Optional[str] = None, competition: Optional[str] = None) -> List[str]:
    """Generates a list of hashtags for a given sport and/or competition."""
    tags: List[str] = []
    target_sports = [sport] if sport and sport in SPORT_TAXONOMY else list(SPORT_TAXONOMY.keys())
    
    for s_name in target_sports:
        s_data = SPORT_TAXONOMY[s_name]
        comps = s_data["competitions"]
        target_comps = [competition] if competition and competition in comps else list(comps.keys())
        for c_name in target_comps:
            tags.extend(comps[c_name].get("hashtags", []))
    return list(dict.fromkeys(tags))


def classify_sport_and_competition(
    text: Optional[str],
    context: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str], float, Dict[str, Any]]:
    """Classifies both Sport and Competition from post text and optional author/source context.
    
    Returns:
        (sport_name, competition_name, confidence_score, evidence_dict)
    """
    if not text or not str(text).strip():
        return None, None, 0.0, {"reason": "empty_text", "matches": []}
    
    clean_text = str(text).lower()
    full_corpus = clean_text + " " + (str(context).lower() if context else "")
    
    best_sport = None
    best_comp = None
    best_score = 0
    best_evidence: List[str] = []
    
    for sport_name, sport_data in SPORT_TAXONOMY.items():
        for comp_name, comp_data in sport_data["competitions"].items():
            matches = []
            
            # Check keywords
            for kw in comp_data["keywords"]:
                pat = r"\b" + re.escape(kw) + r"\b"
                if re.search(pat, full_corpus):
                    matches.append(kw)
            
            # Check hashtags
            for tag in comp_data.get("hashtags", []):
                pat = r"#" + re.escape(tag) + r"\b"
                if re.search(pat, full_corpus):
                    matches.append("#" + tag)
            
            score = len(matches)
            # Give bonus if the competition abbreviation itself is explicitly matched
            if comp_name.lower() in clean_text:
                score += 2
                
            if score > best_score:
                best_score = score
                best_sport = sport_name
                best_comp = comp_name
                best_evidence = matches

    if best_sport and best_score > 0:
        confidence = min(1.0, 0.4 + (best_score * 0.15))
        return best_sport, best_comp, confidence, {
            "status": "matched",
            "score": best_score,
            "evidence": best_evidence[:10],
            "reason": f"Matched {best_sport}/{best_comp} keywords",
        }

    # Fallback to general sport keyword rules if no specific competition matched
    for sport_name, sport_data in SPORT_TAXONOMY.items():
        gen_matches = []
        for kw in sport_data.get("general_keywords", []):
            pat = r"\b" + re.escape(kw) + r"\b"
            if re.search(pat, clean_text):
                gen_matches.append(kw)
        if gen_matches:
            return sport_name, None, 0.5, {
                "status": "sport_only_matched",
                "score": len(gen_matches),
                "evidence": gen_matches[:5],
                "reason": f"Matched general {sport_name} keywords without specific competition",
            }
            
    return None, None, 0.0, {"reason": "no_sport_matched", "matches": []}
