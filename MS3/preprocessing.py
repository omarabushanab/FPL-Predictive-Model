from cohere import Client
from sentence_transformers import SentenceTransformer
import re
import os
from dotenv import load_dotenv
from huggingface_hub import InferenceClient
from google import genai
from google.genai import types
from transformers import T5ForConditionalGeneration, T5Tokenizer
import os
import google.genai as genai
from google.genai.errors import ClientError


load_dotenv()

# INTENT_KEYWORDS = {
#     "compare_players": ["compare", "vs", "versus", "better", "stronger"],
#     "player_performance": ["performance", "points", "stats", "gw", "gameweek"],
#     "player_history": ["history", "season history", "past seasons"],
#     "top_players_position": ["best", "top", "highest", "ranking", "forwards", "midfielders", "defenders"],
#     "team_analysis": ["form", "team form", "how is", "analysis"],
#     "team_fixtures": ["fixtures", "next games", "schedule"],
#     "fixture_difficulty": ["easy fixtures", "difficulty", "FDR"],
#     "search_player": ["who is", "player", "tell me about"],
#     "search_team": ["team", "club", "squad"],
#     "recommend_player": ["recommend", "captain", "buy", "transfer", "who should I"]
# }

VALID_INTENTS = {
        "player_performance", "player_history", "compare_players", 
        "top_players_position", "team_analysis", "team_fixtures", 
        "fixture_difficulty", "search_player", "search_team", "recommend_player"
    }

INTENT_PROMPT = """
You are an intent classifier for Fantasy Premier League queries.
Return ONLY one intent label from this list:

- player_performance
- player_history
- compare_players
- top_players_position
- team_analysis
- team_fixtures
- fixture_difficulty
- search_player
- search_team
- recommend_player
- rank_players_by_stat

User Query: "{}"

Answer with ONLY the label.
"""
#TODO needs fixing
#  b. Entity Extractions 
class FPLEncoderNER:
    """
    Domain-Specific NER for FPL theme.
    Loads lookup dictionaries from the MS2 Neo4j KG.
    """

    def __init__(self, conn):
        self.conn = conn

        # Load lookup tables using MS2 schema
        self.PLAYERS = self._load_unique("Player", "player_name")
        self.TEAMS = self._load_unique("Team", "name")
        self.POSITIONS = self._load_unique("Position", "name")
        self.SEASONS = self._load_unique("Season", "season_name")

        # Stat keywords -> mapped to PLAYED_IN relationship properties
        self.STAT_KEYWORDS = {
            "goals": "goals_scored",
            "goal": "goals_scored",
            "assists": "assists",
            "assist": "assists",
            "points": "total_points",
            "point": "total_points",
            "goals conceded": "goals_conceded",
            "conceded": "goals_conceded",
            " form": "form",
            "clean sheet": "clean_sheets",
            "clean sheets": "clean_sheets",
            "bonus": "bonus",
            "minutes": "minutes",
            "saves": "saves",
            "ict": "ict_index",
            "threat": "threat",
            "creativity": "creativity",
            "creative": "creativity",
            "own goals": "own_goals",
            "yellow cards": "yellow_cards",
            "red cards": "red_cards",
            "penalties saved": "penalties_saved",
            "penalties missed": "penalties_missed",
            "influence": "influence",
            "bps": "bps",
            "xg": "xG",
            "xa": "xA",
            "expected points": "upcoming_total_points",
            "upcoming total points": "upcoming_total_points",
            "upcoming points": "upcoming_total_points"
        }

        # Position synonyms -> mapped to Position.name
        self.POSITION_SYNONYMS = {
            "goalkeeper": "GK",
            "keeper": "GK",
            "goalie": "GK",
            "GK": "GK",
            "gk": "GK",
            "defender": "DEF",
            "DEF": "DEF",
            "def": "DEF",
            "centre back": "DEF",
            "center back": "DEF",
            "cb": "DEF",
            "fullback": "DEF",
            "midfielder": "MID",
            "midfield": "MID",
            "mid": "MID",
            "central mid": "MID",
            "central midfielder": "MID",
            "wing mid": "MID",
            "CM": "MID",
            "CAM": "MID",
            "CDM": "MID",
            "RM": "MID",
            "LM": "MID",
            "winger": "MID",
            "forward": "FWD",
            "striker": "FWD",
            "attacker": "FWD"
        }

    # -----------------------------------------------------
    # Load distinct properties MS2 Neo4j KG
    # -----------------------------------------------------
    def _load_unique(self, label, prop):
        query = f"MATCH (n:{label}) RETURN DISTINCT n.{prop} AS value"
        records = self.conn.execute_query(query)
        return [r["value"] for r in records]

    # -----------------------------------------------------
    # NER Function
    # -----------------------------------------------------
    def extract(self, query):
        q = query.lower()

        entities = {
            "players": [],      # array
            "teams": [],        # array
            "positions": [],    # array
            "season": [],       # array
            "gameweek": [],     # array
            "stat": []          # array
        }

        # Get all words from query (for whole-word matching)
        query_words = set(q.split())
        
        # Common words to avoid matching as player names
        common_words = {
            'a', 'an', 'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
            'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'should',
            'could', 'can', 'may', 'might', 'must', 'shall', 'season', 'gameweek',
            'gw', 'this', 'last', 'next', 'compare', 'show', 'me', 'who', 'what',
            'when', 'where', 'why', 'how', 'best', 'top', 'most', 'least'
        }
        
        # 1. Player names - WHOLE WORD MATCHING ONLY
        # Normalize query tokens once (remove possessives and punctuation)
        def _normalize_token(tok: str) -> str:
            tok = tok.lower()
            # remove trailing possessive "'s" (e.g., "saka's" -> "saka")
            tok = re.sub(r"'s\b", "", tok)
            # remove non-alphanumeric characters
            tok = re.sub(r"[^a-z0-9]", "", tok)
            return tok

        normalized_query_words = set()
        for w in q.split():
            nw = _normalize_token(w)
            if nw:
                normalized_query_words.add(nw)

        # Create a normalized version of common_words for comparison
        normalized_common = { _normalize_token(w) for w in common_words }

        # 1. Player names - WHOLE WORD MATCHING ONLY (robust to case & possessives)
        for p in self.PLAYERS:
            if not p:
                continue

            player_lower = p.lower()
            # split player name into alphanumeric tokens (keeps multi-word names)
            player_raw_words = re.findall(r"[A-Za-z0-9']+", player_lower)

            # normalize each token (remove punctuation/possessive)
            player_words = [ _normalize_token(w) for w in player_raw_words if _normalize_token(w) ]

            if not player_words:
                continue

            # Skip very short player names to avoid false positives
            if len(player_words) == 1 and len(player_words[0]) < 4:
                continue

            # Strategy 1: Check if ALL normalized words in player name are in normalized_query_words
            all_words_match = all(
                (word in normalized_query_words) and (word not in normalized_common)
                for word in player_words
            )

            if all_words_match:
                if p not in entities["players"]:
                    entities["players"].append(p)
                continue  # matched full name, go to next player

            # Strategy 2: Last name only (but avoid common words)
            if len(player_words) > 1:
                last_name = player_words[-1]
                # Only match if normalized last name appears in normalized query AND passes filters
                if (last_name in normalized_query_words and
                    len(last_name) > 3 and
                    last_name not in normalized_common and
                    not last_name.endswith('ason') and   # avoid "season"
                    not last_name.endswith('rward')):    # avoid "forward"
                    if p not in entities["players"]:
                        entities["players"].append(p)
                    continue

        # 2. Team names (handle synonyms like Manchester City / United)
        TEAM_SYNONYMS = {
            "manchester city": "Man City",
            "man city": "Man City",
            "manchester united": "Man Utd",
            "man united": "Man Utd",
            "man utd": "Man Utd",
            "tottenham": "Spurs",
            "forest": "Nott'm Forest",
            "nottm forest": "Nott'm Forest",
            "nottingham": "Nott'm Forest",
            "wolverhampton": "Wolves",
        }

        # normalize query once
        normalized_q = q.lower()

        for t in self.TEAMS:
            if not t:
                continue

            team_lower = t.lower()

            # Direct match (exact DB name appears in query)
            if team_lower in normalized_q:
                if t not in entities["teams"]:
                    entities["teams"].append(t)
                continue

            # Synonym match (Manchester City / United cases)
            for phrase, canonical in TEAM_SYNONYMS.items():
                if phrase in normalized_q and canonical.lower() == team_lower:
                    if t not in entities["teams"]:
                        entities["teams"].append(t)
                    break


        # 3. Position (exact) - now array
        for pos in self.POSITIONS:
            if pos.lower() in q:
                if pos not in entities["positions"]:
                    entities["positions"].append(pos)

        # 3b. Position synonyms - now array
        for word, pos in self.POSITION_SYNONYMS.items():
            if word in q:
                if pos not in entities["positions"]:
                    entities["positions"].append(pos)

        # 4. Season - accepts both "2023/24" and "2023-24", stores as "2023-24"
        season_match = re.findall(r"(20\d{2}[/-]\d{2})", q)
        if season_match:
            for season in season_match:
                # Convert any / to - for consistent storage
                season_dash = season.replace('/', '-')
                if season_dash not in entities["season"]:
                    entities["season"].append(season_dash)

        # 4b. Rule-based - now array
        if "this season" in q:
            current_season = max(self.SEASONS)
            # Convert / to - in the current season
            if current_season not in entities["season"]:
                entities["season"].append(current_season)

        if "last season" in q:
            sorted_s = sorted(self.SEASONS)
            if len(sorted_s) >= 2:
                last_season = sorted_s[-2]
                
                if last_season not in entities["season"]:
                    entities["season"].append(last_season)

        # 5. Gameweek - now array
        # First: explicit "gw" or "gameweek" mentions (captures "gameweek 5 and 6", "gw 5,6", "gameweek 5-6")
        for match in re.finditer(r"(gw|gameweek)\s*(\d{1,2})", q, re.IGNORECASE):
            gw_num = match.group(2)
            if gw_num not in entities["gameweek"]:
                entities["gameweek"].append(gw_num)

            # look ahead for additional numbers joined by "and", commas, or simple ranges
            pos = match.end()
            while True:
                # 1) range: "-6" or "to 6"  (e.g., "gameweek 5-6" or "gameweek 5 to 6")
                range_match = re.match(r"\s*(?:-|to)\s*(\d{1,2})", q[pos:], re.IGNORECASE)
                if range_match:
                    end_gw = range_match.group(1)
                    try:
                        start = int(gw_num)
                        end = int(end_gw)
                        if start <= end:
                            for n in range(start + 1, end + 1):
                                s = str(n)
                                if s not in entities["gameweek"]:
                                    entities["gameweek"].append(s)
                    except ValueError:
                        pass
                    pos += range_match.end()
                    break  # range consumed, stop lookahead for this match

                # 2) comma / and separated next numbers: ", 6" or "and 6"
                sep_match = re.match(r"\s*(?:and|,)\s*(\d{1,2})", q[pos:], re.IGNORECASE)
                if sep_match:
                    extra = sep_match.group(1)
                    if extra not in entities["gameweek"]:
                        entities["gameweek"].append(extra)
                    pos += sep_match.end()
                    # continue loop to allow "and 6, 7" chains
                    continue

                # nothing more in the lookahead
                break

        # Gameweeks
        # If there were explicit GW mentions, prefer them and skip the fallback
        if not entities["gameweek"]:
            # Remove season substrings like "2022-23" or "2022/23" so their numeric parts are not mistaken
            q_no_season = re.sub(r"\b20\d{2}[/-]\d{2}\b", " ", q)

            # Also remove any 'season' word contexts to be safe
            q_no_season = q_no_season.replace("season", " ")

            # Look for standalone numbers that could be GWs (1-38), but avoid numbers inside larger numbers
            standalone_matches = re.findall(r"\b(\d{1,2})\b", q_no_season)
            for num_str in standalone_matches:
                try:
                    num_int = int(num_str)
                    if 1 <= num_int <= 38:
                        if num_str not in entities["gameweek"]:
                            entities["gameweek"].append(num_str)  # Store as string
                except ValueError:
                    pass


        # 6. Stat detection - now array
        for keyword, stat in self.STAT_KEYWORDS.items():
            if keyword in q:
                if stat not in entities["stat"]:
                    entities["stat"].append(stat)

        return entities
    """
    Domain-Specific NER for FPL theme.
    Loads lookup dictionaries from the MS2 Neo4j KG.
    """


# def classify_intent_rule_based(user_input):
#     text = user_input.lower()
#     for intent, keywords in INTENT_KEYWORDS.items():
#         for kw in keywords:
#             if kw in text:
#                 return intent
#     return "unknown"

# Utilizing small gpt2 to classify input intent, 
# if it fails, fallback to another model
# if the other model fails, assume first intent in list to not break the RAG    


    
def intent_classification(user_input, valid_intent= VALID_INTENTS):
    # Get model and API key from .env file
    COHERE_API_KEY = os.getenv("COHERE_API_KEY")
    # Ensure a default model name is available if the .env file is missing COHERE_MODEL
    model_name = os.getenv("COHERE_MODEL")

    client = Client(api_key=COHERE_API_KEY)

    # 3. Generate content
    response = client.chat(
        model=model_name,
        message=INTENT_PROMPT.format(user_input),
        max_tokens=500,
        temperature=0.2
    )
    
    
        # Extract the response text
    intent_response = response.text.strip()
    
    # Check if the response is in valid intents
    if intent_response in VALID_INTENTS:

        return intent_response
    else:
        print(f"Invalid intent received: '{intent_response}'. Valid intents are: {VALID_INTENTS}")
        # You can return a default intent or raise an error
        return "unknown"  # or return None, or raise ValueError



def input_embedding(input, model_name ='all-MiniLM-L6-v2' ):
    # 1. Load the same embedding model used for KG embeddings
    model = SentenceTransformer(model_name)

    # 2. User input
    user_query = "Find hotels in Paris with good reviews"

    # 3. Convert to vector
    query_vector = model.encode(user_query)
    return query_vector


##testing the intent classification
user_input = "Who to captain this week?"
intent = intent_classification(user_input)
print(intent)