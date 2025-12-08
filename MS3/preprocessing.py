from sentence_transformers import SentenceTransformer
import re
import os
from dotenv import load_dotenv
from huggingface_hub import InferenceClient
from google import genai
from google.genai import types

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

User Query: "{}"

Answer with ONLY the label.
"""

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
            "form": "form",
            "clean sheet": "clean_sheets",
            "clean sheets": "clean_sheets",
            "bonus": "bonus",
            "minutes": "minutes",
            "saves": "saves",
            "ict": "ict_index",
            "threat": "threat",
            "creativity": "creativity",
            "influence": "influence",
            "xg": "xG",
            "xa": "xA",
            "expected points": "expected_points"
        }

        # Position synonyms -> mapped to Position.name
        self.POSITION_SYNONYMS = {
            "goalkeeper": "GK",
            "keeper": "GK",
            "defender": "DEF",
            "centre back": "DEF",
            "fullback": "DEF",
            "midfielder": "MID",
            "winger": "MID",
            "forward": "FWD",
            "striker": "FWD",
            "attacker": "FWD"
        }

    # -----------------------------------------------------
    # Load distinct properties from your MS2 Neo4j KG
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
        for p in self.PLAYERS:
            if not p:
                continue
                
            player_lower = p.lower()
            player_words = player_lower.split()
            
            # Skip very short player names to avoid false positives
            if len(player_words) == 1 and len(player_words[0]) < 4:
                continue
                
            # Strategy 1: Check if ALL words in player name are in query as whole words
            # (for full name matches like "Erling Haaland")
            all_words_match = all(
                word in query_words and word not in common_words 
                for word in player_words
            )
            
            if all_words_match:
                if p not in entities["players"]:
                    entities["players"].append(p)
                continue  # Skip to next player if full name matched
                
            # Strategy 2: Last name only (but avoid common words)
            if len(player_words) > 1:
                last_name = player_words[-1]
                # Only match if last name is a whole word in query AND not a common word
                if (last_name in query_words and 
                    len(last_name) > 3 and  # Avoid short names
                    last_name not in common_words and
                    not last_name.endswith('son') and  # Avoid "season" false positives
                    not last_name.endswith('ward')):   # Avoid "forward" false positives
                    
                    if p not in entities["players"]:
                        entities["players"].append(p)
                    continue
        
        # 2. Team names
        for t in self.TEAMS:
            if t and t.lower() in q:
                if t not in entities["teams"]:
                    entities["teams"].append(t)

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

        # 4. Season ("2023/24") - now array
        season_match = re.findall(r"(20\d{2}\/\d{2})", q)
        if season_match:
            for season in season_match:
                if season not in entities["season"]:
                    entities["season"].append(season)

        # 4b. Rule-based - now array
        if "this season" in q:
            current_season = max(self.SEASONS)
            if current_season not in entities["season"]:
                entities["season"].append(current_season)

        if "last season" in q:
            sorted_s = sorted(self.SEASONS)
            if len(sorted_s) >= 2:
                last_season = sorted_s[-2]
                if last_season not in entities["season"]:
                    entities["season"].append(last_season)

        # 5. Gameweek - now array
        gw_matches = re.finditer(r"(gw|gameweek)\s*(\d+)", q, re.IGNORECASE)
        for match in gw_matches:
            gw_num = int(match.group(2))
            if gw_num not in entities["gameweek"]:
                entities["gameweek"].append(gw_num)

        # 6. Stat detection - now array
        for keyword, stat in self.STAT_KEYWORDS.items():
            if keyword in q:
                if stat not in entities["stat"]:
                    entities["stat"].append(stat)

        # Default stat - only add if no stats found
        if not entities["stat"]:
            entities["stat"].append("total_points")

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
def intent_classification(user_input, valid_intent = VALID_INTENTS):
    API_KEY = os.getenv("GEMINI_API_KEY")
   
    client = genai.Client(api_key = API_KEY)
    model_id = os.getenv("GEMINI_MODEL")
    fallback_model_id = model_id
    prompt = INTENT_PROMPT.format(user_input)
   
    def get_llm_response(prompt, model_id=model_id):
        
        response = response = client.models.generate_content(
            model=model_id,
            contents=prompt
)
        cleaned_response = response

        # for intent in valid_intent:
        #     if intent in cleaned_response:
        #         return intent
        return cleaned_response
    
    intent = get_llm_response(prompt)

    # if intent not in valid_intent:
    #     print(f"model {model_id} failed to classify intent, changing to gpt(akhooh el kbeer)")

    #     intent = get_llm_response(prompt,fallback_model_id)
    #     if intent not in valid_intent:
    #         print("gpt couldn't classify intent, assuming intent")
    #         intent = valid_intent[0]
    
    return intent

def input_embedding(input):
    # 1. Load the same embedding model used for KG embeddings
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # 2. User input
    user_query = "Find hotels in Paris with good reviews"

    # 3. Convert to vector
    query_vector = model.encode(user_query)
    return query_vector

def input_preprocessing(input):

    print(intent_classification(input))

    # entity_extraction()

    # if(twoB):
    #     input_embedding()
    return

##testing the intent classification
user_input = "Who to captain this week?"
intent = intent_classification(user_input)
print(intent)