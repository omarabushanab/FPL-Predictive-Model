from sentence_transformers import SentenceTransformer
import re


INTENT_KEYWORDS = {
    "compare_players": ["compare", "vs", "versus", "better", "stronger"],
    "player_performance": ["performance", "points", "stats", "gw", "gameweek"],
    "player_history": ["history", "season history", "past seasons"],
    "top_players_position": ["best", "top", "highest", "ranking", "forwards", "midfielders", "defenders"],
    "team_analysis": ["form", "team form", "how is", "analysis"],
    "team_fixtures": ["fixtures", "next games", "schedule"],
    "fixture_difficulty": ["easy fixtures", "difficulty", "FDR"],
    "search_player": ["who is", "player", "tell me about"],
    "search_team": ["team", "club", "squad"],
    "recommend_player": ["recommend", "captain", "buy", "transfer", "who should I"]
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
            "players": [],
            "team": None,
            "position": None,
            "season": None,
            "gameweek": None,
            "stat": None
        }

        # 1. Player names
        for p in self.PLAYERS:
            if p.lower() in q:
                entities["players"].append(p)

        # 2. Team names
        for t in self.TEAMS:
            if t.lower() in q:
                entities["team"] = t
                break

        # 3. Position (exact)
        for pos in self.POSITIONS:
            if pos.lower() in q:
                entities["position"] = pos
                break

        # 3b. Position synonyms
        for word, pos in self.POSITION_SYNONYMS.items():
            if word in q:
                entities["position"] = pos
                break

        # 4. Season ("2023/24")
        season_match = re.search(r"(20\d{2}\/\d{2})", q)
        if season_match:
            entities["season"] = season_match.group(1)

        # 4b. Rule-based
        if "this season" in q:
            entities["season"] = max(self.SEASONS)

        if "last season" in q:
            sorted_s = sorted(self.SEASONS)
            if len(sorted_s) >= 2:
                entities["season"] = sorted_s[-2]

        # 5. Gameweek
        gw = re.search(r"(gw|gameweek)\s*(\d+)", q)
        if gw:
            entities["gameweek"] = int(gw.group(2))

        # 6. Stat detection
        for keyword, stat in self.STAT_KEYWORDS.items():
            if keyword in q:
                entities["stat"] = stat
                break

        # Default stat
        if not entities["stat"]:
            entities["stat"] = "total_points"

        return entities



def classify_intent_rule_based(user_input):
    text = user_input.lower()
    for intent, keywords in INTENT_KEYWORDS.items():
        for kw in keywords:
            if kw in text:
                return intent
    return "unknown"

def classify_intent_llm(user_input, llm):
    prompt = INTENT_PROMPT.format(user_input)
    result = llm(prompt)
    return result.strip().lower()

def intent_classification(user_input, llm = None):
    rule_intent = classify_intent_rule_based(user_input)
    if rule_intent != "unknown":
        return rule_intent    
    
    output = classify_intent_llm(user_input,llm)
            
    return output

def input_embedding(input):
    # 1. Load the same embedding model used for KG embeddings
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # 2. User input
    user_query = "Find hotels in Paris with good reviews"

    # 3. Convert to vector
    query_vector = model.encode(user_query)
    return

def input_preprocessing(input):

    print(intent_classification(input))

    # entity_extraction()

    # if(twoB):
    #     input_embedding()
    return
