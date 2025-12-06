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
 return

def input_preprocessing(input):

    print(intent_classification(input))

    # entity_extraction()

    # if(twoB):
    #     input_embedding()
    return