import sys
import os

from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

from helpers.neo4j_connection import Neo4jConnection
from preprocessing import FPLEncoderNER, intent_classification

# Add the graphretrievallayer folder to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), "graphRetrievalLayer"))

from graphRetrievalLayer.baseline import QUERY_LIBRARY

model_name = "sentence-transformers/all-MiniLM-L6-v2"

def fix_query_for_schema(query):
    """Fix query to match actual database schema"""
    # Based on your schema:
    # - Player has 'player_name' not 'name'
    # - Season is a node, not a property in all nodes
    # Replace property names to match your schema
    replacements = {
        # Fixture properties
        'f.gameweek': 'f.fixture_number',
        'f.GW_number': 'f.fixture_number',
        'f.season': 'f.season',  # This should be correct
        
        # Player properties
        'p.name': 'p.player_name',
        'p.id': 'p.player_element',
        
        # Relationship properties (PLAYED_IN relationship has total_points)
        'f.total_points': 'stats.total_points',
        'stats.total_points': 'stats.total_points',  # This is correct
        
        # Season matching
        "f.season IN $season": "s.season_name IN $season",  # Match Season node
        "f.season = $season": "s.season_name = $season",
    }
    
    fixed_query = query
    for old, new in replacements.items():
        fixed_query = fixed_query.replace(old, new)
    
    return fixed_query

def execute_player_performance_query(conn, entities):
    """Execute a query for player performance based on your schema"""
    
    # Based on your schema, we need to traverse: Player -> PLAYED_IN -> Fixture
    # The season and gameweek are properties of Fixture
    # The total_points is a property of the PLAYED_IN relationship
    
    query = """
    MATCH (p:Player {player_name: $player_name})
    MATCH (p)-[stats:PLAYED_IN]->(f:Fixture)
    WHERE f.season = $season
    AND f.fixture_number >= $start_fixture
    AND f.fixture_number <= $end_fixture
    RETURN 
    p.player_name AS player_name,
    f.season AS season,
    f.fixture_number AS fixture_number,
    stats.total_points AS total_points,
    stats.minutes AS minutes,
    stats.goals_scored AS goals_scored,
    stats.assists AS assists
    """
    
    # Prepare parameters
    player_name = entities.get("players", [""])[0]
    season = entities.get("season", [""])[0]
    gw = int(entities.get("gameweek", [0])[0])
    start_fixture = (gw - 1) * 10 + 1
    end_fixture = gw * 10

    params = {
        "player_name": player_name,
        "season": season,
        "start_fixture": start_fixture,
        "end_fixture": end_fixture
    }

    
    print(f"Executing query with params: {params}")
    
    try:
        results = conn.execute_query(query, params)
        return [dict(record) for record in results]
    except Exception as e:
        print(f"Query failed: {e}")
        return []

def check_player_exists(conn, player_name):
    """Check if a player exists in the database"""
    query = """
    MATCH (p:Player)
    WHERE p.player_name CONTAINS $partial_name
    RETURN p.player_name as player_name, p.player_element as player_element
    LIMIT 5
    """
    
    try:
        results = conn.execute_query(query, {"partial_name": player_name})
        players = [dict(record) for record in results]
        if players:
            print(f"Found players containing '{player_name}':")
            for player in players:
                print(f"  - {player['player_name']} (element: {player['player_element']})")
            return players
        else:
            print(f"No players found containing '{player_name}'")
            return []
    except Exception as e:
        print(f"Error checking player: {e}")
        return []

def explore_fixture_data(conn, season, gameweek):
    """Check what fixture data exists"""
    query = """
    MATCH (f:Fixture)
    WHERE f.season = $season
        AND f.fixture_number >= $start_fixture
        AND f.fixture_number <= $end_fixture
    RETURN 
        f.season as season,
        f.fixture_number as gameweek,
        f.kickoff_time as kickoff_time,
        [(f)-[:HAS_HOME_TEAM]->(t) | t.name] as home_teams,
        [(f)-[:HAS_AWAY_TEAM]->(t) | t.name] as away_teams
    LIMIT 5
    """
    
    try:
        results = conn.execute_query(query, {"season": season, "gameweek": gameweek})
        fixtures = [dict(record) for record in results]
        if fixtures:
            print(f"Found fixtures for {season} GW{gameweek}:")
            for fixture in fixtures:
                print(f"  - {fixture['home_teams']} vs {fixture['away_teams']} at {fixture['kickoff_time']}")
            return fixtures
        else:
            print(f"No fixtures found for {season} GW{gameweek}")
            return []
    except Exception as e:
        print(f"Error exploring fixtures: {e}")
        return []

def semantic_search_nodes(user_input, model_name, conn, top_k=7):
    """Semantic search implementation"""
    def input_embedding(input, model_name):
        model = SentenceTransformer(model_name)
        query_vector = model.encode(input)
        return query_vector.tolist()
    
    query_embedding = input_embedding(user_input, model_name)
    
    # Use the embedding field that matches our model (384 dimensions)
    embedding_field = "embedding"
    
    cypher = f"""
    MATCH (n)
    WHERE n.{embedding_field} IS NOT NULL AND size(n.{embedding_field}) = $expected_dim
    WITH n, n.{embedding_field} AS node_emb
    
    // Manual cosine similarity calculation
    WITH n, node_emb,
         reduce(s = 0.0, i IN range(0, size(node_emb)-1) | s + node_emb[i] * $query_embedding[i]) AS dot_product,
         sqrt(reduce(s = 0.0, x IN node_emb | s + x * x)) AS norm_a,
         sqrt(reduce(s = 0.0, x IN $query_embedding | s + x * x)) AS norm_b
    
    WITH n, 
         CASE WHEN norm_a > 0 AND norm_b > 0 
              THEN dot_product / (norm_a * norm_b) 
              ELSE 0.0 END AS similarity
    
    RETURN n, similarity
    ORDER BY similarity DESC
    LIMIT $top_k
    """
    
    try:
        results = conn.execute_query(
            cypher,
            {
                "query_embedding": query_embedding,
                "top_k": top_k,
                "expected_dim": len(query_embedding)
            }
        )
        
        output = []
        for r in results:
            node = r["n"]
            score = r["similarity"]
            
            output.append({
                "labels": list(node.labels),
                "properties": dict(node),
                "similarity_score": float(score) if score is not None else 0.0
            })
        
        print(f"Found {len(output)} similar nodes")
        return output
        
    except Exception as e:
        print(f"Error in semantic search: {e}")
        return []

def main():
    load_dotenv()
    URI = os.getenv("URI")
    USERNAME = os.getenv("DB-USERNAME")
    PASSWORD = os.getenv("PASSWORD")
    
    conn = Neo4jConnection(URI, USERNAME, PASSWORD)
    
    user_input = "what is the total points of Mohamed Salah in season 2022-23 gw 4"
    print(f"User input: {user_input}")
    
    # Get intent and entities
    intent = intent_classification(user_input)
    print(f"Intent: {intent}")
    
    ner = FPLEncoderNER(conn) 
    entities = ner.extract(user_input)
    print(f"Entities: {entities}")
    
    # Fix player name (you had 'Moahmed' in original)
    if 'players' in entities and 'Mohamed Salah' in entities['players']:
        entities['players'] = ['Mohamed Salah']
    
    print("\n=== Step 1: Check if player exists ===")
    player_name = entities.get('players', [''])[0]
    players_found = check_player_exists(conn, player_name)
    
    print("\n=== Step 2: Check if fixture exists ===")
    season = entities.get('season', [''])[0]
    gameweek = entities.get('gameweek', [0])[0]
    fixtures_found = explore_fixture_data(conn, season, gameweek)
    
    print("\n=== Step 3: Execute player performance query ===")
    if players_found and fixtures_found:
        # If we found the player, use the exact name from database
        exact_player_name = players_found[0]['player_name']
        entities['players'] = [exact_player_name]
        
        baseline = execute_player_performance_query(conn, entities)
        
        if baseline:
            print(f"\n✅ Success! Found player performance data:")
            for record in baseline:
                print(f"Player: {record['player_name']}")
                print(f"Season: {record['season']}, GW: {record['gameweek']}")
                print(f"Total Points: {record['total_points']}")
                print(f"Minutes: {record['minutes']}, Goals: {record['goals_scored']}, Assists: {record['assists']}")
        else:
            print("\n❌ No performance data found for this player in the specified gameweek")
            
            # Try to see what gameweeks the player played in
            print("\n=== Checking player's gameweeks ===")
            check_gws_query = """
            MATCH (p:Player {player_name: $player_name})-[stats:PLAYED_IN]->(f:Fixture)
            WHERE f.season = $season
            RETURN f.fixture_number as gameweek, stats.total_points as points
            ORDER BY f.fixture_number
            LIMIT 10
            """
            
            try:
                gw_results = conn.execute_query(check_gws_query, {
                    "player_name": exact_player_name,
                    "season": season
                })
                gws = [dict(record) for record in gw_results]
                if gws:
                    print(f"Player {exact_player_name} played in these gameweeks in {season}:")
                    for gw in gws:
                        print(f"  GW{gw['gameweek']}: {gw['points']} points")
                else:
                    print(f"Player {exact_player_name} has no data for {season}")
            except Exception as e:
                print(f"Error checking gameweeks: {e}")
    else:
        print("\n❌ Cannot execute query: Player or fixture not found")
    
    print("\n=== Step 4: Semantic Search ===")
    features = semantic_search_nodes(user_input, model_name, conn)
    
    print(f"\nTop {len(features)} features from semantic search:")
    for i, feat in enumerate(features):
        props = feat['properties']
        name = props.get('player_name', props.get('name', 'Unknown'))
        if 'Player' in feat['labels']:
            print(f"Feature {i+1}: Score={feat['similarity_score']:.4f}, Name={name}")
    
    return baseline if 'baseline' in locals() else [], features

if __name__ == "__main__":
    baseline, features = main()