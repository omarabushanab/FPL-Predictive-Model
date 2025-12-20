import sys
import os
from turtle import st

from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

from helpers.neo4j_connection import Neo4jConnection
from preprocessing import FPLEncoderNER, intent_classification

# Add graphRetrievalLayer path
sys.path.append(os.path.join(os.path.dirname(__file__), "graphRetrievalLayer"))
from graphRetrievalLayer.baseline import QUERY_LIBRARY


model_name = "sentence-transformers/all-MiniLM-L6-v2"


def fix_query_for_schema(query):
    """Fix query to match actual DB schema."""
    replacements = {
        'f.gameweek': 'f.fixture_number',
        'f.GW_number': 'f.fixture_number',
        'p.name': 'p.player_name',
        'p.id': 'p.player_element',
    }
    fixed_query = query
    for old, new in replacements.items():
        fixed_query = fixed_query.replace(old, new)
    return fixed_query


def execute_player_performance_query(conn, entities):
    """Query stats for a player in a GW range (mapped to fixtures)."""

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
    ORDER BY f.fixture_number ASC
    """

    player_name = entities.get("players", [""])[0]
    season = entities.get("season", [""])[0]
    gw_str = entities.get("gameweek", [0])[0]

    try:
        gw = int(gw_str)
    except Exception:
        gw = 0

    start_fixture = (gw - 1) * 10 + 1 if gw > 0 else 0
    end_fixture = gw * 10 if gw > 0 else 0

    params = {
        "player_name": player_name,
        "season": season,
        "start_fixture": start_fixture,
        "end_fixture": end_fixture
    }

    print(f"Executing performance query with params: {params}")

    try:
        results = conn.execute_query(query, params)
        return [dict(record) for record in results]
    except Exception as e:
        print(f"Query failed: {e}")
        return []


def check_player_exists(conn, partial_name):
    query = """
    MATCH (p:Player)
    WHERE p.player_name CONTAINS $partial
    RETURN p.player_name as player_name, p.player_element as player_element
    LIMIT 5
    """

    try:
        results = conn.execute_query(query, {"partial": partial_name})
        players = [dict(record) for record in results]
        if players:
            print(f"Found players matching '{partial_name}':")
            for p in players:
                print(f"  - {p['player_name']} (element: {p['player_element']})")
        else:
            print(f"No players found matching '{partial_name}'")
        return players
    except Exception as e:
        print(f"Error checking player: {e}")
        return []


def explore_fixture_data(conn, season, start_fixture, end_fixture):
    """Check fixture rows for a GW → fixture range."""
    query = """
    MATCH (f:Fixture)
    WHERE f.season = $season
      AND f.fixture_number >= $start_fixture
      AND f.fixture_number <= $end_fixture
    RETURN 
        f.season as season,
        f.fixture_number as fixture_number,
        f.kickoff_time as kickoff_time,
        [(f)-[:HAS_HOME_TEAM]->(t) | t.name] as home_teams,
        [(f)-[:HAS_AWAY_TEAM]->(t) | t.name] as away_teams
    ORDER BY f.fixture_number ASC
    LIMIT 20
    """
    try:
        results = conn.execute_query(
            query,
            {"season": season, "start_fixture": start_fixture, "end_fixture": end_fixture}
        )
        fixtures = [dict(record) for record in results]

        if fixtures:
            print(f"Found {len(fixtures)} fixtures for {season}: fixtures {start_fixture}-{end_fixture}")
            for f in fixtures:
                print(f"  - {f['home_teams']} vs {f['away_teams']} "
                      f"at {f['kickoff_time']} (fixture #{f['fixture_number']})")
        else:
            print(f"No fixtures found for {season}, fixtures {start_fixture}-{end_fixture}")

        return fixtures
    except Exception as e:
        print(f"Error exploring fixtures: {e}")
        return []


def semantic_search_nodes(user_input, model_name, conn, top_k=7):
    """Semantic search with auto-selected embedding field."""

    def input_embedding(text, model_name):
        model = SentenceTransformer(model_name)
        return model.encode(text).tolist()

    query_embedding = input_embedding(user_input, model_name)
    expected_dim = len(query_embedding)

    # Auto-detect embedding field
    try:
        dim_res = conn.execute_query(
            "MATCH (n) WHERE n.embedding IS NOT NULL RETURN size(n.embedding) AS dim LIMIT 1"
        )
        db_dim = int(dim_res[0]["dim"]) if dim_res else None
    except Exception:
        db_dim = None

    embedding_field = "embedding" if db_dim == expected_dim else "embedding_v2"
    print(f"Using embedding field: {embedding_field}")

    cypher = f"""
    MATCH (n)
    WHERE n.{embedding_field} IS NOT NULL AND size(n.{embedding_field}) = $expected_dim
    WITH n, n.{embedding_field} AS node_emb
    WITH 
        n,
        reduce(s = 0.0, i IN range(0, size(node_emb)-1) |
              s + node_emb[i] * $query_embedding[i]) AS dot_product,
        sqrt(reduce(s = 0.0, x IN node_emb | s + x*x)) AS norm_a,
        sqrt(reduce(s = 0.0, x IN $query_embedding | s + x*x)) AS norm_b
    WITH 
        n,
        CASE WHEN norm_a > 0 AND norm_b > 0
             THEN dot_product / (norm_a * norm_b)
             ELSE 0.0 END AS similarity
    RETURN n, similarity
    ORDER BY similarity DESC
    LIMIT $top_k
    """

    try:
        results = conn.execute_query(cypher, {
            "query_embedding": query_embedding,
            "expected_dim": expected_dim,
            "top_k": top_k
        })

        output = []
        for r in results:
            node = r["n"]
            score = r["similarity"]
            output.append({
                "labels": list(node.labels),
                "properties": dict(node),
                "similarity_score": float(score)
            })

        print(f"Found {len(output)} semantically similar nodes.")
        return output

    except Exception as e:
        print(f"Error in semantic search: {e}")
        return []


def main():
    load_dotenv()
    URI = os.getenv("URI") or st.secrets["URI"]
    USERNAME = os.getenv("DB-USERNAME") or st.secrets["DB-USERNAME"]
    PASSWORD = os.getenv("PASSWORD") or st.secrets["PASSWORD"]

    conn = Neo4jConnection(URI, USERNAME, PASSWORD)

    user_input = "what is the total points of Mohamed Salah in season 2021-22 gw 7"
    print(f"User input: {user_input}")

    intent = intent_classification(user_input)
    print(f"Intent: {intent}")

    ner = FPLEncoderNER(conn)
    entities = ner.extract(user_input)
    print(f"Entities: {entities}")

    # Fix common NER misspelling
    if 'players' in entities and any("Moh" in p for p in entities['players']):
        entities['players'] = ["Mohamed Salah"]

    # Extract core values
    player_name = entities.get("players", [""])[0]
    season = entities.get("season", [""])[0]
    gw_str = entities.get("gameweek", [0])[0]

    try:
        gw = int(gw_str)
    except Exception:
        gw = 0

    start_fixture = (gw - 1) * 10 + 1 if gw > 0 else 0
    end_fixture = gw * 10 if gw > 0 else 0

    print("\n=== Step 1: Check player ===")
    players_found = check_player_exists(conn, player_name)

    print("\n=== Step 2: Check fixtures ===")
    fixtures_found = explore_fixture_data(conn, season, start_fixture, end_fixture)

    print("\n=== Step 3: Player Performance ===")

    baseline = []
    if players_found and fixtures_found:
        # Use exact DB player name
        entities['players'] = [players_found[0]['player_name']]
        baseline = execute_player_performance_query(conn, entities)

        if baseline:
            print("\n✅ Performance results:")
            for record in baseline:
                print(f"\nPlayer: {record['player_name']}")
                print(f"Season: {record['season']}, Fixture#: {record['fixture_number']}")
                print(f"Points: {record['total_points']}")
                print(f"Minutes: {record['minutes']}")
                print(f"Goals: {record['goals_scored']}, Assists: {record['assists']}")
        else:
            print("\nNo performance data found.")
    else:
        print("\n❌ Cannot run performance query: player or fixtures missing.")

    print("\n=== Step 4: Semantic Search ===")
    features = semantic_search_nodes(user_input, model_name, conn)

    print(f"\nTop {len(features)} semantic matches:")
    for i, f in enumerate(features):
        props = f["properties"]
        nm = props.get("player_name", props.get("name", ""))
        print(f"[{i+1}] Score={f['similarity_score']:.4f}  Node={nm}")

    return baseline, features


if __name__ == "__main__":
    baseline, features = main()