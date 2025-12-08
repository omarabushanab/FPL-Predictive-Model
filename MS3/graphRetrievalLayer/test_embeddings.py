import sys
import os

# Add the helpers folder to sys.path so Python can find modules there
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "helpers"))

# Now you can import Python modules from helpers
from embeddings import entity_to_string, record_to_string, vectorize_huggingface, vectorize_sentence_transformer
from neo4j_connection import Neo4jConnection
from config_reader import read_config  # if you have a Python file config_reader.py
from baseline import QUERY_LIBRARY

# Read the actual config.txt file
config_path = os.path.join(os.path.dirname(__file__), "..", "helpers", "config.txt")
config = read_config(config_path)  # assuming your read_config function takes the path



# Initialize the connection
conn = Neo4jConnection(config["URI"], config["USERNAME"], config["PASSWORD"]) 

# Test connection
try:
    # Run a simple query to check if Neo4j is reachable
    result = conn.execute_query("RETURN 1")
    if result:
        print("OK")
except Exception as e:
    print("Neo4j connection failed:", e)


intent = "player_performance"
entities = {
    "players": "Aaron Connolly",
    "gameweek": 123,
    "season": "2021-22"
}



def choose_query(intent, entities):
    for name, template in QUERY_LIBRARY.items():
        if template["intent"] == intent and all(
                e in entities and entities[e] for e in template["entities"]):
            return template["cypher"]
    return None


query = """MATCH (p:Player {player_name: "Mohamed Salah"})-[r:PLAYED_IN]->(f:Fixture)
RETURN p, r, f"""

if query:
    values = []
    try:
        result = conn.execute_query(query, parameters=entities)
        for record in result:
            values.append(record_to_string(record))
    except Exception as e:
        print("Query execution failed:", e)
    # print(values)
    print(vectorize_sentence_transformer(values))
else:
    print("No query found for this intent and entities")

