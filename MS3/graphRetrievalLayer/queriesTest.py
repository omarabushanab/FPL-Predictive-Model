import sys
import os

# Add the helpers folder to sys.path so Python can find modules there
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "helpers"))

# Now you can import Python modules from helpers
# from embeddings import record_to_string
from neo4j_connection import Neo4jConnection
from config_reader import read_config  # if you have a Python file config_reader.py
from baseline import QUERY_LIBRARY


# Read the actual config.txt file
config_path = os.path.join(os.path.dirname(__file__), "..", "helpers", "configSeif.txt")
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


intent = "team_analysis"
entities = {
    "teams": ["Arsenal"],
    "season": ["2022-23"]
}


def choose_query(intent, entities):
    for name, template in QUERY_LIBRARY.items():
        if template["intent"] == intent and all(
                e in entities and entities[e] for e in template["entities"]):
            return template["cypher"]
    return None


query = choose_query(intent, entities)

if query:
    try:
        result = conn.execute_query(query, parameters=entities)
        for record in result:
            print(record)
    except Exception as e:
        print("Query execution failed:", e)
else:
    print("No query found for this intent and entities")