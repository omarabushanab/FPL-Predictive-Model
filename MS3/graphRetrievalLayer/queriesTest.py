import sys
import os

# Add the helpers folder to sys.path so Python can find modules there
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "helpers"))

# Now you can import Python modules from helpers
from neo4j_connection import Neo4jConnection
from config_reader import read_config  # if you have a Python file config_reader.py

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



