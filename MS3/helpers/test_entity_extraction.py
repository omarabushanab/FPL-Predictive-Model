import sys, os
import json  # Added for pretty printing

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from neo4j_connection import Neo4jConnection 
from preprocessing import FPLEncoderNER 
from config_reader import read_config 

config = read_config("config.txt") 
conn = Neo4jConnection(config["URI"], config["USERNAME"], config["PASSWORD"]) 
ner = FPLEncoderNER(conn) 

queries = [
    "Is Haaland a good pick for GW10 this season?",
    "Show me the best Arsenal defenders and best Manchester United and Manchester city defenders.",
    "Compare Mohamed Salah's and Saka's and kevin de bruyne from last season Elneny.",
    "Who scored the most goals for Liverpool?",
    "Show me Man City and Chelsea players for gameweek 5 and 6 and gameweek 8 to 11 and gw 11,12,13",
    "tell me what arsenal player scored most goals and got most assists and best form in 2022/23.",
    "Compare forwards and midfielders from 2022/23 season",
    "Compare forwards and defenders from 2020-21 season",
    "How many minutes did Bruno Fernandes play in the 2022-23 season gameweek 38?",
    "In season 2022-23 show Bukayo Saka points in gameweek 8",
    "What were the total points scored by Marcus Rashford in gameweek 15 for the 2022-23 campaign?"
    "Provide the aggregated stats for Harry Kane during the 2022-23 season",
    "How many total points did Bukayo Saka achieve in the 2022-23 season?",
    "Summarize the season points and minutes for Virgil van Dijk in the 2022-23 campaign"
] 

for q in queries: 
    print("\nQuery:", q) 
    entities = ner.extract(q)
    print("Entities:", json.dumps(entities, indent=2))

def get_distinct_gameweeks(conn):
    """Get all distinct gameweeks from the database."""
    query = "MATCH (g:Gameweek) WHERE g.GW_number IS NOT NULL RETURN DISTINCT g.GW_number AS value ORDER BY toInteger(value)"
    records = conn.execute_query(query)
    return [r["value"] for r in records]

# Then call it with:
print("Distinct Gameweeks:", get_distinct_gameweeks(conn))