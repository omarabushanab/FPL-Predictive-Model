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
    "Show me the best Arsenal defenders.",
    "Compare Mohamed Salah and Saka from last season.",
    "Who scored the most goals for Liverpool?",
    "Show me Man City and Chelsea players for gameweek 5 and 6",  # Added example
    "tell me what arsenal player scored most goals and got most assists in 2022/23.",
    "Compare forwards and midfielders from 2022/23 season",  # Added example
] 

for q in queries: 
    print("\nQuery:", q) 
    entities = ner.extract(q)
    print("Entities:", json.dumps(entities, indent=2))