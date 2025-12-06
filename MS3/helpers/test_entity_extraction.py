import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from neo4j_connection import Neo4jConnection 
from preprocessing import FPLEncoderNER 
from config_reader import read_config 

config = read_config("config.txt") 
conn = Neo4jConnection(config["URI"], config["USERNAME"], config["PASSWORD"]) 
ner = FPLEncoderNER(conn) 
queries = [ "Is Haaland a good pick for GW10 this season?", "Show me the best Arsenal defenders.", "Compare Salah and Saka from last season.", "Who scored the most goals for Liverpool?", ] 
for q in queries: print("\nQuery:", q) 
print("Entities:", ner.extract(q))