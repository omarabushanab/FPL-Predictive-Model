import sys
import os

from graphRetrievalLayer.feature_vector_embedding import build_feature_index, get_top_k_features_for_llm, record_to_string, search_similar_features
from helpers.neo4j_connection import Neo4jConnection
from preprocessing import FPLEncoderNER, intent_classification
from helpers.config_reader import read_config

# Add the graphretrievallayer folder to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), "graphRetrievalLayer"))

from graphRetrievalLayer.baseline import QUERY_LIBRARY


# print(QUERY_LIBRARY.keys())


# preprocessing.input_preprocessing("Get top players by position in season 2023")

def choose_query(intent, entities):

    for name, template in QUERY_LIBRARY.items():
        if template["intent"] == intent and all(
                e in entities and entities[e] for e in template["entities"]):
            return template["cypher"]
    return None

def execute_query(query, conn, entities):
    try:
        result = conn.execute_query(query, parameters=entities)
        for record in result:
            print(record)
        return result
    except Exception as e:
            print("Query execution failed:", e)

def send_user_input_to_backend(user_input):
    intent = intent_classification(user_input)
    # intent = "recommend_player"

    if intent:
        print("this is the intent returned from intent_classificatio: ")
        print(intent)
    else:
        print("intent classification part failed")

    config = read_config("config.txt") 
    conn = Neo4jConnection(config["URI"], config["USERNAME"], config["PASSWORD"]) 
    ner = FPLEncoderNER(conn) 

    entities = ner.extract(user_input)
    print(f"this is the entities extracted: {entities}")

    query = choose_query(intent, entities)

    baseline = execute_query(query,conn,entities)

    print(f"this is the baseline nodes and relations outputted{baseline}")

    embedded_data = build_feature_index(baseline)
    
    print(f"this is the embedded data: {embedded_data}")
    features = get_top_k_features_for_llm(embedded_data,user_input)
    print(f"this is the top k features to be entered to the LLM {features}")

    return features
    

user_input = "what is the total points of Moahmed Salah points in season 2022/23 gw 10"
print("user input is: " +user_input)
send_user_input_to_backend(user_input)