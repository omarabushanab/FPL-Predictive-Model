import sys
import os

from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

from helpers.neo4j_connection import Neo4jConnection
from preprocessing import FPLEncoderNER, intent_classification

# Add the graphretrievallayer folder to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), "graphRetrievalLayer"))

from graphRetrievalLayer.baseline import QUERY_LIBRARY


import numpy as np
from neo4j import GraphDatabase

NEW_EMBEDDING_PROPERTY = "embedding_v2" 
OLD_EMBEDDING_PROPERTY ="embedding"

model_name = "sentence-transformers/all-MiniLM-L6-v2"
model_name_v2 ="sentence-transformers/all-mpnet-base-v2"  # Different model

def semantic_search_nodes(user_input, model_name, conn, top_k=7):
    """
    Embed the user input using the specified model, then run similarity search 
    inside Neo4j to find the top-k most similar nodes.

    Args:
        user_input (str): Raw text from the user.
        model_name (str): The embedding model name, e.g. "sentence-transformers/all-MiniLM-L6-v2".
        conn (Neo4jConnection): Your Neo4j connection wrapper.
        top_k (int): How many nodes to return.

    Returns:
        list[dict]: Top-k nodes with similarity score and node data.
    """
    def input_embedding(input,model_name):
        # 1. Load the same embedding model used for KG embeddings
        model = SentenceTransformer(model_name)

        # 3. Convert to vector
        query_vector = model.encode(input)
        return query_vector

    # ----------------------------
    # 1. Embed the input
    # ----------------------------
    query_embedding = input_embedding(user_input, model_name)

    # ----------------------------
    # 2. Determine the embedding field in Neo4j
    # ----------------------------
    # You can customize this mapping
    if "v2" in model_name.lower():
        embedding_field = "embeddings_v2"
    else:
        embedding_field = "embeddings"

    # ----------------------------
    # 3. Cypher Query for Similarity Search
    # ----------------------------

    cypher = f"""
    CALL {{
        MATCH (n)
        WHERE exists(n.{embedding_field})
        WITH n, n.{embedding_field} AS node_emb

        // Compute cosine similarity
        WITH n, gds.similarity.cosine(node_emb, $query_embedding) AS score
        RETURN n, score
        ORDER BY score DESC
        LIMIT $top_k
    }}
    RETURN n AS node, score
    """

    # ----------------------------
    # 4. Execute query
    # ----------------------------
    results = conn.query(
        cypher,
        {
            "query_embedding": query_embedding,
            "top_k": top_k
        }
    )

    # ----------------------------
    # 5. Convert nodes to simple Python dicts
    # ----------------------------
    output = []
    for r in results:
        node = r["node"]
        score = r["score"]

        output.append({
            "labels": list(node.labels),
            "properties": dict(node),
            "similarity_score": score
        })

    return output


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

def send_user_input_to_backend(user_input,conn):
    intent = intent_classification(user_input)
    # intent = "recommend_player"

    if intent:
        print("this is the intent returned from intent_classificatio: ")
        print(intent)
    else:
        print("intent classification part failed")

    ner = FPLEncoderNER(conn) 

    entities = ner.extract(user_input)
    print(f"this is the entities extracted: {entities}")

    query = choose_query(intent, entities)

    baseline = execute_query(query,conn,entities)

    print(f"this is the baseline nodes and relations outputted{baseline}")

    features = semantic_search_nodes(user_input, model_name,conn)
    
    
    print(f"this is the top k features to be entered to the LLM {features}")

    return baseline, features
    
load_dotenv()
URI = os.getenv("URI")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

conn = Neo4jConnection(URI,USERNAME,PASSWORD)

user_input = "what is the total points of Moahmed Salah points in season 2022/23 gw 10"

print("user input is: " +user_input)

send_user_input_to_backend(user_input,conn)