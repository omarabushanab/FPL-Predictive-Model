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
    Embed the user input, detect proper embedding field in Neo4j,
    perform cosine similarity search, and return REAL properties 
    (excluding embedding vectors).
    """

    from sentence_transformers import SentenceTransformer

    # 1. Generate embedding for the input
    model = SentenceTransformer(model_name)
    query_embedding = model.encode(user_input).tolist()
    expected_dim = len(query_embedding)

    # 2. Detect embedding field
    dim_check_query = """
        MATCH (n)
        WHERE n.embedding IS NOT NULL OR n.embedding_v2 IS NOT NULL
        RETURN
            CASE WHEN n.embedding IS NOT NULL THEN size(n.embedding) ELSE null END AS dim_v1,
            CASE WHEN n.embedding_v2 IS NOT NULL THEN size(n.embedding_v2) ELSE null END AS dim_v2
        LIMIT 1
    """
    dims = conn.execute_query(dim_check_query)
    if not dims:
        raise ValueError("No nodes with embeddings found in the database.")

    dim_v1 = dims[0]["dim_v1"]
    dim_v2 = dims[0]["dim_v2"]

    if dim_v2 == expected_dim:
        embedding_field = "embedding_v2"
    elif dim_v1 == expected_dim:
        embedding_field = "embedding"
    else:
        raise ValueError(
            f"No embedding field matches query dimension {expected_dim} "
            f"(found dim_v1={dim_v1}, dim_v2={dim_v2})"
        )

    # 3. Similarity search Cypher
    cypher = f"""
        CALL {{
            MATCH (n)
            WHERE n.{embedding_field} IS NOT NULL 
              AND size(n.{embedding_field}) = $expected_dim
            WITH n, n.{embedding_field} AS node_emb
            WITH n, gds.similarity.cosine(node_emb, $query_embedding) AS score
            RETURN n, score
            ORDER BY score DESC
            LIMIT $top_k
        }}
        RETURN n AS node, score
    """

    results = conn.execute_query(
        cypher,
        {
            "query_embedding": query_embedding,
            "expected_dim": expected_dim,
            "top_k": top_k
        }
    )

    # 4. Remove embedding fields and return actual values
    output = []
    for r in results:
        node = r["node"]
        score = r["score"]

        # Convert node properties to dict
        props = dict(node)

        # REMOVE embedding fields if present
        props.pop("embedding", None)
        props.pop("embedding_v2", None)

        output.append({
            "labels": list(node.labels),
            "properties": props,
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

def send_user_input_to_backend(user_input,conn,embedding_choice):
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
# Convert gameweek strings to integers for Neo4j queries
    if 'gameweek' in entities:
        entities['gameweek'] = [int(gw) for gw in entities['gameweek']]
    query = choose_query(intent, entities)

    baseline = execute_query(query,conn,entities)

    print(f"this is the baseline nodes and relations outputted{baseline}")

    features = semantic_search_nodes(user_input, model_name,conn)
    
    
    for i, feat in enumerate(features):
        props = feat['properties']
        name = props.get('player_name', props.get('name', 'Unknown'))
        print(f"{i+1}. Name: {name}, Labels: {feat['labels']}, Score: {feat['similarity_score']:.4f}")


    return baseline, features


load_dotenv()
URI = os.getenv("URI")
USERNAME = os.getenv("DB-USERNAME")
PASSWORD = os.getenv("PASSWORD")
print(f"this is the URI: {URI}, username: {USERNAME}, password: {PASSWORD}")

conn = Neo4jConnection(URI,USERNAME,PASSWORD)

user_input = "what is the total points of Moahmed Salah points in season 2022/23 gw 10"

print("user input is: " +user_input)
embedding_choice = "sentence-transformers/all-MiniLM-L6-v2"
send_user_input_to_backend(user_input,conn,embedding_choice)