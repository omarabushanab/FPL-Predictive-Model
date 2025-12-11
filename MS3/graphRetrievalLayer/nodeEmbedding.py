import sys
import os
import numpy as np
from sentence_transformers import SentenceTransformer
import warnings
warnings.filterwarnings('ignore')

# Configuration for the new model
NEW_MODEL_NAME = "sentence-transformers/all-mpnet-base-v2"  # Different model
NEW_EMBEDDING_PROPERTY = "embedding_v2"  # Different property name

# 1 Add helpers folder to path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "helpers"))

# 2 Import connection and config reader
from neo4j_connection import Neo4jConnection
from config_reader import read_config  

# 3 Load config
config_path = os.path.join(os.path.dirname(__file__), "..", "helpers", "configSeif.txt")
config = read_config(config_path)
print("[Step 3] Loaded config:", config)

# 4 Initialize Neo4j connection
conn = Neo4jConnection(config["URI"], config["USERNAME"], config["PASSWORD"])

# 5 Test connection
try:
    result = conn.execute_query("RETURN 1")
    if result:
        print("[Step 5] Neo4j connection OK")
except Exception as e:
    print("[Step 5] Neo4j connection failed:", e)

# 6 Fetch all nodes
def fetch_nodes(tx):
    query = """
    MATCH (n)
    RETURN elementId(n) AS node_id, labels(n) AS labels, n AS props
    """
    return list(tx.run(query))

with conn.driver.session() as session:
    nodes = session.execute_read(fetch_nodes)

print(f"[Step 6] Found {len(nodes)} total nodes")

# 7 Check which nodes already have embeddings (for the NEW property)
def check_existing_embeddings(tx, node_ids, embedding_property):
    """Check which nodes already have embeddings in the specified property"""
    query = f"""
    UNWIND $node_ids AS node_id
    MATCH (n) WHERE elementId(n) = node_id
    RETURN node_id, n.{embedding_property} IS NOT NULL AS has_embedding
    """
    result = tx.run(query, node_ids=node_ids)
    return {record["node_id"]: record["has_embedding"] for record in result}

node_ids = [n["node_id"] for n in nodes]

with conn.driver.session() as session:
    existing_embeddings = session.execute_read(
        check_existing_embeddings, 
        node_ids, 
        NEW_EMBEDDING_PROPERTY
    )

# Count nodes with and without embeddings
nodes_with_embeddings = sum(1 for node_id in node_ids if existing_embeddings.get(node_id, False))
nodes_without_embeddings = len(node_ids) - nodes_with_embeddings

print(f"[Step 7] Using embedding property: '{NEW_EMBEDDING_PROPERTY}'")
print(f"[Step 7] {nodes_with_embeddings} nodes already have {NEW_EMBEDDING_PROPERTY}")
print(f"[Step 7] {nodes_without_embeddings} nodes need new {NEW_EMBEDDING_PROPERTY}")

# Convert each node to text
def node_to_text(labels, props):
    if "Player" in labels:
        return f"Player: {props['player_name']}. Team: {props.get('team','')}. Position: {props.get('position','')}."
    if "Team" in labels:
        return f"Team: {props['name']}."
    if "Position" in labels:
        return f"Position: {props['name']}."
    if "Season" in labels:
        return f"Season: {props['season_name']}."
    if "Gameweek" in labels:
        return f"Gameweek {props['GW_number']} in season {props['season']}."
    if "Fixture" in labels:
        return f"Fixture {props['fixture_number']} in season {props['season']}."
    return ""

# 8 Prepare only nodes that need embeddings
nodes_needing_embeddings = []
texts_for_embedding = []
node_ids_needing_embeddings = []

for node in nodes:
    node_id = node["node_id"]
    if not existing_embeddings.get(node_id, False):
        text = node_to_text(node["labels"], node["props"])
        if text:  # Only create embeddings for nodes with valid text
            nodes_needing_embeddings.append(node)
            texts_for_embedding.append(text)
            node_ids_needing_embeddings.append(node_id)

print(f"[Step 8] Preparing to create embeddings for {len(nodes_needing_embeddings)} nodes")
print(f"[Step 8] Using model: {NEW_MODEL_NAME}")

# 9 Create embeddings only for nodes that need them
if nodes_needing_embeddings:
    model = SentenceTransformer(NEW_MODEL_NAME)
    embeddings = model.encode(texts_for_embedding, convert_to_numpy=True).astype(np.float32)
    dim = embeddings.shape[1]
    print(f"[Step 9] Embedding dimension: {dim}")
    print(f"[Step 9] Created {len(embeddings)} new embeddings with model: {NEW_MODEL_NAME}")
else:
    print(f"[Step 9] All nodes already have {NEW_EMBEDDING_PROPERTY}. Skipping creation.")
    # Load model for later use if needed
    model = SentenceTransformer(NEW_MODEL_NAME)
    
    # Still need to get dimension for reference
    sample_text = "Sample text"
    sample_embedding = model.encode([sample_text], convert_to_numpy=True).astype(np.float32)
    dim = sample_embedding.shape[1]
    print(f"[Step 9] Embedding dimension (from sample): {dim}")

# -------------------------------------------------------------------
# ---------- Store embeddings using CHUNKED UNWIND ----------
# -------------------------------------------------------------------
def store_chunk(chunk, embedding_property):
    query = f"""
    UNWIND $rows AS row
    MATCH (n)
    WHERE elementId(n) = row.id
    SET n.{embedding_property} = row.emb
    """
    with conn.driver.session() as session:
        session.run(query, rows=chunk)

def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

# Only store embeddings if we created new ones
if nodes_needing_embeddings:
    # Prepare batch data
    batch_data = [{"id": nid, "emb": emb.tolist()} 
                  for nid, emb in zip(node_ids_needing_embeddings, embeddings)]
    
    total = len(batch_data)
    print(f"[Step 9B] Preparing to store {total} embeddings in property '{NEW_EMBEDDING_PROPERTY}' using UNWIND...")
    
    # CHUNK SIZE
    CHUNK = 100
    
    count = 0
    for chunk in chunk_list(batch_data, CHUNK):
        store_chunk(chunk, NEW_EMBEDDING_PROPERTY)
        count += len(chunk)
        remaining = total - count
        print(f"[Progress] Stored {count}/{total} | Remaining: {remaining}")
    
    print(f"[Step 9B] ✔ All new embeddings saved to property '{NEW_EMBEDDING_PROPERTY}' with CHUNKED UNWIND")
else:
    print(f"[Step 9B] No new embeddings to store in '{NEW_EMBEDDING_PROPERTY}'")

# ---------- Create Regular Index for the new embedding property ----------
def create_regular_index(tx, embedding_property):
    # Check if embedding property exists on any node
    query_check = f"""
    MATCH (n) WHERE n.{embedding_property} IS NOT NULL
    RETURN count(n) as count
    LIMIT 1
    """
    result = tx.run(query_check)
    record = result.single()
    
    if record and record["count"] > 0:
        # Create an index on the embedding property (regular index, not vector)
        query = f"""
        CREATE INDEX {embedding_property}_index IF NOT EXISTS 
        FOR (n:Player) ON (n.{embedding_property})
        """
        tx.run(query)
        return True
    return False

# Try to create index (optional)
try:
    with conn.driver.session() as session:
        created = session.execute_write(create_regular_index, NEW_EMBEDDING_PROPERTY)
        if created:
            print(f"[Step 10] ✔ Regular index created on embedding property '{NEW_EMBEDDING_PROPERTY}'")
        else:
            print(f"[Step 10] No nodes with '{NEW_EMBEDDING_PROPERTY}' found, skipping index creation")
except Exception as e:
    print(f"[Step 10] Note: Index creation might not be needed or failed: {e}")

# ---------- Custom Vector Search using Cosine Similarity (for the new embeddings) ----------
def search_similar_nodes_custom(question, top_k=10, embedding_property="embedding"):
    """Custom vector search using cosine similarity with specified embedding property"""
    # Get query embedding
    q_emb = model.encode([question], convert_to_numpy=True).astype(np.float32)[0]
    q_emb_list = q_emb.tolist()
    
    query = f"""
    MATCH (n)
    WHERE n.{embedding_property} IS NOT NULL
    // Calculate cosine similarity manually
    WITH n, 
         gds.similarity.cosine(n.{embedding_property}, $query_embedding) AS similarity
    RETURN elementId(n) AS node_id, 
           labels(n) AS labels, 
           n.player_name AS name,
           similarity AS score
    ORDER BY similarity DESC
    LIMIT $k
    """
    
    try:
        with conn.driver.session() as session:
            results = list(session.run(query, {
                "k": top_k,
                "query_embedding": q_emb_list
            }))
        return results
    except Exception as e:
        # Fallback if GDS functions aren't available
        print(f"Note: GDS similarity functions not available, using alternative method: {e}")
        
        # Alternative: Simple dot product (less accurate but works)
        query_fallback = f"""
        MATCH (n)
        WHERE n.{embedding_property} IS NOT NULL
        RETURN elementId(n) AS node_id, 
               labels(n) AS labels, 
               n.player_name AS name,
               n.{embedding_property} AS embedding
        """
        
        with conn.driver.session() as session:
            all_nodes = list(session.run(query_fallback))
        
        # Calculate cosine similarity in Python
        results = []
        for record in all_nodes:
            node_emb = np.array(record["embedding"], dtype=np.float32)
            if len(node_emb) == len(q_emb):
                # Calculate cosine similarity
                dot_product = np.dot(q_emb, node_emb)
                norm_q = np.linalg.norm(q_emb)
                norm_n = np.linalg.norm(node_emb)
                if norm_q > 0 and norm_n > 0:
                    similarity = dot_product / (norm_q * norm_n)
                    results.append({
                        "node_id": record["node_id"],
                        "labels": record["labels"],
                        "name": record["name"],
                        "score": float(similarity)
                    })
        
        # Sort by similarity and return top_k
        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:top_k]

# ---------- TEST QUERY with new embeddings ----------
Q = "Who scored the most goals in 2022?"
print(f"\n[Step 11] Searching for: {Q}")
print(f"[Step 11] Using embeddings from property: '{NEW_EMBEDDING_PROPERTY}'")

results = search_similar_nodes_custom(Q, top_k=10, embedding_property=NEW_EMBEDDING_PROPERTY)

print(f"\n[Step 12] Top results from Neo4j (using {NEW_MODEL_NAME} embeddings):")
for i, r in enumerate(results, 1):
    name = r.get('name', 'N/A')
    if not name or name == 'N/A':
        name = f"Node {r['node_id']}"
    print(f"{i}. Score: {r['score']:.4f} | Name: {name} | Labels: {r['labels']}")

# ---------- Statistics ----------
print("\n" + "="*60)
print("EMBEDDING STATISTICS:")
print("="*60)
print(f"Model used: {NEW_MODEL_NAME}")
print(f"Embedding property: '{NEW_EMBEDDING_PROPERTY}'")
print(f"Embedding dimension: {dim}")
print(f"Total nodes in database: {len(nodes)}")
print(f"Nodes with '{NEW_EMBEDDING_PROPERTY}': {nodes_with_embeddings}")
print(f"Nodes without '{NEW_EMBEDDING_PROPERTY}': {nodes_without_embeddings}")
print(f"New '{NEW_EMBEDDING_PROPERTY}' created in this run: {len(nodes_needing_embeddings)}")

# Optional: Compare with original embeddings if they exist
def compare_embedding_counts(tx):
    """Compare counts of different embedding properties"""
    query = """
    MATCH (n)
    RETURN 
        count(n) as total_nodes,
        count(n.embedding) as original_embedding_count,
        count(n.embedding_v2) as v2_embedding_count
    """
    result = tx.run(query)
    return result.single()

try:
    with conn.driver.session() as session:
        counts = session.execute_read(compare_embedding_counts)
    if counts:
        print(f"\nComparison of embedding properties:")
        print(f"  Original 'embedding' property: {counts['original_embedding_count']} nodes")
        print(f"  New '{NEW_EMBEDDING_PROPERTY}' property: {counts['v2_embedding_count']} nodes")
        print(f"  Total nodes: {counts['total_nodes']}")
except Exception as e:
    print(f"\nNote: Could not compare embedding properties: {e}")

# Close connection
conn.close()
print("\n[Step 13] Neo4j connection closed")