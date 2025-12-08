# similarity based on name 

# import sys
# import os
# import pandas as pd
# import numpy as np
# from sklearn.preprocessing import StandardScaler
# from sklearn.decomposition import PCA
# import matplotlib.pyplot as plt

# sys.path.append(os.path.join(os.path.dirname(__file__), "..", "helpers"))
# from neo4j_connection import Neo4jConnection
# from config_reader import read_config  

# # ----------------------------
# # LangChain + FAISS (community) + HuggingFace
# # ----------------------------
# from langchain_community.vectorstores import FAISS
# from langchain_huggingface import HuggingFaceEmbeddings

# # ----------------------------
# # Step 0: Config & Neo4j
# # ----------------------------
# config_path = os.path.join(os.path.dirname(__file__), "..", "helpers", "configSeif.txt")
# config = read_config(config_path)

# conn = Neo4jConnection(config["URI"], config["USERNAME"], config["PASSWORD"])

# # Test connection
# try:
#     result = conn.execute_query("RETURN 1")
#     if result:
#         print("Neo4j connection OK")
# except Exception as e:
#     print("Neo4j connection failed:", e)

# # ----------------------------
# # Step 1: Get numerical features
# # ----------------------------
# query = """
# MATCH (p:Player)-[r:PLAYED_IN]->(f:Fixture)
# RETURN p.player_name AS player,
#        avg(r.minutes) AS minutes,
#        avg(r.goals_scored) AS goals_scored,
#        avg(r.assists) AS assists,
#        avg(r.total_points) AS total_points,
#        avg(r.bonus) AS bonus,
#        avg(r.clean_sheets) AS clean_sheets,
#        avg(r.goals_conceded) AS goals_conceded,
#        avg(r.own_goals) AS own_goals,
#        avg(r.penalties_saved) AS penalties_saved,
#        avg(r.penalties_missed) AS penalties_missed,
#        avg(r.yellow_cards) AS yellow_cards,
#        avg(r.red_cards) AS red_cards,
#        avg(r.saves) AS saves,
#        avg(r.bps) AS bps,
#        avg(r.influence) AS influence,
#        avg(r.creativity) AS creativity,
#        avg(r.threat) AS threat,
#        avg(r.ict_index) AS ict_index,
#        avg(r.form) AS form
# """

# results = conn.execute_query(query)
# records = [record.data() for record in results]
# df_players = pd.DataFrame(records)
# df_players.fillna(0, inplace=True)

# # ----------------------------
# # Step 2: Generate embeddings using external model
# # ----------------------------
# texts = []
# for _, row in df_players.iterrows():
#     text = f"{row['player']} - goals: {row['goals_scored']}, assists: {row['assists']}, points: {row['total_points']}"
#     texts.append(text)

# embedding_model = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")

# # ----------------------------
# # Step 3: Build FAISS index
# # ----------------------------
# vector_store = FAISS.from_texts(texts, embedding_model)
# retriever = vector_store.as_retriever(search_type="similarity", search_kwargs={"k":5})

# # ----------------------------
# # Step 4: Query for similar players
# # ----------------------------
# # from langchain.chains import RetrievalQA
# # from langchain.llms import HuggingFacePipeline
# # from transformers import pipeline

# # Step 4: Query for similar players using RetrievalQA
# # Initialize a dummy HuggingFace text-generation pipeline (LLM)
# player_query = "Mohamed Salah - goals: 0.55, assists: 0.35, points: 6.63"

# # Use the retriever's 'get_relevant_documents' method if available
# try:
#     results = retriever.get_relevant_documents(player_query)
# except AttributeError:
#     # fallback for new version
#     results = retriever._get_relevant_documents(player_query, run_manager=None)

# similar_players = [r.page_content for r in results]
# print(f"Top similar players to Mohamed Salah (via FAISS + external model):")
# for p in similar_players:
#     print(p)

# # ----------------------------
# # Step 5: PCA visualization using numerical stats
# # ----------------------------
# features = df_players.drop(columns=['player']).values
# scaler = StandardScaler()
# features_scaled = scaler.fit_transform(features)

# pca = PCA(n_components=2)
# emb_2d = pca.fit_transform(features_scaled)

# plt.figure(figsize=(10,8))
# plt.scatter(emb_2d[:,0], emb_2d[:,1], color='lightgray', alpha=0.5)
# for i, name in enumerate(df_players['player']):
#     if "Salah" in name or any(p.split(" - ")[0] == name for p in similar_players):
#         plt.scatter(emb_2d[i,0], emb_2d[i,1], label=name, color='red')
#         plt.text(emb_2d[i,0]+0.02, emb_2d[i,1]+0.02, name, fontsize=9)
# plt.title("PCA of Player Numerical Features")
# plt.xlabel("PC1")
# plt.ylabel("PC2")
# plt.legend()
# plt.show()
import sys
import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import faiss

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "helpers"))
from neo4j_connection import Neo4jConnection
from config_reader import read_config  

# ----------------------------
# LangChain + FAISS (optional text-based embeddings)
# ----------------------------
from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings

# ----------------------------
# Step 0: Config & Neo4j
# ----------------------------
config_path = os.path.join(os.path.dirname(__file__), "..", "helpers", "configSeif.txt")
config = read_config(config_path)

conn = Neo4jConnection(config["URI"], config["USERNAME"], config["PASSWORD"])

# Test connection
try:
    result = conn.execute_query("RETURN 1")
    if result:
        print("Neo4j connection OK")
except Exception as e:
    print("Neo4j connection failed:", e)

# ----------------------------
# Step 1: Get numerical features
# ----------------------------
query = """
MATCH (p:Player)-[r:PLAYED_IN]->(f:Fixture)
RETURN p.player_name AS player,
       avg(r.minutes) AS minutes,
       avg(r.goals_scored) AS goals_scored,
       avg(r.assists) AS assists,
       avg(r.total_points) AS total_points,
       avg(r.bonus) AS bonus,
       avg(r.clean_sheets) AS clean_sheets,
       avg(r.goals_conceded) AS goals_conceded,
       avg(r.own_goals) AS own_goals,
       avg(r.penalties_saved) AS penalties_saved,
       avg(r.penalties_missed) AS penalties_missed,
       avg(r.yellow_cards) AS yellow_cards,
       avg(r.red_cards) AS red_cards,
       avg(r.saves) AS saves,
       avg(r.bps) AS bps,
       avg(r.influence) AS influence,
       avg(r.creativity) AS creativity,
       avg(r.threat) AS threat,
       avg(r.ict_index) AS ict_index,
       avg(r.form) AS form
"""

results = conn.execute_query(query)
records = [record.data() for record in results]
df_players = pd.DataFrame(records)
df_players.fillna(0, inplace=True)

# ----------------------------
# Step 2: Numerical embeddings for similarity search
# ----------------------------
features = df_players.drop(columns=['player']).values
scaler = StandardScaler()
features_scaled = scaler.fit_transform(features)

# Build FAISS index for numerical vectors
dimension = features_scaled.shape[1]
index = faiss.IndexFlatL2(dimension)
index.add(features_scaled.astype('float32'))

# Function to find top-k similar players by stats
def get_similar_players(player_name, k=5):
    if player_name not in df_players['player'].values:
        raise ValueError(f"Player {player_name} not found")
    idx = df_players[df_players['player'] == player_name].index[0]
    query_vector = features_scaled[idx].reshape(1, -1).astype('float32')
    distances, indices = index.search(query_vector, k=k+1)  # +1 to exclude self
    similar_indices = [i for i in indices[0] if i != idx][:k]
    return df_players.iloc[similar_indices]['player'].tolist()

# Example usage
similar_players = get_similar_players("Harry Kane")
print("Top similar players to Harry Kane based on stats:")
print(similar_players)

# ----------------------------
# Step 3: Optional - text-based embeddings comparison
# ----------------------------
texts = []
for _, row in df_players.iterrows():
    # Remove player names for numeric/stat-based text
    text = f"goals: {row['goals_scored']}, assists: {row['assists']}, points: {row['total_points']}"
    texts.append(text)

embedding_model = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
vector_store_text = FAISS.from_texts(texts, embedding_model)
retriever_text = vector_store_text.as_retriever(search_type="similarity", search_kwargs={"k":5})

# Query example using text embeddings
query_text = f"goals: 0.55, assists: 0.35, points: 6.63"
try:
    results_text = retriever_text.get_relevant_documents(query_text)
except AttributeError:
    results_text = retriever_text._get_relevant_documents(query_text, run_manager=None)

similar_players_text = [r.page_content for r in results_text]
print("Top similar players (text-based embeddings, stats only):")
print(similar_players_text)

# ----------------------------
# Step 4: PCA visualization
# ----------------------------
pca = PCA(n_components=2)
emb_2d = pca.fit_transform(features_scaled)

plt.figure(figsize=(10,8))
plt.scatter(emb_2d[:,0], emb_2d[:,1], color='lightgray', alpha=0.5)
for i, name in enumerate(df_players['player']):
    if name == "Harry Kane" or name in similar_players:
        plt.scatter(emb_2d[i,0], emb_2d[i,1], label=name, color='red')
        plt.text(emb_2d[i,0]+0.02, emb_2d[i,1]+0.02, name, fontsize=9)
plt.title("PCA of Player Numerical Features")
plt.xlabel("PC1")
plt.ylabel("PC2")
plt.legend()
plt.show()
