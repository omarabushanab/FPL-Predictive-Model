import os
import streamlit as st
from dotenv import load_dotenv
from google import genai
from mistralai import Mistral
import cohere
import pandas as pd

import RAG
from helpers.neo4j_connection import Neo4jConnection
from main import build_context
from main import build_prompt

# --------------------------------------------------
# LOAD ENV
# --------------------------------------------------
load_dotenv()

GEMINI_MODEL = os.getenv("GEMINI_MODEL") or st.secrets["GEMINI_MODEL"]
API_GEMINI_KEY = os.getenv("GEMINI_API_KEY") or st.secrets["GEMINI_API_KEY"]

MISTRAL_MODEL = os.getenv("MISTRAL_MODEL") or st.secrets["MISTRAL_MODEL"]
API_MISTRAL_KEY = os.getenv("MISTRAL_API_KEY") or st.secrets["MISTRAL_API_KEY"]

COHERE_MODEL = os.getenv("COHERE_MODEL") or st.secrets["COHERE_MODEL"]
API_COHERE_KEY = os.getenv("COHERE_API_KEY") or st.secrets["COHERE_API_KEY"]

URI = os.getenv("URI") or st.secrets["URI"]
USERNAME = os.getenv("DB-USERNAME") or st.secrets["DB-USERNAME"]
PASSWORD = os.getenv("PASSWORD") or st.secrets["PASSWORD"]

EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

# --------------------------------------------------
# INIT CLIENTS
# --------------------------------------------------
gemini_client = genai.Client(api_key=API_GEMINI_KEY)
mistral_client = Mistral(api_key=API_MISTRAL_KEY)
cohere_client = cohere.Client(API_COHERE_KEY)

conn = Neo4jConnection(URI, USERNAME, PASSWORD)

# --------------------------------------------------
# FIXED QUESTION
# --------------------------------------------------
QUESTION = "Provide me with the history of stats for Phil Foden"

# --------------------------------------------------
# GET KG CONTEXT ONCE
# --------------------------------------------------
baseline, embedding = RAG.send_user_input_to_backend(
    QUESTION, conn, EMBEDDING_MODEL
)

context = build_context(baseline, embedding)
prompt = build_prompt(QUESTION, context)

# --------------------------------------------------
# RUN MODELS
# --------------------------------------------------
answers = {}

# Gemini
response = gemini_client.models.generate_content(
    model=GEMINI_MODEL,
    contents=prompt
)
answers["Gemini 2.5 Flash"] = response.text

# Mistral
response = mistral_client.chat.complete(
    model=MISTRAL_MODEL,
    messages=[{"role": "user", "content": prompt}],
    stream=False
)
answers["Mistral Small"] = response.choices[0].message.content

# Cohere
response = cohere_client.chat(
    model=COHERE_MODEL,
    message=prompt
)
answers["Cohere"] = response.text

# --------------------------------------------------
# MANUAL QUALITATIVE EVALUATION
# --------------------------------------------------
print("\n=== MODEL ANSWERS ===\n")

evaluations = []

for model, answer in answers.items():
    print(f"\n--- {model} ---\n")
    print(answer)
    print("\nRate this answer from 1 (poor) to 5 (excellent):")

    quality = int(input("Answer Quality: "))
    relevance = int(input("Relevance: "))
    naturalness = int(input("Naturalness: "))
    correctness = int(input("Correctness: "))

    evaluations.append({
        "model": model,
        "answer_quality": quality,
        "relevance": relevance,
        "naturalness": naturalness,
        "correctness": correctness
    })

# --------------------------------------------------
# CREATE QUALITATIVE MATRIX
# --------------------------------------------------
df = pd.DataFrame(evaluations)

print("\n=== QUALITATIVE EVALUATION MATRIX ===\n")
print(df)

print("\n=== AVERAGE SCORES ===\n")
print(df.groupby("model").mean())

print("\nBest model (by average score):")
print(df.groupby("model").mean().mean(axis=1).idxmax())
