import time
import os
import streamlit as st
from dotenv import load_dotenv
import pandas as pd

from google import genai
from mistralai import Mistral
import cohere

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
# FIXED TEST CASE
# --------------------------------------------------
QUESTION = "Provide me with the history of stats for Phil Foden"

EXPECTED_FACTS = [
    "2021-22",
    "2022-23",
    "goals",
    "assists",
    "total points"
]

# --------------------------------------------------
# SIMPLE CORRECTNESS FUNCTION
# --------------------------------------------------
def correctness_score(answer, expected_facts):
    hits = sum(1 for fact in expected_facts if fact.lower() in answer.lower())
    return round(hits / len(expected_facts), 2)

# --------------------------------------------------
# BUILD KG CONTEXT ONCE
# --------------------------------------------------
baseline, embedding = RAG.send_user_input_to_backend(
    QUESTION, conn, EMBEDDING_MODEL
)

context = build_context(baseline, embedding)
prompt = build_prompt(QUESTION, context)

# --------------------------------------------------
# RUN MODELS
# --------------------------------------------------
results = []

# -------- GEMINI --------
start = time.time()
response = gemini_client.models.generate_content(
    model=GEMINI_MODEL,
    contents=prompt
)
latency = round(time.time() - start, 3)

usage = response.usage_metadata
answer = response.text

results.append({
    "model": "Gemini 2.5 Flash",
    "latency_s": latency,
    "input_tokens": usage.prompt_token_count,
    "output_tokens": usage.candidates_token_count,
    "correctness": correctness_score(answer, EXPECTED_FACTS)
})

# -------- MISTRAL --------
start = time.time()
response = mistral_client.chat.complete(
    model=MISTRAL_MODEL,
    messages=[{"role": "user", "content": prompt}],
    stream=False
)
latency = round(time.time() - start, 3)

usage = response.usage
answer = response.choices[0].message.content

results.append({
    "model": "Mistral Small",
    "latency_s": latency,
    "input_tokens": usage.prompt_tokens,
    "output_tokens": usage.completion_tokens,
    "correctness": correctness_score(answer, EXPECTED_FACTS)
})

# -------- COHERE --------
start = time.time()
response = cohere_client.chat(
    model=COHERE_MODEL,
    message=prompt
)
latency = round(time.time() - start, 3)

tokens = response.meta.tokens
answer = response.text

results.append({
    "model": "Cohere",
    "latency_s": latency,
    "input_tokens": int(tokens.input_tokens),
    "output_tokens": int(tokens.output_tokens),
    "correctness": correctness_score(answer, EXPECTED_FACTS)
})

# --------------------------------------------------
# RESULTS
# --------------------------------------------------
df = pd.DataFrame(results)
print("\n=== MODEL COMPARISON RESULTS ===\n")
print(df)
print("\nBest model (by correctness):")
print(df.sort_values("correctness", ascending=False).iloc[0])
