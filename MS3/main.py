import streamlit as st
from google import genai
from mistralai import Mistral
import cohere
import json
import os
from dotenv import load_dotenv
import RAG
from helpers.neo4j_connection import Neo4jConnection

# Load the environment variables from the .env file
load_dotenv()
# ---------------------------
# CONFIG
# ---------------------------
GEMINI_MODEL = os.getenv("GEMINI_MODEL")
API_GEMINI_KEY = os.getenv("GEMINI_API_KEY")

MISTRAL_MODEL = os.getenv("MISTRAL_MODEL")
API_MISTRAL_KEY = os.getenv("MISTRAL_API_KEY")

COHERE_MODEL = os.getenv("COHERE_MODEL")
API_COHERE_KEY = os.getenv("COHERE_API_KEY")

URI = os.getenv("URI")
USERNAME = os.getenv("DB-USERNAME")
PASSWORD = os.getenv("PASSWORD")



# ---------------------------
# DUMMY KG RESULTS
# ---------------------------
dummy_baseline = {
    "players": [
        {"name": "Erling Haaland", "team": "Man City", "points": 250, "position": "FWD"},
        {"name": "Mohamed Salah", "team": "Liverpool", "points": 230, "position": "MID"},
    ]
}

dummy_embeddings = {
    "similar_players": [
        {"name": "Julian Alvarez", "team": "Man City", "similarity": 0.82},
        {"name": "Darwin Nunez", "team": "Liverpool", "similarity": 0.76}
    ]
}
# baseline
# <Record p.player_name='Aaron Connolly' stats=<Relationship element_id='5:6b9eba7c-0fbf-4c8b-b7cd-2e626e7f569d:1179949699440837391' nodes=(<Node element_id='4:6b9eba7c-0fbf-4c8b-b7cd-2e626e7f569d:783' labels=frozenset() properties={}>, <Node element_id='4:6b9eba7c-0fbf-4c8b-b7cd-2e626e7f569d:12' labels=frozenset() properties={}>) type='PLAYED_IN' properties={'goals_scored': 0, 'bps': 0, 'bonus': 0, 'minutes': 0, 'own_goals': 0, 'clean_sheets': 0, 'goals_conceded': 0, 'total_points': 0, 'penalties_missed': 0, 'red_cards': 0, 'yellow_cards': 0, 'influence': 0.0, 'saves': 0, 'form': 0.0, 'assists': 0, 'threat': 0, 'creativity': 0.0, 'ict_index': 0.0, 'penalties_saved': 0}>>
# ---------------------------
# BUILD CONTEXT FROM BASELINE + EMBEDDINGS
# ---------------------------
def build_context(baseline_records, embedding_records):
    context = "=== BASELINE RESULTS ===\n"

    # --------------------------
    # BASELINE (Neo4j Records)
    # --------------------------
    for record in baseline_records:
        try:
            player_name = record["p.player_name"]
        except:
            player_name = record.get("player_name", "Unknown")

        stats_rel = record["stats"]
        stats = stats_rel._properties  # relationship properties

        context += f"\nPlayer: {player_name}\n"
        context += "Match Stats:\n"

        # Loop through stats dictionary
        for key, value in stats.items():
            context += f"  - {key}: {value}\n"

        context += "\n"

    # --------------------------
    # EMBEDDING RESULTS
    # --------------------------
    context += "\n=== EMBEDDING RESULTS ===\n"

    for sp in embedding_records:
        name = sp.get("name", "Unknown")
        team = sp.get("team", "Unknown")
        sim = sp.get("similarity", 0)

        context += f"- {name} ({team}), Similarity: {sim}\n"

    return context



# ---------------------------
# BUILD STRUCTURED PROMPT
# ---------------------------
def build_prompt(user_query, context):
    persona = "You are an FPL expert who must ONLY use the provided context."
    task = (
        "Using ONLY the context below, answer the user's question.\n"
        "If information is missing, reply: 'Not enough information in the KG.'\n"
        "Do NOT hallucinate.\n"
    )

    prompt = f"""
Persona:
{persona}

Task:
{task}

Context:
{context}

User Question:
{user_query}

Answer:
"""
    return prompt


# ---------------------------
# INIT CLIENTS
# ---------------------------
def models():
    st.title("⚽ FPL Graph-RAG Assistant (Gemini + Mistral + Cohere)")

    # Gemini client
    if "gemini_client" not in st.session_state:
        st.session_state.gemini_client = genai.Client(api_key=API_GEMINI_KEY)

    # Mistral client
    if "mistral_client" not in st.session_state:
        st.session_state.mistral_client = Mistral(api_key=API_MISTRAL_KEY)

    # Cohere client
    if "cohere_client" not in st.session_state:
        st.session_state.cohere_client = cohere.Client(API_COHERE_KEY)

    # Unified message history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    



    # ---------------------------
    # UI MODEL DROPDOWN
    # ---------------------------
    model_choice = st.selectbox(
        "Choose Model",
        ["Gemini 2.5 Flash", "Mistral Small", "Cohere"]
    )


    # ---------------------------
    # DISPLAY CHAT HISTORY
    # ---------------------------
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])


    # ---------------------------
    # HANDLE USER INPUT
    # ---------------------------
    query = st.chat_input("Ask something about FPL...")

    if query:

        conn = Neo4jConnection(URI,USERNAME,PASSWORD)
        baseline,feature = RAG.send_user_input_to_backend(query,conn)
        print(f"this is the baseline returned to main.py {baseline}")
        print(f"this is the features returned to main.py {feature}")


        # Display & store user message
        st.session_state.messages.append({"role": "user", "content": query})
        with st.chat_message("user"):
            st.write(query)

        # Build RAG context + prompt
        context = build_context(baseline, feature)
        structured_prompt = build_prompt(query, context)

        # GEMINI
        if model_choice == "Gemini 2.5 Flash":
            with st.chat_message("assistant"):
                with st.spinner("Gemini Thinking..."):
                    response = st.session_state.gemini_client.models.generate_content(
                        model=GEMINI_MODEL,
                        contents=structured_prompt
                    )
                    answer = response.text
                st.write(answer)

        # MISTRAL
        elif model_choice == "Mistral Small":
            with st.chat_message("assistant"):
                with st.spinner("Mistral Thinking..."):
                    try:
                        response = st.session_state.mistral_client.chat.complete(
                            model=MISTRAL_MODEL,
                            messages=[{"role": "user", "content": structured_prompt}],
                            stream=False
                        )
                        answer = response.choices[0].message.content
                    except Exception as e:
                        answer = f"Error with Mistral API: {e}"

                st.write(answer)

        # COHERE
        else:
            with st.chat_message("assistant"):
                with st.spinner("Cohere Thinking..."):
                    try:
                        response = st.session_state.cohere_client.chat(
                            model=COHERE_MODEL,
                            message=structured_prompt,
                            max_tokens=200
                        )
                        answer = response.text
                    except Exception as e:
                        answer = f"Error with Cohere API: {e}"

                st.write(answer)

        # Save assistant response
        st.session_state.messages.append({"role": "assistant", "content": answer})
models()
