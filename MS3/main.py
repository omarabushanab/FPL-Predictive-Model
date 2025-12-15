import streamlit as st
from google import genai
from mistralai import Mistral
import cohere
import json
import os
from dotenv import load_dotenv
import RAG
from helpers.neo4j_connection import Neo4jConnection
import time

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
# MATRIX
# ---------------------------
def compute_metrics(prompt, answer, start_time, model_name):
    latency = round(time.time() - start_time, 3)

    # Approx token counts (OK for academic evaluation)
    prompt_tokens = len(prompt.split())
    answer_tokens = len(answer.split())

    # Rough cost estimation (can be "Free tier")
    cost = "Free tier"

    return {
        "Model": model_name,
        "Latency (s)": latency,
        "Prompt Tokens": prompt_tokens,
        "Answer Tokens": answer_tokens,
        "Estimated Cost": cost
    }

# baseline
# <Record p.player_name='Aaron Connolly' stats=<Relationship element_id='5:6b9eba7c-0fbf-4c8b-b7cd-2e626e7f569d:1179949699440837391' nodes=(<Node element_id='4:6b9eba7c-0fbf-4c8b-b7cd-2e626e7f569d:783' labels=frozenset() properties={}>, <Node element_id='4:6b9eba7c-0fbf-4c8b-b7cd-2e626e7f569d:12' labels=frozenset() properties={}>) type='PLAYED_IN' properties={'goals_scored': 0, 'bps': 0, 'bonus': 0, 'minutes': 0, 'own_goals': 0, 'clean_sheets': 0, 'goals_conceded': 0, 'total_points': 0, 'penalties_missed': 0, 'red_cards': 0, 'yellow_cards': 0, 'influence': 0.0, 'saves': 0, 'form': 0.0, 'assists': 0, 'threat': 0, 'creativity': 0.0, 'ict_index': 0.0, 'penalties_saved': 0}>>
# ---------------------------
# BUILD CONTEXT FROM BASELINE + EMBEDDINGS
# ---------------------------
from neo4j.graph import Relationship

def build_context(baseline_records, embedding_records):
    context = ""

    # ---------------- BASELINE ----------------
    if not baseline_records:
        context += "No baseline results found.\n"
    else:
        context += "=== BASELINE RESULTS ===\n"

        for record in baseline_records:
            for k, v in record.items():

                # ✅ Proper Neo4j relationship detection
                if isinstance(v, Relationship):
                    context += f"- {k} ({v.type}):\n"
                    for stat_key, stat_val in v._properties.items():
                        context += f"    • {stat_key}: {stat_val}\n"

                else:
                    context += f"- {k}: {v}\n"

            context += "\n"

    # ---------------- EMBEDDINGS ----------------
    if not embedding_records:
        context += "\nNo embedding results found.\n"
    else:
        context += "\n=== EMBEDDING RESULTS ===\n"
        for item in embedding_records:
            for k, v in item.items():
                context += f"- {k}: {v}\n"
            context += "\n"

    return context




# ---------------------------
# NICE DISPLAY HELPERS (KG TRANSPARENCY)
# ---------------------------
def display_baseline_results(baseline_records):
    st.subheader("🔗 Baseline KG Results")

    if not baseline_records:
        st.info("No baseline KG results found.")
        return

    for i, record in enumerate(baseline_records, 1):
        with st.expander(f"Baseline Result #{i}", expanded=False):
            for key, value in record.items():

                # Relationship (PLAYED_IN etc.)
                if hasattr(value, "type") and hasattr(value, "properties"):
                    st.markdown(f"**Relationship:** `{value.type}`")
                    st.json(value.properties)

                # Normal attributes (player, season, gameweek, etc.)
                else:
                    st.markdown(f"**{key}:** {value}")

def display_embedding_results(embedding_records):
    st.subheader("🧠 Embedding (Semantic) KG Results")

    if not embedding_records:
        st.info("No embedding results found.")
        return

    for i, item in enumerate(embedding_records, 1):
        with st.expander(f"Similar Node #{i} (score: {item.get('similarity_score', 'N/A')})", expanded=False):
            if "labels" in item:
                st.markdown("**Labels:**")
                st.code(", ".join(item["labels"]))

            if "properties" in item:
                st.markdown("**Properties:**")
                st.json(item["properties"])

            if "similarity_score" in item:
                st.markdown(f"**Similarity Score:** `{item['similarity_score']:.4f}`")



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

    embedding_choice = st.selectbox(
        "Choose Embedding Model",
        ["sentence-transformers/all-MiniLM-L6-v2", "sentence-transformers/all-mpnet-base-v2"]
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
        # 1️⃣ SAVE USER MESSAGE IMMEDIATELY
        st.session_state.messages.append(
            {"role": "user", "content": query}
        )

        # 2️⃣ RENDER USER MESSAGE IMMEDIATELY
        with st.chat_message("user"):
            st.write(query)


        conn = Neo4jConnection(URI,USERNAME,PASSWORD)
        baseline,feature = RAG.send_user_input_to_backend(query,conn,embedding_choice)
        print(f"this is the features returned to main.py {feature}")
        print(f"this is the baseline returned to main.py {baseline}")

        # ---------------------------
        # KG TRANSPARENCY SECTION
        # ---------------------------
        with st.expander("📊 View KG-Retrieved Context (Before LLM)", expanded=False):

            st.markdown(
                """
                This section shows the **raw information retrieved from the Knowledge Graph**
                *before* it is processed by the LLM.
                """
            )

            display_baseline_results(baseline)
            display_embedding_results(feature)

       
        # Build RAG context + prompt
        context = build_context(baseline, feature)
        print(f"this is the context {context}")
        structured_prompt = build_prompt(query, context)

        # GEMINI
        if model_choice == "Gemini 2.5 Flash":
            with st.chat_message("assistant"):
                with st.spinner("Gemini Thinking..."):
                    start_time = time.time()

                    response = st.session_state.gemini_client.models.generate_content(
                        model=GEMINI_MODEL,
                        contents=structured_prompt
                    )
                    answer = response.text

                    metrics = compute_metrics(
                        structured_prompt,
                        answer,
                        start_time,
                        "Gemini 2.5 Flash"
                    )

                st.write(answer)

                # 📊 METRICS DISPLAY
                with st.expander("📈 Model Metrics"):
                    st.json(metrics)


        # MISTRAL
        elif model_choice == "Mistral Small":
            with st.chat_message("assistant"):
                with st.spinner("Mistral Thinking..."):
                    start_time = time.time()

                    try:
                        response = st.session_state.mistral_client.chat.complete(
                            model=MISTRAL_MODEL,
                            messages=[{"role": "user", "content": structured_prompt}],
                            stream=False
                        )
                        answer = response.choices[0].message.content
                    except Exception as e:
                        answer = f"Error with Mistral API: {e}"

                    metrics = compute_metrics(
                        structured_prompt,
                        answer,
                        start_time,
                        "Mistral Small"
                    )

                st.write(answer)

                with st.expander("📈 Model Metrics"):
                    st.json(metrics)


        # COHERE
        else:
            with st.chat_message("assistant"):
                with st.spinner("Cohere Thinking..."):
                    start_time = time.time()

                    try:
                        response = st.session_state.cohere_client.chat(
                            model=COHERE_MODEL,
                            message=structured_prompt,
                        )
                        answer = response.text
                    except Exception as e:
                        answer = f"Error with Cohere API: {e}"

                    metrics = compute_metrics(
                        structured_prompt,
                        answer,
                        start_time,
                        "Cohere"
                    )

                st.write(answer)

                with st.expander("📈 Model Metrics"):
                    st.json(metrics)


        # Save assistant response
        st.session_state.messages.append({"role": "assistant", "content": answer})
models()
