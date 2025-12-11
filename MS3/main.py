import streamlit as st
from google import genai
from mistralai import Mistral
import cohere
import json
import os
from dotenv import load_dotenv

from RAG import send_user_input_to_backend

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


# ---------------------------
# BUILD CONTEXT FROM BASELINE + EMBEDDINGS
# ---------------------------
def build_context(baseline, embeddings):
    context = "=== BASELINE RESULTS ===\n"
    for p in baseline["players"]:
        context += f"- {p['name']} ({p['team']}), Points: {p['points']}, Position: {p['position']}\n"
    
    context += "\n=== EMBEDDING RESULTS ===\n"
    for sp in embeddings["similar_players"]:
        context += f"- {sp['name']} ({sp['team']}), Similarity: {sp['similarity']}\n"

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

        send_user_input_to_backend(query)

        # Display & store user message
        st.session_state.messages.append({"role": "user", "content": query})
        with st.chat_message("user"):
            st.write(query)

        # Build RAG context + prompt
        context = build_context(dummy_baseline, dummy_embeddings)
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
