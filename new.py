import streamlit as st
from google import genai
import json

# ---------------------------
#   CONFIG
# ---------------------------
GEMINI_MODEL = "gemini-2.5-flash"
API_GEMINI_KEY = "AIzaSyCpgHX9YRGSbUL4q9T39OIXXMy2yBsfOPU"

st.title("⚽ FPL Graph-RAG Assistant (LLM Layer)")
st.write("This version uses DUMMY KG data until the retrieval team finishes.")

# ---------------------------
#   DUMMY KG RESULTS
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
#   FUNCTION: BUILD CONTEXT
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
#   FUNCTION: STRUCTURED PROMPT
# ---------------------------
def build_prompt(user_query, context):
    persona = "You are an FPL expert. You ONLY answer based on the provided context."
    task = (
        "Using the context below, answer the user's question.\n"
        "If the answer cannot be found in the context, say: 'Not enough information in the KG.'\n"
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
#   INITIALIZE GEMINI CLIENT
# ---------------------------
if "client" not in st.session_state:
    st.session_state.client = genai.Client(api_key=API_GEMINI_KEY)

if "messages" not in st.session_state:
    st.session_state.messages = []


# ---------------------------
#   UI
# ---------------------------
model_choice = st.selectbox("Choose LLM", ["Gemini 2.5 Flash"])  # later add more models

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

query = st.chat_input("Ask something about FPL...")

if query:
    # display user message
    st.session_state.messages.append({"role": "user", "content": query})
    with st.chat_message("user"):
        st.write(query)

    # Build context & prompt
    context = build_context(dummy_baseline, dummy_embeddings)
    prompt = build_prompt(query, context)

    # Call model
    with st.chat_message("assistant"):
        with st.spinner("Thinking using the KG..."):
            response = st.session_state.client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt
            )
            answer = response.text
        st.write(answer)

    # store assistant message
    st.session_state.messages.append({"role": "assistant", "content": answer})
