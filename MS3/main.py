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
from neo4j.graph import Relationship

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
# METRICS
# ---------------------------
def compute_metrics(input, output, start_time, model_name):
    latency = round(time.time() - start_time, 3)
    cost = "Free tier"
    
    return {
        "Model": model_name,
        "Latency (s)": latency,
        "Prompt Tokens": input,
        "Answer Tokens": output,
        "Estimated Cost": cost
    }


# ---------------------------
# BUILD CONTEXT FROM BASELINE + EMBEDDINGS
# ---------------------------
def build_context(baseline_records, embedding_records):
    context = ""

    # ---------------- BASELINE ----------------
    if not baseline_records:
        context += "No baseline results found.\n"
    else:
        context += "=== BASELINE RESULTS ===\n"

        for record in baseline_records:
            for k, v in record.items():
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
# DISPLAY HELPERS
# ---------------------------
def display_baseline_results(baseline_records):
    st.subheader("🔗 Baseline KG Results")

    if not baseline_records:
        st.info("No baseline KG results found.")
        return

    for i, record in enumerate(baseline_records, 1):
        with st.expander(f"Baseline Result #{i}", expanded=False):
            for key, value in record.items():
                if hasattr(value, "type") and hasattr(value, "properties"):
                    st.markdown(f"**Relationship:** `{value.type}`")
                    st.json(value.properties)
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
# ERROR HANDLING WRAPPER
# ---------------------------
def handle_model_error(error, model_name):
    """Return user-friendly error messages"""
    error_str = str(error).lower()
    
    if "rate limit" in error_str or "quota" in error_str:
        return f"⚠️ **Rate Limit Exceeded**: The {model_name} API has reached its rate limit. Please wait a moment and try again."
    elif "overloaded" in error_str or "overwhelmed" in error_str or "503" in error_str:
        return f"⚠️ **Server Overloaded**: The {model_name} servers are currently experiencing high traffic. Please try again in a few moments."
    elif "timeout" in error_str:
        return f"⚠️ **Request Timeout**: The {model_name} API took too long to respond. Please try again."
    elif "authentication" in error_str or "api key" in error_str or "401" in error_str:
        return f"⚠️ **Authentication Error**: There's an issue with the {model_name} API key. Please check your configuration."
    elif "connection" in error_str or "network" in error_str:
        return f"⚠️ **Connection Error**: Unable to connect to {model_name}. Please check your internet connection."
    else:
        return f"⚠️ **Error with {model_name}**: {str(error)}\n\nPlease try again or select a different model."


# ---------------------------
# MAIN APP
# ---------------------------
def models():
    # Page configuration
    st.set_page_config(
        page_title="FPL Graph-RAG Assistant",
        page_icon="⚽",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Main title
    st.title("⚽ FPL Graph-RAG Assistant")
    st.markdown("*Powered by Knowledge Graphs & Multi-Model AI*")
    st.divider()

    # ---------------------------
    # SIDEBAR CONFIGURATION
    # ---------------------------
    with st.sidebar:
        st.header("⚙️ Configuration")
        st.markdown("---")
        
        # Model Selection
        st.subheader("🤖 Language Model")
        model_choice = st.selectbox(
            "Choose LLM",
            ["Gemini 2.5 Flash", "Mistral Small", "Cohere"],
            help="Select the AI model to generate responses"
        )
        
        st.markdown("---")
        
        # Embedding Model Selection
        st.subheader("🧠 Embedding Model")
        embedding_choice = st.selectbox(
            "Choose Embedder",
            [
                "sentence-transformers/all-MiniLM-L6-v2",
                "sentence-transformers/all-mpnet-base-v2"
            ],
            help="Select the model for semantic similarity search"
        )
        
        st.markdown("---")
        
        # Additional Info
        with st.expander("ℹ️ About", expanded=False):
            st.markdown("""
            **FPL Graph-RAG** combines:
            - 🔗 Knowledge Graph retrieval
            - 🧠 Semantic embeddings
            - 🤖 LLM reasoning
            
            Ask questions about Fantasy Premier League data!
            """)
        
        # Clear Chat Button
        st.markdown("---")
        if st.button("🗑️ Clear Chat History", use_container_width=True):
            st.session_state.messages = []
            st.rerun()

    # ---------------------------
    # INITIALIZE CLIENTS
    # ---------------------------
    if "gemini_client" not in st.session_state:
        try:
            st.session_state.gemini_client = genai.Client(api_key=API_GEMINI_KEY)
        except Exception as e:
            st.sidebar.error(f"Failed to initialize Gemini: {e}")

    if "mistral_client" not in st.session_state:
        try:
            st.session_state.mistral_client = Mistral(api_key=API_MISTRAL_KEY)
        except Exception as e:
            st.sidebar.error(f"Failed to initialize Mistral: {e}")

    if "cohere_client" not in st.session_state:
        try:
            st.session_state.cohere_client = cohere.Client(API_COHERE_KEY)
        except Exception as e:
            st.sidebar.error(f"Failed to initialize Cohere: {e}")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # ---------------------------
    # DISPLAY CHAT HISTORY
    # ---------------------------
    chat_container = st.container()
    
    with chat_container:
        for msg in st.session_state.messages:
            with st.chat_message(msg["role"]):
                if msg["role"] == "user":
                    st.markdown(msg["content"])
                else:
                    st.markdown(msg["content"])
                    
                    # Display KG context if available
                    if "kg_context" in msg:
                        with st.expander("📊 View KG-Retrieved Context", expanded=False):
                            st.markdown("*Raw information from Knowledge Graph before LLM processing*")
                            display_baseline_results(msg["kg_context"]["baseline"])
                            display_embedding_results(msg["kg_context"]["embedding"])
                    
                    # Display metrics if available
                    if "metrics" in msg:
                        with st.expander("📈 Model Metrics"):
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Latency", f"{msg['metrics']['Latency (s)']}s")
                            with col2:
                                st.metric("Prompt Tokens", msg['metrics']['Prompt Tokens'])
                            with col3:
                                st.metric("Answer Tokens", msg['metrics']['Answer Tokens'])
                            with col4:
                                st.metric("Cost", msg['metrics']['Estimated Cost'])

    # ---------------------------
    # HANDLE USER INPUT
    # ---------------------------
    query = st.chat_input("Ask something about FPL...")

    if query:
        # Save and render user message
        st.session_state.messages.append({"role": "user", "content": query})
        
        with st.chat_message("user"):
            st.write(query)

        # Retrieve from Knowledge Graph
        try:
            with st.spinner("🔍 Searching Knowledge Graph..."):
                conn = Neo4jConnection(URI, USERNAME, PASSWORD)
                baseline, feature = RAG.send_user_input_to_backend(query, conn, embedding_choice)
                
            # Build context and prompt
            context = build_context(baseline, feature)
            structured_prompt = build_prompt(query, context)

        except Exception as e:
            with st.chat_message("assistant"):
                error_msg = f"⚠️ **Knowledge Graph Error**: Unable to retrieve data from the database.\n\nError: {str(e)}"
                st.error(error_msg)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_msg
                })
            st.stop()

        # Generate response based on selected model
        with st.chat_message("assistant"):
            answer = None
            metrics = None
            
            # GEMINI
            if model_choice == "Gemini 2.5 Flash":
                with st.spinner("🤖 Gemini is thinking..."):
                    start_time = time.time()
                    try:
                        response = st.session_state.gemini_client.models.generate_content(
                            model=GEMINI_MODEL,
                            contents=structured_prompt
                        )
                        answer = response.text
                        usage = response.usage_metadata
                        metrics = compute_metrics(
                            usage.prompt_token_count,
                            usage.candidates_token_count,
                            start_time,
                            "Gemini 2.5 Flash"
                        )
                    except Exception as e:
                        answer = handle_model_error(e, "Gemini")
                        st.error(answer)

            # MISTRAL
            elif model_choice == "Mistral Small":
                with st.spinner("🤖 Mistral is thinking..."):
                    start_time = time.time()
                    try:
                        response = st.session_state.mistral_client.chat.complete(
                            model=MISTRAL_MODEL,
                            messages=[{"role": "user", "content": structured_prompt}],
                            stream=False
                        )
                        answer = response.choices[0].message.content
                        usage = response.usage
                        metrics = compute_metrics(
                            usage.prompt_tokens,
                            usage.completion_tokens,
                            start_time,
                            "Mistral Small"
                        )
                    except Exception as e:
                        answer = handle_model_error(e, "Mistral")
                        st.error(answer)

            # COHERE
            else:
                with st.spinner("🤖 Cohere is thinking..."):
                    start_time = time.time()
                    try:
                        response = st.session_state.cohere_client.chat(
                            model=COHERE_MODEL,
                            message=structured_prompt,
                        )
                        answer = response.text
                        tokens = response.meta.tokens
                        metrics = compute_metrics(
                            int(tokens.input_tokens),
                            int(tokens.output_tokens),
                            start_time,
                            "Cohere"
                        )
                    except Exception as e:
                        answer = handle_model_error(e, "Cohere")
                        st.error(answer)

            # Display answer if successful
            if answer and not answer.startswith("⚠️"):
                st.markdown(answer)
                
                # Display KG context
                with st.expander("📊 View KG-Retrieved Context", expanded=False):
                    st.markdown("*Raw information from Knowledge Graph before LLM processing*")
                    display_baseline_results(baseline)
                    display_embedding_results(feature)
                
                # Display metrics
                if metrics:
                    with st.expander("📈 Model Metrics"):
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Latency", f"{metrics['Latency (s)']}s")
                        with col2:
                            st.metric("Prompt Tokens", metrics['Prompt Tokens'])
                        with col3:
                            st.metric("Answer Tokens", metrics['Answer Tokens'])
                        with col4:
                            st.metric("Cost", metrics['Estimated Cost'])

            # Save assistant response
            st.session_state.messages.append({
                "role": "assistant",
                "content": answer,
                "kg_context": {
                    "baseline": baseline,
                    "embedding": feature
                } if answer and not answer.startswith("⚠️") else None,
                "metrics": metrics
            })


if __name__ == "__main__":
    models()