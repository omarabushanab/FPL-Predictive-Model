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
# CUSTOM CSS FOR PREMIUM UI
# ---------------------------
def inject_custom_css():
    st.markdown("""
    <style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
    
    /* Global Styles */
    * {
        font-family: 'Inter', sans-serif;
    }
    
    /* Main container styling */
    .main {
        background: linear-gradient(135deg, #0f0f1e 0%, #1a1a2e 50%, #16213e 100%);
        padding: 0 !important;
    }
    
    /* Header styling */
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 3rem 2rem;
        border-radius: 0 0 30px 30px;
        margin: -1rem -1rem 2rem -1rem;
        box-shadow: 0 10px 40px rgba(102, 126, 234, 0.3);
        text-align: center;
        animation: fadeInDown 0.8s ease;
    }
    
    .main-header h1 {
        color: white;
        font-size: 3.5rem;
        font-weight: 800;
        margin: 0;
        text-shadow: 0 4px 20px rgba(0,0,0,0.3);
        letter-spacing: -1px;
    }
    
    .main-header p {
        color: rgba(255,255,255,0.9);
        font-size: 1.2rem;
        margin-top: 0.5rem;
        font-weight: 300;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1e1e30 0%, #252538 100%);
        border-right: 1px solid rgba(255,255,255,0.1);
    }
    
    [data-testid="stSidebar"] h1, 
    [data-testid="stSidebar"] h2, 
    [data-testid="stSidebar"] h3 {
        color: #ffffff;
        font-weight: 700;
    }
    
    /* Chat message containers */
    [data-testid="stChatMessageContent"] {
        background: rgba(255, 255, 255, 0.05);
        backdrop-filter: blur(10px);
        border-radius: 20px;
        border: 1px solid rgba(255, 255, 255, 0.1);
        padding: 1.5rem;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        animation: fadeIn 0.5s ease;
    }
    
    /* User message */
    [data-testid="stChatMessage"][data-testid-type="user"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 20px 20px 5px 20px;
        margin-bottom: 1rem;
    }
    
    /* Assistant message */
    [data-testid="stChatMessage"][data-testid-type="assistant"] {
        background: rgba(255, 255, 255, 0.03);
        border-radius: 20px 20px 20px 5px;
        border: 1px solid rgba(102, 126, 234, 0.3);
        margin-bottom: 1rem;
    }
    
    /* Input box styling */
    [data-testid="stChatInput"] {
        background: rgba(255, 255, 255, 0.05);
        backdrop-filter: blur(10px);
        border-radius: 25px;
        border: 2px solid rgba(102, 126, 234, 0.3);
        padding: 0.5rem;
        transition: all 0.3s ease;
    }
    
    [data-testid="stChatInput"]:focus-within {
        border-color: #667eea;
        box-shadow: 0 0 20px rgba(102, 126, 234, 0.4);
        transform: translateY(-2px);
    }
    
    /* Expander styling */
    [data-testid="stExpander"] {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(102, 126, 234, 0.2);
        border-radius: 15px;
        margin: 1rem 0;
        overflow: hidden;
        transition: all 0.3s ease;
    }
    
    [data-testid="stExpander"]:hover {
        border-color: rgba(102, 126, 234, 0.5);
        box-shadow: 0 5px 20px rgba(102, 126, 234, 0.2);
    }
    
    /* Metric cards */
    [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 700;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    
    [data-testid="stMetricLabel"] {
        color: rgba(255, 255, 255, 0.7);
        font-size: 0.9rem;
        font-weight: 500;
    }
    
    /* Button styling */
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 15px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 5px 15px rgba(102, 126, 234, 0.3);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(102, 126, 234, 0.5);
    }
    
    /* Selectbox styling */
    [data-baseweb="select"] {
        background: rgba(255, 255, 255, 0.05);
        border-radius: 12px;
        border: 1px solid rgba(102, 126, 234, 0.3);
    }
    
    /* Spinner */
    [data-testid="stSpinner"] > div {
        border-top-color: #667eea !important;
    }
    
    /* Info/Success/Error boxes */
    .stAlert {
        background: rgba(255, 255, 255, 0.05);
        backdrop-filter: blur(10px);
        border-radius: 15px;
        border: 1px solid rgba(102, 126, 234, 0.3);
        color: white;
    }
    
    /* Scrollbar */
    ::-webkit-scrollbar {
        width: 10px;
        height: 10px;
    }
    
    ::-webkit-scrollbar-track {
        background: rgba(255, 255, 255, 0.05);
        border-radius: 10px;
    }
    
    ::-webkit-scrollbar-thumb {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
    }
    
    ::-webkit-scrollbar-thumb:hover {
        background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
    }
    
    /* Animations */
    @keyframes fadeIn {
        from {
            opacity: 0;
            transform: translateY(10px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    @keyframes fadeInDown {
        from {
            opacity: 0;
            transform: translateY(-20px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    /* Code blocks */
    code {
        background: rgba(102, 126, 234, 0.2);
        padding: 0.2rem 0.5rem;
        border-radius: 8px;
        color: #a8b2ff;
        font-family: 'Courier New', monospace;
    }
    
    /* JSON display */
    [data-testid="stJson"] {
        background: rgba(255, 255, 255, 0.03);
        border-radius: 12px;
        border: 1px solid rgba(102, 126, 234, 0.2);
    }
    
    /* Divider */
    hr {
        border: none;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(102, 126, 234, 0.5), transparent);
        margin: 2rem 0;
    }
    
    /* Tooltips */
    [data-testid="stTooltipIcon"] {
        color: #667eea;
    }
    
    /* Make text more readable */
    p, li, span {
        color: rgba(255, 255, 255, 0.9);
        line-height: 1.6;
    }
    
    /* Status indicators */
    .status-indicator {
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        margin-right: 8px;
        animation: pulse 2s infinite;
    }
    
    .status-active {
        background: #4ade80;
        box-shadow: 0 0 10px #4ade80;
    }
    
    @keyframes pulse {
        0%, 100% {
            opacity: 1;
        }
        50% {
            opacity: 0.5;
        }
    }
    
    /* Card effect for sections */
    .card {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(102, 126, 234, 0.2);
        border-radius: 20px;
        padding: 2rem;
        margin: 1rem 0;
        backdrop-filter: blur(10px);
        transition: all 0.3s ease;
    }
    
    .card:hover {
        border-color: rgba(102, 126, 234, 0.5);
        box-shadow: 0 10px 40px rgba(102, 126, 234, 0.2);
        transform: translateY(-5px);
    }
    </style>
    """, unsafe_allow_html=True)


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
    st.markdown("### 🔗 Baseline KG Results")

    if not baseline_records:
        st.info("No baseline KG results found.")
        return

    for i, record in enumerate(baseline_records, 1):
        with st.expander(f"📊 Baseline Result #{i}", expanded=False):
            for key, value in record.items():
                if hasattr(value, "type") and hasattr(value, "properties"):
                    st.markdown(f"**🔄 Relationship:** `{value.type}`")
                    st.json(value.properties)
                else:
                    st.markdown(f"**{key}:** {value}")


def display_embedding_results(embedding_records):
    st.markdown("### 🧠 Embedding (Semantic) KG Results")

    if not embedding_records:
        st.info("No embedding results found.")
        return

    for i, item in enumerate(embedding_records, 1):
        score = item.get('similarity_score', 0)
        with st.expander(f"✨ Similar Node #{i} • Score: {score:.4f}", expanded=False):
            if "labels" in item:
                st.markdown("**🏷️ Labels:**")
                st.code(", ".join(item["labels"]))
            if "properties" in item:
                st.markdown("**📋 Properties:**")
                st.json(item["properties"])
            if "similarity_score" in item:
                st.markdown(f"**📈 Similarity Score:** `{item['similarity_score']:.4f}`")


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

    # Inject custom CSS
    inject_custom_css()

    # Main header
    st.markdown("""
    <div class="main-header">
        <h1>⚽ FPL Graph-RAG Assistant</h1>
    </div>
    """, unsafe_allow_html=True)

    # ---------------------------
    # SIDEBAR CONFIGURATION
    # ---------------------------
    with st.sidebar:
        st.markdown("## ⚙️ Settings")
        st.markdown("---")
        
        # Model Selection
        st.markdown("### 🤖 Language Model")
        model_choice = st.selectbox(
            "Choose your AI brain",
            ["Gemini 2.5 Flash", "Mistral Small", "Cohere"],
            help="Select the AI model to generate intelligent responses"
        )
        
        # Model status indicator
        st.markdown(f'<span class="status-indicator status-active"></span> {model_choice} Active', unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Embedding Model Selection
        st.markdown("### 🧠 Embedding Engine")
        embedding_choice = st.selectbox(
            "Choose semantic search model",
            [
                "sentence-transformers/all-MiniLM-L6-v2",
                "sentence-transformers/all-mpnet-base-v2"
            ],
            help="Powers the semantic similarity search in your knowledge graph"
        )
        
        st.markdown("---")
        
        # About section
        with st.expander("ℹ️ About", expanded=False):
            st.markdown("""
            e2fl tany b2a
            """)
        
        st.markdown("---")
        
        # Clear Chat Button
        if st.button("🗑️ Clear Chat History", use_container_width=True, type="primary"):
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
    # WELCOME MESSAGE
    # ---------------------------
    if len(st.session_state.messages) == 0:
        st.markdown("""
        <div class="card">
            <h2 style="text-align: center; color: #667eea;">👋 Welcome to FPL Graph-RAG</h2>
            <p style="text-align: center; font-size: 1.1rem; margin-top: 1rem;">
                Ask me anything about FPL players, teams, statistics, and performance data.
                I'll search through the knowledge graph to give you accurate, context-aware answers.
            </p>
            <p style="text-align: center; margin-top: 1.5rem; color: rgba(255,255,255,0.6);">
            </p>
        </div>
        """, unsafe_allow_html=True)

    # ---------------------------
    # DISPLAY CHAT HISTORY
    # ---------------------------
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            if msg["role"] == "user":
                st.markdown(msg["content"])
            else:
                st.markdown(msg["content"])
                
                # Display KG context if available
                if "kg_context" in msg and msg["kg_context"]:
                    with st.expander("📊 Knowledge Graph Context", expanded=False):
                        st.markdown("*Raw data retrieved from the Knowledge Graph*")
                        display_baseline_results(msg["kg_context"]["baseline"])
                        display_embedding_results(msg["kg_context"]["embedding"])
                
                # Display metrics if available
                if "metrics" in msg:
                    with st.expander("📈 Performance Metrics", expanded=False):
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("⚡ Latency", f"{msg['metrics']['Latency (s)']}s")
                        with col2:
                            st.metric("📝 Prompt", msg['metrics']['Prompt Tokens'])
                        with col3:
                            st.metric("💬 Response", msg['metrics']['Answer Tokens'])
                        with col4:
                            st.metric("💰 Cost", msg['metrics']['Estimated Cost'])

    # ---------------------------
    # HANDLE USER INPUT
    # ---------------------------
    query = st.chat_input("⚽ Ask anything about FPL...")

    if query:
        # Save and render user message
        st.session_state.messages.append({"role": "user", "content": query})
        
        with st.chat_message("user"):
            st.markdown(query)

        # Retrieve from Knowledge Graph
        try:
            with st.spinner("🔍 Querying Knowledge Graph..."):
                conn = Neo4jConnection(URI, USERNAME, PASSWORD)
                baseline, feature = RAG.send_user_input_to_backend(query, conn, embedding_choice)
                
            # Build context and prompt
            context = build_context(baseline, feature)
            structured_prompt = build_prompt(query, context)

        except Exception as e:
            with st.chat_message("assistant"):
                error_msg = f"⚠️ **Knowledge Graph Error**: Unable to retrieve data.\n\n`{str(e)}`"
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
                with st.spinner("🤖 Gemini is analyzing..."):
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
                with st.spinner("🤖 Mistral is processing..."):
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
                with st.expander("📊 Knowledge Graph Context", expanded=False):
                    st.markdown("*Raw data retrieved from the Knowledge Graph*")
                    display_baseline_results(baseline)
                    display_embedding_results(feature)
                
                # Display metrics
                if metrics:
                    with st.expander("📈 Performance Metrics", expanded=False):
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("⚡ Latency", f"{metrics['Latency (s)']}s")
                        with col2:
                            st.metric("📝 Prompt", metrics['Prompt Tokens'])
                        with col3:
                            st.metric("💬 Response", metrics['Answer Tokens'])
                        with col4:
                            st.metric("💰 Cost", metrics['Estimated Cost'])

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