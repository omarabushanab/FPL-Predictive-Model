import streamlit as st
from google import genai
import os
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv() 
# --- Configuration ---
GEMINI_MODEL = os.getenv("GEMINI_MODEL")
st.title("💬 Gemini Chat App")

# 1. Initialize the Client and API Key securely
# We'll use st.secrets.toml for best practice, but for a quick fix,
# we'll store the API key in a constant or environment variable.
# NOTE: It's best practice to use os.getenv("GEMINI_API_KEY")
API_GEMINI_KEY = os.getenv("GEMINI_API_KEY")

# Store the Client and Chat Session in st.session_state
if "client" not in st.session_state:
    try:
        # Create the client instance and store it
        st.session_state.client = genai.Client(api_key=API_GEMINI_KEY)
    except Exception as e:
        st.error(f"Error initializing Gemini client: {e}")
        st.stop()

if "chat_session" not in st.session_state:
    # Use the persistent client to create the chat session
    st.session_state.chat_session = st.session_state.client.chats.create(model=GEMINI_MODEL)
    st.session_state.messages = [] # Use simpler list for UI display

# --- Display existing messages (remains the same) ---
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# --- Handle new user input (REVISED) ---
if prompt := st.chat_input("Ask Gemini a question..."):
    # 1. Add user message to UI history and display it
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # 2. Get and display model response using the chat session
    with st.chat_message("assistant"):
        with st.spinner("Gemini is thinking..."):
            # Use the .send_message method on the stored chat session object
            # The client is guaranteed to be open because it's in session_state
            response = st.session_state.chat_session.send_message(prompt)
            response_text = response.text
        st.markdown(response_text)
    
    # 3. Add model response to UI history
    st.session_state.messages.append({"role": "assistant", "content": response_text})