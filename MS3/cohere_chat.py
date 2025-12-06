import streamlit as st
import os
from cohere import Client
from cohere.core.api_error import ApiError 
from dotenv import load_dotenv

# --- Configuration & Initialization ---
load_dotenv()

# Get model and API key from .env file
COHERE_API_KEY = os.getenv("COHERE_API_KEY")
# Ensure a default model name is available if the .env file is missing COHERE_MODEL
DEFAULT_MODEL = os.getenv("COHERE_MODEL") 

st.title("💬 Cohere Chat App")
st.caption(f"Model: {DEFAULT_MODEL}")

# 1. Initialize the Client
# Store the Client in st.session_state to ensure it persists across reruns.
if "client" not in st.session_state:
    if not COHERE_API_KEY:
        st.error("🚨 COHERE_API_KEY not found. Please set it in your .env file.")
        st.stop()
    try:
        # Create the Cohere Client instance and store it
        st.session_state.client = Client(api_key=COHERE_API_KEY)
    except Exception as e:
        st.error(f"Error initializing Cohere client: {e}")
        st.stop()

# 2. Initialize conversation history
# History will store simple dicts: {"role": "user"/"assistant", "content": "text"}
if "messages" not in st.session_state:
    st.session_state.messages = []

# --- Display existing messages ---
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# --- Handle new user input ---
if prompt := st.chat_input("Ask Cohere a question..."):
    
    # 3. Add user message to UI history and display it
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # 4. Prepare the conversation history for the API call
    # Cohere accepts a list of objects with a 'role' and a 'message' (not 'content')
    # For a simple chat, we will let client.chat handle history for the first turn,
    # and just send the current message. For persistent chat, the "history" parameter
    # would be used, but since 'command-light' may not support persistent chat, 
    # we'll stick to a single-turn call, which is easier for now.
    
    # NOTE: The simplest method is to let client.chat handle the prompt and history.
    
    # 5. Get and display model response (using streaming for better UX)
# 5. Get and display model response (using streaming for better UX)
    with st.chat_message("assistant"):
        with st.spinner(f"Cohere is thinking..."):
            try:
                # Use the client stored in session state
                stream_response = st.session_state.client.chat_stream(
                    model=DEFAULT_MODEL,
                    message=prompt,
                    max_tokens=500,
                    temperature=0.7
                )
                
                # 🚨 THE FIX: Filter out chunks that don't have the 'text' attribute 🚨
                def generate_stream_text():
                    full_response = ""
                    for chunk in stream_response:
                        # Check if the chunk has the 'text' attribute (which holds the streamed content)
                        if hasattr(chunk, 'text'):
                            text_to_yield = chunk.text
                            full_response += text_to_yield
                            yield text_to_yield
                    return full_response

                # st.write_stream calls the generator and handles displaying it
                response_text = st.write_stream(generate_stream_text())

            except ApiError as e:
                # Handle API errors gracefully in the UI
                response_text = f"❌ Cohere API Error: {e}"
                st.error(response_text)
            except Exception as e:
                response_text = f"❌ An unexpected error occurred: {e}"
                st.error(response_text)

    # 6. Add final model response to UI history (only if it was successful text)
    if not response_text.startswith("❌"):
        st.session_state.messages.append({"role": "assistant", "content": response_text})