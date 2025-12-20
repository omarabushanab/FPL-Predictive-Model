import streamlit as st
from mistralai import Mistral
import os
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv() 
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL") or st.secrets["MISTRAL_MODEL"]
API_MISTRAL_KEY = os.getenv("MISTRAL_API_KEY") or st.secrets["MISTRAL_API_KEY"]

mistral_api_key =API_MISTRAL_KEY
if not mistral_api_key:
    st.error("Please set the MISTRAL_API_KEY environment variable.")
    st.stop()

mistral_client = Mistral(api_key=mistral_api_key)

st.title("Mistral Chatbot")

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# User input
if user_question := st.chat_input("Your question:"):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": user_question})
    # Display user message
    with st.chat_message("user"):
        st.markdown(user_question)

    # Call Mistral API
    with mistral_client as client:
        response = client.chat.complete(
            model=MISTRAL_MODEL,
            messages=[{"role": "user", "content": user_question}],
            stream=False
        )
        # Add assistant response to chat history
        assistant_response = response.choices[0].message.content
        st.session_state.messages.append({"role": "assistant", "content": assistant_response})
        # Display assistant response
        with st.chat_message("assistant"):
            st.markdown(assistant_response)
