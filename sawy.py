import os
from google import genai
from google.genai import types

# 1. Initialize the Client
# The client automatically picks up the API key from the GEMINI_API_KEY environment variable.
try:
    API_KEY = "AIzaSyCpgHX9YRGSbUL4q9T39OIXXMy2yBsfOPU"
    client = genai.Client(api_key=API_KEY)
except Exception as e:
    print(f"Error initializing client: {e}")
    print("Please ensure your GEMINI_API_KEY environment variable is set correctly.")
    exit()

# 2. Define your prompt and model
model_name = 'gemini-2.5-flash'
prompt = "Explain the concept of a black hole in one sentence."

# 3. Generate content
print(f"Sending prompt to model: {model_name}...")
response = client.models.generate_content(
    model=model_name,
    contents=prompt
)

# 4. Print the response
print("\n--- Gemini Response ---")
print(response.text)
print("-----------------------")