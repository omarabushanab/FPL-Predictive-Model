import os
from google import genai
from google.genai import types

def run_interactive_generation():
    """
    Initializes the Gemini client and takes an interactive prompt from the user.
    """
    try:
        API_KEY = "AIzaSyCpgHX9YRGSbUL4q9T39OIXXMy2yBsfOPU"
        client = genai.Client(api_key=API_KEY)
    except Exception as e:
        print(f"🛑 Error initializing client: {e}")
        print("Please ensure your GEMINI_API_KEY environment variable is set correctly.")
        return

    model_name = 'gemini-2.5-flash'
    print("--- Gemini Interactive Prompt ---")
    print(f"Model selected: {model_name}")
    print("Type 'exit' or 'quit' to end the session.")
    print("-" * 35)

    while True:
        # Get the user's prompt
        user_prompt = input("👤 You: ")

        # Check for exit commands
        if user_prompt.lower() in ['exit', 'quit']:
            print("\n👋 Goodbye!")
            break

        if not user_prompt.strip():
            continue

        # Generate content
        try:
            print("🧠 Thinking...")
            response = client.models.generate_content(
                model=model_name,
                contents=user_prompt
            )

            # Print the response
            print("\n✨ Gemini:")
            print(response.text)
            print("-" * 35)

        except Exception as e:
            print(f"\n❌ An error occurred: {e}")
            print("-" * 35)
            # You might want to break here or continue, depending on desired robustness.
            # We'll continue the loop for simplicity.
            continue

if __name__ == "__main__":
    # Ensure the environment variable is set before running the function
    run_interactive_generation()