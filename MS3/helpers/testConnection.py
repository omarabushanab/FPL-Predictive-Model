from dotenv import load_dotenv
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()
uri = os.getenv("URI")
username = os.getenv("DB-USERNAME")
password = os.getenv("PASSWORD")
print("URI:", uri, "USERNAME:", username, "PASSWORD:", password)