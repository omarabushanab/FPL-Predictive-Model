from dotenv import load_dotenv
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()
uri = os.getenv("URI") or st.secrets["URI"]
username = os.getenv("DB-USERNAME") or st.secrets["DB-USERNAME"]
password = os.getenv("PASSWORD") or st.secrets["PASSWORD"]
print("URI:", uri, "USERNAME:", username, "PASSWORD:", password)