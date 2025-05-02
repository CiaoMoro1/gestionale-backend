import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()  # 👈 Carica .env nella memoria di os.environ

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
