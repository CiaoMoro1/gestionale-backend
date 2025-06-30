import os
import logging
from dotenv import load_dotenv
from supabase import create_client

# Inizializza logging strutturato
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
)

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logging.critical("❌ SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set in environment variables!")
    raise EnvironmentError("Supabase credentials missing!")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
logging.info("✅ Supabase client initialized.")
