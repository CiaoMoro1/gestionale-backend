from functools import wraps
from flask import request, jsonify
from supabase import create_client
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

# ✅ Prende DEV_MODE dalla variabile d’ambiente
DEV_MODE = os.getenv("DEV_MODE") == "1"

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def require_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        # ✅ Modalità sviluppo: bypass con x-user-id
        if DEV_MODE:
            dev_user_id = request.headers.get("x-user-id")
            if dev_user_id:
                print("👤 [DEV MODE] x-user-id:", dev_user_id)
                return fn(dev_user_id, *args, **kwargs)

        # ✅ Modalità produzione: token JWT
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"error": "Token mancante o malformato"}), 401

        token = auth_header.split(" ")[1]
        print("🔎 DEBUG JWT\n→ Token ricevuto:", token[:50] + "...")

        try:
            user = supabase.auth.get_user(token)
            if user and user.user and user.user.id:
                print("✅ Utente verificato:", user.user.email)
                return fn(user.user.id, *args, **kwargs)
            else:
                return jsonify({"error": "Token non valido"}), 401
        except Exception as e:
            print("❌ Errore Supabase Auth:", str(e))
            return jsonify({"error": "Token non valido"}), 401

    return wrapper
