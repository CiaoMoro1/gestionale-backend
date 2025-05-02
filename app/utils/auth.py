import os
import jwt
from functools import wraps
from flask import request, abort

from dotenv import load_dotenv
load_dotenv()

SUPABASE_JWT_SECRET = os.environ.get("SUPABASE_JWT_SECRET")

def require_auth(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        auth_header = request.headers.get("Authorization", "")
        token = auth_header.replace("Bearer ", "")

        try:
            payload = jwt.decode(token, SUPABASE_JWT_SECRET, algorithms=["HS256"], options={"verify_aud": False})
            user_id = payload["sub"]
            print("✅ JWT OK — user_id:", user_id)
        except Exception as e:
            print("❌ Errore JWT:", str(e))
            abort(401, "Token non valido")

        return func(user_id=user_id, *args, **kwargs)
    return wrapper
