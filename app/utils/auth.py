import os
import logging
from functools import wraps
from flask import request, jsonify, g
import jwt
from jwt.exceptions import InvalidTokenError

DEV_MODE = os.getenv("DEV_MODE") == "1"

def get_jwt_secret():
    return os.environ.get("SUPABASE_JWT_SECRET")

def require_auth(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if DEV_MODE:
            dev_user_id = request.headers.get("x-user-id")
            if dev_user_id:
                logging.warning("[DEV MODE] x-user-id: %s", dev_user_id)
                g.user_id = dev_user_id
                g.email = "dev@example.com"
                return fn(*args, **kwargs)
            return jsonify({"error": "x-user-id header richiesto in DEV_MODE"}), 401

        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return jsonify({"error": "Token mancante o malformato"}), 401

        token = auth_header.split(" ")[1]
        try:
            decoded = jwt.decode(
                token,
                get_jwt_secret(),
                algorithms=["HS256"],
                audience=None  # imposta se vuoi controllare audience
            )
            user_id = decoded.get("sub")
            email = decoded.get("email")
            if not user_id:
                return jsonify({"error": "Token non valido"}), 401
            g.user_id = user_id
            g.email = email
            logging.info("User %s autenticato (user_id=%s)", email, user_id)
            return fn(*args, **kwargs)
        except InvalidTokenError as e:
            logging.warning("Token JWT non valido: %s", str(e))
            return jsonify({"error": "Token non valido"}), 401
        except Exception as e:
            logging.error("Errore validazione JWT: %s", str(e))
            return jsonify({"error": "Errore autenticazione"}), 500
    return wrapper
