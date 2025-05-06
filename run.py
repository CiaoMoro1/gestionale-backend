from flask import Flask
from flask_cors import CORS
from dotenv import load_dotenv
import os
import ssl
import certifi  # ✅ Per risolvere problemi SSL su Windows

# 🔃 Carica variabili .env
load_dotenv()

# ✅ Forza uso certificati ufficiali CA (fix per Windows + Shopify)
ssl._create_default_https_context = ssl.create_default_context
ssl._create_default_https_context().load_verify_locations(certifi.where())

# 📦 Importa blueprint delle rotte
from app.routes.bulk_sync import bulk_sync
from app.routes.webhook import webhook
from app.routes.orders import orders


def create_app():
    app = Flask(__name__)

    # ✅ Abilita CORS solo per il frontend (vite) CORS(app, resources={r"/*": {"origins": "http://localhost:5173"}})
    FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "http://localhost:5173")
    CORS(app, resources={r"/*": {"origins": FRONTEND_ORIGIN}}, supports_credentials=True)


    # ✅ Registra blueprint
    app.register_blueprint(bulk_sync)
    
    app.register_blueprint(webhook)

    app.register_blueprint(orders)

    return app

app = create_app()

if __name__ == "__main__":
    app.run(debug=True)
