from flask import Flask
from flask_cors import CORS
from dotenv import load_dotenv
import os
import ssl
import certifi
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
)

from app.routes.bulk_disable_tracking import bulk_routes
from app.routes.bulk_sync import bulk_sync
from app.routes.webhook import webhook
from app.routes.shopify_sync import shopify
from app.routes.brt import brt  # <--- AGGIUNGI QUESTA LINEA se hai creato il blueprint BRT
from app.routes.validate_address import validate_address_bp
from app.routes.amazon_vendor import bp as amazon_vendor_bp
from app.routes.oauth_callback import bp as amazon_oauth_bp
from app.routes import cavallotti
from app.routes.jobs import bp as jobs_bp
from app.routes.fatture_amazon_vendor import bp as fatture_amazon_vendor_bp
from app.routes.notecredito_amazon_reso import bp as notecredito_amazon_reso_bp
from app.routes.notecredito import bp as notecredito_tools_bp
from app.routes.produzione import bp as produzione_bp  # NEW
from app.routes.prelievo import bp as prelievo_bp  # NEW



load_dotenv()

ssl._create_default_https_context = ssl.create_default_context
ssl._create_default_https_context().load_verify_locations(certifi.where())

def create_app():
    app = Flask(__name__)

    # Health check env base (facoltativo ma consigliato)
    REQUIRED_ENVS = [
        "SUPABASE_PROJECT_ID", "BRT_USER_ID", "BRT_PASSWORD",
        "BRT_DEPARTURE_DEPOT", "BRT_API_URL"
    ]
    for key in REQUIRED_ENVS:
        if not os.getenv(key):
            logging.error(f"❌ Variabile d'ambiente mancante: {key}")
            raise RuntimeError(f"Variabile d'ambiente mancante: {key}")

    origins = [x.strip() for x in os.getenv("FRONTEND_ORIGIN", "http://localhost:5173").split(",")]
    CORS(app, resources={r"/*": {"origins": origins}}, supports_credentials=True)

    app.register_blueprint(bulk_sync)
    app.register_blueprint(webhook)
    app.register_blueprint(shopify)
    app.register_blueprint(bulk_routes)
    app.register_blueprint(brt)  # <--- AGGIUNGI QUI
    app.register_blueprint(validate_address_bp)
    app.register_blueprint(amazon_vendor_bp)
    app.register_blueprint(produzione_bp) 
    app.register_blueprint(fatture_amazon_vendor_bp)
    app.register_blueprint(notecredito_amazon_reso_bp)
    app.register_blueprint(notecredito_tools_bp)
    app.register_blueprint(amazon_oauth_bp)
    app.register_blueprint(cavallotti.bp)
    app.register_blueprint(jobs_bp)
    app.register_blueprint(prelievo_bp)  # NEW
    names = ", ".join(sorted(app.blueprints.keys()))
    logging.info(f"App Flask avviata. Blueprint registrati: {names}")
    return app

app = create_app()

if __name__ == "__main__":
    app.run(debug=True)
