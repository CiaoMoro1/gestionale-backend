from flask import Flask
from flask_cors import CORS
from dotenv import load_dotenv
import os
import ssl
import certifi
import logging  # <--- AGGIUNGI QUI

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
)

from app.routes.bulk_disable_tracking import bulk_routes

load_dotenv()

ssl._create_default_https_context = ssl.create_default_context
ssl._create_default_https_context().load_verify_locations(certifi.where())

from app.routes.bulk_sync import bulk_sync
from app.routes.webhook import webhook
from app.routes.orders import orders
from app.routes.shopify_sync import shopify

def create_app():
    app = Flask(__name__)

    # Supporta anche multi-origin (dev/prod)
    origins = [x.strip() for x in os.getenv("FRONTEND_ORIGIN", "http://localhost:5173").split(",")]
    CORS(app, resources={r"/*": {"origins": origins}}, supports_credentials=True)

    app.register_blueprint(bulk_sync)
    app.register_blueprint(webhook)
    app.register_blueprint(orders)
    app.register_blueprint(shopify)
    app.register_blueprint(bulk_routes)

    logging.info("App Flask avviata. Blueprint registrati: bulk_sync, webhook, orders, shopify, bulk_routes")
    return app

app = create_app()

if __name__ == "__main__":
    app.run(debug=True)
