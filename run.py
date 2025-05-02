from flask import Flask
from flask_cors import CORS

from app.routes import webhook
from app.routes.sync import sync
from app.routes.bulk_sync import bulk_sync  # 👈 aggiunto

def create_app():
    app = Flask(__name__)
    CORS(app)

    app.register_blueprint(webhook.bp)
    app.register_blueprint(sync)
    app.register_blueprint(bulk_sync)  # 👈 registra anche le rotte bulk

    return app

app = create_app()

if __name__ == "__main__":
    app.run(debug=True)
