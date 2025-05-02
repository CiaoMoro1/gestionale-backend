from flask import Flask
from flask_cors import CORS
from app.routes import webhook

def create_app():
    app = Flask(__name__)
    CORS(app)
    app.register_blueprint(webhook.bp)
    return app

app = create_app()

if __name__ == "__main__":
    app.run(debug=True)
