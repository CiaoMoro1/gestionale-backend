from flask import Blueprint, request
import os
import requests

bp = Blueprint('amazon_oauth', __name__)

@bp.route('/api/amazon/oauth/callback')
def amazon_oauth_callback():
    code = request.args.get('code')
    state = request.args.get('state')
    error = request.args.get('error')
    if error:
        return f"Errore OAuth: {error}", 400
    if not code:
        return "Code non trovato!", 400

    # Scambia il code per il refresh_token (facoltativo, puoi anche solo visualizzare il code!)
    url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": os.getenv("SPAPI_CLIENT_ID"),
        "client_secret": os.getenv("SPAPI_CLIENT_SECRET"),
        "redirect_uri": "https://gestionale-api.onrender.com/api/amazon/oauth/callback"
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(url, data=data, headers=headers)
    try:
        resp.raise_for_status()
        tokens = resp.json()
        return f"""
        <h2>SUCCESSO!</h2>
        <b>refresh_token:</b><br>
        <code style='font-size:1.1rem'>{tokens.get('refresh_token')}</code>
        <br><br>
        <b>access_token (valido 1h):</b><br>
        <code style='font-size:1.1rem'>{tokens.get('access_token')}</code>
        <br><br>
        <i>Salva il refresh_token nel tuo .env come <b>SPAPI_REFRESH_TOKEN</b></i>
        """
    except Exception as e:
        return f"Errore nel recupero del refresh_token: {e} <br> {resp.text}", 500
