from flask import Blueprint, request, jsonify
import requests
import os

validate_address_bp = Blueprint("validate_address", __name__)
GOOGLE_GEOCODING_KEY = os.getenv("GOOGLE_GEOCODING_KEY")  # Cambia la chiave (può essere la stessa, ma serve abilitare la Geocoding API!)

@validate_address_bp.route("/api/validate-address", methods=["POST"])
def validate_address():
    body = request.json

    if not body or "address" not in body:
        return jsonify({"error": "Missing 'address' in request"}), 400

    # Unisci tutti i campi address in una stringa unica (come Google Maps)
    addr = body["address"]
    address_str = ", ".join([
        addr.get("addressLines", [""])[0],
        addr.get("postalCode", ""),
        addr.get("locality", ""),
        addr.get("administrativeArea", ""),
        addr.get("regionCode", "")
    ])
    
    params = {
        "address": address_str,
        "key": GOOGLE_GEOCODING_KEY
    }

    resp = requests.get(
        "https://maps.googleapis.com/maps/api/geocode/json",
        params=params
    )

    if resp.status_code != 200:
        return jsonify({"error": "Google Geocoding API error", "details": resp.text}), resp.status_code

    data = resp.json()

    if not data.get("results"):
        return jsonify({"error": "Nessun indirizzo trovato da Google Maps."}), 404

    best = data["results"][0]

    # Ricava info utili da address_components
    result = {
        "formatted_address": best.get("formatted_address"),
        "address_components": best.get("address_components"),
        "location": best.get("geometry", {}).get("location"),
        "types": best.get("types"),
        "partial_match": best.get("partial_match", False)
    }

    return jsonify(result)
