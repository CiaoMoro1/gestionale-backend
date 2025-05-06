from flask import Blueprint, request, jsonify, abort
import os
import json
import hmac
import base64
import hashlib
from app.services.supabase_write import upsert_variant

webhook = Blueprint("webhook", __name__)

# ✅ Verifica HMAC Shopify
def verify_webhook(data, hmac_header):
    secret = os.environ.get("SHOPIFY_WEBHOOK_SECRET")
    digest = hmac.new(secret.encode(), data, hashlib.sha256).digest()
    computed_hmac = base64.b64encode(digest).decode()
    return hmac.compare_digest(computed_hmac, hmac_header)

# ✅ Webhook per products/create e products/update
@webhook.route("/webhook/product-update", methods=["POST"])
def handle_product_update():
    raw_body = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")

    if not verify_webhook(raw_body, hmac_header):
        abort(401, "Invalid HMAC")

    payload = json.loads(raw_body)

    variants = payload.get("variants", [])
    product_id = payload.get("id")
    product_title = payload.get("title", "")
    image_url = (payload.get("image") or {}).get("src", "")

    # ✅ ID statico, puoi cambiarlo con auth se necessario
    user_id = os.environ.get("DEFAULT_USER_ID", "admin-sync")

    for variant in variants:
        record = {
            "shopify_product_id": product_id,
            "shopify_variant_id": variant["id"],
            "sku": variant.get("sku", ""),
            "product_title": product_title,
            "variant_title": variant.get("title", ""),
            "price": float(variant.get("price", 0)),
            "ean": variant.get("barcode", ""),
            "image_url": image_url,
            "user_id": user_id,
        }
        upsert_variant(record)

    return jsonify({"status": "success", "imported": len(variants)}), 200
