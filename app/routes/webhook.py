from flask import Blueprint, request, abort
from app.supabase_client import supabase
import hmac
import hashlib
import os

bp = Blueprint("webhook", __name__)

def verify_shopify_hmac(request_data, hmac_header):
    secret = os.getenv("SHOPIFY_WEBHOOK_SECRET", "").encode()
    digest = hmac.new(secret, request_data, hashlib.sha256).hexdigest()
    return hmac.compare_digest(digest, hmac_header)

@bp.route("/webhook/product-update", methods=["POST"])
def handle_product_update():
    raw_body = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256", "")
    if not verify_shopify_hmac(raw_body, hmac_header):
        abort(401)

    data = request.json
    variant_id = data.get("variant_id")
    quantity = data.get("quantity")

    supabase.table("products").update({"quantity": quantity}).eq("shopify_variant_id", variant_id).execute()
    return "", 204
