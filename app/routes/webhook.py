from flask import Blueprint, request, jsonify, abort
import os
import json
import hmac
import base64
import hashlib
from app.supabase_client import supabase
from app.services.supabase_write import upsert_variant
from app.routes.bulk_sync import normalize_gid

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
            "shopify_product_id": normalize_gid(product_id),
            "shopify_variant_id": normalize_gid(variant["id"]),
            "product_title": product_title,
            "variant_title": variant.get("title", ""),
            "price": float(variant.get("price", 0)),
            "ean": variant.get("barcode", ""),
            "sku": variant.get("sku") or payload.get("sku") or "",  # ✅ fix qui
            "image_url": image_url,
            "user_id": user_id,
        }
        upsert_variant(record)

# ✅ Webhook per products/delete

    return jsonify({"status": "success", "imported": len(variants)}), 200
@webhook.route("/webhook/product-delete", methods=["POST"])
def handle_product_delete():
    raw_body = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")

    if not verify_webhook(raw_body, hmac_header):
        abort(401, "Invalid HMAC")

    payload = json.loads(raw_body)
    shopify_product_id = normalize_gid(payload.get("id"))

    from app.supabase_client import supabase
    response = supabase.table("products") \
        .delete() \
        .eq("shopify_product_id", shopify_product_id) \
        .execute()

    print(f"🗑️ Prodotto eliminato: {shopify_product_id} — {response}")

    return jsonify({"status": "deleted", "shopify_product_id": shopify_product_id}), 200

# ✅ Webhook per order-update
@webhook.route("/webhook/order-update", methods=["POST"])
def handle_order_update():
    raw_body = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")

    if not verify_webhook(raw_body, hmac_header):
        abort(401, "Invalid HMAC")

    payload = json.loads(raw_body)

    # Ignora se l'ordine NON è evaso
    fulfillment_status = payload.get("fulfillment_status")
    if fulfillment_status != "fulfilled":
        return jsonify({"status": "skipped", "reason": "not fulfilled"}), 200

    shopify_order_id = int(normalize_gid(payload.get("id")))

    # Recupera ordine Supabase tramite shopify_order_id
    order_resp = supabase.table("orders").select("id").eq("shopify_order_id", shopify_order_id).single().execute()
    if not order_resp.data:
        return jsonify({"status": "skipped", "reason": "order not found"}), 200

    order_id = order_resp.data["id"]

    # Chiama la funzione evadi_ordine() su Supabase
    supabase.rpc("evadi_ordine", { "ordine_id": order_id }).execute()

    print(f"✅ Ordine evaso via webhook: {shopify_order_id}")
    return jsonify({"status": "fulfilled and cleaned", "order_id": order_id}), 200


# ✅ Webhook per order-create
@webhook.route("/webhook/order-create", methods=["POST"])
def handle_order_create():
    raw_body = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")

    if not verify_webhook(raw_body, hmac_header):
        abort(401, "Invalid HMAC")

    payload = json.loads(raw_body)

    # Verifica stato
    if payload.get("financial_status") != "paid" or payload.get("fulfillment_status") != "unfulfilled":
        return jsonify({"status": "skipped", "reason": "not paid or already fulfilled"}), 200

    shopify_order_id = int(normalize_gid(payload["id"]))

    # Verifica duplicati
    exists = supabase.table("orders").select("id").eq("shopify_order_id", shopify_order_id).execute()
    if exists.data:
        return jsonify({"status": "skipped", "reason": "already imported"}), 200

    user_id = os.environ.get("DEFAULT_USER_ID", None)

    # Inserisci ordine
    order_resp = supabase.table("orders").insert({
        "shopify_order_id": shopify_order_id,
        "number": payload.get("name"),
        "customer_name": (payload.get("customer") or {}).get("first_name", "Ospite"),
        "channel": (payload.get("app") or {}).get("name", "Online Store"),
        "created_at": payload.get("created_at"),
        "payment_status": "pagato",
        "fulfillment_status": "inevaso",
        "total": float(payload.get("total_price", 0)),
        "user_id": user_id
    }).execute()

    order_id = order_resp.data[0]["id"]

    for item in payload.get("line_items", []):
        shopify_variant_id = normalize_gid(item.get("variant_id"))
        quantity = item.get("quantity", 1)
        sku = item.get("sku") or item.get("title") or "Senza SKU"
        product_id = None

        # Collega al product_id (se esiste)
        product = supabase.table("products").select("id").eq("shopify_variant_id", shopify_variant_id).execute()
        if product.data:
            product_id = product.data[0]["id"]

        supabase.table("order_items").insert({
            "order_id": order_id,
            "shopify_variant_id": shopify_variant_id,
            "product_id": product_id,
            "sku": sku,
            "quantity": quantity
        }).execute()

        # Aggiorna riservato_sito
        if product_id:
            supabase.rpc("adjust_inventory_after_fulfillment", {
                "pid": product_id,
                "delta": -quantity * -1  # somma positiva
            }).execute()

    print(f"🛒 Nuovo ordine importato: {shopify_order_id}")
    return jsonify({"status": "order created", "order_id": shopify_order_id}), 200
