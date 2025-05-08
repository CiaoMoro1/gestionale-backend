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
    user_id = os.environ.get("DEFAULT_USER_ID", "admin-sync")

    for variant in variants:
        record = {
            "shopify_product_id": normalize_gid(product_id),
            "shopify_variant_id": normalize_gid(variant["id"]),
            "product_title": product_title,
            "variant_title": variant.get("title", ""),
            "price": float(variant.get("price", 0)),
            "ean": variant.get("barcode", ""),
            "sku": variant.get("sku") or payload.get("sku") or "",
            "image_url": image_url,
            # Solo se disponibili nei webhook:
            "inventory_policy": variant.get("inventory_policy", ""),
            "status": payload.get("status", ""),  # spesso mancante
            "user_id": user_id,
        }
        upsert_variant(record)

    print(f"✅ Prodotto aggiornato: {product_title} ({product_id})")
    return jsonify({"status": "success", "imported": len(variants)}), 200

# ✅ Webhook per products/delete
@webhook.route("/webhook/product-delete", methods=["POST"])
def handle_product_delete():
    raw_body = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")
    if not verify_webhook(raw_body, hmac_header):
        abort(401, "Invalid HMAC")

    payload = json.loads(raw_body)
    shopify_product_id = normalize_gid(payload.get("id"))
    response = supabase.table("products").delete().eq("shopify_product_id", shopify_product_id).execute()

    print(f"🗑️ Prodotto eliminato: {shopify_product_id} — {response}")
    return jsonify({"status": "deleted", "shopify_product_id": shopify_product_id}), 200

# ✅ Webhook per order-create
@webhook.route("/webhook/order-create", methods=["POST"])
def handle_order_create():
    raw_body = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")
    if not verify_webhook(raw_body, hmac_header):
        abort(401, "Invalid HMAC")

    payload = json.loads(raw_body)
    shopify_order_id = int(normalize_gid(payload["id"]))
    financial_status = (payload.get("financial_status") or "").upper()
    fulfillment_status = payload.get("fulfillment_status")

    line_items = payload.get("line_items", [])
    shipping_lines = payload.get("shipping_lines", [])

    COD_KEYWORDS = [
        "contrassegno",
        "pagamento alla consegna",
        "cash on delivery",
        "commissione pagamento"
    ]

    has_cod_fee = any(
        any(kw in (item.get("title") or "").lower() for kw in COD_KEYWORDS)
        for item in line_items
    ) or any(
        (line.get("title") or "").lower() == "spedizione non richiesta"
        for line in shipping_lines
    )

    if financial_status == "PAID":
        payment_status = "pagato"
    elif financial_status == "PENDING" and has_cod_fee:
        payment_status = "contrassegno"
    else:
        print(f"⚠️ Ordine skippato: non valido per l'import.")
        return jsonify({"status": "skipped", "reason": "not paid or not COD"}), 200

    if fulfillment_status not in [None, "unfulfilled"]:
        print(f"⚠️ Ordine skippato: già evaso.")
        return jsonify({"status": "skipped", "reason": "already fulfilled"}), 200

    exists = supabase.table("orders").select("id").eq("shopify_order_id", shopify_order_id).execute()
    if exists.data:
        print(f"⛔ Ordine già presente: {shopify_order_id}")
        return jsonify({"status": "skipped", "reason": "already imported"}), 200

    user_id = os.environ.get("DEFAULT_USER_ID", None)

    customer = payload.get("customer") or {}
    first_name = customer.get("first_name", "")
    last_name = customer.get("last_name", "")
    customer_name = f"{first_name} {last_name}".strip() or "Ospite"

    order_resp = supabase.table("orders").insert({
        "shopify_order_id": shopify_order_id,
        "number": payload.get("name"),
        "customer_name": customer_name,
        "channel": (payload.get("app") or {}).get("name", "Online Store"),
        "created_at": payload.get("created_at"),
        "payment_status": payment_status,
        "fulfillment_status": "inevaso",
        "total": 0,  # inizialmente 0, lo aggiorniamo sotto
        "user_id": user_id
    }).execute()

    order_id = order_resp.data[0]["id"]
    totale = 0

    for item in line_items:
        shopify_variant_id = normalize_gid(item.get("variant_id"))
        quantity = item.get("quantity", 1)
        price = float(item.get("price", 0))  # 💰 Prezzo reale dell'ordine Shopify
        sku = (item.get("sku") or item.get("title") or "Senza SKU").strip().upper()
        product_id = None

        product = supabase.table("products").select("id").eq("shopify_variant_id", shopify_variant_id).execute()
        if product.data:
            product_id = product.data[0]["id"]
        else:
            print(f"⚠️ Variante non trovata su Supabase: {shopify_variant_id}")

        supabase.table("order_items").insert({
            "order_id": order_id,
            "shopify_variant_id": shopify_variant_id,
            "product_id": product_id,
            "sku": sku,
            "quantity": quantity,
            "price": price  # se hai il campo
        }).execute()

        if product_id:
            supabase.rpc("adjust_inventory_after_fulfillment", {
                "pid": product_id,
                "delta": quantity
            }).execute()

        totale += quantity * price

    # 🔄 Ricalcola riservato_sito
    supabase.rpc("repair_riservato_by_order", {
        "ordine_id": order_id
    }).execute()

    # 💰 Aggiorna il totale reale da Shopify
    supabase.table("orders").update({
        "total": totale
    }).eq("id", order_id).execute()

    print(f"🛒 Nuovo ordine importato: {shopify_order_id}")
    return jsonify({"status": "order created", "order_id": order_id}), 200



# ✅ Webhook per order-update ##################
@webhook.route("/webhook/order-update", methods=["POST"])
def handle_order_update():
    raw_body = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")
    if not verify_webhook(raw_body, hmac_header):
        abort(401, "Invalid HMAC")

    payload = json.loads(raw_body)
    raw_id = payload.get("id")
    if not raw_id:
        print("❌ Webhook ricevuto senza ID ordine valido.")
        return jsonify({"status": "skipped", "reason": "missing ID"}), 400

    shopify_order_id = int(normalize_gid(raw_id))
    order_resp = supabase.table("orders").select("id").eq("shopify_order_id", shopify_order_id).limit(1).execute()

    if not order_resp.data:
        print(f"🔁 Ordine {shopify_order_id} non trovato → fallback a create.")
        return handle_order_create()

    order_id = order_resp.data[0]["id"]
    items = payload.get("line_items", [])

    # 🔄 Cancella tutti gli articoli esistenti
    supabase.table("order_items").delete().eq("order_id", order_id).execute()

    totale = 0

    for item in items:
        shopify_variant_id = normalize_gid(item.get("variant_id"))
        sku = (item.get("sku") or item.get("title") or "Senza SKU").strip().upper()
        quantity = item.get("quantity", 1)
        price = float(item.get("price", 0))  # 💰 Prezzo preso da Shopify

        product = supabase.table("products").select("id").eq("shopify_variant_id", shopify_variant_id).execute()
        product_id = product.data[0]["id"] if product.data else None

        supabase.table("order_items").insert({
            "order_id": order_id,
            "shopify_variant_id": shopify_variant_id,
            "product_id": product_id,
            "sku": sku,
            "quantity": quantity,
            "price": price  # se hai la colonna, altrimenti rimuovi
        }).execute()

        if product_id:
            supabase.rpc("adjust_inventory_after_fulfillment", {
                "pid": product_id,
                "delta": quantity
            }).execute()

        totale += quantity * price

    # 📦 Evadi se necessario
    if payload.get("fulfillment_status") == "fulfilled":
        supabase.rpc("evadi_ordine", {"ordine_id": order_id}).execute()
        print(f"✅ Ordine {shopify_order_id} evaso via webhook")

    # 🔄 Ricalcola riservato_sito
    supabase.rpc("repair_riservato_by_order", {
        "ordine_id": order_id
    }).execute()

    # 💰 Aggiorna totale
    supabase.table("orders").update({
        "total": totale
    }).eq("id", order_id).execute()

    print(f"🔁 Ordine aggiornato correttamente: {shopify_order_id}")
    return jsonify({"status": "updated", "order_id": order_id}), 200




# ✅ Webhook per order-cancel — versione corretta!
@webhook.route("/webhook/order-cancel", methods=["POST"])
def handle_order_cancel():
    raw_body = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")
    if not verify_webhook(raw_body, hmac_header):
        abort(401, "Invalid HMAC")

    payload = json.loads(raw_body)
    shopify_order_id = int(normalize_gid(payload.get("id")))

    try:
        order_resp = supabase.table("orders").select("id, fulfillment_status").eq("shopify_order_id", shopify_order_id).limit(1).execute()

        if not order_resp.data:
            print(f"🛑 Ordine {shopify_order_id} non trovato → impossibile annullarlo.")
            return jsonify({"status": "skipped", "reason": "ordine non trovato"}), 200

        order = order_resp.data[0]
        order_id = order["id"]
        current_status = order["fulfillment_status"]

        if current_status == "annullato":
            print(f"⚠️ Ordine {shopify_order_id} già annullato.")
            return jsonify({"status": "skipped", "reason": "già annullato"}), 200

        # Aggiorna lo status
        supabase.table("orders").update({
            "fulfillment_status": "annullato"
        }).eq("id", order_id).execute()

        print(f"🗑️ Ordine annullato: {shopify_order_id}")
        return jsonify({"status": "cancelled", "order_id": order_id}), 200

    except Exception as e:
        print(f"❌ Errore durante annullamento ordine {shopify_order_id}: {e}")
        return jsonify({"status": "error", "reason": str(e)}), 500
