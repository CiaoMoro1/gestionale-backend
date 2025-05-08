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
        "total": float(payload.get("total_price", 0)),  # iniziale da Shopify
        "user_id": user_id
    }).execute()

    order_id = order_resp.data[0]["id"]

    for item in line_items:
        shopify_variant_id = normalize_gid(item.get("variant_id"))
        quantity = item.get("quantity", 1)
        sku = item.get("sku") or item.get("title") or "Senza SKU"
        product_id = None

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

        if product_id:
            supabase.rpc("adjust_inventory_after_fulfillment", {
                "pid": product_id,
                "delta": -quantity * -1
            }).execute()

    # 🔄 Ricalcola riservato_sito dopo inserimento ordine
    supabase.rpc("repair_riservato_by_order", {
        "ordine_id": order_id
    }).execute()

    # 💰 Ricalcola totale in base ai prezzi attuali da products
    totale = 0
    order_items_resp = supabase.table("order_items").select("quantity, product_id").eq("order_id", order_id).execute()
    for r in order_items_resp.data:
        if r["product_id"]:
            prezzo = supabase.table("products").select("price").eq("id", r["product_id"]).single().execute()
            totale += r["quantity"] * float(prezzo.data["price"] or 0)

    supabase.table("orders").update({
        "total": totale
    }).eq("id", order_id).execute()

    print(f"🛒 Nuovo ordine importato: {shopify_order_id}")
    return jsonify({"status": "order created", "order_id": order_id}), 200


# ✅ Webhook per order-update
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
        return jsonify({"status": "skipped", "reason": "missing order ID"}), 400

    shopify_order_id = int(normalize_gid(raw_id))

    order_resp = supabase.table("orders").select("id").eq("shopify_order_id", shopify_order_id).limit(1).execute()

    if not order_resp.data:
        print(f"🔁 Ordine {shopify_order_id} non trovato → provo a importarlo.")

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
            print(f"⚠️ Fallback: ordine ancora non valido per importazione.")
            return jsonify({"status": "skipped", "reason": "not paid or not COD"}), 200

        if fulfillment_status not in [None, "unfulfilled"]:
            print(f"⚠️ Fallback: ordine già evaso, non importato.")
            return jsonify({"status": "skipped", "reason": "already fulfilled"}), 200

        user_id = os.environ.get("DEFAULT_USER_ID", None)
        customer = payload.get("customer") or {}
        first_name = customer.get("first_name", "")
        last_name = customer.get("last_name", "")
        customer_name = f"{first_name} {last_name}".strip() or "Ospite"

        order_insert = supabase.table("orders").insert({
            "shopify_order_id": shopify_order_id,
            "number": payload.get("name"),
            "customer_name": customer_name,
            "channel": (payload.get("app") or {}).get("name", "Online Store"),
            "created_at": payload.get("created_at"),
            "payment_status": payment_status,
            "fulfillment_status": "inevaso",
            "total": float(payload.get("total_price", 0)),
            "user_id": user_id
        }).execute()

        order_id = order_insert.data[0]["id"]

        for item in line_items:
            shopify_variant_id = normalize_gid(item.get("variant_id"))
            quantity = item.get("quantity", 1)
            sku = item.get("sku") or item.get("title") or "Senza SKU"

            product = supabase.table("products").select("id").eq("shopify_variant_id", shopify_variant_id).execute()
            product_id = product.data[0]["id"] if product.data else None

            supabase.table("order_items").insert({
                "order_id": order_id,
                "shopify_variant_id": shopify_variant_id,
                "product_id": product_id,
                "sku": sku,
                "quantity": quantity
            }).execute()

            if product_id:
                supabase.rpc("adjust_inventory_after_fulfillment", {
                    "pid": product_id,
                    "delta": -quantity * -1
                }).execute()

        # 🔄 Ricalcola riservato_sito
        supabase.rpc("repair_riservato_by_order", {
            "ordine_id": order_id
        }).execute()

        # 💰 Ricalcola totale ordine
        totale = 0
        order_items_resp = supabase.table("order_items").select("quantity, product_id").eq("order_id", order_id).execute()
        for r in order_items_resp.data:
            if r["product_id"]:
                prezzo = supabase.table("products").select("price").eq("id", r["product_id"]).single().execute()
                totale += r["quantity"] * float(prezzo.data["price"] or 0)

        supabase.table("orders").update({
            "total": totale
        }).eq("id", order_id).execute()

        print(f"🆕 Ordine {shopify_order_id} creato da webhook update.")
    else:
        order_id = order_resp.data[0]["id"]
        items = payload.get("line_items", [])

        for item in items:
            shopify_variant_id = normalize_gid(item.get("variant_id"))
            sku = item.get("sku") or item.get("title") or "Senza SKU"
            quantity = item.get("quantity", 1)

            product = supabase.table("products").select("id").eq("shopify_variant_id", shopify_variant_id).execute()
            product_id = product.data[0]["id"] if product.data else None

            existing = supabase.table("order_items").select("id, quantity").eq("order_id", order_id).eq("sku", sku).single().execute()

            if not existing.data:
                supabase.table("order_items").insert({
                    "order_id": order_id,
                    "shopify_variant_id": shopify_variant_id,
                    "product_id": product_id,
                    "sku": sku,
                    "quantity": quantity
                }).execute()
                delta = quantity
            else:
                previous_qty = existing.data["quantity"]
                delta = quantity - previous_qty
                if delta != 0:
                    supabase.table("order_items").update({
                        "quantity": quantity
                    }).eq("id", existing.data["id"]).execute()

            if product_id and delta != 0:
                supabase.rpc("adjust_inventory_after_fulfillment", {
                    "pid": product_id,
                    "delta": delta
                }).execute()


        if payload.get("fulfillment_status") == "fulfilled":
            supabase.rpc("evadi_ordine", {"ordine_id": order_id}).execute()
            print(f"✅ Ordine {shopify_order_id} evaso via webhook")

        print(f"🔁 Ordine aggiornato: {shopify_order_id}")

        # 🔄 Ricalcola riservato_sito
        supabase.rpc("repair_riservato_by_order", {
            "ordine_id": order_id
        }).execute()

        # 💰 Ricalcola totale ordine
        totale = 0
        order_items_resp = supabase.table("order_items").select("quantity, product_id").eq("order_id", order_id).execute()
        for r in order_items_resp.data:
            if r["product_id"]:
                prezzo = supabase.table("products").select("price").eq("id", r["product_id"]).single().execute()
                totale += r["quantity"] * float(prezzo.data["price"] or 0)

        supabase.table("orders").update({
            "total": totale
        }).eq("id", order_id).execute()

    return jsonify({"status": "updated", "order_id": shopify_order_id}), 200


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
        order_resp = supabase.table("orders").select("id").eq("shopify_order_id", shopify_order_id).limit(1).execute()
        if not order_resp.data:
            print(f"🛑 Ordine {shopify_order_id} non trovato → impossibile annullarlo.")
            return jsonify({"status": "skipped", "reason": "ordine non trovato"}), 200

        order_id = order_resp.data[0]["id"]

    except Exception as e:
        print(f"🗑️ Errore nel recupero ordine {shopify_order_id}: {e}")
        return jsonify({"status": "error", "reason": "errore durante il recupero ordine"}), 500

    # ✅ Aggiorna solo lo status. Il trigger farà il resto.
    supabase.table("orders").update({
        "fulfillment_status": "annullato"
    }).eq("id", order_id).execute()

    print(f"🗑️ Ordine annullato: {shopify_order_id}")
    return jsonify({"status": "cancelled", "order_id": order_id}), 200
