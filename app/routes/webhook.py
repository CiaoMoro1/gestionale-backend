from flask import Blueprint, request, jsonify, abort
import os
import json
import hmac
import base64
import hashlib
import logging
import time
import httpx
from app.supabase_client import supabase
from app.services.supabase_write import upsert_variant
from app.routes.bulk_sync import normalize_gid

webhook = Blueprint("webhook", __name__)

# ------------------------------------------------------------------
# Utility: Retry per errori di rete Supabase
# ------------------------------------------------------------------

def safe_execute(builder, retries=3, sleep=0.7):
    """Esegue una query Supabase con retry su errori di rete temporanei."""
    for attempt in range(retries):
        try:
            return builder.execute()
        except httpx.RemoteProtocolError as ex:
            if attempt < retries - 1:
                logging.warning("Retry Supabase (tentativo %s/%s): %s", attempt+1, retries, ex)
                time.sleep(sleep)
            else:
                logging.error("Errore Supabase non recuperabile: %s", ex, exc_info=True)
                raise

def verify_webhook(data: bytes, hmac_header: str | None) -> bool:
    """Validate the HMAC signature sent by Shopify."""
    if not hmac_header:
        return False
    secret = os.environ.get("SHOPIFY_WEBHOOK_SECRET", "")
    digest = hmac.new(secret.encode(), data, hashlib.sha256).digest()
    computed_hmac = base64.b64encode(digest).decode()
    return hmac.compare_digest(computed_hmac, hmac_header)

# -----------------------------------------------------------------------------
# Product Webhooks
# -----------------------------------------------------------------------------

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

    imported = 0
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
            "inventory_policy": variant.get("inventory_policy", ""),
            "status": payload.get("status", ""),
            "user_id": user_id,
        }
        try:
            upsert_variant(record)
            imported += 1
        except Exception as ex:
            logging.error("❌ Exception in upsert_variant for SKU %s: %s", record.get("sku"), ex, exc_info=True)
    logging.info("✅ Prodotto aggiornato: %s (%s)", product_title, product_id)
    return jsonify({"status": "success", "imported": imported}), 200

@webhook.route("/webhook/product-delete", methods=["POST"])
def handle_product_delete():
    raw_body = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")
    if not verify_webhook(raw_body, hmac_header):
        abort(401, "Invalid HMAC")

    payload = json.loads(raw_body)
    shopify_product_id = normalize_gid(payload.get("id"))

    response = safe_execute(
        supabase.table("products")
        .delete()
        .eq("shopify_product_id", shopify_product_id)
    )

    logging.info("🗑️ Prodotto eliminato: %s — %s", shopify_product_id, response)
    return jsonify({"status": "deleted", "shopify_product_id": shopify_product_id}), 200

# -----------------------------------------------------------------------------
# Order Webhooks
# -----------------------------------------------------------------------------

COD_KEYWORDS = [
    "contrassegno",
    "pagamento alla consegna",
    "cash on delivery",
    "commissione pagamento",
]

def _payment_label(financial_status: str, has_cod_fee: bool) -> str:
    status = (financial_status or "").upper()
    if status == "PAID":
        return "pagato"
    if status == "PENDING" and has_cod_fee:
        return "contrassegno"
    return ""  # invalid / ignored

@webhook.route("/webhook/order-create", methods=["POST"])
def handle_order_create():
    raw_body = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")
    if not verify_webhook(raw_body, hmac_header):
        abort(401, "Invalid HMAC")

    payload = json.loads(raw_body)

    shopify_order_id = int(normalize_gid(payload["id"]))
    financial_status = payload.get("financial_status", "")
    fulfillment_status = payload.get("fulfillment_status")
    total_price = float(payload.get("total_price", 0))

    line_items = payload.get("line_items", [])
    shipping_lines = payload.get("shipping_lines", [])

    has_cod_fee = any(
        any(kw in (item.get("title") or "").lower() for kw in COD_KEYWORDS)
        for item in line_items
    ) or any(
        (line.get("title") or "").lower() == "spedizione non richiesta" for line in shipping_lines
    )

    payment_status = _payment_label(financial_status, has_cod_fee)
    if not payment_status:
        logging.warning("⚠️ Ordine skippato: non valido per l'import.")
        return jsonify({"status": "skipped", "reason": "not paid or not COD"}), 200

    if fulfillment_status not in [None, "unfulfilled"]:
        logging.warning("⚠️ Ordine skippato: già evaso.")
        return jsonify({"status": "skipped", "reason": "already fulfilled"}), 200

    exists = safe_execute(
        supabase.table("orders")
        .select("id")
        .eq("shopify_order_id", shopify_order_id)
    )
    if exists.data:
        logging.info("⛔ Ordine già presente: %s", shopify_order_id)
        return jsonify({"status": "skipped", "reason": "already imported"}), 200

    user_id = os.environ.get("DEFAULT_USER_ID", None)

    customer = payload.get("customer") or {}
    shipping = payload.get("shipping_address") or {}

    customer_name = f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip() or "Ospite"
    customer_email = customer.get("email")
    customer_phone = shipping.get("phone") or customer.get("phone") or None

    shipping_address = shipping.get("address1")
    shipping_city = shipping.get("city")
    shipping_zip = shipping.get("zip")
    shipping_province = shipping.get("province")
    shipping_country = shipping.get("country")

    order_resp = safe_execute(
        supabase.table("orders")
        .insert(
            {
                "shopify_order_id": shopify_order_id,
                "number": payload.get("name"),
                "customer_name": customer_name,
                "customer_email": customer_email,
                "customer_phone": customer_phone,
                "shipping_address": shipping_address,
                "shipping_city": shipping_city,
                "shipping_zip": shipping_zip,
                "shipping_province": shipping_province,
                "shipping_country": shipping_country,
                "channel": (payload.get("app") or {}).get("name", "Online Store"),
                "created_at": payload.get("created_at"),
                "payment_status": payment_status,
                "fulfillment_status": "inevaso",
                "total": total_price,
                "user_id": user_id,
            }
        )
    )

    order_id = order_resp.data[0]["id"]

    for item in line_items:
        quantity = item.get("quantity", 1)
        if quantity == 0:
            continue

        variant_id_raw = item.get("variant_id")
        sku = (item.get("sku") or item.get("title") or "SENZA SKU").strip().upper()
        shopify_variant_id = normalize_gid(variant_id_raw) if variant_id_raw else f"no-variant-{sku}"
        price = float(item.get("price", 0))
        product_id = None

        if variant_id_raw:
            product = safe_execute(
                supabase.table("products")
                .select("id")
                .eq("shopify_variant_id", shopify_variant_id)
            )
            if product.data:
                product_id = product.data[0]["id"]

        safe_execute(
            supabase.table("order_items").insert(
                {
                    "order_id": order_id,
                    "shopify_variant_id": shopify_variant_id,
                    "product_id": product_id,
                    "sku": sku,
                    "quantity": quantity,
                    "price": price,
                }
            )
        )

        if product_id:
            safe_execute(
                supabase.rpc(
                    "adjust_inventory_after_fulfillment",
                    {"pid": product_id, "delta": quantity},
                )
            )

    safe_execute(supabase.rpc("repair_riservato_by_order", {"ordine_id": order_id}))

    logging.info("🛒 Nuovo ordine importato: %s", shopify_order_id)
    return jsonify({"status": "order created", "order_id": order_id}), 200

@webhook.route("/webhook/order-update", methods=["POST"])
def handle_order_update():
    raw_body = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")
    if not verify_webhook(raw_body, hmac_header):
        abort(401, "Invalid HMAC")

    payload = json.loads(raw_body)
    raw_id = payload.get("id")
    if not raw_id:
        logging.error("❌ Webhook ricevuto senza ID ordine valido.")
        return jsonify({"status": "skipped", "reason": "missing ID"}), 200

    shopify_order_id = int(normalize_gid(raw_id))
    order_resp = safe_execute(
        supabase.table("orders")
        .select("id")
        .eq("shopify_order_id", shopify_order_id)
        .limit(1)
    )

    if not order_resp.data:
        logging.info("🔁 Ordine %s non trovato → fallback a create.", shopify_order_id)
        return handle_order_create()

    order_id = order_resp.data[0]["id"]

    customer = payload.get("customer") or {}
    shipping = payload.get("shipping_address") or {}

    customer_name = f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip() or "Ospite"
    customer_email = customer.get("email")
    customer_phone = shipping.get("phone") or customer.get("phone") or None

    shipping_address = shipping.get("address1")
    shipping_city = shipping.get("city")
    shipping_zip = shipping.get("zip")
    shipping_province = shipping.get("province")
    shipping_country = shipping.get("country")

    # Reset & re‑insert items
    safe_execute(
        supabase.table("order_items").delete().eq("order_id", order_id)
    )

    total_price = float(payload.get("total_price", 0))

    for item in payload.get("line_items", []):
        quantity = item.get("quantity", 1)
        if quantity == 0:
            continue

        variant_id_raw = item.get("variant_id")
        sku = (item.get("sku") or item.get("title") or "SENZA SKU").strip().upper()
        shopify_variant_id = normalize_gid(variant_id_raw) if variant_id_raw else f"no-variant-{sku}"
        price = float(item.get("price", 0))
        product_id = None

        if variant_id_raw:
            product = safe_execute(
                supabase.table("products")
                .select("id")
                .eq("shopify_variant_id", shopify_variant_id)
            )
            if product.data:
                product_id = product.data[0]["id"]

        safe_execute(
            supabase.table("order_items").insert(
                {
                    "order_id": order_id,
                    "shopify_variant_id": shopify_variant_id,
                    "product_id": product_id,
                    "sku": sku,
                    "quantity": quantity,
                    "price": price,
                }
            )
        )

        if product_id:
            safe_execute(
                supabase.rpc(
                    "adjust_inventory_after_fulfillment",
                    {"pid": product_id, "delta": quantity},
                )
            )

    # Fulfillment state → if fully fulfilled, mark as so
    if payload.get("fulfillment_status") == "fulfilled":
        safe_execute(supabase.rpc("evadi_ordine", {"ordine_id": order_id}))
        logging.info("✅ Ordine %s evaso via webhook", shopify_order_id)

    # Riservato & aggiornamento ordine
    safe_execute(supabase.rpc("repair_riservato_by_order", {"ordine_id": order_id}))

    safe_execute(
        supabase.table("orders").update({
            "customer_name": customer_name,
            "customer_email": customer_email,
            "customer_phone": customer_phone,
            "shipping_address": shipping_address,
            "shipping_city": shipping_city,
            "shipping_zip": shipping_zip,
            "shipping_province": shipping_province,
            "shipping_country": shipping_country,
            "total": total_price,
        }).eq("id", order_id)
    )

    logging.info("🔁 Ordine aggiornato correttamente: %s", shopify_order_id)
    return jsonify({"status": "updated", "order_id": order_id}), 200

@webhook.route("/webhook/order-cancel", methods=["POST"])
def handle_order_cancel():
    raw_body = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")
    if not verify_webhook(raw_body, hmac_header):
        abort(401, "Invalid HMAC")

    payload = json.loads(raw_body)
    shopify_order_id = int(normalize_gid(payload.get("id")))

    try:
        order_resp = safe_execute(
            supabase.table("orders")
            .select("id, fulfillment_status")
            .eq("shopify_order_id", shopify_order_id)
            .limit(1)
        )

        if not order_resp.data:
            logging.warning("🛑 Ordine %s non trovato → impossibile annullarlo.", shopify_order_id)
            return jsonify({"status": "skipped", "reason": "ordine non trovato"}), 200

        order = order_resp.data[0]
        order_id = order["id"]
        current_status = order["fulfillment_status"]

        if current_status == "annullato":
            logging.warning("⚠️ Ordine %s già annullato.", shopify_order_id)
            return jsonify({"status": "skipped", "reason": "già annullato"}), 200

        safe_execute(
            supabase.table("orders").update({"fulfillment_status": "annullato"}).eq("id", order_id)
        )

        logging.info("🗑️ Ordine annullato: %s", shopify_order_id)
        return jsonify({"status": "cancelled", "order_id": order_id}), 200

    except Exception as exc:
        logging.error("❌ Errore durante annullamento ordine %s: %s", shopify_order_id, exc, exc_info=True)
        return jsonify({"status": "error", "reason": str(exc)}), 200  # 200 per Shopify

