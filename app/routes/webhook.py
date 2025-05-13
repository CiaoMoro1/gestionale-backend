from flask import Blueprint, request, jsonify, abort
import os
import json
import hmac
import base64
import hashlib
from app.supabase_client import supabase
from app.services.supabase_write import upsert_variant
from app.routes.bulk_sync import normalize_gid

# -----------------------------------------------------------------------------
# Blueprint init
# -----------------------------------------------------------------------------

webhook = Blueprint("webhook", __name__)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

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
    """Create / Update product variants inside Supabase."""
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
            "inventory_policy": variant.get("inventory_policy", ""),
            "status": payload.get("status", ""),
            "user_id": user_id,
        }
        upsert_variant(record)

    print(f"✅ Prodotto aggiornato: {product_title} ({product_id})")
    return jsonify({"status": "success", "imported": len(variants)}), 200


@webhook.route("/webhook/product-delete", methods=["POST"])
def handle_product_delete():
    """Delete product + its variants from Supabase when removed from Shopify."""
    raw_body = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")
    if not verify_webhook(raw_body, hmac_header):
        abort(401, "Invalid HMAC")

    payload = json.loads(raw_body)
    shopify_product_id = normalize_gid(payload.get("id"))

    response = (
        supabase.table("products")
        .delete()
        .eq("shopify_product_id", shopify_product_id)
        .execute()
    )

    print(f"🗑️ Prodotto eliminato: {shopify_product_id} — {response}")
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
    status = financial_status.upper()
    if status == "PAID":
        return "pagato"
    if status == "PENDING" and has_cod_fee:
        return "contrassegno"
    return ""  # invalid / ignored


@webhook.route("/webhook/order-create", methods=["POST"])
def handle_order_create():
    """Insert a brand‑new order with its items, adjusting inventory."""
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
        print("⚠️ Ordine skippato: non valido per l'import.")
        return jsonify({"status": "skipped", "reason": "not paid or not COD"}), 200

    if fulfillment_status not in [None, "unfulfilled"]:
        print("⚠️ Ordine skippato: già evaso.")
        return jsonify({"status": "skipped", "reason": "already fulfilled"}), 200

    exists = (
        supabase.table("orders")
        .select("id")
        .eq("shopify_order_id", shopify_order_id)
        .execute()
    )
    if exists.data:
        print(f"⛔ Ordine già presente: {shopify_order_id}")
        return jsonify({"status": "skipped", "reason": "already imported"}), 200

    user_id = os.environ.get("DEFAULT_USER_ID", None)

    customer = payload.get("customer") or {}
    customer_name = f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip() or "Ospite"
    customer_email = customer.get("email")
    customer_phone = customer.get("phone")

    shipping = payload.get("shipping_address") or {}
    shipping_address = shipping.get("address1")
    shipping_city = shipping.get("city")
    shipping_zip = shipping.get("zip")
    shipping_province = shipping.get("province")
    shipping_country = shipping.get("country")

    order_resp = (
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
        .execute()
    )

    order_id = order_resp.data[0]["id"]

    for item in line_items:
        quantity = item.get("quantity", 1)
        if quantity == 0:
            print(f"⚠️ Skip articolo '{item.get('title')}' con quantità 0")
            continue

        variant_id_raw = item.get("variant_id")
        sku = (item.get("sku") or item.get("title") or "SENZA SKU").strip().upper()
        shopify_variant_id = normalize_gid(variant_id_raw) if variant_id_raw else f"no-variant-{sku}"
        price = float(item.get("price", 0))
        product_id = None

        if variant_id_raw:
            product = (
                supabase.table("products")
                .select("id")
                .eq("shopify_variant_id", shopify_variant_id)
                .execute()
            )
            if product.data:
                product_id = product.data[0]["id"]

        supabase.table("order_items").insert(
            {
                "order_id": order_id,
                "shopify_variant_id": shopify_variant_id,
                "product_id": product_id,
                "sku": sku,
                "quantity": quantity,
                "price": price,
            }
        ).execute()

        if product_id:
            supabase.rpc(
                "adjust_inventory_after_fulfillment",
                {"pid": product_id, "delta": quantity},
            ).execute()

    supabase.rpc("repair_riservato_by_order", {"ordine_id": order_id}).execute()

    print(f"🛒 Nuovo ordine importato: {shopify_order_id}")
    return jsonify({"status": "order created", "order_id": order_id}), 200



@webhook.route("/webhook/order-update", methods=["POST"])
def handle_order_update():
    """Overwrite an existing order with Shopify's latest state."""
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
    order_resp = (
        supabase.table("orders")
        .select("id")
        .eq("shopify_order_id", shopify_order_id)
        .limit(1)
        .execute()
    )

    if not order_resp.data:
        print(f"🔁 Ordine {shopify_order_id} non trovato → fallback a create.")
        return handle_order_create()

    order_id = order_resp.data[0]["id"]

    # Customer & shipping fields
    customer = payload.get("customer") or {}
    customer_name = f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip() or "Ospite"
    customer_email = customer.get("email")
    customer_phone = customer.get("phone")

    shipping = payload.get("shipping_address") or {}
    shipping_address = shipping.get("address1")
    shipping_city = shipping.get("city")
    shipping_zip = shipping.get("zip")
    shipping_province = shipping.get("province")
    shipping_country = shipping.get("country")

    # ------------------------------------------------------------------
    # Reset & re‑insert items
    # ------------------------------------------------------------------

    supabase.table("order_items").delete().eq("order_id", order_id).execute()

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
            product = (
                supabase.table("products")
                .select("id")
                .eq("shopify_variant_id", shopify_variant_id)
                .execute()
            )
            if product.data:
                product_id = product.data[0]["id"]

        supabase.table("order_items").insert(
            {
                "order_id": order_id,
                "shopify_variant_id": shopify_variant_id,
                "product_id": product_id,
                "sku": sku,
                "quantity": quantity,
                "price": price,
            }
        ).execute()

        if product_id:
            supabase.rpc(
                "adjust_inventory_after_fulfillment",
                {"pid": product_id, "delta": quantity},
            ).execute()

    # Fulfillment state → if fully fulfilled, mark as so
    if payload.get("fulfillment_status") == "fulfilled":
        supabase.rpc("evadi_ordine", {"ordine_id": order_id}).execute()
        print(f"✅ Ordine {shopify_order_id} evaso via webhook")

    # Riservato & aggiornamento ordine
    supabase.rpc("repair_riservato_by_order", {"ordine_id": order_id}).execute()

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
    }).eq("id", order_id).execute()

    print(f"🔁 Ordine aggiornato correttamente: {shopify_order_id}")
    return jsonify({"status": "updated", "order_id": order_id}), 200



@webhook.route("/webhook/order-cancel", methods=["POST"])
def handle_order_cancel():
    """Mark order as cancelled (no inventory roll‑back, handled by DB triggers)."""
    raw_body = request.get_data()
    hmac_header = request.headers.get("X-Shopify-Hmac-Sha256")
    if not verify_webhook(raw_body, hmac_header):
        abort(401, "Invalid HMAC")

    payload = json.loads(raw_body)
    shopify_order_id = int(normalize_gid(payload.get("id")))

    try:
        order_resp = (
            supabase.table("orders")
            .select("id, fulfillment_status")
            .eq("shopify_order_id", shopify_order_id)
            .limit(1)
            .execute()
        )

        if not order_resp.data:
            print(f"🛑 Ordine {shopify_order_id} non trovato → impossibile annullarlo.")
            return jsonify({"status": "skipped", "reason": "ordine non trovato"}), 200

        order = order_resp.data[0]
        order_id = order["id"]
        current_status = order["fulfillment_status"]

        if current_status == "annullato":
            print(f"⚠️ Ordine {shopify_order_id} già annullato.")
            return jsonify({"status": "skipped", "reason": "già annullato"}), 200

        supabase.table("orders").update({"fulfillment_status": "annullato"}).eq("id", order_id).execute()

        print(f"🗑️ Ordine annullato: {shopify_order_id}")
        return jsonify({"status": "cancelled", "order_id": order_id}), 200

    except Exception as exc:
        print(f"❌ Errore durante annullamento ordine {shopify_order_id}: {exc}")
        return jsonify({"status": "error", "reason": str(exc)}), 500
