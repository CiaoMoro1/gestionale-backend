"""Manual sync of Shopify orders → Supabase.
Pulito secondo le nuove regole 2025:
- ❌ Nessuna tabella `movements`
- ❌ Nessuna logica `delta`
- ✅ Supporto item senza `variant_id`
- ✅ Aggiornamento/creazione ordini + articoli + fix `riservato`
"""

from flask import Blueprint, jsonify, request, abort
import json
import os
import httpx
import certifi
from app.supabase_client import supabase
from app.utils.auth import require_auth

orders = Blueprint("shopify", __name__)

SHOPIFY_GRAPHQL_URL = os.environ.get("SHOPIFY_GRAPHQL_URL")
SHOPIFY_ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN")

HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type": "application/json",
}

def normalize_gid(gid: str | int | None) -> str:
    if gid is None:
        return ""
    gid = str(gid)
    return gid.split("/")[-1] if "/" in gid else gid

@orders.route("/shopify/manual-sync-orders", methods=["POST"])
@require_auth
def import_orders(user_id):
    try:
        imported, updated, skipped = 0, 0, 0
        errors: list[str] = []
        cursor: str | None = None
        has_next_page: bool = True

        COD_KEYWORDS = [
            "contrassegno",
            "pagamento alla consegna",
            "cash on delivery",
            "commissione pagamento",
        ]

        while has_next_page:
            after_clause = f', after: "{cursor}"' if cursor else ""
            query = f"""
            {{
              orders(first: 50{after_clause}, query: "(created_at:>='2025-03-01') AND (financial_status:paid OR financial_status:pending) AND fulfillment_status:unfulfilled") {{
                pageInfo {{ hasNextPage endCursor }}
                edges {{
                  node {{
                    id
                    name
                    createdAt
                    displayFinancialStatus
                    fulfillments {{ status }}
                    totalPriceSet {{ shopMoney {{ amount }} }}
                    customer {{ displayName email phone }}
                    shippingAddress {{ address1 city zip province country }}
                    app {{ name }}
                    shippingLines(first: 5) {{ edges {{ node {{ title }} }} }}
                    lineItems(first: 50) {{
                      edges {{
                        node {{
                          title
                          sku
                          quantity
                          originalUnitPriceSet {{ shopMoney {{ amount currencyCode }} }}
                          variant {{ id }}
                        }}
                      }}
                    }}
                  }}
                }}
              }}
            }}
            """

            with httpx.Client(verify=False, timeout=10.0) as client:
                resp = client.post(SHOPIFY_GRAPHQL_URL, headers=HEADERS, json={"query": query})
            resp.raise_for_status()
            data = resp.json()

            if "errors" in data:
                raise Exception(f"GraphQL error: {data['errors']}")

            orders_data = data["data"]["orders"]
            has_next_page = orders_data["pageInfo"]["hasNextPage"]
            cursor = orders_data["pageInfo"]["endCursor"]

            for edge in orders_data["edges"]:
                order = edge["node"]
                shopify_order_id = int(normalize_gid(order["id"]))

                fulfillment_statuses = order.get("fulfillments", [])
                is_fulfilled = any(f.get("status") == "FULFILLED" for f in fulfillment_statuses)

                exists_resp = supabase.table("orders").select("id").eq("shopify_order_id", shopify_order_id).execute()
                order_exists = bool(exists_resp.data)
                order_id = exists_resp.data[0]["id"] if order_exists else None

                line_items = order["lineItems"]["edges"]
                shipping_lines = order.get("shippingLines", {}).get("edges", [])
                financial_status = order["displayFinancialStatus"]

                has_cod_fee = any(
                    any(kw in (i["node"].get("title") or "").lower() for kw in COD_KEYWORDS)
                    for i in line_items
                ) or any(
                    (l["node"].get("title") or "").lower() == "spedizione non richiesta" for l in shipping_lines
                )

                if financial_status == "PAID":
                    payment_status = "pagato"
                elif financial_status == "PENDING" and has_cod_fee:
                    payment_status = "contrassegno"
                else:
                    skipped += 1
                    continue

                customer = order.get("customer") or {}
                shipping = order.get("shippingAddress") or {}

                customer_name = customer.get("displayName") or "Ospite"
                customer_email = customer.get("email")
                customer_phone = customer.get("phone")

                shipping_address = shipping.get("address1")
                shipping_city = shipping.get("city")
                shipping_zip = shipping.get("zip")
                shipping_province = shipping.get("province")
                shipping_country = shipping.get("country")

                if order_exists:
                    if not line_items:
                        supabase.table("order_items").delete().eq("order_id", order_id).execute()
                        supabase.rpc("repair_riservato_by_order", {"ordine_id": order_id}).execute()
                        supabase.table("orders").update({"total": 0}).eq("id", order_id).execute()
                        updated += 1
                        continue

                    supabase.table("order_items").delete().eq("order_id", order_id).execute()

                    for item_edge in line_items:
                        item = item_edge["node"]
                        quantity = item.get("quantity", 1)
                        if quantity == 0:
                            continue

                        variant = item.get("variant")
                        variant_id_raw = variant.get("id") if variant else None
                        sku = (item.get("sku") or item.get("title") or "SENZA SKU").strip().upper()
                        shopify_variant_id = normalize_gid(variant_id_raw) if variant_id_raw else f"no-variant-{sku}"
                        price = float(item.get("originalUnitPriceSet", {}).get("shopMoney", {}).get("amount", 0))
                        product_id = None

                        if variant_id_raw:
                            prod_resp = supabase.table("products").select("id").eq("shopify_variant_id", shopify_variant_id).execute()
                            if prod_resp.data:
                                product_id = prod_resp.data[0]["id"]

                        supabase.table("order_items").insert({
                            "order_id": order_id,
                            "shopify_variant_id": shopify_variant_id,
                            "product_id": product_id,
                            "sku": sku,
                            "quantity": quantity,
                            "price": price,
                        }).execute()

                        if product_id:
                            supabase.rpc("adjust_inventory_after_fulfillment", {"pid": product_id, "delta": quantity}).execute()

                    if is_fulfilled:
                        supabase.rpc("evadi_ordine", {"ordine_id": order_id}).execute()

                    supabase.table("orders").update({
                        "customer_name": customer_name,
                        "customer_email": customer_email,
                        "customer_phone": customer_phone,
                        "shipping_address": shipping_address,
                        "shipping_city": shipping_city,
                        "shipping_zip": shipping_zip,
                        "shipping_province": shipping_province,
                        "shipping_country": shipping_country,
                        "payment_status": payment_status,
                        "fulfillment_status": "evaso" if is_fulfilled else "inevaso",
                        "total": float(order["totalPriceSet"]["shopMoney"]["amount"]),
                    }).eq("id", order_id).execute()

                    supabase.rpc("repair_riservato_by_order", {"ordine_id": order_id}).execute()
                    updated += 1
                    continue

                order_resp = supabase.table("orders").insert({
                    "shopify_order_id": shopify_order_id,
                    "number": order["name"],
                    "customer_name": customer_name,
                    "customer_email": customer_email,
                    "customer_phone": customer_phone,
                    "shipping_address": shipping_address,
                    "shipping_city": shipping_city,
                    "shipping_zip": shipping_zip,
                    "shipping_province": shipping_province,
                    "shipping_country": shipping_country,
                    "channel": (order.get("app") or {}).get("name", "Online Store"),
                    "created_at": order["createdAt"],
                    "payment_status": payment_status,
                    "fulfillment_status": "evaso" if is_fulfilled else "inevaso",
                    "total": float(order["totalPriceSet"]["shopMoney"]["amount"]),
                    "user_id": user_id,
                }).execute()

                order_id = order_resp.data[0]["id"]

                for item_edge in line_items:
                    item = item_edge["node"]
                    quantity = item.get("quantity", 1)
                    if quantity == 0:
                        continue

                    variant = item.get("variant")
                    variant_id_raw = variant.get("id") if variant else None
                    sku = (item.get("sku") or item.get("title") or "SENZA SKU").strip().upper()
                    shopify_variant_id = normalize_gid(variant_id_raw) if variant_id_raw else f"no-variant-{sku}"
                    price = float(item.get("originalUnitPriceSet", {}).get("shopMoney", {}).get("amount", 0))
                    product_id = None

                    if variant_id_raw:
                        prod_resp = supabase.table("products").select("id").eq("shopify_variant_id", shopify_variant_id).execute()
                        if prod_resp.data:
                            product_id = prod_resp.data[0]["id"]

                    supabase.table("order_items").insert({
                        "order_id": order_id,
                        "shopify_variant_id": shopify_variant_id,
                        "product_id": product_id,
                        "sku": sku,
                        "quantity": quantity,
                        "price": price,
                    }).execute()

                    if product_id:
                        supabase.rpc("adjust_inventory_after_fulfillment", {"pid": product_id, "delta": quantity}).execute()

                supabase.rpc("repair_riservato_by_order", {"ordine_id": order_id}).execute()
                imported += 1

        return jsonify({
            "status": "success",
            "imported": imported,
            "updated": updated,
            "skipped": skipped,
            "errors": errors,
        }), 200

    except Exception as exc:
        import traceback
        trace = traceback.format_exc()
        print("❌ ERRORE manual-sync-orders:", type(exc).__name__, str(exc))
        print(trace)
        return jsonify({
            "status": "error",
            "message": str(exc),
            "trace": trace,
        }), 500

# 🔁 Blueprint da registrare in run.py
shopify = orders