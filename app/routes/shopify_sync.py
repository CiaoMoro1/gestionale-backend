from flask import Blueprint, jsonify, request
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
    "Content-Type": "application/json"
}

def normalize_gid(gid) -> str:
    gid = str(gid)
    return gid.split("/")[-1] if "/" in gid else gid

@orders.route("/shopify/manual-sync-orders", methods=["POST"])
@require_auth
def import_orders(user_id):
    try:
        imported = 0
        skipped = 0
        updated = 0
        errors = []
        cursor = None
        has_next_page = True

        COD_KEYWORDS = [
            "contrassegno",
            "pagamento alla consegna",
            "cash on delivery",
            "commissione pagamento"
        ]

        while has_next_page:
            after_clause = f', after: "{cursor}"' if cursor else ""
            query = f"""
            {{
            orders(first: 50{after_clause}, query: "(created_at:>='2025-03-01') AND (financial_status:paid OR financial_status:pending) AND fulfillment_status:unfulfilled") {{
                pageInfo {{
                hasNextPage
                endCursor
                }}
                edges {{
                node {{
                    id
                    name
                    createdAt
                    displayFinancialStatus
                    fulfillments {{ status }}
                    totalPriceSet {{ shopMoney {{ amount }} }}
                    customer {{ displayName }}
                    app {{ name }}
                    shippingLines(first: 5) {{
                    edges {{ node {{ title }} }}
                    }}
                    lineItems(first: 50) {{
                    edges {{
                        node {{
                        title
                        sku
                        quantity
                        originalUnitPriceSet {{
                            shopMoney {{
                            amount
                            currencyCode
                            }}
                        }}
                        variant {{ id }}
                        }}
                    }}
                    }}
                }}
                }}
            }}
            }}
            """


            with httpx.Client(verify=False) as client:
                response = client.post(
                    SHOPIFY_GRAPHQL_URL,
                    headers=HEADERS,
                    json={"query": query},
                    timeout=10.0,
                )
            response.raise_for_status()
            data = response.json()
            if "errors" in data:
                raise Exception(f"GraphQL error: {data['errors']}")

            orders_data = data["data"]["orders"]
            has_next_page = orders_data["pageInfo"]["hasNextPage"]
            cursor = orders_data["pageInfo"]["endCursor"]

            for edge in orders_data["edges"]:
                order = edge["node"]
                shopify_order_id = int(normalize_gid(order["id"]))
                fulfillments = order.get("fulfillments", [])
                is_fulfilled = any(f.get("status") == "FULFILLED" for f in fulfillments)

                exists = supabase.table("orders").select("id").eq("shopify_order_id", shopify_order_id).execute()

                line_items = order["lineItems"]["edges"]
                shipping_lines = order.get("shippingLines", {}).get("edges", [])
                financial_status = order["displayFinancialStatus"]

                has_cod_fee = any(
                    any(kw in (item["node"]["title"] or "").lower() for kw in COD_KEYWORDS)
                    for item in line_items
                ) or any(
                    (line["node"]["title"] or "").lower() == "spedizione non richiesta"
                    for line in shipping_lines
                )

                if financial_status == "PAID":
                    payment_status = "pagato"
                elif financial_status == "PENDING" and has_cod_fee:
                    payment_status = "contrassegno"
                else:
                    skipped += 1
                    continue

                if exists.data:
                    order_id = exists.data[0]["id"]
                    supabase.table("order_items").delete().eq("order_id", order_id).execute()
                    totale = 0

                    for item_edge in line_items:
                        item = item_edge["node"]
                        variant = item.get("variant")
                        quantity = item.get("quantity", 1)
                        price = float(
                            item.get("originalUnitPriceSet", {})
                                .get("shopMoney", {})
                                .get("amount", 0)
                        )
                        sku = (item.get("sku") or item.get("title") or "SENZA SKU").strip().upper()
                        shopify_variant_id = normalize_gid(variant["id"]) if variant else None
                        product_id = None

                        if shopify_variant_id:
                            product = supabase.table("products").select("id").eq("shopify_variant_id", shopify_variant_id).execute()
                            if product.data:
                                product_id = product.data[0]["id"]

                        supabase.table("order_items").insert({
                            "order_id": order_id,
                            "shopify_variant_id": shopify_variant_id,
                            "product_id": product_id,
                            "sku": sku,
                            "quantity": quantity,
                            "price": price
                        }).execute()

                        if product_id:
                            supabase.rpc("adjust_inventory_after_fulfillment", {
                                "pid": product_id,
                                "delta": quantity
                            }).execute()

                        totale += quantity * price

                    if is_fulfilled:
                        supabase.rpc("evadi_ordine", {"ordine_id": order_id}).execute()
                        print(f"✅ Ordine aggiornato e evaso: {shopify_order_id}")

                    supabase.rpc("repair_riservato_by_order", {"ordine_id": order_id}).execute()
                    supabase.table("orders").update({"total": totale}).eq("id", order_id).execute()

                    updated += 1
                    print(f"🔁 Ordine aggiornato: {shopify_order_id}")
                    continue

                # ✨ Ordine nuovo
                order_resp = supabase.table("orders").insert({
                    "shopify_order_id": shopify_order_id,
                    "number": order["name"],
                    "customer_name": (order.get("customer") or {}).get("displayName", "Ospite"),
                    "channel": (order.get("app") or {}).get("name", "Online Store"),
                    "created_at": order["createdAt"],
                    "payment_status": payment_status,
                    "fulfillment_status": "inevaso",
                    "total": float(order["totalPriceSet"]["shopMoney"]["amount"]),
                    "user_id": user_id
                }).execute()

                order_id = order_resp.data[0]["id"]

                for item_edge in line_items:
                    item = item_edge["node"]
                    variant = item.get("variant")
                    quantity = item.get("quantity", 1)
                    if quantity == 0:
                        continue
                    price = float(item.get("price", 0)) if "price" in item else 0
                    sku = (item.get("sku") or item.get("title") or "SENZA SKU").strip().upper()
                    shopify_variant_id = normalize_gid(variant["id"]) if variant else None
                    product_id = None

                    if shopify_variant_id:
                        product = supabase.table("products").select("id").eq("shopify_variant_id", shopify_variant_id).execute()
                        if product.data:
                            product_id = product.data[0]["id"]

                    supabase.table("order_items").insert({
                        "order_id": order_id,
                        "shopify_variant_id": shopify_variant_id,
                        "product_id": product_id,
                        "sku": sku,
                        "quantity": quantity,
                        "price": price
                    }).execute()

                supabase.rpc("repair_riservato_by_order", {"ordine_id": order_id}).execute()

                imported += 1
                print(f"🆕 Ordine importato: {shopify_order_id}")

        return jsonify({
            "status": "success",
            "imported": imported,
            "updated": updated,
            "skipped": skipped,
            "errors": errors
        }), 200

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print("❌ ERRORE nel sync manuale:")
        print("📄 Tipo:", type(e).__name__)
        print("💬 Messaggio:", str(e))
        print("🧵 Traceback:\n", error_trace)

        return jsonify({
            "status": "error",
            "message": f"Errore interno: {type(e).__name__}",
            "details": str(e),
            "trace": error_trace
        }), 500

# 🔁 Blueprint da registrare in run.py
shopify = orders
