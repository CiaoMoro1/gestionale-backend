from flask import Blueprint, request, jsonify
import os
import httpx
import certifi
from datetime import datetime, timedelta

from app.routes.bulk_sync import normalize_gid
from app.supabase_client import supabase
from app.utils.auth import require_auth

shopify = Blueprint("shopify", __name__)

SHOPIFY_GRAPHQL_URL = os.environ.get("SHOPIFY_GRAPHQL_URL")
SHOPIFY_ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN")

HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type": "application/json"
}

@shopify.route("/shopify/manual-sync-orders", methods=["POST"])
@require_auth
def manual_sync_orders(user_id):
    imported = 0
    skipped = 0
    errors = []
    cursor = None
    has_next_page = True

    from_date = (datetime.utcnow() - timedelta(days=3)).strftime("%Y-%m-%d")

    while has_next_page:
        after_clause = f', after: "{cursor}"' if cursor else ""
        query = f"""
        {{
          orders(first: 50{after_clause}, query: "(created_at:>='{from_date}') AND (financial_status:paid OR financial_status:pending) AND fulfillment_status:unfulfilled") {{
            pageInfo {{
              hasNextPage
              endCursor
            }}
            edges {{
              node {{
                id
                name
                createdAt
                fulfillmentStatus
                totalPriceSet {{ shopMoney {{ amount }} }}
                customer {{ displayName }}
                app {{ name }}
                lineItems(first: 50) {{
                  edges {{
                    node {{
                      sku
                      quantity
                      title
                      variant {{ id }}
                    }}
                  }}
                }}
              }}
            }}
          }}
        }}
        """

        try:
            with httpx.Client(verify=certifi.where()) as client:
                response = client.post(
                    SHOPIFY_GRAPHQL_URL,
                    headers=HEADERS,
                    json={"query": query},
                    timeout=10.0,
                )
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"❌ Shopify API error: {e}")
            return jsonify({"error": "Errore nella chiamata a Shopify"}), 500

        if "errors" in data:
            print("❌ GraphQL error:", data["errors"])
            return jsonify({"error": "Errore GraphQL", "details": data["errors"]}), 500

        orders_data = data["data"]["orders"]
        has_next_page = orders_data["pageInfo"]["hasNextPage"]
        cursor = orders_data["pageInfo"]["endCursor"]

        for edge in orders_data["edges"]:
            order = edge["node"]
            shopify_order_id = int(normalize_gid(order["id"]))

            exists = supabase.table("orders").select("id").eq("shopify_order_id", shopify_order_id).execute()
            if exists.data:
                if order.get("fulfillmentStatus") == "FULFILLED":
                    order_id = exists.data[0]["id"]
                    supabase.rpc("evadi_ordine", { "ordine_id": order_id }).execute()
                    print(f"✅ Ordine {shopify_order_id} evaso via sync manuale.")
                else:
                    skipped += 1
                continue

            # Campi sicuri per customer e app
            customer_name = (order.get("customer") or {}).get("displayName", "Ospite")
            channel = (order.get("app") or {}).get("name", "Online Store")

            order_resp = supabase.table("orders").insert({
                "shopify_order_id": shopify_order_id,
                "number": order["name"],
                "customer_name": customer_name,
                "channel": channel,
                "created_at": order["createdAt"],
                "payment_status": "pagato",
                "fulfillment_status": "inevaso",
                "total": float(order["totalPriceSet"]["shopMoney"]["amount"]),
                "user_id": user_id
            }).execute()

            order_id = order_resp.data[0]["id"]

            for item_edge in order["lineItems"]["edges"]:
                item = item_edge["node"]
                variant_id = item.get("variant", {}).get("id")
                shopify_variant_id = normalize_gid(variant_id) if variant_id else None
                quantity = item.get("quantity", 1)
                sku = item.get("sku") or item.get("title") or "Senza SKU"
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
                    "quantity": quantity
                }).execute()

                if product_id:
                    supabase.rpc("adjust_inventory_after_fulfillment", {
                        "pid": product_id,
                        "delta": -quantity * -1
                    }).execute()

            imported += 1

    return jsonify({
        "status": "ok",
        "imported": imported,
        "skipped": skipped,
        "errors": errors
    }), 200
