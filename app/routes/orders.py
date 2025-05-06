from flask import Blueprint, jsonify, request
import os
import httpx
import certifi
from app.supabase_client import supabase
from app.utils.auth import require_auth

orders = Blueprint("orders", __name__)

SHOPIFY_GRAPHQL_URL = os.environ.get("SHOPIFY_GRAPHQL_URL")
SHOPIFY_ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN")

HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type": "application/json"
}


def normalize_gid(gid: str) -> str:
    return gid.split("/")[-1] if gid and "/" in gid else gid


@orders.route("/shopify/import-orders", methods=["POST"])
@require_auth
def import_orders(user_id):
    imported = 0
    skipped = 0
    errors = []
    cursor = None
    has_next_page = True

    while has_next_page:
        after_clause = f', after: "{cursor}"' if cursor else ""
        query = f"""
        {{
          orders(first: 50{after_clause}, query: "financial_status:paid AND fulfillment_status:unfulfilled") {{
            pageInfo {{
              hasNextPage
              endCursor
            }}
            edges {{
              node {{
                id
                name
                createdAt
                totalPriceSet {{ shopMoney {{ amount }} }}
                customer {{ displayName }}
                app {{ name }}
                lineItems(first: 50) {{
                  edges {{
                    node {{
                      sku
                      quantity
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
            with httpx.Client(verify=False) as client:
                response = client.post(
                    SHOPIFY_GRAPHQL_URL,
                    headers=HEADERS,
                    json={"query": query},
                    timeout=10.0,
                )
            response.raise_for_status()
            data = response.json()

            # 🔍 Se GraphQL ha errori
            if "errors" in data:
                print("❌ Shopify GraphQL error:", data["errors"])
                return jsonify({"error": "Errore GraphQL da Shopify", "details": data["errors"]}), 500

        except Exception as e:
            print(f"❌ Shopify API error: {e}")
            return jsonify({"error": "Errore nella chiamata a Shopify"}), 500

        orders_data = data["data"]["orders"]
        has_next_page = orders_data["pageInfo"]["hasNextPage"]
        cursor = orders_data["pageInfo"]["endCursor"]

        for edge in orders_data["edges"]:
            order = edge["node"]

            shopify_order_id = int(normalize_gid(order["id"]))
            exists = supabase.table("orders").select("id").eq("shopify_order_id", shopify_order_id).execute()
            if exists.data:
                skipped += 1
                continue

            order_resp = supabase.table("orders").insert({
                "shopify_order_id": shopify_order_id,
                "number": order["name"],
                "customer_name": order["customer"]["displayName"] if order["customer"] else "Ospite",
                "channel": order["app"]["name"] if order["app"] else "Online Store",
                "created_at": order["createdAt"],
                "payment_status": "pagato",
                "fulfillment_status": "inevaso",
                "total": float(order["totalPriceSet"]["shopMoney"]["amount"]),
                "user_id": user_id
            }).execute()

            order_id = order_resp.data[0]["id"]

            for item_edge in order["lineItems"]["edges"]:
                item = item_edge["node"]
                variant = item.get("variant")
                quantity = item.get("quantity", 1)
                sku = item.get("sku") or ""
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
                    "quantity": quantity
                }).execute()

                if product_id:
                    inv = supabase.table("inventory").select("riservato_sito").eq("product_id", product_id).single().execute()
                    current = inv.data.get("riservato_sito") or 0
                    supabase.table("inventory").update({
                        "riservato_sito": current + quantity
                    }).eq("product_id", product_id).execute()
                else:
                    print(f"⚠️ Riga con variante non trovata → ordine {order['name']}, SKU: {sku}, qty: {quantity}")

            imported += 1

    return jsonify({
        "status": "success",
        "imported": imported,
        "skipped": skipped,
        "errors": errors
    }), 200
