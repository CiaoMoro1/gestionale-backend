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
    imported = 0
    skipped = 0
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
            if "errors" in data:
                return jsonify({"error": "GraphQL error", "details": data["errors"]}), 500
        except Exception as e:
            return jsonify({"error": str(e)}), 500

        orders_data = data["data"]["orders"]
        has_next_page = orders_data["pageInfo"]["hasNextPage"]
        cursor = orders_data["pageInfo"]["endCursor"]

        for edge in orders_data["edges"]:
            order = edge["node"]
            shopify_order_id = int(normalize_gid(order["id"]))
            fulfillments = order.get("fulfillments", [])
            is_fulfilled = any(f.get("status") == "FULFILLED" for f in fulfillments)

            # Verifica se ordine esiste già
            exists = supabase.table("orders").select("id").eq("shopify_order_id", shopify_order_id).execute()
            if exists.data:
                if is_fulfilled:
                    order_id = exists.data[0]["id"]
                    supabase.rpc("evadi_ordine", { "ordine_id": order_id }).execute()
                    print(f"✅ Ordine {shopify_order_id} evaso")
                else:
                    skipped += 1
                continue

            line_items = order["lineItems"]["edges"]
            shipping_lines = order.get("shippingLines", {}).get("edges", [])
            financial_status = order["displayFinancialStatus"]

            # Contrassegno
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

            # Crea ordine
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

            # Aggiungi articoli
            for item_edge in line_items:
                item = item_edge["node"]
                variant = item.get("variant")
                quantity = item.get("quantity", 1)
                sku = item.get("sku") or item.get("title") or "Senza SKU"
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

            imported += 1

    return jsonify({
        "status": "success",
        "imported": imported,
        "skipped": skipped,
        "errors": errors
    }), 200

shopify = orders
