from flask import Blueprint, jsonify, request
import os
import requests
import json
from app.supabase_client import supabase
from app.utils.auth import require_auth  # ✅ usa autenticazione via token

bulk_sync = Blueprint("bulk_sync", __name__)

SHOPIFY_GRAPHQL_URL = os.environ.get("SHOPIFY_GRAPHQL_URL")
SHOPIFY_ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN")

# 🟩 Query per bulk operation
BULK_QUERY = """
mutation {
  bulkOperationRunQuery(
    query: \"\"\"
    {
      products {
        edges {
          node {
            id
            title
            variants {
              edges {
                node {
                  id
                  sku
                  inventoryQuantity
                }
              }
            }
          }
        }
      }
    }
    \"\"\"
  ) {
    bulkOperation {
      id
      status
    }
    userErrors {
      field
      message
    }
  }
}
"""

@bulk_sync.route("/shopify/bulk-launch", methods=["POST"])
def launch_bulk_sync():
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    res = requests.post(
        SHOPIFY_GRAPHQL_URL,
        json={"query": BULK_QUERY},
        headers=headers,
        verify=False  # solo sviluppo
    )

    return jsonify(res.json()), res.status_code


@bulk_sync.route("/shopify/bulk-status", methods=["GET"])
def get_bulk_status():
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    query = """
    {
      currentBulkOperation {
        id
        status
        errorCode
        createdAt
        completedAt
        objectCount
        fileSize
        url
      }
    }
    """

    res = requests.post(
        SHOPIFY_GRAPHQL_URL,
        json={"query": query},
        headers=headers,
        verify=False
    )

    return jsonify(res.json()), res.status_code


@bulk_sync.route("/shopify/bulk-fetch", methods=["POST"])
@require_auth
def fetch_bulk_data(user_id):
    url = request.json.get("url")
    if not url:
        return jsonify({"error": "Missing bulk file URL"}), 400

    try:
        print(f"⬇️ Scarico bulk file da:\n{url}")
        response = requests.get(url, verify=False)
        lines = response.text.strip().split("\n")
        count = 0

        for line in lines:
            node = json.loads(line)
            if not node or not node.get("id"):
                continue

            data = {
                "shopify_product_id": node.get("product", {}).get("id", ""),
                "shopify_variant_id": node["id"],
                "name": node.get("title", ""),
                "sku": node.get("sku") or "",
                "quantity": node.get("inventoryQuantity", 0),
                "user_id": user_id  # ✅ dinamico da JWT
            }

            print("📥 Inserisco variante:", data)
            supabase.table("products").upsert(data, on_conflict=["shopify_variant_id"]).execute()
            count += 1

        return jsonify({"status": "success", "imported_variants": count}), 200

    except Exception as e:
        print("❌ Errore fetch/import:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500
