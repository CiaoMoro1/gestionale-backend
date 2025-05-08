from flask import Blueprint, jsonify, request
import os
import requests
import json
import certifi
from app.supabase_client import supabase
from app.utils.auth import require_auth
from app.services.supabase_write import upsert_variant

bulk_sync = Blueprint("bulk_sync", __name__)

SHOPIFY_GRAPHQL_URL = os.environ.get("SHOPIFY_GRAPHQL_URL")
SHOPIFY_ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN")

# 🔧 Funzione per normalizzare GID Shopify
def normalize_gid(gid) -> str:
    gid = str(gid)
    return gid.split("/")[-1] if "/" in gid else gid

# 🔹 1. Bulk query (inclusi status e cost)
BULK_QUERY = '''
mutation {
  bulkOperationRunQuery(
    query: """
    {
      productVariants(first: 250) {
        edges {
          node {
            id
            title
            sku
            barcode
            price
            inventoryPolicy
            inventoryItem {
              cost
            }
            product {
              id
              title
              status
              images(first: 1) {
                edges {
                  node {
                    originalSrc
                  }
                }
              }
            }
          }
        }
      }
    }
    """
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
'''

# 🔹 2. Avvia bulk
@bulk_sync.route("/shopify/bulk-launch", methods=["POST"])
def launch_bulk_sync():
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(
            SHOPIFY_GRAPHQL_URL,
            json={"query": BULK_QUERY},
            headers=headers,
            verify=False
        )

        print("✅ Shopify response status:", response.status_code)
        print("📦 Shopify response body:", response.text)
        return jsonify(response.json()), response.status_code

    except Exception as e:
        print("❌ [bulk-launch ERROR]:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

# 🔹 3. Stato della bulk operation
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

    try:
        response = requests.post(
            SHOPIFY_GRAPHQL_URL,
            json={"query": query},
            headers=headers,
            verify=False
        )
        return jsonify(response.json()), response.status_code

    except Exception as e:
        print("❌ [bulk-status ERROR]:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

# 🔹 4. Importa e salva varianti dal file JSONL
@bulk_sync.route("/shopify/bulk-fetch", methods=["POST"])
@require_auth
def fetch_bulk_data(user_id):
    url = request.json.get("url")
    if not url:
        return jsonify({"error": "Missing bulk file URL"}), 400

    print(f"👤 [USER] user_id: {user_id}")
    print(f"⬇️ [FETCH] Shopify bulk file: {url}")

    try:
        response = requests.get(url, verify=False)
        response.raise_for_status()
        lines = response.text.strip().split("\n")

        print(f"📄 [LINES] Total: {len(lines)}")

        count = 0
        errors = []

        image_map = {}
        variant_rows = []

        parsed = [json.loads(line) for line in lines]

        # Step 1: raccogli immagini
        for obj in parsed:
            if "originalSrc" in obj and "__parentId" in obj:
                parent_id = normalize_gid(obj["__parentId"])
                if parent_id not in image_map:
                    image_map[parent_id] = obj["originalSrc"]

        # Step 2: raccogli varianti valide
        for obj in parsed:
            if "id" not in obj or "product" not in obj:
                continue

            variant_id = normalize_gid(obj["id"])
            product = obj["product"]
            product_id = product.get("id")

            if not variant_id or not product_id:
                print("⚠️ Skip: riga incompleta", obj)
                continue

            variant_rows.append({
                "variant": obj,
                "variant_id": variant_id,
                "image_url": image_map.get(variant_id, "")
            })

        print(f"✅ Varianti da importare: {len(variant_rows)}")
        print(f"🖼️ Immagini associate: {len(image_map)}")

        # Step 3: upsert in Supabase
        for entry in variant_rows:
            try:
                variant = entry["variant"]
                product = variant["product"]
                record = {
                    "shopify_variant_id": normalize_gid(variant["id"]),
                    "shopify_product_id": normalize_gid(product["id"]),
                    "sku": variant.get("sku", ""),
                    "ean": variant.get("barcode", ""),
                    "variant_title": variant.get("title", ""),
                    "product_title": product.get("title", ""),
                    "image_url": entry["image_url"],
                    "price": float(variant.get("price") or 0),
                    "inventory_policy": variant.get("inventoryPolicy", ""),
                    "status": product.get("status", ""),
                    "cost": float(variant.get("inventoryItem", {}).get("cost") or 0),
                    "user_id": user_id
                }


                if upsert_variant(record):
                    count += 1
                else:
                    errors.append(f"❌ SKU: {record['sku']} - upsert fallito")

            except Exception as e:
                print(f"⚠️ [bulk-fetch ERROR]: {e}")
                errors.append(str(e))

        # Step 4: salva log errori su disco
        if errors:
            log_path = f"/tmp/shopify_import_log_{user_id}.txt"
            with open(log_path, "w", encoding="utf-8") as f:
                f.write("\n".join(errors))
            print(f"📝 Log salvato in: {log_path}")

        msg = f"✅ {count} varianti importate con successo."
        if errors:
            msg += f" ⚠️ {len(errors)} errori durante l'importazione."

        return jsonify({
            "status": "success",
            "message": msg,
            "imported_variants": count,
            "errors": errors
        }), 200

    except Exception as e:
        print("❌ [bulk-fetch ERROR]:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

# 🔹 5. Scarica log errori
@bulk_sync.route("/shopify/log", methods=["GET"])
@require_auth
def get_error_log(user_id):
    log_path = f"/tmp/shopify_import_log_{user_id}.txt"
    if not os.path.exists(log_path):
        return jsonify({"error": "Nessun log disponibile"}), 404

    with open(log_path, "r", encoding="utf-8") as f:
        content = f.read()

    return jsonify({"log": content})
