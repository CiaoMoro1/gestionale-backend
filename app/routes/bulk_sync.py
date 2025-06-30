"""
Modulo bulk_sync.py
Blueprint Flask per la gestione della sincronizzazione massiva (Bulk Sync) di varianti prodotto da Shopify.
Include logging strutturato, validazione input, retry per operazioni critiche e commenti estesi.
"""

from flask import Blueprint, jsonify, request, g
import os
import requests
import json
import certifi
import logging
from app.supabase_client import supabase
from app.utils.auth import require_auth
from app.services.supabase_write import upsert_variant
from marshmallow import Schema, fields, ValidationError
from tenacity import retry, stop_after_attempt, wait_fixed

bulk_sync = Blueprint("bulk_sync", __name__)

# === Configurazione Shopify ===
SHOPIFY_GRAPHQL_URL = os.environ.get("SHOPIFY_GRAPHQL_URL")
SHOPIFY_ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN")

# === Schema di validazione input per il fetch bulk ===
class BulkFetchSchema(Schema):
    url = fields.String(required=True)

# === Funzione per POST verso Shopify con headers corretti ===
def _shopify_post(query):
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    return requests.post(
        SHOPIFY_GRAPHQL_URL,
        json={"query": query},
        headers=headers,
        verify=certifi.where()
    )

# === Normalizzazione GID Shopify ===
def normalize_gid(gid) -> str:
    gid = str(gid)
    return gid.split("/")[-1] if "/" in gid else gid

# === Bulk query GraphQL Shopify ===
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

# === 1. Avvia la bulk operation su Shopify ===
@bulk_sync.route("/shopify/bulk-launch", methods=["POST"])
@require_auth
def launch_bulk_sync():
    """Avvia la bulk operation su Shopify per l'utente autenticato."""
    user_id = g.user_id
    try:
        response = _shopify_post(BULK_QUERY)
        logging.info("Shopify bulk-launch da user %s, status: %s", user_id, response.status_code)
        return jsonify(response.json()), response.status_code
    except Exception as e:
        logging.error("[bulk-launch ERROR]: %s", str(e), exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

# === 2. Stato della bulk operation ===
@bulk_sync.route("/shopify/bulk-status", methods=["GET"])
@require_auth
def get_bulk_status():
    """Restituisce lo stato corrente della bulk operation Shopify."""
    user_id = g.user_id
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
        response = _shopify_post(query)
        logging.info("Shopify bulk-status da user %s, status: %s", user_id, response.status_code)
        return jsonify(response.json()), response.status_code
    except Exception as e:
        logging.error("[bulk-status ERROR]: %s", str(e), exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

# === 3. Importa e salva varianti dal file JSONL ===
@bulk_sync.route("/shopify/bulk-fetch", methods=["POST"])
@require_auth
def fetch_bulk_data():
    """
    Importa e salva le varianti dal file JSONL generato da Shopify Bulk.
    Esegue validazione, logging e retry per ogni upsert.
    """
    user_id = g.user_id
    # Validazione input
    try:
        data = request.get_json(force=True)
        BulkFetchSchema().load(data)
    except ValidationError as err:
        logging.warning("Validation error on /shopify/bulk-fetch: %s", err.messages)
        return jsonify({"error": "Invalid input", "details": err.messages}), 400

    url = data.get("url")
    logging.info("[USER] user_id: %s", user_id)
    logging.info("[FETCH] Shopify bulk file: %s", url)

    try:
        response = requests.get(url, verify=certifi.where(), timeout=30)
        response.raise_for_status()
        lines = response.text.strip().split("\n")
        logging.info("[LINES] Total: %s", len(lines))

        count = 0
        errors = []
        image_map = {}
        variant_rows = []
        parsed = [json.loads(line) for line in lines]

        # --- Step 1: raccogli immagini prodotto
        for obj in parsed:
            if "originalSrc" in obj and "__parentId" in obj:
                parent_id = normalize_gid(obj["__parentId"])
                if parent_id not in image_map:
                    image_map[parent_id] = obj["originalSrc"]

        # --- Step 2: raccogli tutte le varianti valide
        for obj in parsed:
            if "id" not in obj or "product" not in obj:
                continue
            variant_id = normalize_gid(obj["id"])
            product = obj["product"]
            product_id = product.get("id")
            if not variant_id or not product_id:
                logging.warning("Skip: riga incompleta %s", obj)
                continue
            variant_rows.append({
                "variant": obj,
                "variant_id": variant_id,
                "image_url": image_map.get(variant_id, "")
            })

        logging.info("Varianti da importare: %s", len(variant_rows))
        logging.info("Immagini associate: %s", len(image_map))

        # --- Step 3: upsert in Supabase con retry
        @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
        def safe_upsert_variant(record):
            return upsert_variant(record)

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
                    "user_id": user_id
                }
                if safe_upsert_variant(record):
                    count += 1
                else:
                    errors.append(f"❌ SKU: {record['sku']} - upsert fallito")
            except Exception as e:
                logging.error("[bulk-fetch ERROR]: %s", e, exc_info=True)
                errors.append(str(e))

        # --- Step 4: salva log errori su disco se presenti
        if errors:
            log_path = f"/tmp/shopify_import_log_{user_id}.txt"
            with open(log_path, "w", encoding="utf-8") as f:
                f.write("\n".join(errors))
            logging.info("Log salvato in: %s", log_path)

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
        logging.error("[bulk-fetch ERROR]: %s", str(e), exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500

# === 4. Scarica log errori import ===
@bulk_sync.route("/shopify/log", methods=["GET"])
@require_auth
def get_error_log():
    """Restituisce il log errori dell'import bulk per lo user."""
    user_id = g.user_id
    log_path = f"/tmp/shopify_import_log_{user_id}.txt"
    if not os.path.exists(log_path):
        return jsonify({"error": "Nessun log disponibile"}), 404
    with open(log_path, "r", encoding="utf-8") as f:
        content = f.read()
    return jsonify({"log": content})

# EOF
