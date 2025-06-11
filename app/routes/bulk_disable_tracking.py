from flask import Blueprint, jsonify, g
import httpx
import os
import certifi
import time
import logging

from app.utils.auth import require_auth

SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")
SHOPIFY_STORE = os.getenv("SHOP_DOMAIN")  # es: petti-artigiani-italiani-1968.myshopify.com

def shopify_request(method, path, json=None):
    url = f"https://{SHOPIFY_STORE}/admin/api/2023-04{path}"
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    return httpx.request(
        method,
        url,
        headers=headers,
        json=json,
        verify=certifi.where(),
        timeout=httpx.Timeout(30.0)  # timeout aumentato a 30s
    )

bulk_routes = Blueprint("bulk_routes", __name__)

@bulk_routes.route("/shopify/disable-all-inventory-tracking", methods=["POST"])
@require_auth
def disable_all_tracking():
    user_id = g.user_id
    logging.info("Richiesta di DISABILITAZIONE MASSIVA TRACKING da user %s", user_id)

    last_product_id = 0
    success_count = 0
    failed = []

    while True:
        res = shopify_request("GET", f"/products.json?limit=250&since_id={last_product_id}")
        products = res.json().get("products", [])
        if not products:
            break

        for product in products:
            last_product_id = max(last_product_id, product["id"])
            logging.info("Processing product: %s (%s)", product['title'], product['id'])

            for variant in product.get("variants", []):
                variant_id = variant["id"]
                inventory_mgmt = variant.get("inventory_management")

                if inventory_mgmt == "shopify":
                    resp = shopify_request("PUT", f"/variants/{variant_id}.json", json={
                        "variant": {
                            "id": variant_id,
                            "inventory_management": None
                        }
                    })
                    time.sleep(0.5)  # rispetta il limite di 2 req/sec

                    if resp.status_code == 200:
                        logging.info("Disabled tracking for variant %s", variant_id)
                        success_count += 1
                    else:
                        logging.error("Variant %s failed: %s - %s", variant_id, resp.status_code, resp.text)
                        failed.append({
                            "variant_id": variant_id,
                            "status": resp.status_code,
                            "error": resp.text
                        })
                else:
                    logging.info("Variant %s skipped (inventory_management: %s)", variant_id, inventory_mgmt)

    return jsonify({
        "updated_variants": success_count,
        "failed_variants": failed
    })
