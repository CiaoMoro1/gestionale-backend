from flask import Blueprint, request, jsonify
from app.shopify import fetch_all_products
from app.supabase_client import supabase
from app.utils.auth import require_auth

sync = Blueprint("sync", __name__)

@sync.route("/shopify/manual-sync", methods=["POST"])
@require_auth
def manual_sync(user_id):
    try:
        print(f"✅ JWT OK — user_id: {user_id}")
        print("🔁 Avvio fetch prodotti da Shopify...")

        products = fetch_all_products()
        print(f"📦 Trovati {len(products)} prodotti")

        count = 0

        for product in products:
            for variant in product.get("variants", []):
                if not isinstance(variant, dict):
                    print(f"⚠️ Variante malformata — saltata: {variant}")
                    continue

                sku = variant.get("sku") or ""  # permettiamo anche SKU vuoto

                data = {
                    "shopify_product_id": product["id"],
                    "shopify_variant_id": variant["id"],
                    "name": product["title"],
                    "sku": sku,
                    "quantity": variant.get("inventory_quantity", 0),
                    "user_id": user_id,
                }

                print("📥 Inserisco variante:", data)
                supabase.table("products").upsert(data, on_conflict=["shopify_variant_id"]).execute()
                count += 1

        return jsonify({"status": "success", "imported_variants": count}), 200

    except Exception as e:
        print("❌ ERRORE SYNC:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500
