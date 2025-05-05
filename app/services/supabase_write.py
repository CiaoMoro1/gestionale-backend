from app.supabase_client import supabase

def upsert_variant(record: dict):
    try:
        response = supabase.table("products").upsert(
            record,
            on_conflict=["shopify_variant_id"]
        ).execute()

        # ✅ Verifica se la risposta ha effettivamente salvato qualcosa
        if not response.data:
            print(f"❌ Supabase returned empty response for SKU: {record.get('sku')}")
            print("📦 Payload:", record)
            return False

        return True

    except Exception as e:
        print(f"❌ Exception in upsert_variant for SKU {record.get('sku')}: {e}")
        return False
