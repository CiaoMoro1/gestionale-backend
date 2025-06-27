from flask import Blueprint, jsonify, request
import requests
import os
from requests_aws4auth import AWS4Auth
from app.supabase_client import supabase
from datetime import datetime

bp = Blueprint('amazon_vendor', __name__)

def get_spapi_access_token():
    url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": os.getenv("SPAPI_REFRESH_TOKEN"),
        "client_id": os.getenv("SPAPI_CLIENT_ID"),
        "client_secret": os.getenv("SPAPI_CLIENT_SECRET")
    }
    resp = requests.post(url, data=data)
    print("===== AMAZON OAUTH2 DEBUG =====")
    print("Request data:", data)
    print("Response:", resp.status_code, resp.text)
    resp.raise_for_status()
    return resp.json()["access_token"]

def save_vendor_order_items(po_number, po_items, ordini_vendor_id):
    for item in po_items:
        vendor_product_id = item.get("vendorProductIdentifier", "")
        asin = item.get("amazonProductIdentifier", "")
        data = {
            "order_id": ordini_vendor_id,
            "line_number": item.get("itemSequenceNumber"),
            "sku": vendor_product_id,         # EAN/codice fornitore Vendor
            "product_title": "",              # Vendor NON restituisce titolo
            "ean": vendor_product_id,         # Così hai sempre almeno l'EAN
            "asin": asin,                     # (se hai aggiunto la colonna!)
            "ordered_quantity": item.get("orderedQuantity", {}).get("amount", None),
            "confirmed_quantity": item.get("acknowledgedQuantity", {}).get("amount", None),
            "price": item.get("netCost", {}).get("amount", None),
            "raw_data": item
        }
        supabase.table("ordini_vendor_items").insert(data).execute()

@bp.route('/api/amazon/vendor/orders/sync', methods=['POST'])
def sync_vendor_orders():
    imported = 0
    errors = []
    try:
        access_token = get_spapi_access_token()
        awsauth = AWS4Auth(
            os.getenv("AWS_ACCESS_KEY"),
            os.getenv("AWS_SECRET_KEY"),
            'eu-west-1', 'execute-api',
            session_token=os.getenv("AWS_SESSION_TOKEN")
        )
        url = "https://sellingpartnerapi-eu.amazon.com/vendor/orders/v1/purchaseOrders"
        headers = {
            "x-amz-access-token": access_token,
            "Content-Type": "application/json"
        }
        limit = request.json.get("limit", 100)
        created_after = request.json.get("createdAfter", "2024-06-01T00:00:00Z")
        params = {"limit": limit, "createdAfter": created_after}
        all_orders = []
        next_token = None

        while True:
            if next_token:
                params = {"nextToken": next_token}
            resp = requests.get(url, auth=awsauth, headers=headers, params=params)
            print("Amazon Vendor Orders Response:", resp.status_code, resp.text)
            resp.raise_for_status()
            payload = resp.json()["payload"]
            orders = payload.get("orders", [])
            all_orders.extend(orders)
            next_token = payload.get("pagination", {}).get("nextToken")
            if not next_token:
                break

        for po in all_orders:
            try:
                order_details = po.get("orderDetails", {})
                items = order_details.get("items", [])

                # total_amount/currency
                total_amount = None
                currency = None
                if items:
                    try:
                        total_amount = sum(float(it.get("netCost", {}).get("amount", 0) or 0) for it in items)
                        currency = items[0].get("netCost", {}).get("currencyCode", None)
                    except Exception:
                        total_amount = None
                        currency = None

                delivery_window = order_details.get("deliveryWindow", None)
                delivery_date = delivery_window.split("--")[0] if delivery_window and "--" in delivery_window else delivery_window

                data = {
                    "po_number": po.get("purchaseOrderNumber", ""),
                    "status": po.get("purchaseOrderState", ""),
                    "order_date": order_details.get("purchaseOrderDate", None),
                    "delivery_date": delivery_date,
                    "sold_to_party": order_details.get("sellingParty", {}).get("partyId", ""),
                    "ship_to_party": order_details.get("shipToParty", {}).get("partyId", ""),
                    "total_amount": total_amount,
                    "currency": currency,
                    "creation_timestamp": datetime.utcnow().isoformat(),
                    "raw_data": po
                }
                print("DATA TO UPSERT:", data)
                result = supabase.table("ordini_vendor").upsert(data, on_conflict="po_number").execute()
                print("SUPABASE UPSERT RESPONSE:", result)
                ord = supabase.table("ordini_vendor").select("id").eq("po_number", data["po_number"]).single().execute()
                ordini_vendor_id = ord.data["id"]
                supabase.table("ordini_vendor_items").delete().eq("order_id", ordini_vendor_id).execute()
                if items:
                    save_vendor_order_items(data["po_number"], items, ordini_vendor_id)
                imported += 1
            except Exception as e:
                print("SUPABASE UPSERT ERROR:", e)
                errors.append(f"PO {po.get('purchaseOrderNumber', 'n/a')}: {e}")
    except Exception as outer:
        print("FATAL ERROR SYNC VENDOR ORDERS:", outer)
        errors.append(str(outer))
    # RISPOSTA SEMPRE GARANTITA
    return jsonify({"status": "ok", "imported": imported, "errors": errors})
