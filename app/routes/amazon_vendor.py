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
    """
    Salva le righe dell’ordine vendor.
    po_items: lista di item Amazon (dict)
    ordini_vendor_id: chiave primaria tabella ordini_vendor (usata come FK)
    """
    for item in po_items:
        data = {
            "order_id": ordini_vendor_id,
            "line_number": item.get("itemSequenceNumber"),
            "sku": item.get("vendorProductIdentifier", ""),
            "product_title": item.get("itemDescription", ""),
            "ean": item.get("buyerProductIdentifier", ""),
            "ordered_quantity": item.get("orderedQuantity", {}).get("amount", None),
            "confirmed_quantity": item.get("acknowledgedQuantity", {}).get("amount", None),
            "price": item.get("netCost", {}).get("amount", None),
            "raw_data": item
        }
        supabase.table("ordini_vendor_items").insert(data).execute()

@bp.route('/api/amazon/vendor/orders/sync', methods=['POST'])
def sync_vendor_orders():
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
    params = {
        "limit": 20,
        "createdAfter": request.json.get("createdAfter", "2024-06-01T00:00:00Z")
    }
    resp = requests.get(url, auth=awsauth, headers=headers, params=params)
    resp.raise_for_status()
    orders = resp.json().get("purchaseOrders", [])
    imported = 0
    errors = []

    for po in orders:
        try:
            data = {
                "po_number": po.get("purchaseOrderNumber"),
                "status": po.get("purchaseOrderState"),
                "order_date": po.get("purchaseOrderDate"),
                "delivery_date": po.get("deliveryDate", None),
                "sold_to_party": po.get("soldToParty", {}).get("name", ""),
                "ship_to_party": po.get("shipToParty", {}).get("name", ""),
                "total_amount": po.get("orderTotal", {}).get("amount", None),
                "currency": po.get("orderTotal", {}).get("currencyCode", None),
                "creation_timestamp": datetime.utcnow().isoformat(),
                "raw_data": po
            }
            # Upsert dell'ordine vendor
            supabase.table("ordini_vendor").upsert(data, on_conflict="po_number").execute()

            # Recupera l'id dell'ordine appena inserito/aggiornato
            ord = supabase.table("ordini_vendor").select("id").eq("po_number", data["po_number"]).single().execute()
            ordini_vendor_id = ord.data["id"]

            # Cancella le vecchie righe item (così eviti duplicati)
            supabase.table("ordini_vendor_items").delete().eq("order_id", ordini_vendor_id).execute()

            # Salva le nuove righe articolo
            po_items = po.get("items", [])
            if po_items:
                save_vendor_order_items(data["po_number"], po_items, ordini_vendor_id)

            imported += 1
        except Exception as e:
            errors.append(f"PO {po.get('purchaseOrderNumber', 'n/a')}: {e}")

    return jsonify({"status": "ok", "imported": imported, "errors": errors})
