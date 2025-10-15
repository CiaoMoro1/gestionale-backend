# prelievo.py

from flask import Blueprint, request, jsonify
from app.services.prelievo_service import (
    date_importabili,
    importa_prelievi_da_data,
    lista_prelievi,
    aggiorna_prelievo,
    aggiorna_prelievi_bulk,
    svuota_prelievi
)
from app.supabase_client import supabase

bp = Blueprint("prelievo", __name__)

@bp.route('/api/prelievi/date-importabili', methods=['GET'])
def get_date_importabili():
    return jsonify(date_importabili())

@bp.route('/api/prelievi/importa', methods=['POST'])
def post_importa():
    data = request.json.get("data")
    if not data: return jsonify({"error":"Data richiesta"}), 400
    importa_prelievi_da_data(data)
    return jsonify({"ok": True})

@bp.route('/api/prelievi', methods=['GET'])
def get_prelievi():
    data = request.args.get("data")
    radice = request.args.get("radice")
    return jsonify(lista_prelievi(data=data, radice=radice))

@bp.route('/api/prelievi/<int:id>', methods=['PATCH'])
def patch_prelievo(id):
    payload = request.json or {}
    try:
        aggiorna_prelievo(id, payload)
        return jsonify({"ok": True})
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 404
    except Exception as ex:
        return jsonify({"error": str(ex)}), 400

@bp.route('/api/prelievi/bulk', methods=['PATCH'])
def patch_prelievi_bulk():
    ids = request.json.get("ids", [])
    fields = request.json.get("fields", {})
    if not ids or not fields: return jsonify({"error":"Nessun id/campo"}), 400
    aggiorna_prelievi_bulk(ids, fields)
    return jsonify({"ok": True, "updated_count": len(ids)})

@bp.route('/api/prelievi/svuota', methods=['DELETE'])
def delete_svuota():
    svuota_prelievi()
    return jsonify({"ok": True})

@bp.route('/api/magazzino/availability', methods=['GET'])
def magazzino_availability():
    sku = (request.args.get("sku") or "").strip()
    ean = (request.args.get("ean") or "").strip()
    canale = (request.args.get("canale") or "Amazon Vendor").strip() or "Amazon Vendor"
    if not sku or not ean:
        return jsonify({"qty": 0})
    res = supabase.table("magazzino_giacenze") \
        .select("qty") \
        .eq("sku", sku).eq("ean", ean).eq("canale", canale) \
        .limit(1).execute()
    data = getattr(res, "data", None) or []
    qty = int((data[0]["qty"] if data else 0) or 0)
    return jsonify({"qty": qty})

@bp.route('/api/magazzino/giacenza-aggregata', methods=['GET'])
def giacenza_aggregata():
    sku = (request.args.get("sku") or "").strip()
    ean = (request.args.get("ean") or "").strip()
    if not sku or not ean:
        return jsonify({"error": "sku e ean richiesti"}), 400

    res = supabase.table("v_magazzino_totali") \
        .select("*").eq("sku", sku).eq("ean", ean).limit(1).execute()
    data = getattr(res, "data", None) or []
    return jsonify(data[0] if data else {
        "sku": sku, "ean": ean,
        "totale": 0, "totale_amazon_vendor": 0, "totale_sito": 0, "totale_amazon_seller": 0
    })