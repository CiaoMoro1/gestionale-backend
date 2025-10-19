# app/routes/prelievo.py
# -*- coding: utf-8 -*-

from flask import Blueprint, request, jsonify
import logging

from app.services.prelievo_service import (
    date_importabili,
    importa_prelievi_da_data,
    lista_prelievi,
    aggiorna_prelievo,
    aggiorna_prelievi_bulk,
    svuota_prelievi,
)
from app.supabase_client import supabase

bp = Blueprint("prelievo", __name__)

# ------------------------------------------------------------
# Date importabili (da riepiloghi 'nuovo')
# ------------------------------------------------------------
@bp.route('/api/prelievi/date-importabili', methods=['GET'])
def get_date_importabili():
    try:
        return jsonify(date_importabili())
    except Exception as ex:
        logging.exception("[get_date_importabili] errore")
        return jsonify({"error": str(ex)}), 500


# ------------------------------------------------------------
# Import da ordini_vendor_* per una data
# ------------------------------------------------------------
@bp.route('/api/prelievi/importa', methods=['POST'])
def post_importa():
    try:
        data = (request.json or {}).get("data")
        if not data:
            return jsonify({"error": "Data richiesta"}), 400
        importa_prelievi_da_data(data)
        return jsonify({"ok": True})
    except Exception as ex:
        logging.exception("[post_importa] errore import prelievi")
        return jsonify({"error": str(ex)}), 500


# ------------------------------------------------------------
# Lista prelievi (filtri: data, radice)
# ------------------------------------------------------------
@bp.route('/api/prelievi', methods=['GET'])
def get_prelievi():
    try:
        data = request.args.get("data")
        radice = request.args.get("radice")
        rows = lista_prelievi(data=data, radice=radice)
        return jsonify(rows)
    except Exception as ex:
        logging.exception("[get_prelievi] errore lista")
        return jsonify({"error": str(ex)}), 500


# ------------------------------------------------------------
# Patch singolo prelievo
# ------------------------------------------------------------
@bp.route('/api/prelievi/<int:id>', methods=['PATCH'])
def patch_prelievo_route(id: int):
    try:
        payload = request.json or {}
        aggiorna_prelievo(id, payload)
        return jsonify({"ok": True})
    except ValueError as ve:
        # not found / validation
        return jsonify({"error": str(ve)}), 404
    except Exception as ex:
        logging.exception("[patch_prelievo_route] errore patch singolo")
        return jsonify({"error": str(ex)}), 400


# ------------------------------------------------------------
# Patch bulk prelievi
# ------------------------------------------------------------
@bp.route('/api/prelievi/bulk', methods=['PATCH'])
def patch_prelievi_bulk_route():
    try:
        body = request.json or {}
        ids = body.get("ids", [])
        fields = body.get("fields", {})
        if not ids or not fields:
            return jsonify({"error": "Nessun id/campo"}), 400
        aggiorna_prelievi_bulk(ids, fields)
        return jsonify({"ok": True, "updated_count": len(ids)})
    except Exception as ex:
        logging.exception("[patch_prelievi_bulk_route] errore bulk")
        return jsonify({"error": str(ex)}), 400


# ------------------------------------------------------------
# Svuota prelievi
# ------------------------------------------------------------
@bp.route('/api/prelievi/svuota', methods=['DELETE'])
def delete_svuota():
    try:
        svuota_prelievi()
        return jsonify({"ok": True})
    except Exception as ex:
        logging.exception("[delete_svuota] errore svuota prelievi")
        return jsonify({"error": str(ex)}), 500


# ------------------------------------------------------------
# Letture magazzino di supporto UI
# (NON duplichiamo /api/magazzino/giacenze: resta in amazon_vendor.py)
# ------------------------------------------------------------
@bp.route('/api/magazzino/availability', methods=['GET'])
def magazzino_availability():
    """
    Ritorna qty disponibile per (sku, ean, canale).
    Default canale: 'Amazon Vendor'.
    """
    try:
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
    except Exception as ex:
        logging.exception("[magazzino_availability] errore")
        return jsonify({"error": str(ex)}), 500


@bp.route('/api/magazzino/giacenza-aggregata', methods=['GET'])
def giacenza_aggregata():
    """
    Legge una vista aggregata (v_magazzino_totali) per SKU/EAN:
    - totale complessivo
    - per canale (amazon_vendor / sito / amazon_seller)
    """
    try:
        sku = (request.args.get("sku") or "").strip()
        ean = (request.args.get("ean") or "").strip()
        if not sku or not ean:
            return jsonify({"error": "sku e ean richiesti"}), 400

        res = supabase.table("v_magazzino_totali") \
            .select("*").eq("sku", sku).eq("ean", ean).limit(1).execute()

        data = getattr(res, "data", None) or []
        return jsonify(data[0] if data else {
            "sku": sku,
            "ean": ean,
            "totale": 0,
            "totale_amazon_vendor": 0,
            "totale_sito": 0,
            "totale_amazon_seller": 0
        })
    except Exception as ex:
        logging.exception("[giacenza_aggregata] errore")
        return jsonify({"error": str(ex)}), 500


# ------------------------------------------------------------
# Carico magazzino da "Produzione" (bulk)
# Body: { "items": [{ "id": int, "sku": str, "ean": str|null, "canale": str, "qty": int }] }
# ------------------------------------------------------------
@bp.route('/api/magazzino/carica-da-produzione', methods=['POST'])
def magazzino_carica_da_produzione():
    try:
        body = request.get_json(force=True, silent=True) or {}
        items = body.get("items") or []
        if not isinstance(items, list):
            return jsonify({"error": "Parametro 'items' dev'essere una lista"}), 400

        from app.services.prelievo_service import carica_magazzino_da_produzione
        out = carica_magazzino_da_produzione(items)

        # se alcuni item hanno errori, 207 (multi-status) è più onesto di 200
        status = 200 if not out.get("errors") else 207
        return jsonify(out), status
    except Exception as ex:
        logging.exception("[magazzino_carica_da_produzione] errore")
        return jsonify({"error": str(ex)}), 400
