from flask import Blueprint, jsonify, request, Response, jsonify
import pandas as pd
import io
from app.supabase_client import supabase
from datetime import datetime
import math
from collections import defaultdict
import os
import requests
from requests_aws4auth import AWS4Auth
from fpdf import FPDF
import barcode
from barcode.writer import ImageWriter
from io import BytesIO
from PIL import Image
import time
import logging
import uuid
import httpx
from barcode import get_barcode_class
import json

bp = Blueprint('amazon_vendor', __name__)

def get_spapi_access_token():
    url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": os.getenv("SPAPI_REFRESH_TOKEN"),   # <-- Il tuo refresh token ottenuto prima!
        "client_id": os.getenv("SPAPI_CLIENT_ID"),
        "client_secret": os.getenv("SPAPI_CLIENT_SECRET"),
    }
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

def safe_value(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d")
    return v

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in {"xls", "xlsx"}


def get_all_items_by_po(po_list):
    all_items = []
    BATCH_SIZE = 50  # Limita il numero di PO per singola query
    for i in range(0, len(po_list), BATCH_SIZE):
        batch_po = po_list[i:i+BATCH_SIZE]
        offset = 0
        limit = 500
        while True:
            batch = supabase.table("ordini_vendor_items") \
                .select("po_number, qty_ordered, fulfillment_center, start_delivery") \
                .in_("po_number", batch_po) \
                .range(offset, offset+limit-1) \
                .execute().data
            if not batch:
                break
            all_items.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
        time.sleep(0.1)  # Pausa per non saturare Supabase tra un batch e l'altro
    return all_items




def sync_produzione_from_prelievo(prelievo_id):
    """
    Sincronizza produzione_vendor a partire dal prelievo, con retry e logging robusto.
    """
    import logging
    max_retries = 3

    for attempt in range(max_retries):
        try:
            prelievo = supabase.table("prelievi_ordini_amazon").select("*").eq("id", prelievo_id).single().execute().data
            if not prelievo:
                logging.warning(f"[sync_produzione_from_prelievo] Prelievo ID {prelievo_id} non trovato")
                return

            stato = prelievo["stato"]
            qty = int(prelievo.get("qty") or 0)
            riscontro = int(prelievo.get("riscontro") or 0)
            plus = int(prelievo.get("plus") or 0)
            da_produrre = 0

            if stato == "manca":
                da_produrre = qty + plus
            elif stato == "parziale":
                da_produrre = (qty - riscontro) + plus
            elif stato == "completo" and plus > 0:
                da_produrre = plus
            else:
                # Puoi eventualmente cancellare la riga se serve, qui solo log
                # supabase.table("produzione_vendor").delete().eq("prelievo_id", prelievo_id).execute()
                logging.info(f"[sync_produzione_from_prelievo] Niente da produrre per prelievo {prelievo_id}, stato: {stato}")
                return

            row = {
                "prelievo_id": prelievo["id"],
                "sku": prelievo["sku"],
                "ean": prelievo["ean"],
                "qty": qty,
                "riscontro": riscontro,
                "plus": plus,
                "radice": prelievo.get("radice"),
                "start_delivery": prelievo.get("start_delivery"),
                "stato": stato,
                "stato_produzione": "Da Stampare",
                "da_produrre": da_produrre,
                "note": prelievo.get("note"),
                "centri": prelievo.get("centri") or {},
                # "cavallotti": False, # se serve lo gestisci qui
                "updated_at": datetime.utcnow().isoformat()  # timestamp aggiornato
            }

            # Se vuoi, puoi loggare ogni variazione qui (log_movimento_produzione)
            # log_movimento_produzione(row, utente="sistema", motivo="Sync da prelievo")

            supabase.table("produzione_vendor").upsert(row, on_conflict="prelievo_id").execute()
            logging.info(f"[sync_produzione_from_prelievo] Upsert produzione_vendor completata per prelievo {prelievo_id}")
            return

        except Exception as ex:
            logging.error(f"[sync_produzione_from_prelievo] Errore tentativo {attempt+1}/{max_retries} per prelievo {prelievo_id}: {ex}")
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                logging.exception(f"[sync_produzione_from_prelievo] Errore definitivo per prelievo {prelievo_id}: {ex}")


# ------------------ UPLOAD ORDINI -------------------
@bp.route('/api/amazon/vendor/orders/upload', methods=['POST'])
def upload_vendor_orders():
    """
    Nuova logica: salva file Excel in Supabase Storage, crea job "pending" su Supabase, risponde subito.
    """
    if 'file' not in request.files:
        return jsonify({"error": "Nessun file fornito"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "Nessun file selezionato"}), 400

    if not allowed_file(file.filename):
        return jsonify({"error": "Formato file non valido"}), 400

    try:
        # Crea filename unico
        file_id = str(uuid.uuid4())
        filename = f"{file_id}_{file.filename}"
        bucket_name = "vendorimports"

        # Carica su Supabase Storage
        file.seek(0)
        res = supabase.storage.from_(bucket_name).upload(
            filename,
            file.read(),
            {"content-type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
        )
        if hasattr(res, 'error') and res.error:
            raise Exception(f"Errore upload Storage: {res.error}")


        # Path dove il worker troverà il file
        storage_path = f"{bucket_name}/{filename}"

        # Prepara payload per il worker job
        payload = {
            "storage_path": storage_path,
            "file_name": file.filename,
            # Puoi aggiungere altri dati se vuoi!
        }

        user_id = request.headers.get('X-USER-ID')
        # Inserisci job su Supabase
        job_res = supabase.table('jobs').insert([{
            "type": "import_vendor_orders",
            "payload": payload,
            "status": "pending",
            "user_id": user_id,
            "created_at": datetime.utcnow().isoformat()
        }]).execute()
        job_id = job_res.data[0]['id'] if job_res.data else None

        return jsonify({"job_id": job_id}), 201

    except Exception as e:
        import logging
        logging.exception("Errore durante upload ordini vendor")
        return jsonify({"error": f"Errore upload: {e}"}), 500


@bp.route('/api/amazon/vendor/orders/riepilogo/nuovi', methods=['GET'])
def get_riepilogo_nuovi():
    offset = int(request.args.get("offset", 0))
    limit = int(request.args.get("limit", 100))
    max_retries = 3

    for attempt in range(max_retries):
        try:
            res = supabase.table("ordini_vendor_riepilogo") \
                .select("*") \
                .eq("stato_ordine", "nuovo") \
                .order("created_at") \
                .range(offset, offset + limit - 1) \
                .execute()
            riepiloghi = res.data if hasattr(res, 'data') else res

            tutti_po = set()
            for r in riepiloghi:
                if r.get("po_list"):
                    tutti_po.update(r["po_list"])
            if not tutti_po:
                return jsonify([])

            dettagli = get_all_items_by_po(list(tutti_po))

            articoli_per_po = {}
            for x in dettagli:
                key = (x["po_number"], x["fulfillment_center"], str(x["start_delivery"])[:10])
                articoli_per_po[key] = articoli_per_po.get(key, 0) + int(x["qty_ordered"])

            risposta = []
            for r in riepiloghi:
                po_list = []
                if not r.get("po_list"):
                    continue
                for po in r["po_list"]:
                    key = (po, r["fulfillment_center"], str(r["start_delivery"])[:10])
                    po_list.append({
                        "po_number": po,
                        "numero_articoli": articoli_per_po.get(key, 0)
                    })
                totale_articoli = sum(x["numero_articoli"] for x in po_list)
                risposta.append({
                    "fulfillment_center": r["fulfillment_center"],
                    "start_delivery": r["start_delivery"],
                    "po_list": po_list,
                    "totale_articoli": totale_articoli,
                    "stato_ordine": r["stato_ordine"]
                })
            return jsonify(risposta)
        except httpx.RemoteProtocolError:
            logging.warning(f"[get_riepilogo_nuovi] Errore di rete verso Supabase (tentativo {attempt+1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                return jsonify({"error": "Errore di connessione a Supabase. Riprova."}), 503
        except Exception as ex:
            logging.exception(f"[get_riepilogo_nuovi] Errore interno: {ex}")
            return jsonify({"error": f"Errore interno: {str(ex)}"}), 500

import httpx
import logging

@bp.route('/api/amazon/vendor/orders/dettaglio-destinazione', methods=['GET'])
def dettaglio_destinazione():
    center = request.args.get("center")
    data = request.args.get("data")
    offset = int(request.args.get("offset", 0))
    limit = int(request.args.get("limit", 100))
    if not center or not data:
        return jsonify([])

    max_retries = 3
    for attempt in range(max_retries):
        try:
            riepilogo_res = supabase.table("ordini_vendor_riepilogo") \
                .select("id, po_list") \
                .eq("fulfillment_center", center) \
                .eq("start_delivery", data) \
                .execute()
            if not riepilogo_res.data or not riepilogo_res.data[0].get("po_list"):
                return jsonify([])

            po_list = riepilogo_res.data[0]["po_list"]
            riepilogo_id = riepilogo_res.data[0]["id"]

            articoli = supabase.table("ordini_vendor_items") \
                .select("po_number, model_number, vendor_product_id, title, qty_ordered") \
                .in_("po_number", po_list) \
                .range(offset, offset+limit-1) \
                .execute().data

            return jsonify({"articoli": articoli, "riepilogo_id": riepilogo_id})

        except httpx.RemoteProtocolError:
            logging.warning(f"[dettaglio_destinazione] Connessione interrotta verso Supabase (tentativo {attempt+1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                return jsonify({"error": "Connessione a Supabase interrotta. Riprova tra poco."}), 503
        except Exception as ex:
            logging.exception("[dettaglio_destinazione] Errore interno")
            return jsonify({"error": f"Errore interno: {str(ex)}"}), 500



import logging

@bp.route('/api/amazon/vendor/riepilogo-id', methods=['GET'])
def get_riepilogo_id():
    center = request.args.get("center")
    data = request.args.get("data")
    if not center or not data:
        return jsonify({"error": "center/data richiesti"}), 400

    try:
        res = supabase.table("ordini_vendor_riepilogo")\
            .select("id")\
            .eq("fulfillment_center", center)\
            .eq("start_delivery", data)\
            .execute()
        if res.data and len(res.data) > 0:
            return jsonify({"riepilogo_id": res.data[0]['id']})
        return jsonify({"riepilogo_id": None})
    except Exception as ex:
        logging.exception("[get_riepilogo_id] Errore nel recupero ID riepilogo")
        return jsonify({"error": f"Errore interno: {str(ex)}"}), 500


import logging

@bp.route('/api/amazon/vendor/parziali', methods=['GET'])
def get_parziali():
    riepilogo_id = request.args.get('riepilogo_id')
    try:
        offset = int(request.args.get("offset", 0))
        limit = int(request.args.get("limit", 100))
    except Exception:
        return jsonify({"error": "Offset/limit non validi"}), 400

    if not riepilogo_id:
        return jsonify({"error": "riepilogo_id mancante"}), 400
    if limit > 200:
        return jsonify({"error": "Limit troppo alto (max 200)"}), 400
    if offset < 0:
        return jsonify({"error": "Offset non valido"}), 400

    max_retries = 3
    for attempt in range(max_retries):
        try:
            parziali = supabase.table("ordini_vendor_parziali") \
                .select("*") \
                .eq("riepilogo_id", riepilogo_id) \
                .order("numero_parziale") \
                .range(offset, offset+limit-1) \
                .execute().data
            return jsonify(parziali)
        except Exception as ex:
            logging.exception(f"[get_parziali] Errore tentativo {attempt+1}/{max_retries}")
            if attempt < max_retries - 1:
                import time
                time.sleep(0.7)
            else:
                return jsonify({"error": f"Errore: {str(ex)}"}), 500



import logging
import time
from datetime import datetime

@bp.route('/api/amazon/vendor/parziali', methods=['POST'])
def save_parziale():
    max_retries = 3
    try:
        data = request.json
        riepilogo_id = data.get("riepilogo_id")
        dati = data.get("dati")  # array di {model_number, quantita, collo}
        if not riepilogo_id or not dati:
            return jsonify({"error": "Dati mancanti"}), 400

        res = supabase.table("ordini_vendor_parziali") \
            .select("numero_parziale") \
            .eq("riepilogo_id", riepilogo_id) \
            .order("numero_parziale", desc=True) \
            .limit(1) \
            .execute()
        max_num = 1
        if res.data and len(res.data) > 0:
            max_num = int(res.data[0]["numero_parziale"]) + 1

        parziale = {
            "riepilogo_id": riepilogo_id,
            "numero_parziale": max_num,
            "dati": dati,
            "confermato": False,
            "created_at": datetime.utcnow().isoformat(),
            "last_modified_at": datetime.utcnow().isoformat()
        }
        for attempt in range(max_retries):
            try:
                supabase.table("ordini_vendor_parziali").upsert(parziale, on_conflict="riepilogo_id,numero_parziale").execute()
                break
            except Exception as ex:
                logging.warning(f"[save_parziale] Errore tentativo {attempt+1}/{max_retries}: {ex}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise
        return jsonify({"ok": True, "numero_parziale": max_num})

    except Exception as ex:
        logging.exception("[save_parziale] Errore salvataggio parziale")
        return jsonify({"error": f"Errore salvataggio: {str(ex)}"}), 500




@bp.route('/api/amazon/vendor/parziali/<int:riepilogo_id>', methods=['GET'])
def get_parziali_riepilogo(riepilogo_id):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            res = supabase.table("ordini_vendor_parziali") \
                .select("*") \
                .eq("riepilogo_id", riepilogo_id) \
                .order("numero_parziale", desc=True) \
                .limit(1) \
                .execute()
            if not res.data:
                return jsonify({"parziali": [], "confermaCollo": {}})
            parz = res.data[0]
            return jsonify({
                "parziali": parz.get("dati", []),
                "confermaCollo": parz.get("conferma_collo", {})
            })
        except Exception as ex:
            logging.warning(f"[get_parziali_riepilogo] Errore tentativo {attempt+1}/{max_retries}: {ex}")
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                logging.exception("Errore lettura parziali riepilogo")
                return jsonify({"error": f"Errore: {str(ex)}"}), 500

@bp.route('/api/amazon/vendor/parziali/<int:riepilogo_id>', methods=['POST'])
def post_parziali_riepilogo(riepilogo_id):
    max_retries = 3
    try:
        dati = request.json
        numero_parziale = dati.get("numero_parziale", 1)
        parziale_data = {
            "riepilogo_id": riepilogo_id,
            "numero_parziale": numero_parziale,
            "dati": dati.get("parziali", []),
            "conferma_collo": dati.get("confermaCollo", {}),
            "confermato": False,
            "created_at": datetime.utcnow().isoformat(),
            "last_modified_at": datetime.utcnow().isoformat()
        }
        for attempt in range(max_retries):
            try:
                supabase.table("ordini_vendor_parziali").upsert(
                    parziale_data,
                    on_conflict="riepilogo_id,numero_parziale"
                ).execute()
                break
            except Exception as ex:
                logging.warning(f"[post_parziali_riepilogo] Errore tentativo {attempt+1}/{max_retries}: {ex}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise
        return jsonify({"ok": True})
    except Exception as ex:
        logging.exception("Errore patch parziali riepilogo")
        return jsonify({"error": f"Errore salvataggio: {str(ex)}"}), 500


@bp.route('/api/amazon/vendor/parziali-storici', methods=['GET'])
def get_parziali_storici():
    center = request.args.get("center")
    data = request.args.get("data")
    offset = int(request.args.get("offset", 0))
    limit = int(request.args.get("limit", 100))
    if not center or not data:
        return jsonify([])
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            riepilogo = supabase.table("ordini_vendor_riepilogo") \
                .select("id") \
                .eq("fulfillment_center", center) \
                .eq("start_delivery", data) \
                .execute().data
            if not riepilogo:
                return jsonify([])
            riepilogo_id = riepilogo[0]["id"]

            batch = supabase.table("ordini_vendor_parziali") \
                .select("dati") \
                .eq("riepilogo_id", riepilogo_id) \
                .eq("confermato", True) \
                .order("numero_parziale") \
                .range(offset, offset+limit-1) \
                .execute().data

            parziali = []
            for p in batch:
                parziali.extend(p["dati"])
            return jsonify(parziali)
        except Exception as ex:
            logging.warning(f"[get_parziali_storici] Errore tentativo {attempt+1}/{max_retries}: {ex}")
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                logging.exception("[get_parziali_storici] Errore definitivo")
                return jsonify({"error": f"Errore: {str(ex)}"}), 500

@bp.route('/api/amazon/vendor/parziali-wip', methods=['GET'])
def get_parziali_wip():
    center = request.args.get("center")
    data = request.args.get("data")
    if not center or not data:
        return jsonify([])

    max_retries = 3
    for attempt in range(max_retries):
        try:
            riepilogo = supabase.table("ordini_vendor_riepilogo") \
                .select("id") \
                .eq("fulfillment_center", center) \
                .eq("start_delivery", data) \
                .execute().data
            if not riepilogo:
                return jsonify([])
            riepilogo_id = riepilogo[0]["id"]
            parziali = supabase.table("ordini_vendor_parziali") \
                .select("dati, numero_parziale") \
                .eq("riepilogo_id", riepilogo_id) \
                .eq("confermato", False) \
                .order("numero_parziale", desc=True) \
                .limit(1) \
                .execute().data
            if parziali and len(parziali) > 0:
                return jsonify(parziali[0]["dati"])
            return jsonify([])
        except Exception as ex:
            logging.warning(f"[get_parziali_wip] Errore tentativo {attempt+1}/{max_retries}: {ex}")
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                logging.exception("[get_parziali_wip] Errore definitivo")
                return jsonify({"error": f"Errore: {str(ex)}"}), 500

@bp.route('/api/amazon/vendor/parziali-wip', methods=['POST'])
def save_parziali_wip():
    center = request.args.get("center")
    start_delivery = request.args.get("data")
    data = request.json
    parziali = data.get("parziali")
    conferma_collo = data.get("confermaCollo", {})
    if not center or not start_delivery or parziali is None:
        return jsonify({"error": "center/data/parziali richiesti"}), 400

    max_retries = 3
    try:
        riepilogo = supabase.table("ordini_vendor_riepilogo") \
            .select("id") \
            .eq("fulfillment_center", center) \
            .eq("start_delivery", start_delivery) \
            .execute().data
        if not riepilogo:
            return jsonify({"error": "riepilogo non trovato"}), 400
        riepilogo_id = riepilogo[0]["id"]

        res = supabase.table("ordini_vendor_parziali") \
            .select("numero_parziale") \
            .eq("riepilogo_id", riepilogo_id) \
            .eq("confermato", False) \
            .order("numero_parziale", desc=True) \
            .limit(1) \
            .execute().data

        if res and len(res) > 0:
            numero_parziale = res[0]["numero_parziale"]
        else:
            conf = supabase.table("ordini_vendor_parziali") \
                .select("numero_parziale") \
                .eq("riepilogo_id", riepilogo_id) \
                .eq("confermato", True) \
                .order("numero_parziale", desc=True) \
                .limit(1) \
                .execute().data
            max_num = conf[0]["numero_parziale"] if conf and len(conf) > 0 else 0
            numero_parziale = max_num + 1

        parziale_data = {
            "riepilogo_id": riepilogo_id,
            "numero_parziale": numero_parziale,
            "dati": parziali,
            "conferma_collo": conferma_collo,
            "confermato": False,
            "created_at": datetime.utcnow().isoformat(),
            "last_modified_at": datetime.utcnow().isoformat()
        }

        for attempt in range(max_retries):
            try:
                supabase.table("ordini_vendor_parziali").upsert(
                    parziale_data,
                    on_conflict="riepilogo_id,numero_parziale"
                ).execute()
                break
            except Exception as ex:
                logging.warning(f"[save_parziali_wip] Errore tentativo {attempt+1}/{max_retries}: {ex}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise

        return jsonify({"ok": True, "numero_parziale": numero_parziale})
    except Exception as ex:
        logging.exception("[save_parziali_wip] Errore salvataggio parziali wip")
        return jsonify({"error": f"Errore salvataggio: {str(ex)}"}), 500

@bp.route('/api/amazon/vendor/parziali-wip/conferma-parziale', methods=['POST'])
def conferma_parziale():
    max_retries = 3
    try:
        center = request.json.get("center")
        start_delivery = request.json.get("data")
        if not center or not start_delivery:
            return jsonify({"error": "center/data richiesti"}), 400

        for attempt in range(max_retries):
            try:
                riepilogo = supabase.table("ordini_vendor_riepilogo") \
                    .select("id") \
                    .eq("fulfillment_center", center) \
                    .eq("start_delivery", start_delivery) \
                    .execute().data
                if not riepilogo:
                    return jsonify({"error": "riepilogo non trovato"}), 400
                riepilogo_id = riepilogo[0]["id"]
                parziale = supabase.table("ordini_vendor_parziali") \
                    .select("*") \
                    .eq("riepilogo_id", riepilogo_id) \
                    .eq("confermato", False) \
                    .order("numero_parziale", desc=True) \
                    .limit(1) \
                    .execute().data
                if not parziale:
                    return jsonify({"error": "nessun parziale da confermare"}), 400
                num_parz = parziale[0]["numero_parziale"]
                supabase.table("ordini_vendor_parziali") \
                    .update({"confermato": True}) \
                    .eq("riepilogo_id", riepilogo_id) \
                    .eq("numero_parziale", num_parz) \
                    .execute()
                supabase.table("ordini_vendor_riepilogo") \
                    .update({"stato_ordine": "parziale"}) \
                    .eq("id", riepilogo_id) \
                    .execute()
                return jsonify({"ok": True})
            except Exception as ex:
                logging.warning(f"[conferma_parziale] Errore tentativo {attempt+1}/{max_retries}: {ex}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise
    except Exception as ex:
        logging.exception("Errore conferma parziale")
        return jsonify({"error": f"Errore conferma: {str(ex)}"}), 500


@bp.route('/api/amazon/vendor/parziali-wip/conferma', methods=['POST'])
def conferma_chiudi_ordine():
    max_retries = 3
    try:
        center = request.json.get("center")
        start_delivery = request.json.get("data")
        if not center or not start_delivery:
            return jsonify({"error": "center/data richiesti"}), 400

        for attempt in range(max_retries):
            try:
                riepilogo = supabase.table("ordini_vendor_riepilogo") \
                    .select("id, po_list") \
                    .eq("fulfillment_center", center) \
                    .eq("start_delivery", start_delivery) \
                    .execute().data
                if not riepilogo:
                    return jsonify({"error": "riepilogo non trovato"}), 400
                riepilogo_id = riepilogo[0]["id"]
                po_list = riepilogo[0]["po_list"]

                wip = supabase.table("ordini_vendor_parziali") \
                    .select("*") \
                    .eq("riepilogo_id", riepilogo_id) \
                    .eq("confermato", False) \
                    .order("numero_parziale", desc=True) \
                    .limit(1) \
                    .execute().data
                if not wip:
                    return jsonify({"error": "nessun parziale da confermare"}), 400
                num_parz = wip[0]["numero_parziale"]
                dati_wip = wip[0]["dati"]

                # Segna il parziale come confermato
                supabase.table("ordini_vendor_parziali") \
                    .update({"confermato": True}) \
                    .eq("riepilogo_id", riepilogo_id) \
                    .eq("numero_parziale", num_parz) \
                    .execute()

                # Prendi tutti i parziali STORICI confermati (inclusi gli altri parziali di questo riepilogo)
                parziali_storici = supabase.table("ordini_vendor_parziali") \
                    .select("dati") \
                    .eq("riepilogo_id", riepilogo_id) \
                    .eq("confermato", True) \
                    .order("numero_parziale") \
                    .execute().data
                totali_sku = defaultdict(int)
                for p in parziali_storici:
                    for r in p["dati"]:
                        totali_sku[r["model_number"]] += r["quantita"]
                for r in dati_wip:
                    totali_sku[r["model_number"]] += r["quantita"]

                # Aggiorna tutte le righe articolo (qty_confirmed su tutti gli articoli della spedizione)
                for model_number, qty in totali_sku.items():
                    supabase.table("ordini_vendor_items") \
                        .update({"qty_confirmed": qty}) \
                        .in_("po_number", po_list) \
                        .eq("model_number", model_number) \
                        .execute()

                # Aggiorna stato_ordine a "parziale" o "completato"
                stato_ordine = "parziale"
                supabase.table("ordini_vendor_riepilogo") \
                    .update({"stato_ordine": stato_ordine}) \
                    .eq("id", riepilogo_id) \
                    .execute()
                return jsonify({"ok": True})
            except Exception as ex:
                logging.warning(f"[conferma_chiudi_ordine] Errore tentativo {attempt+1}/{max_retries}: {ex}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise
    except Exception as ex:
        logging.exception("Errore chiusura ordine")
        return jsonify({"error": f"Errore chiusura ordine: {str(ex)}"}), 500

@bp.route('/api/amazon/vendor/parziali-wip/reset', methods=['POST'])
def reset_parziali_wip():
    max_retries = 3
    try:
        center = request.json.get("center")
        start_delivery = request.json.get("data")
        if not center or not start_delivery:
            return jsonify({"error": "center/data richiesti"}), 400

        for attempt in range(max_retries):
            try:
                riepilogo = supabase.table("ordini_vendor_riepilogo") \
                    .select("id") \
                    .eq("fulfillment_center", center) \
                    .eq("start_delivery", start_delivery) \
                    .execute().data
                if not riepilogo:
                    return jsonify({"error": "riepilogo non trovato"}), 400
                riepilogo_id = riepilogo[0]["id"]
                supabase.table("ordini_vendor_parziali") \
                    .delete() \
                    .eq("riepilogo_id", riepilogo_id) \
                    .eq("confermato", False) \
                    .execute()
                return jsonify({"ok": True})
            except Exception as ex:
                logging.warning(f"[reset_parziali_wip] Errore tentativo {attempt+1}/{max_retries}: {ex}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise
    except Exception as ex:
        logging.exception("Errore reset parziali WIP")
        return jsonify({"error": f"Errore reset: {str(ex)}"}), 500

@bp.route('/api/amazon/vendor/parziali-wip/chiudi', methods=['POST'])
def chiudi_ordine():
    max_retries = 3
    try:
        data = request.json
        center = data.get("center")
        start_delivery = data.get("data")
        if not center or not start_delivery:
            return jsonify({"error": "center/data richiesti"}), 400

        for attempt in range(max_retries):
            try:
                riepilogo = supabase.table("ordini_vendor_riepilogo") \
                    .select("id, po_list") \
                    .eq("fulfillment_center", center) \
                    .eq("start_delivery", start_delivery) \
                    .execute().data
                if not riepilogo:
                    return jsonify({"error": "riepilogo non trovato"}), 400
                riepilogo_id = riepilogo[0]["id"]
                po_list = riepilogo[0]["po_list"]

                # Batch carico i parziali confermati
                offset = 0
                limit = 100
                parziali = []
                while True:
                    batch = supabase.table("ordini_vendor_parziali") \
                        .select("dati") \
                        .eq("riepilogo_id", riepilogo_id) \
                        .eq("confermato", True) \
                        .order("numero_parziale") \
                        .range(offset, offset+limit-1) \
                        .execute().data
                    if not batch:
                        break
                    parziali.extend(batch)
                    if len(batch) < limit:
                        break
                    offset += limit

                # Può esserci anche una bozza WIP da chiudere ora
                parziali_wip = supabase.table("ordini_vendor_parziali") \
                    .select("dati") \
                    .eq("riepilogo_id", riepilogo_id) \
                    .eq("confermato", False) \
                    .order("numero_parziale", desc=True) \
                    .limit(1) \
                    .execute().data
                if parziali_wip:
                    parziali.append(parziali_wip[0])

                # Calcola quantità totali
                qty_per_model = {}
                for p in parziali:
                    for r in p["dati"]:
                        model = r["model_number"]
                        qty_per_model[model] = qty_per_model.get(model, 0) + int(r["quantita"])

                articoli = supabase.table("ordini_vendor_items") \
                    .select("id, model_number") \
                    .in_("po_number", po_list) \
                    .execute().data

                for art in articoli:
                    nuova_qty = qty_per_model.get(art["model_number"], 0)
                    supabase.table("ordini_vendor_items") \
                        .update({"qty_confirmed": nuova_qty}) \
                        .eq("id", art["id"]) \
                        .execute()

                supabase.table("ordini_vendor_riepilogo") \
                    .update({"stato_ordine": "completato"}) \
                    .eq("id", riepilogo_id) \
                    .execute()

                # Marca bozza come confermata, se c’era
                if parziali_wip:
                    numero_parziale = supabase.table("ordini_vendor_parziali") \
                        .select("numero_parziale") \
                        .eq("riepilogo_id", riepilogo_id) \
                        .eq("confermato", False) \
                        .order("numero_parziale", desc=True) \
                        .limit(1) \
                        .execute().data
                    if numero_parziale:
                        num = numero_parziale[0]["numero_parziale"]
                        supabase.table("ordini_vendor_parziali") \
                            .update({"confermato": True}) \
                            .eq("riepilogo_id", riepilogo_id) \
                            .eq("numero_parziale", num) \
                            .execute()

                return jsonify({"ok": True, "qty_confirmed": qty_per_model})
            except Exception as ex:
                logging.warning(f"[chiudi_ordine] Errore tentativo {attempt+1}/{max_retries}: {ex}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise
    except Exception as ex:
        logging.exception("Errore chiusura ordine")
        return jsonify({"error": f"Errore chiusura ordine: {str(ex)}"}), 500


# Mostra tutti i riepiloghi con stato PARZIALE (e anche completato se vuoi)
@bp.route('/api/amazon/vendor/orders/riepilogo/parziali', methods=['GET'])
def get_riepilogo_parziali():
    try:
        res = supabase.table("ordini_vendor_riepilogo")\
            .select("*")\
            .in_("stato_ordine", ["parziale"])\
            .order("created_at", desc=True)\
            .execute()
        return jsonify(res.data if hasattr(res, 'data') else res)
    except Exception as ex:
        logging.exception("Errore in get_riepilogo_parziali")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500

@bp.route('/api/amazon/vendor/items', methods=['GET'])
def get_items_by_po():
    try:
        po_list = request.args.get("po_list")
        offset = int(request.args.get("offset", 0))
        limit = int(request.args.get("limit", 200))
        MAX_PO = 10
        MAX_LIMIT = 500
        MAX_OFFSET = 10000

        if not po_list:
            return jsonify([])

        pos = [p.strip().upper() for p in po_list.split(",") if p.strip()]
        if len(pos) > MAX_PO:
            return jsonify({"error": f"Massimo {MAX_PO} PO per richiesta"}), 400

        if limit > MAX_LIMIT:
            limit = MAX_LIMIT

        if offset > MAX_OFFSET:
            return jsonify({"error": "Offset troppo grande!"}), 400

        items = supabase.table("ordini_vendor_items")\
            .select("po_number,model_number,qty_ordered,qty_confirmed,cost")\
            .in_("po_number", pos)\
            .order("po_number")\
            .order("model_number")\
            .range(offset, offset + limit - 1)\
            .execute().data

        return jsonify(items)
    except Exception as ex:
        import logging
        logging.exception("Errore in get_items_by_po")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500





@bp.route('/api/amazon/vendor/parziali-ordine', methods=['GET'])
def parziali_per_ordine():
    center = request.args.get("center")
    data = request.args.get("data")
    offset = int(request.args.get("offset", 0))
    limit = int(request.args.get("limit", 100))
    if not center or not data:
        return jsonify([])
    try:
        riepilogo = supabase.table("ordini_vendor_riepilogo") \
            .select("id") \
            .eq("fulfillment_center", center) \
            .eq("start_delivery", data) \
            .execute().data
        if not riepilogo:
            return jsonify([])
        riepilogo_id = riepilogo[0]["id"]
        parziali = supabase.table("ordini_vendor_parziali") \
            .select("numero_parziale, dati, confermato, gestito, created_at, conferma_collo") \
            .eq("riepilogo_id", riepilogo_id) \
            .eq("confermato", True) \
            .order("numero_parziale") \
            .range(offset, offset+limit-1) \
            .execute().data
        return jsonify(parziali)
    except Exception as ex:
        logging.exception("Errore in parziali_per_ordine")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500


@bp.route('/api/amazon/vendor/orders/list', methods=['GET'])
def list_vendor_pos():
    try:
        access_token = get_spapi_access_token()
        awsauth = AWS4Auth(
            os.getenv("AWS_ACCESS_KEY"),
            os.getenv("AWS_SECRET_KEY"),
            'eu-west-1', 'execute-api',
            session_token=os.getenv("AWS_SESSION_TOKEN")
        )
        url = "https://sellingpartnerapi-eu.amazon.com/vendor/orders/v1/purchaseOrders"
        from datetime import datetime, timedelta
        today = datetime.utcnow()
        seven_days_ago = today - timedelta(days=7)
        params = {
            "createdAfter": seven_days_ago.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "createdBefore": today.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "limit": request.args.get("limit", 50)
        }
        headers = {
            "x-amz-access-token": access_token,
            "Content-Type": "application/json"
        }
        resp = requests.get(url, auth=awsauth, headers=headers, params=params)
        logging.info(f"Amazon Vendor Orders Response: {resp.status_code} {resp.text[:200]}")
        # Torna semplicemente la risposta di Amazon (anche errore)
        return (resp.text, resp.status_code, {'Content-Type': 'application/json'})
    except Exception as ex:
        logging.exception("Errore chiamata Amazon Vendor Orders")
        return jsonify({"error": f"Errore chiamata Amazon: {str(ex)}"}), 500


@bp.route('/api/amazon/vendor/orders/lista-prelievo/nuovi/pdf', methods=['GET'])
def export_lista_prelievo_nuovi_pdf():
    try:
        filtro_data = request.args.get("data")
        query = supabase.table("ordini_vendor_riepilogo") \
            .select("fulfillment_center, start_delivery, po_list") \
            .eq("stato_ordine", "nuovo")
        if filtro_data:
            query = query.eq("start_delivery", filtro_data)
        riepiloghi = query.execute().data

        if not riepiloghi:
            return Response("Nessun articolo trovato.", status=404)

        tutte_le_date = set(r["start_delivery"] for r in riepiloghi if r.get("start_delivery"))

        def get_titolo_data(filtro_data, tutte_le_date):
            def format_it(dt):
                if not dt:
                    return ""
                parts = dt.split("-")
                if len(parts) == 3:
                    return f"{parts[2]}-{parts[1]}-{parts[0]}"
                return dt
            if filtro_data:
                return format_it(filtro_data)
            tutte = sorted(list(tutte_le_date))
            if len(tutte) == 1:
                return format_it(tutte[0])
            else:
                return ", ".join(format_it(x) for x in tutte)

        titolo_data = get_titolo_data(filtro_data, tutte_le_date)

        po_set = set()
        for r in riepiloghi:
            for po in r["po_list"] or []:
                po_set.add(po)
        if not po_set:
            return Response("Nessun articolo trovato.", status=404)

        articoli = supabase.table("ordini_vendor_items") \
            .select("model_number,vendor_product_id,title,qty_ordered,fulfillment_center") \
            .in_("po_number", list(po_set)) \
            .execute().data

        sku_map = {}
        for art in articoli:
            sku = art["model_number"]
            barcode_val = art.get("vendor_product_id", "")
            centro = art["fulfillment_center"]
            qty = int(art["qty_ordered"])
            if sku not in sku_map:
                sku_map[sku] = {
                    "barcode": barcode_val,
                    "centri": {},
                    "totale": 0,
                    "radice": sku.split("-")[0] if sku else "",
                }
            sku_map[sku]["centri"][centro] = sku_map[sku]["centri"].get(centro, 0) + qty
            sku_map[sku]["totale"] += qty

        gruppi = {}
        for sku, dati in sku_map.items():
            radice = dati["radice"]
            gruppi.setdefault(radice, []).append((sku, dati))
        for v in gruppi.values():
            v.sort(key=lambda x: x[0])
        sorted_radici = sorted(gruppi.items(), key=lambda x: x[0])

        pdf_width = 297
        margin = 10
        table_width = pdf_width - 2 * margin

        widths = {
            "Barcode": 40,
            "SKU": 55,
            "EAN": 38,
            "Centri": 105,
            "Totale": 20,
            "Riscontro": 19
        }
        widths_sum = sum(widths.values())
        factor = table_width / widths_sum
        for k in widths:
            widths[k] = widths[k] * factor
        header = ["Barcode", "SKU", "EAN", "Centri", "Totale", "Riscontro"]
        row_height = 18

        def add_header(pdf, radice):
            pdf.add_page()
            pdf.set_left_margin(margin)
            pdf.set_right_margin(margin)
            pdf.set_x(margin)
            pdf.set_font("Arial", "B", 14)
            pdf.cell(table_width, 10, f"Lista Prelievo Articoli {titolo_data}", 0, 1, "C")
            pdf.set_font("Arial", "B", 11)
            pdf.set_x(margin)
            pdf.cell(table_width, 7, f"Tipologia: {radice}", 0, 1, "L")
            pdf.ln(2)
            pdf.set_fill_color(210, 210, 210)
            pdf.set_font("Arial", "B", 9)
            pdf.set_x(margin)
            for k in header:
                pdf.cell(widths[k], 8, k, border=1, align="C", fill=True)
            pdf.ln()
            pdf.set_font("Arial", "", 8)

        pdf = FPDF(orientation='L', unit='mm', format='A4')
        pdf.set_auto_page_break(auto=False)
        margin_bottom = 10

        for idx_radice, (radice, sku_group) in enumerate(sorted_radici):
            add_header(pdf, radice)
            for sku, dati in sku_group:
                barcode_val = str(dati["barcode"] or "")
                centri_attivi = [f"{c}({dati['centri'][c]})" for c in sorted(dati["centri"]) if dati["centri"][c] > 0]
                centri_str = " ".join(centri_attivi)
                row = [
                    barcode_val,
                    sku or "",
                    barcode_val,
                    centri_str,
                    str(dati["totale"]),
                    ""
                ]
                # Check for space for a new row, else new page (NO header)
                if pdf.get_y() + row_height + margin_bottom > 210:
                    pdf.add_page()
                    pdf.set_left_margin(margin)
                    pdf.set_right_margin(margin)
                    pdf.set_x(margin)

                y = pdf.get_y()
                pdf.set_x(margin)
                barcode_written = False
                if barcode_val.isdigit() and 8 <= len(barcode_val) <= 13:
                    if len(barcode_val) == 13:
                        barcode_type = 'ean13'
                    else:
                        barcode_type = 'code128'
                    CODE = get_barcode_class(barcode_type)
                    rv = BytesIO()
                    bc = CODE(barcode_val, writer=ImageWriter())
                    bc.write(rv)
                    rv.seek(0)
                    img = Image.open(rv)
                    img_buffer = BytesIO()
                    img.save(img_buffer, format="PNG")
                    img_buffer.seek(0)
                    pdf.cell(widths["Barcode"], row_height, "", border=1, align="C")
                    img_y = y + 2
                    img_x = pdf.get_x() - widths["Barcode"] + 2
                    pdf.image(img_buffer, x=img_x, y=img_y, w=widths["Barcode"]-4, h=row_height-4)
                    pdf.set_x(pdf.get_x())
                    barcode_written = True
                if not barcode_written:
                    pdf.cell(widths["Barcode"], row_height, barcode_val, border=1, align="C")

                row_data = row[1:]
                keys = ["SKU", "EAN", "Centri", "Totale", "Riscontro"]
                for key, val in zip(keys, row_data):
                    pdf.cell(widths[key], row_height, val, border=1, align="C")
                pdf.ln(row_height)

        pdf_bytes = bytes(pdf.output(dest='S'))
        return Response(
            pdf_bytes,
            mimetype='application/pdf',
            headers={"Content-disposition": f"attachment; filename=lista_prelievo_{titolo_data.replace(', ', '_')}_{datetime.utcnow().date()}.pdf"}
        )
    except Exception as ex:
        logging.exception("[export_lista_prelievo_nuovi_pdf] Errore generazione PDF")
        return Response(f"Errore generazione PDF: {str(ex)}", status=500)
    
    
    # TEEESTTTTTTTTTT
import logging

@bp.route('/api/amazon/vendor/asn/test', methods=['POST'])
def test_asn_submit():
    try:
        payload = request.json
        access_token = get_spapi_access_token()

        url = "https://sellingpartnerapi-eu.amazon.com/vendor/directFulfillment/shipping/2021-12-28/shipmentConfirmations"
        headers = {
            "x-amz-access-token": access_token,
            "Content-Type": "application/json"
        }

        # LOGGA la richiesta completa prima di inviarla!
        logging.warning(f"ASN SUBMIT REQUEST URL: {url}")
        logging.warning(f"ASN SUBMIT HEADERS: {headers}")
        logging.warning(f"ASN SUBMIT BODY: {payload}")

        resp = requests.post(url, json=payload, headers=headers)
        
        # LOGGA la response (status + text)
        logging.warning(f"ASN SUBMIT RESPONSE STATUS: {resp.status_code}")
        logging.warning(f"ASN SUBMIT RESPONSE TEXT: {resp.text}")

        # Se la risposta non è 2xx, logga anche i dettagli
        if resp.status_code >= 400:
            logging.error(f"ASN ERROR RESPONSE: {resp.text}")

        # Torna la risposta Amazon "grezza" con anche dettagli utili per il frontend
        return jsonify({
            "status_code": resp.status_code,
            "request_url": url,
            "request_headers": dict(headers),
            "request_body": payload,
            "amazon_response": resp.json() if resp.text.startswith("{") else resp.text
        }), resp.status_code

    except Exception as ex:
        logging.exception("Errore durante la submit ASN!")
        return jsonify({
            "error": "Eccezione interna ASN",
            "detail": str(ex)
        }), 500


@bp.route('/api/amazon/vendor/items/by-barcode', methods=['GET'])
def find_items_by_barcode():
    max_retries = 3
    for attempt in range(max_retries):
        try:
            barcode = request.args.get('barcode')
            if not barcode:
                return jsonify([])

            riepiloghi = supabase.table("ordini_vendor_riepilogo") \
                .select("po_list,fulfillment_center,start_delivery,id") \
                .in_("stato_ordine", ["nuovo", "parziale"]) \
                .execute().data

            po_centro_map = {}
            po_riepilogo_id_map = {}
            for r in riepiloghi:
                for po in r["po_list"]:
                    po_centro_map[po] = {
                        "fulfillment_center": r["fulfillment_center"],
                        "start_delivery": r["start_delivery"],
                    }
                    po_riepilogo_id_map[po] = r.get("id") 

            po_list = list(po_centro_map.keys())
            if not po_list:
                return jsonify([])

            # Limit per evitare carico inutile!
            articoli = supabase.table("ordini_vendor_items") \
                .select("*") \
                .in_("po_number", po_list) \
                .or_(f"vendor_product_id.eq.{barcode},model_number.eq.{barcode}") \
                .limit(30) \
                .execute().data

            for a in articoli:
                info = po_centro_map.get(a["po_number"], {})
                a["fulfillment_center"] = info.get("fulfillment_center")
                a["start_delivery"] = info.get("start_delivery")

            riepilogo_ids = list(set(po_riepilogo_id_map.get(a["po_number"]) for a in articoli if po_riepilogo_id_map.get(a["po_number"])))
            if not riepilogo_ids:
                for a in articoli:
                    a["qty_inserted"] = 0
                return jsonify(articoli)

            parziali = supabase.table("ordini_vendor_parziali") \
                .select("dati") \
                .in_("riepilogo_id", riepilogo_ids) \
                .execute().data

            from collections import defaultdict
            import json
            qty_inserted_map = defaultdict(int)
            for p in parziali:
                dati = p["dati"]
                if isinstance(dati, str):
                    try:
                        dati = json.loads(dati)
                    except Exception:
                        dati = []
                if not isinstance(dati, list):
                    continue
                for d in dati:
                    key = (d.get("po_number"), d.get("model_number"))
                    try:
                        qty_inserted_map[key] += int(d.get("quantita", 0))
                    except Exception:
                        pass

            for a in articoli:
                key = (a["po_number"], a["model_number"])
                a["qty_inserted"] = qty_inserted_map.get(key, 0)

            return jsonify(articoli)

        except httpx.RemoteProtocolError:
            import time
            logging.warning(f"[find_items_by_barcode] Errore di rete verso Supabase (tentativo {attempt+1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            else:
                return jsonify({"error": "Errore di connessione a Supabase. Riprova."}), 503
        except Exception as ex:
            logging.exception("[find_items_by_barcode] Errore nella ricerca per barcode")
            return jsonify({"error": f"Errore interno: {str(ex)}"}), 500



@bp.route('/api/amazon/vendor/orders/riepilogo/dashboard', methods=['GET'])
def riepilogo_dashboard_parziali():
    max_retries = 3
    for attempt in range(max_retries):
        try:
            offset = int(request.args.get("offset", 0))
            limit = int(request.args.get("limit", 100))
            dashboard = []

            # Prendi max 100 riepiloghi "nuovo/parziale" (con paginazione)
            riepiloghi = supabase.table("ordini_vendor_riepilogo") \
                .select("*") \
                .in_("stato_ordine", ["nuovo", "parziale"]) \
                .order("created_at", desc=True) \
                .range(offset, offset+limit-1) \
                .execute().data

            if not riepiloghi:
                return jsonify([])

            riepilogo_ids = [r.get("id") or r.get("riepilogo_id") for r in riepiloghi]
            parziali = supabase.table("ordini_vendor_parziali") \
                .select("riepilogo_id,numero_parziale,dati,conferma_collo") \
                .in_("riepilogo_id", riepilogo_ids) \
                .execute().data

            from collections import defaultdict
            parziali_per_riep = defaultdict(list)
            for p in parziali:
                parziali_per_riep[p["riepilogo_id"]].append(p)

            import json
            for r in riepiloghi:
                fulfillment_center = r["fulfillment_center"]
                start_delivery = r["start_delivery"]
                stato_ordine = r["stato_ordine"]
                po_list = r["po_list"]
                riepilogo_id = r.get("id") or r.get("riepilogo_id")

                my_parziali = parziali_per_riep.get(riepilogo_id, [])

                if not my_parziali:
                    dashboard.append({
                        "fulfillment_center": fulfillment_center,
                        "start_delivery": start_delivery,
                        "stato_ordine": stato_ordine,
                        "numero_parziale": None,
                        "colli_totali": 0,
                        "colli_confermati": 0,
                        "po_list": po_list,
                        "riepilogo_id": riepilogo_id,
                    })
                    continue

                for p in my_parziali:
                    numero_parziale = p.get("numero_parziale") or 1
                    dati = p["dati"]
                    if isinstance(dati, str):
                        try:
                            dati = json.loads(dati)
                        except Exception:
                            dati = []
                    colli_totali_set = set()
                    if isinstance(dati, list):
                        for d in dati:
                            collo = d.get("collo")
                            if collo is not None:
                                colli_totali_set.add(collo)
                    conferma_collo = p.get("conferma_collo")
                    if isinstance(conferma_collo, str):
                        try:
                            conferma_collo = json.loads(conferma_collo)
                        except Exception:
                            conferma_collo = {}
                    colli_confermati_set = set()
                    if isinstance(conferma_collo, dict):
                        for k, v in conferma_collo.items():
                            if v:
                                try:
                                    colli_confermati_set.add(int(k))
                                except Exception:
                                    pass
                    dashboard.append({
                        "fulfillment_center": fulfillment_center,
                        "start_delivery": start_delivery,
                        "stato_ordine": stato_ordine,
                        "numero_parziale": numero_parziale,
                        "colli_totali": len(colli_totali_set),
                        "colli_confermati": len(colli_confermati_set),
                        "po_list": po_list,
                        "riepilogo_id": riepilogo_id,
                    })
            return jsonify(dashboard)
        except Exception as ex:
            import httpx
            import time
            import logging
            if isinstance(ex, httpx.RemoteProtocolError):
                logging.warning(f"[riepilogo_dashboard_parziali] Connessione interrotta verso Supabase (tentativo {attempt+1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                else:
                    logging.exception("[riepilogo_dashboard_parziali] Errore definitivo")
                    return jsonify({"error": "Connessione a Supabase interrotta. Riprova tra poco."}), 503
            logging.exception("[riepilogo_dashboard_parziali] Errore dashboard parziali")
            return jsonify({"error": f"Errore interno: {str(ex)}"}), 500


@bp.route('/api/amazon/vendor/orders/lista-ordini/nuovi/pdf', methods=['GET'])
def export_lista_ordini_nuovi_pdf():
    try:
        # 1. Prendi tutti i riepiloghi "nuovo"
        riepiloghi = supabase.table("ordini_vendor_riepilogo") \
            .select("fulfillment_center, start_delivery, po_list") \
            .eq("stato_ordine", "nuovo") \
            .execute().data
        if not riepiloghi:
            return Response("Nessun ordine trovato.", status=404)

        # 2. Raggruppa per centro (destinazione)
        centri_map = {}
        for r in riepiloghi:
            centro = r["fulfillment_center"]
            if centro not in centri_map:
                centri_map[centro] = {
                    "start_delivery": r["start_delivery"],
                    "po_list": set(r["po_list"] or []),
                }
            else:
                centri_map[centro]["po_list"].update(r["po_list"] or [])

        # 3. Prendi TUTTI gli articoli dei PO coinvolti
        all_po = set()
        for v in centri_map.values():
            all_po.update(v["po_list"])
        if not all_po:
            return Response("Nessun articolo trovato.", status=404)

        articoli = supabase.table("ordini_vendor_items") \
            .select("model_number,vendor_product_id,title,qty_ordered,fulfillment_center") \
            .in_("po_number", list(all_po)) \
            .execute().data

        # 4. Raggruppa articoli per centro e SKU, somma quantità
        centri_articoli = {}
        for centro, info in centri_map.items():
            lista = [
                a for a in articoli
                if a["fulfillment_center"] == centro
            ]
            sku_map = {}
            for art in lista:
                sku = art["model_number"]
                ean = art.get("vendor_product_id") or ""
                qty = int(art.get("qty_ordered") or 0)
                if sku not in sku_map:
                    sku_map[sku] = {
                        "sku": sku,
                        "ean": ean,
                        "qty": 0,
                    }
                sku_map[sku]["qty"] += qty
            centri_articoli[centro] = {
                "start_delivery": info["start_delivery"],
                "articoli": sorted(sku_map.values(), key=lambda x: x["sku"])
            }

        # 5. Genera PDF
        pdf = FPDF(orientation='L', unit='mm', format='A4')
        margin = 10
        table_width = 297 - 2 * margin
        widths = {
            "SKU": 58,
            "EAN": 37,
            "Qta": 22,
            "Riscontro": 18
        }
        widths_sum = sum(widths.values())
        factor = table_width / widths_sum
        for k in widths:
            widths[k] = widths[k] * factor
        header = ["SKU", "EAN", "Qta", "Riscontro"]
        row_height = 10

        def add_header(pdf, centro, data):
            pdf.add_page()
            pdf.set_left_margin(margin)
            pdf.set_right_margin(margin)
            pdf.set_font("Arial", "B", 15)
            pdf.cell(table_width, 10, f"Ordine {centro}" , 0, 1, "C")
            pdf.set_font("Arial", "", 10)
            pdf.set_font("Arial", "B", 10)
            pdf.set_fill_color(210, 210, 210)
            for k in header:
                pdf.cell(widths[k], 8, k, border=1, align="C", fill=True)
            pdf.ln()

        for centro, info in centri_articoli.items():
            add_header(pdf, centro, info["start_delivery"])
            for art in info["articoli"]:
                row = [
                    art["sku"],
                    art["ean"],
                    str(art["qty"]),
                    ""
                ]
                for key, val in zip(header, row):
                    pdf.cell(widths[key], row_height, val, border=1, align="C")
                pdf.ln(row_height)

        # 6. Output PDF
        pdf_bytes = bytes(pdf.output(dest='S'))
        return Response(
            pdf_bytes,
            mimetype='application/pdf',
            headers={"Content-disposition": f"attachment; filename=lista_ordini_per_centro_{datetime.utcnow().date()}.pdf"}
        )
    except Exception as ex:
        logging.exception("[export_lista_ordini_nuovi_pdf] Errore generazione PDF")
        return Response(f"Errore generazione PDF: {str(ex)}", status=500)
    
    
@bp.route('/api/amazon/vendor/orders/riepilogo/completati', methods=['GET'])
def riepilogo_completati():
    try:
        riepiloghi = supabase.table("ordini_vendor_riepilogo") \
            .select("*") \
            .eq("stato_ordine", "completato") \
            .order("created_at", desc=False) \
            .execute().data
        return jsonify(riepiloghi)
    except Exception as ex:
        logging.exception("Errore in riepilogo_completati")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500

@bp.route('/api/amazon/vendor/parziali/gestito', methods=['PATCH'])
def aggiorna_parziale_gestito():
    try:
        data = request.json
        riepilogo_id = data.get("riepilogo_id")
        numero_parziale = data.get("numero_parziale")
        gestito = data.get("gestito")

        if riepilogo_id is None or numero_parziale is None or gestito is None:
            return jsonify({"error": "Parametri mancanti"}), 400

        supabase.table("ordini_vendor_parziali") \
            .update({"gestito": gestito}) \
            .eq("riepilogo_id", riepilogo_id) \
            .eq("numero_parziale", numero_parziale) \
            .execute()

        return jsonify({"ok": True, "gestito": gestito})
    except Exception as ex:
        logging.exception("Errore in aggiorna_parziale_gestito")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500



# --- FUNZIONE DI LOG MOVIMENTI PRODUZIONE ---
def log_movimento_produzione(
    produzione_row,
    utente,
    motivo,
    stato_vecchio=None,
    stato_nuovo=None,
    qty_vecchia=None,
    qty_nuova=None,
    plus_vecchio=None,
    plus_nuovo=None,
    dettaglio=None
):
    supabase.table("movimenti_produzione_vendor").insert({
        "produzione_id": produzione_row["id"],
        "sku": produzione_row.get("sku"),
        "ean": produzione_row.get("ean"),
        "start_delivery": produzione_row.get("start_delivery"),
        "stato_vecchio": stato_vecchio,
        "stato_nuovo": stato_nuovo,
        "qty_vecchia": qty_vecchia,
        "qty_nuova": qty_nuova,
        "plus_vecchio": plus_vecchio,
        "plus_nuovo": plus_nuovo,
        "utente": utente,
        "motivo": motivo,
        "dettaglio": dettaglio,
        "created_at": datetime.now().isoformat()
    }).execute()


def log_movimenti_produzione_bulk(rows, utente, motivo):
    """
    Inserisce i movimenti di log in bulk, un solo insert.
    rows: lista di dict (produzione_vendor) da loggare
    utente: string
    motivo: string
    """
    logs = []
    now = datetime.now().isoformat()
    for r in rows:
        logs.append({
            "produzione_id": r["id"],
            "sku": r.get("sku"),
            "ean": r.get("ean"),
            "start_delivery": r.get("start_delivery"),
            "stato_vecchio": r.get("stato_produzione"),
            "stato_nuovo": None,
            "qty_vecchia": r.get("da_produrre"),
            "qty_nuova": None,
            "plus_vecchio": r.get("plus"),
            "plus_nuovo": None,
            "utente": utente,
            "motivo": motivo,
            "dettaglio": None,
            "created_at": now
        })
    if logs:
        supabase.table("movimenti_produzione_vendor").insert(logs).execute()


# --- DATE DISPONIBILI ---
@bp.route('/api/prelievi/date-importabili', methods=['GET'])
def date_importabili_prelievo():
    try:
        res = supabase.table("ordini_vendor_riepilogo")\
            .select("start_delivery")\
            .eq("stato_ordine", "nuovo")\
            .order("start_delivery")\
            .execute()
        date_set = sorted(list(set(r["start_delivery"] for r in res.data)))
        return jsonify(date_set)
    except Exception as ex:
        logging.exception("Errore in date_importabili_prelievo")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500

# --- IMPORTA PRELIEVI ---
import logging

@bp.route('/api/prelievi/importa', methods=['POST'])
def importa_prelievi():
    max_retries = 3
    try:
        data = request.json.get("data")
        if not data:
            return jsonify({"error": "Data richiesta"}), 400

        # Elimina i prelievi precedenti (su questa data)
        for attempt in range(max_retries):
            try:
                supabase.table("prelievi_ordini_amazon").delete().eq("start_delivery", data).execute()
                break
            except Exception as ex:
                logging.warning(f"[importa_prelievi] Errore delete tentativo {attempt+1}/{max_retries}: {ex}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise

        items = supabase.table("ordini_vendor_items").select("*").eq("start_delivery", data).execute().data
        riepiloghi = supabase.table("ordini_vendor_riepilogo")\
            .select("fulfillment_center,start_delivery,stato_ordine")\
            .eq("start_delivery", data)\
            .eq("stato_ordine", "nuovo")\
            .execute().data

        centri_validi = set((r["fulfillment_center"], str(r["start_delivery"])) for r in riepiloghi)
        articoli = [i for i in items if (i["fulfillment_center"], str(i["start_delivery"])) in centri_validi]

        aggrega = {}
        for a in articoli:
            key = (a["model_number"], a["vendor_product_id"], str(a["start_delivery"]))
            if key not in aggrega:
                aggrega[key] = {
                    "sku": a["model_number"],
                    "ean": a["vendor_product_id"],
                    "radice": a["model_number"].split("-")[0] if a["model_number"] else "",
                    "start_delivery": a["start_delivery"][:10],
                    "qty": 0,
                    "centri": {}
                }
            centro = a["fulfillment_center"]
            qty = int(a["qty_ordered"] or 0)
            aggrega[key]["qty"] += qty
            aggrega[key]["centri"][centro] = aggrega[key]["centri"].get(centro, 0) + qty

        lista_to_insert = []
        for agg in aggrega.values():
            lista_to_insert.append({
                "sku": agg["sku"],
                "ean": agg["ean"],
                "qty": agg["qty"],
                "radice": agg["radice"],
                "start_delivery": agg["start_delivery"],
                "centri": agg["centri"],
                "stato": "in verifica"
            })

        batch_size = 200
        batch_results = []
        errors = []
        inserted_total = 0

        for i in range(0, len(lista_to_insert), batch_size):
            batch = lista_to_insert[i:i+batch_size]
            for attempt in range(max_retries):
                try:
                    result = supabase.table("prelievi_ordini_amazon").insert(batch).execute()
                    inserted_total += len(batch)
                    batch_results.append({"start": i, "end": i+len(batch)-1, "ok": True})
                    break
                except Exception as ex:
                    logging.warning(f"Errore batch import prelievo [{i}-{i+len(batch)-1}], tentativo {attempt+1}/{max_retries}: {ex}")
                    if attempt < max_retries - 1:
                        time.sleep(1)
                    else:
                        errors.append({"start": i, "end": i+len(batch)-1, "error": str(ex)})
                        batch_results.append({"start": i, "end": i+len(batch)-1, "ok": False, "error": str(ex)})

        return jsonify({
            "ok": inserted_total == len(lista_to_insert),
            "importati": inserted_total,
            "totali": len(lista_to_insert),
            "batch_results": batch_results,
            "errors": errors
        })
    except Exception as ex:
        logging.exception("Errore generale in importa_prelievi")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500


# --- LISTA PRELIEVI ---
@bp.route('/api/prelievi', methods=['GET'])
def lista_prelievi():
    try:
        data = request.args.get("data")
        radice = request.args.get("radice")
        search = request.args.get("search", "").strip()

        query = supabase.table("prelievi_ordini_amazon").select(
            "id,stato,sku,ean,qty,riscontro,plus,radice,note"
        )
        if data:
            query = query.eq("start_delivery", data)
        if radice:
            query = query.eq("radice", radice)
        if search:
            query = query.or_(f"sku.ilike.%{search}%,ean.ilike.%{search}%")
        query = query.order("radice").order("sku")
        prelievi = query.execute().data
        return jsonify(prelievi)
    except Exception as ex:
        logging.exception("[lista_prelievi] Errore in GET prelievi")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500

# --- FUNZIONE CENTRALE SYNC PRODUZIONE (usata SOLO su patch singolo/bulk) ---
def sync_produzione(prelievi_modificati, utente="operatore", motivo="Modifica prelievo"):
    # Carica tutte le righe produzione non "Rimossi"
    tutte = [
        r for r in supabase.table("produzione_vendor").select("*").execute().data
        if r["stato_produzione"] != "Rimossi"
    ]

    # CLEANUP: elimina vecchie "Da Stampare" (stesso SKU+EAN ma con data diversa)
    chiavi_nuovi = set((p["sku"], p.get("ean")) for p in prelievi_modificati)
    date_nuove = set(p.get("start_delivery") for p in prelievi_modificati)

    vecchie_da_stampare = [
        r for r in tutte
        if r["stato_produzione"] == "Da Stampare"
        and (r["sku"], r.get("ean")) in chiavi_nuovi
        and r.get("start_delivery") not in date_nuove
    ]

    log_entries = []
    # Batch delete vecchie "Da Stampare"
    if vecchie_da_stampare:
        ids_da_eliminare = [r["id"] for r in vecchie_da_stampare]
        for r in vecchie_da_stampare:
            log_entries.append(dict(
                produzione_row=r,
                utente=utente,
                motivo="Auto-eliminazione Da Stampare su cambio data",
                qty_vecchia=r["da_produrre"],
                qty_nuova=0
            ))
        batch_size = 100
        for i in range(0, len(ids_da_eliminare), batch_size):
            batch = ids_da_eliminare[i:i+batch_size]
            try:
                supabase.table("produzione_vendor").delete().in_("id", batch).execute()
            except Exception as ex:
                logging.error(f"Errore delete produzione_vendor batch={i}-{i+batch_size}: {ex}")

    # PREPARA le modifiche batch (update/delete/insert)
    to_update = []
    to_delete = []
    to_insert = []

    for p in prelievi_modificati:
        key = (p["sku"], p.get("ean"), p.get("start_delivery"))
        righe_attuali = [r for r in tutte if (r["sku"], r.get("ean"), r.get("start_delivery")) == key]
        righe_lavorate = [
            r for r in tutte
            if r["sku"] == p["sku"]
            and r.get("ean") == p.get("ean")
            and r["stato_produzione"] != "Da Stampare"
        ]
        lavorato = sum(r["da_produrre"] for r in righe_lavorate)
        da_stampare_righe = [r for r in righe_attuali if r["stato_produzione"] == "Da Stampare"]
        qty = p["qty"]
        riscontro = p.get("riscontro") or 0
        plus = p.get("plus") or 0
        stato = p["stato"]

        if stato == "manca":
            richiesta = qty
        elif stato == "parziale":
            richiesta = qty - riscontro
        elif stato == "completo":
            richiesta = 0
        else:
            richiesta = qty

        if lavorato >= richiesta:
            da_produrre = plus if plus > 0 else 0
        else:
            da_produrre = (richiesta - lavorato) + plus

        if da_stampare_righe:
            r_da_stampare = da_stampare_righe[0]
            if da_produrre > 0:
                if r_da_stampare["da_produrre"] != da_produrre:
                    log_entries.append(dict(
                        produzione_row=r_da_stampare,
                        utente=utente,
                        motivo=motivo,
                        qty_vecchia=r_da_stampare["da_produrre"],
                        qty_nuova=da_produrre
                    ))
                to_update.append({
                    "id": r_da_stampare["id"],
                    "da_produrre": da_produrre,
                    "qty": qty,
                    "riscontro": riscontro,
                    "plus": plus,
                    "stato": stato,
                    "note": p.get("note") or "",
                    "stato_produzione": "Da Stampare",
                    "modificata_manualmente": False
                })
            else:
                log_entries.append(dict(
                    produzione_row=r_da_stampare,
                    utente=utente,
                    motivo="Auto-eliminazione Da Stampare su sync",
                    qty_vecchia=r_da_stampare["da_produrre"],
                    qty_nuova=0
                ))
                to_delete.append(r_da_stampare["id"])
        else:
            if da_produrre > 0:
                nuovo = {
                    "prelievo_id": p["id"],
                    "sku": p["sku"],
                    "ean": p["ean"],
                    "qty": qty,
                    "riscontro": riscontro,
                    "plus": plus,
                    "radice": p["radice"],
                    "start_delivery": p.get("start_delivery"),
                    "stato": stato,
                    "stato_produzione": "Da Stampare",
                    "da_produrre": da_produrre,
                    "cavallotti": p.get("cavallotti", False),
                    "note": p.get("note") or "",
                }
                to_insert.append(nuovo)

    # BATCH UPDATE
    if to_update:
        for row in to_update:
            id_val = row.pop("id")
            try:
                supabase.table("produzione_vendor").update(row).eq("id", id_val).execute()
            except Exception as ex:
                logging.error(f"Errore update produzione_vendor id={id_val}: {ex}")

    # BATCH DELETE
    if to_delete:
        for id_del in to_delete:
            try:
                supabase.table("produzione_vendor").delete().eq("id", id_del).execute()
            except Exception as ex:
                logging.error(f"Errore delete produzione_vendor id={id_del}: {ex}")

    # BATCH INSERT
    if to_insert:
        batch_size = 100
        for i in range(0, len(to_insert), batch_size):
            batch = to_insert[i:i+batch_size]
            try:
                inserted = supabase.table("produzione_vendor").insert(batch).execute().data
                for irow in inserted or []:
                    log_entries.append(dict(
                        produzione_row=irow,
                        utente=utente,
                        motivo="Creazione da patch prelievo",
                        qty_nuova=irow["da_produrre"]
                    ))
            except Exception as ex:
                logging.error(f"Errore insert produzione_vendor batch={i}-{i+batch_size}: {ex}")

    # LOG MOVIMENTI IN BATCH
    if log_entries:
        mov_rows = []
        for entry in log_entries:
            r = entry.get("produzione_row")
            mov_rows.append({
                "produzione_id": r["id"],
                "sku": r.get("sku"),
                "ean": r.get("ean"),
                "start_delivery": r.get("start_delivery"),
                "stato_vecchio": entry.get("stato_vecchio"),
                "stato_nuovo": entry.get("stato_nuovo"),
                "qty_vecchia": entry.get("qty_vecchia"),
                "qty_nuova": entry.get("qty_nuova"),
                "plus_vecchio": entry.get("plus_vecchio"),
                "plus_nuovo": entry.get("plus_nuovo"),
                "utente": entry.get("utente"),
                "motivo": entry.get("motivo"),
                "dettaglio": entry.get("dettaglio"),
                "created_at": datetime.now().isoformat()
            })
        batch_size = 200
        for i in range(0, len(mov_rows), batch_size):
            batch = mov_rows[i:i+batch_size]
            try:
                supabase.table("movimenti_produzione_vendor").insert(batch).execute()
            except Exception as ex:
                logging.error(f"Errore insert movimenti_produzione_vendor: {ex}")


# --- PATCH SINGOLO PRELIEVO + SYNC PRODUZIONE ---
@bp.route('/api/prelievi/<int:id>', methods=['PATCH'])
def patch_prelievo(id):
    try:
        data = request.json
        fields = {}
        for f in ["riscontro", "plus", "note"]:
            if f in data:
                fields[f] = data[f]

        # ===== VALIDAZIONE DATI =====
        # Riscontro: deve essere un int >= 0 o None
        if "riscontro" in fields:
            riscontro = fields["riscontro"]
            if riscontro is not None and (not isinstance(riscontro, int) or riscontro < 0):
                return jsonify({"error": "Riscontro non valido: deve essere un numero >= 0"}), 400

        # Plus: deve essere un int >= 0 o None
        if "plus" in fields:
            plus = fields["plus"]
            if plus is not None and (not isinstance(plus, int) or plus < 0):
                return jsonify({"error": "Plus non valido: deve essere un numero >= 0"}), 400

        # Nota: max 255 caratteri, solo caratteri accettati
        import re
        if "note" in fields:
            note = fields["note"] or ""
            if len(note) > 255:
                return jsonify({"error": "Nota troppo lunga (max 255 caratteri)"}), 400
            # Togli il controllo sui caratteri consentiti!
            fields["note"] = note.strip()


        if "riscontro" in data:
            prelievo = supabase.table("prelievi_ordini_amazon").select("qty").eq("id", id).single().execute().data
            if not prelievo:
                return jsonify({"error": "Prelievo non trovato"}), 404
            qty = prelievo["qty"]
            riscontro = data["riscontro"]
            if riscontro is None:
                stato = "in verifica"
            elif riscontro == 0:
                stato = "manca"
            elif 0 < riscontro < qty:
                stato = "parziale"
            elif riscontro == qty:
                stato = "completo"
            else:
                stato = "in verifica"
            fields["stato"] = stato

        if not fields:
            return jsonify({"error": "Nessun campo da aggiornare"}), 400

        # Update del prelievo
        supabase.table("prelievi_ordini_amazon").update(fields).eq("id", id).execute()

        # Ricarica il prelievo completo per sync_produzione
        prelievo = supabase.table("prelievi_ordini_amazon").select("*").eq("id", id).single().execute().data
        if not prelievo:
            return jsonify({"error": "Prelievo non trovato dopo update"}), 404

        sync_produzione([prelievo], utente="operatore", motivo="Patch singolo prelievo")
        return jsonify({"ok": True})

    except Exception as ex:
        logging.exception("[patch_prelievo] Errore patch singolo prelievo")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500



@bp.route('/api/prelievi/bulk', methods=['PATCH'])
def patch_prelievi_bulk():
    try:
        ids = request.json.get("ids", [])
        update_fields = request.json.get("fields", {})
        if not ids or not update_fields:
            return jsonify({"error": "Nessun id/campo"}), 400

        # === VALIDAZIONE NUMERICA/BASIC PER BULK ===
        import re
        if "riscontro" in update_fields:
            r = update_fields["riscontro"]
            if r is not None and (not isinstance(r, int) or r < 0):
                return jsonify({"error": "Riscontro non valido: deve essere un numero >= 0"}), 400
        if "plus" in update_fields:
            p = update_fields["plus"]
            if p is not None and (not isinstance(p, int) or p < 0):
                return jsonify({"error": "Plus non valido: deve essere un numero >= 0"}), 400
        if "note" in update_fields:
            note = update_fields["note"] or ""
            if len(note) > 255:
                return jsonify({"error": "Nota troppo lunga (max 255 caratteri)"}), 400
            if not re.match(r'^[\w\s.,;:!?"\'àèéìòù()\-_/]*$', note):
                return jsonify({"error": "Caratteri non validi nelle note"}), 400
            update_fields["note"] = note.strip()

        stato_per_id = {}
        if "riscontro" in update_fields:
            riscontro_val = update_fields["riscontro"]
            prelievi = supabase.table("prelievi_ordini_amazon").select("id,qty").in_("id", ids).execute().data
            for p in prelievi:
                qty = p["qty"]
                if riscontro_val is None:
                    stato = "in verifica"
                elif riscontro_val == 0:
                    stato = "manca"
                elif 0 < riscontro_val < qty:
                    stato = "parziale"
                elif riscontro_val == qty:
                    stato = "completo"
                else:
                    stato = "in verifica"
                stato_per_id[p["id"]] = stato
            for stato in set(stato_per_id.values()):
                ids_group = [pid for pid, st in stato_per_id.items() if st == stato]
                if ids_group:
                    supabase.table("prelievi_ordini_amazon").update({
                        **update_fields,
                        "stato": stato
                    }).in_("id", ids_group).execute()
        else:
            supabase.table("prelievi_ordini_amazon").update(update_fields).in_("id", ids).execute()

        prelievi_full = supabase.table("prelievi_ordini_amazon").select("*").in_("id", ids).execute().data
        sync_produzione(prelievi_full, utente="operatore", motivo="Patch bulk prelievi")
        return jsonify({"ok": True, "updated_count": len(ids)})

    except Exception as ex:
        logging.exception("[patch_prelievi_bulk] Errore patch bulk prelievi")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500

    
# --- LISTA PRODUZIONE ---
@bp.route('/api/produzione', methods=['GET'])
def lista_produzione():
    try:
        stato = request.args.get("stato_produzione")
        radice = request.args.get("radice")
        search = request.args.get("search", "").strip()

        query = supabase.table("produzione_vendor").select("*")
        if stato:
            query = query.eq("stato_produzione", stato)
        if radice:
            query = query.eq("radice", radice)
        if search:
            query = query.or_(f"sku.ilike.%{search}%,ean.ilike.%{search}%")
        query = query.order("start_delivery").order("sku")
        rows = query.execute().data

        all_rows = supabase.table("produzione_vendor").select("stato_produzione,radice").execute().data

        badge_stati = {}
        badge_radici = {}
        for r in all_rows:
            s = r.get("stato_produzione", "Da Stampare")
            badge_stati[s] = badge_stati.get(s, 0) + 1
            rd = r.get("radice") or "?"
            badge_radici[rd] = badge_radici.get(rd, 0) + 1

        return jsonify({
            "data": rows,
            "badge_stati": badge_stati,
            "badge_radici": badge_radici,
            "all_radici": sorted(set(r.get("radice") for r in all_rows if r.get("radice")))
        })
    except Exception as ex:
        logging.exception("[lista_produzione] Errore nella GET produzione")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500


@bp.route('/api/produzione/<int:id>', methods=['PATCH'])
def patch_produzione(id):
    try:
        data = request.json
        fields = {}
        utente = "operatore"  # Modifica se vuoi prendere da sessione/jwt

        # Prendi vecchia riga per il log
        old = supabase.table("produzione_vendor").select("*").eq("id", id).single().execute().data
        if not old:
            return jsonify({"error": "Produzione non trovata"}), 404

        log_entries = []

        if "stato_produzione" in data and data["stato_produzione"] != old["stato_produzione"]:
            fields["stato_produzione"] = data["stato_produzione"]
            log_entries.append(dict(
                produzione_row=old,
                utente=utente,
                motivo="Cambio stato",
                stato_vecchio=old["stato_produzione"],
                stato_nuovo=data["stato_produzione"]
            ))
        if "da_produrre" in data and data["da_produrre"] != old["da_produrre"]:
            fields["da_produrre"] = data["da_produrre"]
            fields["modificata_manualmente"] = True
            log_entries.append(dict(
                produzione_row=old,
                utente=utente,
                motivo="Modifica quantità",
                qty_vecchia=old["da_produrre"],
                qty_nuova=data["da_produrre"]
            ))
        if "plus" in data and (old.get("plus") or 0) != (data.get("plus") or 0):
            fields["plus"] = data["plus"]
            log_entries.append(dict(
                produzione_row=old,
                utente=utente,
                motivo="Modifica plus",
                plus_vecchio=old.get("plus") or 0,
                plus_nuovo=data["plus"]
            ))
        for f in ["cavallotti", "note"]:
            if f in data:
                fields[f] = data[f]

        if "da_produrre" in data:
            if old["stato_produzione"] != "Da Stampare":
                if data.get("password") != "oreste":
                    return jsonify({"error": "Password richiesta per modificare la quantità in questo stato."}), 403

        if not fields:
            return jsonify({"error": "Nessun campo da aggiornare"}), 400

        # -------- PATCH CON RETRY ----------
        max_retries = 3
        for attempt in range(max_retries):
            try:
                res = supabase.table("produzione_vendor").update(fields).eq("id", id).execute()
                break
            except Exception as ex:
                logging.warning(f"[patch_produzione] Errore update tentativo {attempt+1}/{max_retries}: {ex}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise

        # Logga i movimenti
        for entry in log_entries:
            log_movimento_produzione(**entry)

        return jsonify({"ok": True, "updated": res.data})

    except Exception as ex:
        logging.exception("[patch_produzione] Errore patch produzione")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500

    
    
# --- PATCH BULK PRODUZIONE ---
@bp.route('/api/produzione/bulk', methods=['PATCH'])
def patch_produzione_bulk():
    try:
        ids = request.json.get("ids", [])
        update_fields = request.json.get("fields", {})
        if not ids or not update_fields:
            return jsonify({"error": "Nessun id/campo"}), 400

        utente = "operatore"
        rows = supabase.table("produzione_vendor").select("*").in_("id", ids).execute().data
        logs = []
        for r in rows:
            if "stato_produzione" in update_fields and update_fields["stato_produzione"] != r["stato_produzione"]:
                logs.append(dict(
                    produzione_row=r,
                    utente=utente,
                    motivo="Cambio stato (bulk)",
                    stato_vecchio=r["stato_produzione"],
                    stato_nuovo=update_fields["stato_produzione"]
                ))
            if "da_produrre" in update_fields and update_fields["da_produrre"] != r["da_produrre"]:
                logs.append(dict(
                    produzione_row=r,
                    utente=utente,
                    motivo="Modifica quantità (bulk)",
                    qty_vecchia=r["da_produrre"],
                    qty_nuova=update_fields["da_produrre"]
                ))
            if "plus" in update_fields and (r.get("plus") or 0) != (update_fields.get("plus") or 0):
                logs.append(dict(
                    produzione_row=r,
                    utente=utente,
                    motivo="Modifica plus (bulk)",
                    plus_vecchio=r.get("plus") or 0,
                    plus_nuovo=update_fields["plus"]
                ))

        # -------- PATCH BULK CON RETRY ----------
        max_retries = 3
        for attempt in range(max_retries):
            try:
                supabase.table("produzione_vendor").update(update_fields).in_("id", ids).execute()
                break
            except Exception as ex:
                logging.warning(f"[patch_produzione_bulk] Errore update bulk tentativo {attempt+1}/{max_retries}: {ex}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    raise

        for entry in logs:
            log_movimento_produzione(**entry)

        return jsonify({"ok": True, "updated_count": len(ids)})
    except Exception as ex:
        logging.exception("[patch_produzione_bulk] Errore PATCH bulk produzione")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500


# --- GET PRODUZIONE BY ID ---
@bp.route('/api/produzione/<int:id>', methods=['GET'])
def get_produzione_by_id(id):
    try:
        res = supabase.table("produzione_vendor").select("*").eq("id", id).single().execute()
        return jsonify(res.data)
    except Exception as ex:
        logging.exception(f"[get_produzione_by_id] Errore GET produzione ID {id}")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500

# --- LOG STORICO DI UNA RIGA ---
@bp.route('/api/produzione/<int:id>/log', methods=['GET'])
def get_log_movimenti(id):
    try:
        logs = supabase.table("movimenti_produzione_vendor")\
            .select("*")\
            .eq("produzione_id", id)\
            .order("created_at", desc=True)\
            .execute().data
        return jsonify(logs)
    except Exception as ex:
        logging.exception(f"[get_log_movimenti] Errore GET log movimenti produzione {id}")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500

# --- BULK DELETE ---
import time

@bp.route('/api/produzione/bulk', methods=['DELETE'])
def delete_produzione_bulk():
    try:
        ids = request.json.get("ids", [])
        if not ids:
            return jsonify({"error": "Nessun id"}), 400

        # Elimina PRIMA tutti i movimenti collegati
        BATCH_SIZE = 100
        for i in range(0, len(ids), BATCH_SIZE):
            batch_ids = ids[i:i+BATCH_SIZE]
            supabase.table("movimenti_produzione_vendor").delete().in_("produzione_id", batch_ids).execute()
            time.sleep(0.05)  # micro pausa, opzionale

        # Poi elimina le righe di produzione_vendor
        for i in range(0, len(ids), BATCH_SIZE):
            batch_ids = ids[i:i+BATCH_SIZE]
            supabase.table("produzione_vendor").delete().in_("id", batch_ids).execute()
            time.sleep(0.05)

        return jsonify({
            "ok": True,
            "deleted_count": len(ids)
        })
    except Exception as ex:
        logging.exception("[delete_produzione_bulk] Errore DELETE bulk produzione")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500




@bp.route('/api/prelievi/svuota', methods=['DELETE'])
def svuota_prelievi():
    try:
        supabase.table("prelievi_ordini_amazon").delete().neq("id", 0).execute()  # cancella tutto
        return jsonify({"ok": True})
    except Exception as ex:
        logging.exception("[svuota_prelievi] Errore DELETE svuota prelievi")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500


@bp.route('/api/produzione/pulisci-da-stampare', methods=['POST'])
def pulisci_da_stampare_endpoint():
    try:
        def norm(x):
            return (
                (x.get("sku") or "").strip().lower().replace(" ", ""),
                (x.get("ean") or "").strip().lower().replace(" ", "")
            )

        produzione = supabase.table("produzione_vendor").select("id,sku,ean,start_delivery").eq("stato_produzione", "Da Stampare").execute().data
        prelievi = supabase.table("prelievi_ordini_amazon").select("sku,ean,start_delivery").execute().data

        max_data_per_sku_ean = defaultdict(str)
        for p in prelievi:
            chiave = norm(p)
            data = str(p.get("start_delivery") or "")[:10]
            if data and (data > max_data_per_sku_ean[chiave]):
                max_data_per_sku_ean[chiave] = data

        ids_da_eliminare = []
        for r in produzione:
            chiave = norm(r)
            data_riga = str(r.get("start_delivery") or "")[:10]
            if max_data_per_sku_ean.get(chiave) and data_riga != max_data_per_sku_ean[chiave]:
                ids_da_eliminare.append(r["id"])
            elif chiave not in max_data_per_sku_ean:
                ids_da_eliminare.append(r["id"])

        if ids_da_eliminare:
            rows_log = supabase.table("produzione_vendor").select("*").in_("id", ids_da_eliminare).execute().data
            for riga in rows_log:
                log_movimento_produzione(
                    riga, utente="operatore",
                    motivo="Auto-eliminazione da pulizia prelievo (vecchia data o assente)"
                )
            supabase.table("produzione_vendor").delete().in_("id", ids_da_eliminare).execute()

        return jsonify({"ok": True, "deleted": len(ids_da_eliminare)})

    except Exception as ex:
        logging.exception("[pulisci_da_stampare_endpoint] Errore pulizia produzione da stampare")
        return jsonify({"error": f"Errore pulizia: {str(ex)}"}), 500
    
    
    
@bp.route('/api/produzione/pulisci-da-stampare-parziale', methods=['POST'])
def pulisci_da_stampare_parziale():
    try:
        data = request.json
        radice = data.get("radice")
        ids = data.get("ids", [])

        def norm(x):
            return (
                (x.get("sku") or "").strip().lower().replace(" ", ""),
                (x.get("ean") or "").strip().lower().replace(" ", "")
            )

        produzione_query = supabase.table("produzione_vendor").select("id,sku,ean,start_delivery,prelievo_id")
        if ids:
            produzione_query = produzione_query.in_("prelievo_id", ids)
        elif radice:
            produzione_query = produzione_query.eq("radice", radice)
        produzione = produzione_query.eq("stato_produzione", "Da Stampare").execute().data

        prelievi_query = supabase.table("prelievi_ordini_amazon").select("id,sku,ean,start_delivery")
        if ids:
            prelievi_query = prelievi_query.in_("id", ids)
        elif radice:
            prelievi_query = prelievi_query.eq("radice", radice)
        prelievi = prelievi_query.execute().data

        max_data_per_sku_ean = defaultdict(str)
        for p in prelievi:
            chiave = norm(p)
            data = str(p.get("start_delivery") or "")[:10]
            if data and (data > max_data_per_sku_ean[chiave]):
                max_data_per_sku_ean[chiave] = data

        ids_da_eliminare = []
        for r in produzione:
            chiave = norm(r)
            data_riga = str(r.get("start_delivery") or "")[:10]
            if max_data_per_sku_ean.get(chiave) and data_riga != max_data_per_sku_ean[chiave]:
                ids_da_eliminare.append(r["id"])
            elif chiave not in max_data_per_sku_ean:
                ids_da_eliminare.append(r["id"])

        if ids_da_eliminare:
            rows_log = supabase.table("produzione_vendor").select("*").in_("id", ids_da_eliminare).execute().data
            for riga in rows_log:
                log_movimento_produzione(
                    riga, utente="operatore",
                    motivo="Auto-eliminazione da pulizia parziale prelievo"
                )
            supabase.table("produzione_vendor").delete().in_("id", ids_da_eliminare).execute()

        return jsonify({"ok": True, "deleted": len(ids_da_eliminare)})

    except Exception as ex:
        logging.exception("[pulisci_da_stampare_parziale] Errore pulizia parziale da stampare")
        return jsonify({"error": f"Errore pulizia parziale: {str(ex)}"}), 500
    

@bp.route('/api/amazon/vendor/orders/badge-counts', methods=['GET'])
def badge_counts():
    try:
        n_nuovi = supabase.table("ordini_vendor_riepilogo")\
            .select("id", count='exact')\
            .eq("stato_ordine", "nuovo")\
            .execute().count or 0
        n_parziali = supabase.table("ordini_vendor_riepilogo")\
            .select("id", count='exact')\
            .eq("stato_ordine", "parziale")\
            .execute().count or 0
        return jsonify({"nuovi": n_nuovi, "parziali": n_parziali})
    except Exception as ex:
        logging.exception("Errore badge_counts")
        return jsonify({"nuovi": 0, "parziali": 0}), 200
