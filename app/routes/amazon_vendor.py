from flask import Blueprint, jsonify, request
import pandas as pd
import io
from app.supabase_client import supabase
from datetime import datetime
import math
from collections import defaultdict
import os
import requests
from requests_aws4auth import AWS4Auth
from flask import Response
from datetime import datetime
from fpdf import FPDF
from flask import Response
from datetime import datetime
import barcode
from barcode.writer import ImageWriter
from io import BytesIO
from PIL import Image


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

# ------------------ UPLOAD ORDINI -------------------
@bp.route('/api/amazon/vendor/orders/upload', methods=['POST'])
def upload_vendor_orders():
    if 'file' not in request.files:
        return jsonify({"error": "Nessun file fornito"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "Nessun file selezionato"}), 400

    if not allowed_file(file.filename):
        return jsonify({"error": "Formato file non valido"}), 400

    try:
        excel_bytes = file.read()
        df = pd.read_excel(io.BytesIO(excel_bytes), header=2, sheet_name="Articoli")
        df.columns = [str(c).strip().replace('\n', ' ').replace('\r', '').replace('  ', ' ') for c in df.columns]

        required_columns = [
            'Numero ordine/ordine d’acquisto',
            'Codice identificativo esterno',
            'Numero di modello',
            'ASIN',
            'Titolo',
            'Costo',
            'Quantità ordinata',
            'Quantità confermata',
            'Inizio consegna',
            'Termine consegna',
            'Data di consegna prevista',
            'Stato disponibilità',
            'Codice fornitore',
            'Fulfillment Center'
        ]
        for col in required_columns:
            if col not in df.columns:
                return jsonify({"error": f"Colonna mancante: {col}"}), 400

        # Preleva chiavi già presenti (controllo doppioni)
        res = supabase.table("ordini_vendor_items").select(
            "po_number,model_number,qty_ordered,start_delivery,fulfillment_center"
        ).execute()
        ordini_esistenti = res.data if hasattr(res, 'data') else res

        def is_duplicate(row):
            chiave_new = (
                str(row["Numero ordine/ordine d’acquisto"]).strip(),
                str(row["Numero di modello"]).strip(),
                int(row["Quantità ordinata"]),
                str(row["Inizio consegna"]).strip()[:10],
                str(row["Fulfillment Center"]).strip()
            )
            for ord_db in ordini_esistenti:
                chiave_db = (
                    str(ord_db.get("po_number", "")).strip(),
                    str(ord_db.get("model_number", "")).strip(),
                    int(ord_db.get("qty_ordered", 0)),
                    str(ord_db.get("start_delivery", "")).strip()[:10],
                    str(ord_db.get("fulfillment_center", "")).strip()
                )
                if chiave_new == chiave_db:
                    return True
            return False

        importati = 0
        po_numbers = set()
        errors = []
        doppioni = []

        for _, row in df.iterrows():
            try:
                if is_duplicate(row):
                    doppioni.append(
                        f"Doppione: Ordine={row['Numero ordine/ordine d’acquisto']} | Modello={row['Numero di modello']} | Quantità={row['Quantità ordinata']}"
                    )
                    continue

                ordine = {
                    "po_number": str(row["Numero ordine/ordine d’acquisto"]).strip(),
                    "vendor_product_id": str(row["Codice identificativo esterno"]).strip(),
                    "model_number": str(row["Numero di modello"]).strip(),
                    "asin": str(row["ASIN"]).strip(),
                    "title": safe_value(row["Titolo"]),
                    "cost": safe_value(row["Costo"]),
                    "qty_ordered": safe_value(row["Quantità ordinata"]),
                    "qty_confirmed": safe_value(row["Quantità confermata"]),
                    "start_delivery": safe_value(row["Inizio consegna"]),
                    "end_delivery": safe_value(row["Termine consegna"]),
                    "delivery_date": safe_value(row["Data di consegna prevista"]),
                    "status": safe_value(row["Stato disponibilità"]),
                    "vendor_code": safe_value(row["Codice fornitore"]),
                    "fulfillment_center": safe_value(row["Fulfillment Center"]),
                    "created_at": datetime.utcnow().isoformat(),
                }
                supabase.table("ordini_vendor_items").insert(ordine).execute()
                po_numbers.add(ordine["po_number"])
                importati += 1
            except Exception as ex:
                errors.append(str(ex))

        # RIEPILOGO (aggiorna sempre!)
        ordini = supabase.table("ordini_vendor_items").select(
            "po_number, qty_ordered, fulfillment_center, start_delivery"
        ).execute().data

        gruppi = defaultdict(lambda: {"po_list": set(), "totale_articoli": 0})
        for o in ordini:
            key = (o["fulfillment_center"], str(o["start_delivery"])[:10])
            gruppi[key]["po_list"].add(o["po_number"])
            gruppi[key]["totale_articoli"] += int(o["qty_ordered"])

        for (fc, data), dati in gruppi.items():
            riepilogo = {
                "fulfillment_center": fc,
                "start_delivery": data,
                "po_list": list(dati["po_list"]),
                "totale_articoli": dati["totale_articoli"],
                "stato_ordine": "nuovo"
            }
            # Per semplicità: cancella vecchi e inserisci nuovo riepilogo
            res = supabase.table("ordini_vendor_riepilogo") \
                .select("id") \
                .eq("fulfillment_center", fc) \
                .eq("start_delivery", data) \
                .execute()
            if res.data and len(res.data) > 0:
                id_riep = res.data[0]['id']
                supabase.table("ordini_vendor_riepilogo").delete().eq("id", id_riep).execute()
            supabase.table("ordini_vendor_riepilogo").insert(riepilogo).execute()

        return jsonify({
            "status": "ok",
            "importati": importati,
            "doppioni": doppioni,
            "po_unici": len(po_numbers),
            "po_list": list(po_numbers),
            "errors": errors
        })

    except Exception as e:
        return jsonify({"error": f"Errore durante l'importazione: {e}"}), 500

# --------------- RIEPILOGO ORDINI NUOVI -------------------
@bp.route('/api/amazon/vendor/orders/riepilogo/nuovi', methods=['GET'])
def get_riepilogo_nuovi():
    res = supabase.table("ordini_vendor_riepilogo").select("*").eq("stato_ordine", "nuovo").execute()
    riepiloghi = res.data if hasattr(res, 'data') else res
    tutti_po = set()
    for r in riepiloghi:
        if r["po_list"]:
            tutti_po.update(r["po_list"])
    if not tutti_po:
        return jsonify([])
    dettagli = supabase.table("ordini_vendor_items") \
        .select("po_number, qty_ordered, fulfillment_center, start_delivery") \
        .in_("po_number", list(tutti_po)) \
        .execute().data
    articoli_per_po = {}
    for x in dettagli:
        key = (x["po_number"], x["fulfillment_center"], str(x["start_delivery"])[:10])
        articoli_per_po[key] = articoli_per_po.get(key, 0) + int(x["qty_ordered"])
    risposta = []
    for r in riepiloghi:
        po_list = []
        if not r["po_list"]:
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

# --------- DETTAGLIO ARTICOLI DI UNA DESTINAZIONE+DATA ----------
@bp.route('/api/amazon/vendor/orders/dettaglio-destinazione', methods=['GET'])
def dettaglio_destinazione():
    center = request.args.get("center")
    data = request.args.get("data")
    if not center or not data:
        return jsonify([])

    riepilogo_res = supabase.table("ordini_vendor_riepilogo") \
        .select("id, po_list") \
        .eq("fulfillment_center", center) \
        .eq("start_delivery", data) \
        .execute()
    if not riepilogo_res.data or not riepilogo_res.data[0]["po_list"]:
        return jsonify([])

    po_list = riepilogo_res.data[0]["po_list"]
    riepilogo_id = riepilogo_res.data[0]["id"]

    articoli = supabase.table("ordini_vendor_items") \
        .select("po_number, model_number, vendor_product_id, title, qty_ordered") \
        .in_("po_number", po_list) \
        .execute().data

    return jsonify({"articoli": articoli, "riepilogo_id": riepilogo_id})

# ---------- UTILITY: Recupera ID riepilogo --------------
@bp.route('/api/amazon/vendor/riepilogo-id', methods=['GET'])
def get_riepilogo_id():
    center = request.args.get("center")
    data = request.args.get("data")
    if not center or not data:
        return jsonify({"error": "center/data richiesti"}), 400

    res = supabase.table("ordini_vendor_riepilogo")\
        .select("id")\
        .eq("fulfillment_center", center)\
        .eq("start_delivery", data)\
        .execute()
    if res.data and len(res.data) > 0:
        return jsonify({"riepilogo_id": res.data[0]['id']})
    return jsonify({"riepilogo_id": None})

# ----------------- LEGGI PARZIALI SPEDIZIONE ---------------
@bp.route('/api/amazon/vendor/parziali', methods=['GET'])
def get_parziali():
    riepilogo_id = request.args.get('riepilogo_id')
    if not riepilogo_id:
        return jsonify({"error": "riepilogo_id mancante"}), 400
    res = supabase.table("ordini_vendor_parziali")\
        .select("*")\
        .eq("riepilogo_id", riepilogo_id)\
        .order("numero_parziale")\
        .execute()
    return jsonify(res.data if hasattr(res, 'data') else res)

# -------------- SALVA NUOVO PARZIALE SPEDIZIONE ----------------
@bp.route('/api/amazon/vendor/parziali', methods=['POST'])
def save_parziale():
    data = request.json
    riepilogo_id = data.get("riepilogo_id")
    dati = data.get("dati")  # array di {model_number, quantita, collo}
    if not riepilogo_id or not dati:
        return jsonify({"error": "Dati mancanti"}), 400

    res = supabase.table("ordini_vendor_parziali")\
        .select("numero_parziale")\
        .eq("riepilogo_id", riepilogo_id)\
        .order("numero_parziale", desc=True)\
        .limit(1)\
        .execute()
    max_num = 1
    if res.data and len(res.data) > 0:
        max_num = int(res.data[0]["numero_parziale"]) + 1

    parziale = {
        "riepilogo_id": riepilogo_id,
        "numero_parziale": max_num,
        "dati": dati,
        "confermato": False,
    }
    supabase.table("ordini_vendor_parziali").insert(parziale).execute()
    return jsonify({"ok": True, "numero_parziale": max_num})



@bp.route('/api/amazon/vendor/parziali/<int:riepilogo_id>', methods=['GET'])
def get_parziali_riepilogo(riepilogo_id):
    # Tutti i parziali NON confermati (bozza in lavorazione)
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
        "parziali": parz["dati"],
        "confermaCollo": parz.get("conferma_collo", {}) # opzionale se vuoi trackare colli confermati live!
    })

@bp.route('/api/amazon/vendor/parziali/<int:riepilogo_id>', methods=['POST'])
def post_parziali_riepilogo(riepilogo_id):
    dati = request.json
    # Esempio: dati = {"parziali": [...], "confermaCollo": {...}}
    numero_parziale = dati.get("numero_parziale", 1)
    # Aggiorna o crea (semplice upsert se esiste già NON confermato)
    supabase.table("ordini_vendor_parziali").upsert({
        "riepilogo_id": riepilogo_id,
        "numero_parziale": numero_parziale,
        "dati": dati.get("parziali", []),
        "conferma_collo": dati.get("confermaCollo", {}),
        "confermato": False,
        "created_at": datetime.utcnow().isoformat()
    }, on_conflict="riepilogo_id,numero_parziale").execute()
    return jsonify({"ok": True})

# ---- PARZIALI STORICI (confermati) ----
@bp.route('/api/amazon/vendor/parziali-storici', methods=['GET'])
def get_parziali_storici():
    center = request.args.get("center")
    data = request.args.get("data")
    if not center or not data:
        return jsonify([])

    riepilogo = supabase.table("ordini_vendor_riepilogo") \
        .select("id") \
        .eq("fulfillment_center", center) \
        .eq("start_delivery", data) \
        .execute().data
    if not riepilogo:
        return jsonify([])
    riepilogo_id = riepilogo[0]["id"]
    parziali = supabase.table("ordini_vendor_parziali") \
        .select("dati") \
        .eq("riepilogo_id", riepilogo_id) \
        .eq("confermato", True) \
        .order("numero_parziale") \
        .execute().data
    result = []
    for p in parziali:
        result.extend(p["dati"])
    return jsonify(result)

# ---- PARZIALI WIP (in lavorazione, non confermati) ----
@bp.route('/api/amazon/vendor/parziali-wip', methods=['GET'])
def get_parziali_wip():
    center = request.args.get("center")
    data = request.args.get("data")
    if not center or not data:
        return jsonify([])

    riepilogo = supabase.table("ordini_vendor_riepilogo") \
        .select("id") \
        .eq("fulfillment_center", center) \
        .eq("start_delivery", data) \
        .execute().data
    if not riepilogo:
        return jsonify([])
    riepilogo_id = riepilogo[0]["id"]

    # Prendi l’ultima bozza non confermata (se c’è)
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

@bp.route('/api/amazon/vendor/parziali-wip', methods=['POST'])
def save_parziali_wip():
    center = request.args.get("center")   # <-- Prendi dalla query string
    start_delivery = request.args.get("data")  # <-- Prendi dalla query string
    data = request.json
    parziali = data.get("parziali")  # array [{model_number, quantita, collo, ...}, ...]
    conferma_collo = data.get("confermaCollo", {})
    if not center or not start_delivery or parziali is None:
        return jsonify({"error": "center/data/parziali richiesti"}), 400

    # Trova riepilogo_id
    riepilogo = supabase.table("ordini_vendor_riepilogo") \
        .select("id") \
        .eq("fulfillment_center", center) \
        .eq("start_delivery", start_delivery) \
        .execute().data
    if not riepilogo:
        return jsonify({"error": "riepilogo non trovato"}), 400
    riepilogo_id = riepilogo[0]["id"]

    # Trova (o crea) la bozza attuale
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
        # Trova max numero_parziale tra i confermati
        conf = supabase.table("ordini_vendor_parziali") \
            .select("numero_parziale") \
            .eq("riepilogo_id", riepilogo_id) \
            .eq("confermato", True) \
            .order("numero_parziale", desc=True) \
            .limit(1) \
            .execute().data
        max_num = conf[0]["numero_parziale"] if conf and len(conf) > 0 else 0
        numero_parziale = max_num + 1

    # Upsert: sovrascrivi bozza corrente
    supabase.table("ordini_vendor_parziali").upsert({
        "riepilogo_id": riepilogo_id,
        "numero_parziale": numero_parziale,
        "dati": parziali,
        "conferma_collo": conferma_collo,
        "confermato": False,
        "created_at": datetime.utcnow().isoformat()
    }, on_conflict="riepilogo_id,numero_parziale").execute()
    return jsonify({"ok": True, "numero_parziale": numero_parziale})


@bp.route('/api/amazon/vendor/parziali-wip/conferma-parziale', methods=['POST'])
def conferma_parziale():
    center = request.json.get("center")
    start_delivery = request.json.get("data")
    if not center or not start_delivery:
        return jsonify({"error": "center/data richiesti"}), 400
    riepilogo = supabase.table("ordini_vendor_riepilogo") \
        .select("id") \
        .eq("fulfillment_center", center) \
        .eq("start_delivery", start_delivery) \
        .execute().data
    if not riepilogo:
        return jsonify({"error": "riepilogo non trovato"}), 400
    riepilogo_id = riepilogo[0]["id"]

    # Prendi ultimo WIP
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
    # Aggiorna come confermato
    supabase.table("ordini_vendor_parziali") \
        .update({"confermato": True}) \
        .eq("riepilogo_id", riepilogo_id) \
        .eq("numero_parziale", num_parz) \
        .execute()
    # Aggiorna stato_ordine a "parziale"
    supabase.table("ordini_vendor_riepilogo") \
        .update({"stato_ordine": "parziale"}) \
        .eq("id", riepilogo_id) \
        .execute()
    return jsonify({"ok": True})


@bp.route('/api/amazon/vendor/parziali-wip/conferma', methods=['POST'])
def conferma_chiudi_ordine():
    center = request.json.get("center")
    start_delivery = request.json.get("data")
    if not center or not start_delivery:
        return jsonify({"error": "center/data richiesti"}), 400
    riepilogo = supabase.table("ordini_vendor_riepilogo") \
        .select("id, po_list") \
        .eq("fulfillment_center", center) \
        .eq("start_delivery", start_delivery) \
        .execute().data
    if not riepilogo:
        return jsonify({"error": "riepilogo non trovato"}), 400
    riepilogo_id = riepilogo[0]["id"]
    po_list = riepilogo[0]["po_list"]

    # Prendi l’ULTIMO parziale NON confermato
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
    dati_wip = wip[0]["dati"]  # [{model_number, quantita, collo, ...}]

    # Segna il parziale come confermato
    supabase.table("ordini_vendor_parziali") \
        .update({"confermato": True}) \
        .eq("riepilogo_id", riepilogo_id) \
        .eq("numero_parziale", num_parz) \
        .execute()

    # Per ogni articolo dell’ordine: aggiorna qty_confirmed = somma di tutti i parziali (storici + wip)
    # 1. Prendi tutti i parziali STORICI confermati (inclusi gli altri parziali di questo riepilogo)
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

    # 2. Aggiorna tutte le righe articolo (qty_confirmed su tutti gli articoli della spedizione)
    for model_number, qty in totali_sku.items():
        # Può essere su più PO! Li aggiorni tutti.
        supabase.table("ordini_vendor_items") \
            .update({"qty_confirmed": qty}) \
            .in_("po_number", po_list) \
            .eq("model_number", model_number) \
            .execute()

    # Aggiorna stato_ordine a "parziale" o "completato"
    stato_ordine = "parziale"
    # TODO: se vuoi, qui puoi controllare se tutto qty_confirmed == qty_ordered => "completato"
    supabase.table("ordini_vendor_riepilogo") \
        .update({"stato_ordine": stato_ordine}) \
        .eq("id", riepilogo_id) \
        .execute()
    return jsonify({"ok": True})


@bp.route('/api/amazon/vendor/parziali-wip/reset', methods=['POST'])
def reset_parziali_wip():
    center = request.json.get("center")
    start_delivery = request.json.get("data")
    if not center or not start_delivery:
        return jsonify({"error": "center/data richiesti"}), 400
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


@bp.route('/api/amazon/vendor/parziali-wip/chiudi', methods=['POST'])
def chiudi_ordine():
    data = request.json
    center = data.get("center")
    start_delivery = data.get("data")
    if not center or not start_delivery:
        return jsonify({"error": "center/data richiesti"}), 400

    # 1. Trova riepilogo_id
    riepilogo = supabase.table("ordini_vendor_riepilogo") \
        .select("id, po_list") \
        .eq("fulfillment_center", center) \
        .eq("start_delivery", start_delivery) \
        .execute().data
    if not riepilogo:
        return jsonify({"error": "riepilogo non trovato"}), 400
    riepilogo_id = riepilogo[0]["id"]
    po_list = riepilogo[0]["po_list"]

    # 2. Somma tutte le quantità confermate per ogni model_number dai parziali confermati
    parziali = supabase.table("ordini_vendor_parziali") \
        .select("dati") \
        .eq("riepilogo_id", riepilogo_id) \
        .eq("confermato", True) \
        .order("numero_parziale") \
        .execute().data

    # Può esserci anche una bozza WIP da chiudere ora: prendi anche quella
    parziali_wip = supabase.table("ordini_vendor_parziali") \
        .select("dati") \
        .eq("riepilogo_id", riepilogo_id) \
        .eq("confermato", False) \
        .order("numero_parziale", desc=True) \
        .limit(1) \
        .execute().data
    if parziali_wip:
        parziali.append(parziali_wip[0])

    # 3. Calcola la quantità totale confermata per ogni SKU
    qty_per_model = {}
    for p in parziali:
        for r in p["dati"]:
            model = r["model_number"]
            qty_per_model[model] = qty_per_model.get(model, 0) + int(r["quantita"])

    # 4. Prendi tutti gli articoli di questo riepilogo (quelli da aggiornare)
    articoli = supabase.table("ordini_vendor_items") \
        .select("id, model_number") \
        .in_("po_number", po_list) \
        .execute().data

    # 5. Aggiorna qty_confirmed per ognuno
    for art in articoli:
        nuova_qty = qty_per_model.get(art["model_number"], 0)
        supabase.table("ordini_vendor_items") \
            .update({"qty_confirmed": nuova_qty}) \
            .eq("id", art["id"]) \
            .execute()

    # 6. Aggiorna lo stato_ordine
    supabase.table("ordini_vendor_riepilogo") \
        .update({"stato_ordine": "completato"}) \
        .eq("id", riepilogo_id) \
        .execute()

    # 7. Marca anche la bozza WIP come confermata, se c’era
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


# Mostra tutti i riepiloghi con stato PARZIALE (e anche completato se vuoi)
@bp.route('/api/amazon/vendor/orders/riepilogo/parziali', methods=['GET'])
def get_riepilogo_parziali():
    res = supabase.table("ordini_vendor_riepilogo")\
        .select("*")\
        .in_("stato_ordine", ["parziale"])\
        .order("created_at", desc=True)\
        .execute()
    return jsonify(res.data if hasattr(res, 'data') else res)

@bp.route('/api/amazon/vendor/items', methods=['GET'])
def get_items_by_po():
    po_list = request.args.get("po_list")
    if not po_list:
        return jsonify([])
    pos = po_list.split(",")
    items = supabase.table("ordini_vendor_items").select("po_number,model_number,qty_ordered,qty_confirmed").in_("po_number", pos).execute().data
    return jsonify(items)

@bp.route('/api/amazon/vendor/parziali-ordine', methods=['GET'])
def parziali_per_ordine():
    center = request.args.get("center")
    data = request.args.get("data")
    if not center or not data:
        return jsonify([])
    riepilogo = supabase.table("ordini_vendor_riepilogo") \
        .select("id") \
        .eq("fulfillment_center", center) \
        .eq("start_delivery", data) \
        .execute().data
    if not riepilogo:
        return jsonify([])
    riepilogo_id = riepilogo[0]["id"]
    parziali = supabase.table("ordini_vendor_parziali") \
        .select("numero_parziale, dati, confermato, created_at, conferma_collo") \
        .eq("riepilogo_id", riepilogo_id) \
        .eq("confermato", True) \
        .order("numero_parziale") \
        .execute().data
    return jsonify(parziali)


@bp.route('/api/amazon/vendor/orders/list', methods=['GET'])
def list_vendor_pos():
    access_token = get_spapi_access_token()
    awsauth = AWS4Auth(
        os.getenv("AWS_ACCESS_KEY"),
        os.getenv("AWS_SECRET_KEY"),
        'eu-west-1', 'execute-api',
        session_token=os.getenv("AWS_SESSION_TOKEN")
    )
    url = "https://sellingpartnerapi-eu.amazon.com/vendor/orders/v1/purchaseOrders"
    # Default: ultimi 30 giorni (massimo 7gg per chiamata, puoi modificare)
    from datetime import datetime, timedelta
    today = datetime.utcnow()
    seven_days_ago = today - timedelta(days=7)
    params = {
        "createdAfter": seven_days_ago.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "createdBefore": today.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "limit": request.args.get("limit", 50)
    }
    # Puoi aggiungere altri filtri come purchaseOrderState se vuoi
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }
    resp = requests.get(url, auth=awsauth, headers=headers, params=params)
    print("Amazon Vendor Orders Response:", resp.status_code, resp.text)
    # Torna semplicemente la risposta di Amazon
    return (resp.text, resp.status_code, {'Content-Type': 'application/json'})




@bp.route('/api/amazon/vendor/orders/lista-prelievo/nuovi/pdf', methods=['GET'])
def export_lista_prelievo_nuovi_pdf():
    riepiloghi = supabase.table("ordini_vendor_riepilogo") \
        .select("fulfillment_center, start_delivery, po_list") \
        .eq("stato_ordine", "nuovo") \
        .execute().data
    if not riepiloghi:
        return Response("Nessun articolo trovato.", status=404)

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

    # Colonne
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
        pdf.cell(table_width, 10, "Lista Prelievo Articoli Ordini Nuovi", 0, 1, "C")
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
            if pdf.get_y() + row_height + margin_bottom > 210:  # A4 landscape = 210mm
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
                CODE = barcode.get_barcode_class(barcode_type)
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
        headers={"Content-disposition": f"attachment; filename=lista_prelievo_{datetime.utcnow().date()}.pdf"}
    )



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
    import json
    from collections import defaultdict

    barcode = request.args.get('barcode')
    if not barcode:
        return jsonify([])

    # 1. Trova tutte le po_list per riepiloghi "nuovo/parziale"
    riepiloghi = supabase.table("ordini_vendor_riepilogo") \
        .select("po_list,fulfillment_center,start_delivery,id") \
        .in_("stato_ordine", ["nuovo", "parziale"]) \
        .execute().data

    # Crea lista di PO con info centro e data
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

    # 2. Cerca articoli per PO, con barcode o SKU
    articoli = supabase.table("ordini_vendor_items") \
        .select("*") \
        .in_("po_number", po_list) \
        .or_(f"vendor_product_id.eq.{barcode},model_number.eq.{barcode}") \
        .execute().data

    # 3. Aggiungi info centro/data da po_centro_map (utile per frontend)
    for a in articoli:
        info = po_centro_map.get(a["po_number"], {})
        a["fulfillment_center"] = info.get("fulfillment_center")
        a["start_delivery"] = info.get("start_delivery")

    # 4. Recupera TUTTI i parziali delle PO trovate (da tutti i riepilogo collegati)
    # Prendi tutte le riepilogo_id coinvolte:
    riepilogo_ids = list(set(po_riepilogo_id_map.get(a["po_number"]) for a in articoli if po_riepilogo_id_map.get(a["po_number"])))
    if not riepilogo_ids:
        for a in articoli:
            a["qty_inserted"] = 0
        return jsonify(articoli)

    parziali = supabase.table("ordini_vendor_parziali") \
        .select("dati") \
        .in_("riepilogo_id", riepilogo_ids) \
        .execute().data

    # 5. Crea una mappa (po_number, model_number) -> qty_inserted
    qty_inserted_map = defaultdict(int)
    for p in parziali:
        # dati può essere una lista o una stringa JSON
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

    # 6. Aggiungi qty_inserted a ciascun articolo
    for a in articoli:
        key = (a["po_number"], a["model_number"])
        a["qty_inserted"] = qty_inserted_map.get(key, 0)

    return jsonify(articoli)



@bp.route('/api/amazon/vendor/orders/riepilogo/dashboard', methods=['GET'])
def riepilogo_dashboard_parziali():
    import json
    from collections import defaultdict

    dashboard = []

    # 1. Prendi tutti i riepiloghi con stato nuovo/parziale
    riepiloghi = supabase.table("ordini_vendor_riepilogo") \
        .select("*") \
        .in_("stato_ordine", ["nuovo", "parziale"]) \
        .execute().data

    if not riepiloghi:
        return jsonify([])

    # 2. Prendi tutti i parziali di TUTTI i riepilogo_id insieme
    riepilogo_ids = [r.get("id") or r.get("riepilogo_id") for r in riepiloghi]
    parziali = supabase.table("ordini_vendor_parziali") \
        .select("riepilogo_id,numero_parziale,dati,conferma_collo") \
        .in_("riepilogo_id", riepilogo_ids) \
        .execute().data

    # 3. Raggruppa parziali per riepilogo_id
    parziali_per_riep = defaultdict(list)
    for p in parziali:
        parziali_per_riep[p["riepilogo_id"]].append(p)

    for r in riepiloghi:
        fulfillment_center = r["fulfillment_center"]
        start_delivery = r["start_delivery"]
        stato_ordine = r["stato_ordine"]
        po_list = r["po_list"]
        riepilogo_id = r.get("id") or r.get("riepilogo_id")

        # Tutti i parziali per questo riepilogo
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


@bp.route('/api/amazon/vendor/orders/lista-ordini/nuovi/pdf', methods=['GET'])
def export_lista_ordini_nuovi_pdf():
    import json

    # 1. Prendi tutti i riepiloghi "nuovo"
    riepiloghi = supabase.table("ordini_vendor_riepilogo") \
        .select("fulfillment_center, start_delivery, po_list") \
        .eq("stato_ordine", "nuovo") \
        .execute().data
    if not riepiloghi:
        return Response("Nessun ordine trovato.", status=404)

    # 2. Raggruppa per centro (destinazione)
    centri_map = {}  # fulfillment_center -> {"start_delivery": ..., "po_list": set()}
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
        # Raggruppa per SKU
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
        # Ordina per SKU
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
    
    
@bp.route('/api/amazon/vendor/orders/riepilogo/completati', methods=['GET'])
def riepilogo_completati():
    riepiloghi = supabase.table("ordini_vendor_riepilogo") \
        .select("*") \
        .eq("stato_ordine", "completato") \
        .order("created_at", desc=False) \
        .execute().data
    return jsonify(riepiloghi)
