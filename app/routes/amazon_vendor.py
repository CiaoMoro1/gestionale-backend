from flask import Blueprint, jsonify, request
import pandas as pd
import io
from app.supabase_client import supabase
from datetime import datetime
import math
from collections import defaultdict

bp = Blueprint('amazon_vendor', __name__)

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
