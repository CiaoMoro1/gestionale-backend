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

@bp.route('/api/amazon/vendor/orders/upload', methods=['POST'])
def upload_vendor_orders():
    print("📥 Richiesta ricevuta su /api/amazon/vendor/orders/upload")
    
    if 'file' not in request.files:
        print("❌ Nessun file fornito in request.files")
        return jsonify({"error": "Nessun file fornito"}), 400

    file = request.files['file']
    print(f"📄 File ricevuto: {file.filename}")

    if file.filename == '':
        print("❌ File con nome vuoto")
        return jsonify({"error": "Nessun file selezionato"}), 400

    if not allowed_file(file.filename):
        print(f"❌ Formato file non valido: {file.filename}")
        return jsonify({"error": "Formato file non valido"}), 400

    try:
        excel_bytes = file.read()
        print(f"📊 Lettura file Excel in memoria ({len(excel_bytes)} bytes)")

        df = pd.read_excel(io.BytesIO(excel_bytes), header=2, sheet_name="Articoli")
        df.columns = [str(c).strip().replace('\n', ' ').replace('\r', '').replace('  ', ' ') for c in df.columns]
        print("🔎 Colonne lette:", df.columns.tolist())

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
                print(f"❌ Colonna mancante nel file: {col}")
                return jsonify({"error": f"Colonna mancante: {col}"}), 400

        # Scarico chiavi esistenti per doppioni
        print("🔍 Scarico chiavi esistenti da Supabase...")
        res = supabase.table("ordini_vendor_items").select(
            "po_number,model_number,qty_ordered,start_delivery,fulfillment_center"
        ).execute()
        ordini_esistenti = res.data if hasattr(res, 'data') else res
        print(f"🔍 {len(ordini_esistenti)} righe già presenti nel DB.")

        def is_duplicate(row):
            chiave_new = (
                str(row["Numero ordine/ordine d’acquisto"]).strip(),
                str(row["Numero di modello"]).strip(),
                int(row["Quantità ordinata"]),
                str(row["Inizio consegna"]).strip()[:10],  # solo la data
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
                    msg = f"Doppione trovato: Ordine={row['Numero ordine/ordine d’acquisto']} | Modello={row['Numero di modello']} | Quantità={row['Quantità ordinata']} | Inizio consegna={row['Inizio consegna']} | FC={row['Fulfillment Center']}"
                    print(f"⚠️ {msg}")
                    doppioni.append(msg)
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
                print(f"📦 Inserito ordine: {ordine['po_number']} / MODEL: {ordine['model_number']}")
                po_numbers.add(ordine["po_number"])
                importati += 1
            except Exception as ex:
                error_message = f"❌ Errore su riga: {row.to_dict()} → {ex}"
                print(error_message)
                errors.append(error_message)

        # === LOGICA DI RIEPILOGO ===
        print("🔄 Creo riepilogo per Fulfillment Center & Data...")
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
            supabase.table("ordini_vendor_riepilogo").insert(riepilogo).execute()
        print("✅ Riepilogo ordini creato/aggiornato.")

        print(f"✅ Importazione completata. Totale nuovi ordini: {importati}, PO unici: {len(po_numbers)}")
        print(f"⚠️ Doppioni saltati: {len(doppioni)}")
        return jsonify({
            "status": "ok",
            "importati": importati,
            "doppioni": doppioni,
            "po_unici": len(po_numbers),
            "po_list": list(po_numbers),
            "errors": errors
        })

    except Exception as e:
        print(f"❌ Errore generale durante l'importazione: {e}")
        return jsonify({"error": f"Errore durante l'importazione: {e}"}), 500


@bp.route('/api/amazon/vendor/orders/riepilogo/nuovi', methods=['GET'])
def get_riepilogo_nuovi():
    res = supabase.table("ordini_vendor_riepilogo").select("*").eq("stato_ordine", "nuovo").execute()
    riepiloghi = res.data if hasattr(res, 'data') else res

    risposta = []
    for r in riepiloghi:
        po_list = []
        # Query UNICA: tutte le righe di quel gruppo
        if not r["po_list"]:
            continue
        dettaglio = supabase.table("ordini_vendor_items") \
            .select("po_number, qty_ordered") \
            .eq("fulfillment_center", r["fulfillment_center"]) \
            .eq("start_delivery", r["start_delivery"]) \
            .in_("po_number", r["po_list"]) \
            .execute().data

        articoli_per_po = {}
        for x in dettaglio:
            po_n = x["po_number"]
            articoli_per_po[po_n] = articoli_per_po.get(po_n, 0) + int(x["qty_ordered"])
        for po in r["po_list"]:
            po_list.append({
                "po_number": po,
                "numero_articoli": articoli_per_po.get(po, 0)
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
