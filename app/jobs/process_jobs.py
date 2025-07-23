import io
import os
import time
import traceback
import pandas as pd
from datetime import datetime, timezone
from collections import defaultdict
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()

def fix_date(val):
    if pd.isna(val):
        return None
    if hasattr(val, "date"):
        return val.date().isoformat()
    s = str(val).strip()
    if "T" in s:
        return s.split("T")[0]
    return s

# Configura qui il client Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def process_import_vendor_orders_job(job):
    try:
        supabase.table("jobs").update({
            "status": "in_progress",
            "started_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()

        storage_path = job["payload"]["storage_path"]
        bucket, filename = storage_path.split("/", 1)
        print(f"[worker] Scarico file {storage_path} da storage...")
        file_resp = supabase.storage.from_(bucket).download(filename)
        if hasattr(file_resp, 'error') and file_resp.error:
            raise Exception(f"Errore download da storage: {file_resp.error}")
        excel_bytes = file_resp

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
                raise Exception(f"Colonna mancante: {col}")

        res = supabase.table("ordini_vendor_items").select(
            "po_number,model_number,qty_ordered,start_delivery,fulfillment_center"
        ).execute()
        ordini_esistenti = res.data if hasattr(res, 'data') else res

        def is_duplicate(row):
            chiave_new = (
                str(row["Numero ordine/ordine d’acquisto"]).strip(),
                str(row["Numero di modello"]).strip(),
                int(row["Quantità ordinata"]),
                fix_date(row["Inizio consegna"]),
                str(row["Fulfillment Center"]).strip()
            )
            for ord_db in ordini_esistenti:
                chiave_db = (
                    str(ord_db.get("po_number", "")).strip(),
                    str(ord_db.get("model_number", "")).strip(),
                    int(ord_db.get("qty_ordered", 0)),
                    fix_date(ord_db.get("start_delivery", "")),
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
                    "title": str(row["Titolo"]),
                    "cost": row["Costo"],
                    "qty_ordered": row["Quantità ordinata"],
                    "qty_confirmed": row["Quantità confermata"],
                    "start_delivery": fix_date(row["Inizio consegna"]),
                    "end_delivery": fix_date(row["Termine consegna"]),
                    "delivery_date": fix_date(row["Data di consegna prevista"]),
                    "status": row["Stato disponibilità"],
                    "vendor_code": row["Codice fornitore"],
                    "fulfillment_center": row["Fulfillment Center"],
                    "created_at": datetime.now(timezone.utc).isoformat()
                }

                supabase.table("ordini_vendor_items").insert(ordine).execute()
                po_numbers.add(ordine["po_number"])
                importati += 1
            except Exception as ex:
                errors.append(str(ex))

        # --- RIEPILOGO: aggiorna sempre dopo import! ---
        ordini = supabase.table("ordini_vendor_items").select(
            "po_number, qty_ordered, fulfillment_center, start_delivery"
        ).execute().data

        gruppi = defaultdict(lambda: {"po_list": set(), "totale_articoli": 0})
        for o in ordini:
            key = (o["fulfillment_center"], fix_date(o["start_delivery"]))
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
            res = supabase.table("ordini_vendor_riepilogo") \
                .select("id") \
                .eq("fulfillment_center", fc) \
                .eq("start_delivery", data) \
                .execute()
            if res.data and len(res.data) > 0:
                id_riep = res.data[0]['id']
                supabase.table("ordini_vendor_riepilogo") \
                    .update({
                        "po_list": list(dati["po_list"]),
                        "totale_articoli": dati["totale_articoli"]
                    }) \
                    .eq("id", id_riep) \
                    .execute()
            else:
                supabase.table("ordini_vendor_riepilogo").insert(riepilogo).execute()

        # Aggiorna stato job DONE
        supabase.table("jobs").update({
            "status": "done",
            "result": {
                "importati": importati,
                "doppioni": doppioni,
                "po_unici": len(po_numbers),
                "po_list": list(po_numbers),
                "errors": errors
            },
            "finished_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()

        print(f"[worker] Import terminato! {importati} righe, {len(doppioni)} doppioni.")

    except Exception as e:
        print("[worker] ERRORE!", e)
        supabase.table("jobs").update({
            "status": "failed",
            "error": str(e),
            "stacktrace": traceback.format_exc(),
            "finished_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()

def main_loop():
    while True:
        jobs = supabase.table("jobs").select("*").eq("status", "pending").execute().data
        if not jobs:
            time.sleep(5)
            continue
        for job in jobs:
            print(f"[worker] Processo job {job['id']} ({job['type']})...")
            if job["type"] == "import_vendor_orders":
                process_import_vendor_orders_job(job)
        time.sleep(1)

if __name__ == "__main__":
    main_loop()
