# jobs/update_dashboard_summary.py
import os
import time
import json
from supabase import create_client
from collections import defaultdict
from datetime import datetime

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def update_dashboard_summary():
    print("[dashboard] Ricalcolo summary dashboard...")
    dashboard = []

    # Carica tutti i riepiloghi (o solo i "attivi")
    riepiloghi = supabase.table("ordini_vendor_riepilogo") \
        .select("*") \
        .in_("stato_ordine", ["nuovo", "parziale"]) \
        .execute().data

    if not riepiloghi:
        return

    riepilogo_ids = [r.get("id") or r.get("riepilogo_id") for r in riepiloghi]
    parziali = supabase.table("ordini_vendor_parziali") \
        .select("riepilogo_id,numero_parziale,dati,conferma_collo") \
        .in_("riepilogo_id", riepilogo_ids) \
        .execute().data

    parziali_per_riep = defaultdict(list)
    for p in parziali:
        parziali_per_riep[p["riepilogo_id"]].append(p)

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
                "updated_at": datetime.utcnow().isoformat()
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
                "updated_at": datetime.utcnow().isoformat()
            })

    # Sostituisci tutti i dati nella summary (puoi fare anche upsert singoli)
    supabase.table("ordini_vendor_dashboard").delete().neq("riepilogo_id", None).execute()
    if dashboard:
        supabase.table("ordini_vendor_dashboard").insert(dashboard).execute()

if __name__ == "__main__":
    update_dashboard_summary()
