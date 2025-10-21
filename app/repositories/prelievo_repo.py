# repositories/prelievo_repo.py
from app.supabase import sb_table, supa_with_retry


def sel_date_importabili():
    res = supa_with_retry(lambda: (
        sb_table("ordini_vendor_riepilogo")
        .select("start_delivery")
        .eq("stato_ordine","nuovo")
        .order("start_delivery")
        .execute()
    ))
    return sorted({ r["start_delivery"] for r in (res.data or []) })

def sel_prelievi(data: str | None = None, radice: str | None = None, ids: list[int] | None = None, canale: str | None = None,  ):
    """
    Ritorna SEMPRE una lista (anche vuota). NIENTE .single().
    """
    q = sb_table("prelievi_ordini_amazon").select("*")
    if ids:
        q = q.in_("id", ids)
    if data:
        q = q.eq("start_delivery", data)
    if radice:
        q = q.eq("radice", radice)
    if canale:                              # <-- AGGIUNTO: filtro Vendor/Sito/Seller
        q = q.eq("canale", canale)    
    res = supa_with_retry(lambda: q.order("id").execute())
    return res.data or []

def upd_prelievo(pid:int, fields:dict):
    supa_with_retry(lambda: sb_table("prelievi_ordini_amazon").update(fields).eq("id", pid).execute())

def upd_prelievi_bulk(ids:list[int], fields:dict):
    if not ids: return
    supa_with_retry(lambda: sb_table("prelievi_ordini_amazon").update(fields).in_("id", ids).execute())

def del_prelievi(data=None):
    q = sb_table("prelievi_ordini_amazon").delete()
    q = q.eq("start_delivery", data) if data else q.neq("id", 0)
    supa_with_retry(lambda: q.execute())

def import_da_ordini(data: str) -> dict:
    """
    Importa i prelievi a partire dagli ordini Vendor "nuovi" della data indicata.
    Ritorna un piccolo report {ok, importati, totali, errors}.
    """
    if not data:
        return {"ok": False, "error": "Data richiesta", "importati": 0, "totali": 0, "errors": []}

    # 1) pulisco eventuali prelievi di quella data
    supa_with_retry(lambda: sb_table("prelievi_ordini_amazon")
                    .delete()
                    .eq("start_delivery", data)
                    .execute())

    # 2) leggo items e riepiloghi "nuovi" della data
    items_res = supa_with_retry(lambda: (
        sb_table("ordini_vendor_items")
        .select("*")
        .eq("start_delivery", data)
        .execute()
    ))
    riepiloghi_res = supa_with_retry(lambda: (
        sb_table("ordini_vendor_riepilogo")
        .select("fulfillment_center,start_delivery,stato_ordine")
        .eq("start_delivery", data)
        .eq("stato_ordine", "nuovo")
        .execute()
    ))

    items = items_res.data or []
    riepiloghi = riepiloghi_res.data or []

    # 3) tengo solo i centri validi (stato_ordine = nuovo)
    centri_validi = {(r["fulfillment_center"], str(r["start_delivery"])) for r in riepiloghi}
    articoli = [
        i for i in items
        if (i.get("fulfillment_center"), str(i.get("start_delivery"))) in centri_validi
    ]

    # 4) aggrego per (sku, ean, data) e colleziono "centri": {FC: qty}
    def _radice(sku: str) -> str:
        return (sku or "").split("-")[0].strip().upper()

    aggrega: dict[tuple[str, str | None, str], dict] = {}

    for a in articoli:
        sku = a.get("model_number")
        ean = a.get("vendor_product_id")
        sd  = str(a.get("start_delivery") or "")[:10]
        if not sku or not sd:
            continue

        key = (sku, ean, sd)
        if key not in aggrega:
            aggrega[key] = {
                "sku": sku,
                "ean": ean,
                "radice": _radice(sku),
                "start_delivery": sd,
                "qty": 0,
                "centri": {}
            }

        qty = int(a.get("qty_ordered") or 0)
        fc  = a.get("fulfillment_center") or ""
        aggrega[key]["qty"] += qty
        aggrega[key]["centri"][fc] = int(aggrega[key]["centri"].get(fc, 0)) + qty

    # 5) preparo le righe per prelievi_ordini_amazon
    rows = []
    for agg in aggrega.values():
        rows.append({
            "sku": agg["sku"],
            "ean": agg["ean"],
            "qty": int(agg["qty"] or 0),
            "radice": agg["radice"],
            "start_delivery": agg["start_delivery"],
            "centri": agg["centri"],
            "stato": "in verifica",
            "riscontro": 0,
            "plus": 0,
            "note": ""
        })

    # 6) insert batch con best-effort e report
    BATCH = 200
    errors = []
    inserted_total = 0
    totali = len(rows)

    for i in range(0, totali, BATCH):
        batch = rows[i:i + BATCH]
        try:
            supa_with_retry(lambda b=batch: (
                sb_table("prelievi_ordini_amazon").insert(b).execute()
            ))
            inserted_total += len(batch)
        except Exception as ex:
            errors.append({"range": [i, i + len(batch) - 1], "error": str(ex)})

    return {
        "ok": inserted_total == totali,
        "importati": inserted_total,
        "totali": totali,
        "errors": errors
    }
