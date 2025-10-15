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

def sel_prelievi(data: str | None = None, radice: str | None = None, ids: list[int] | None = None):
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

def import_da_ordini(data:str):
    # qui incolla 1:1 la tua logica di import che oggi è in amazon_vendor.py
    ...
