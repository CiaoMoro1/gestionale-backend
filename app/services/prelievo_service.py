# app/services/prelievo_service.py

from typing import Any
from app.repositories.prelievo_repo import (
    sel_date_importabili, sel_prelievi, upd_prelievo, upd_prelievi_bulk,
    del_prelievi, import_da_ordini
)
from app.services.produzione_service import sync_produzione_from_prelievo_ids
from app.supabase_client import supabase

STATI = ("manca", "parziale", "completo", "in_verifica")

def _to_int_or_none(v: Any):
    if v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.strip().isdigit():
        return int(v)
    raise ValueError("Valore numerico non valido")

def _deriva_stato(qty:int, riscontro:int|None)->str:
    r = int(riscontro or 0)
    if r < 0 or r > qty: return "in_verifica"
    if r == 0: return "manca"
    if 0 < r < qty: return "parziale"
    return "completo"

def date_importabili():
    return sel_date_importabili()

def importa_prelievi_da_data(data:str):
    del_prelievi(data=data)
    import_da_ordini(data)

def lista_prelievi(data:str|None, radice:str|None):
    return sel_prelievi(data=data, radice=radice)

def _validate_payload(p: dict):
    if "riscontro" in p and p["riscontro"] is not None:
        p["riscontro"] = _to_int_or_none(p["riscontro"])
        if p["riscontro"] < 0:
            raise ValueError("Riscontro non valido (>=0)")
    if "plus" in p and p["plus"] is not None:
        p["plus"] = _to_int_or_none(p["plus"])
        if p["plus"] < 0:
            raise ValueError("Plus non valido (>=0)")
    if "magazzino_usato" in p and p["magazzino_usato"] is not None:
        p["magazzino_usato"] = _to_int_or_none(p["magazzino_usato"])
        if p["magazzino_usato"] < 0:
            raise ValueError("Magazzino usato non valido (>=0)")

def _scarica_da_magazzino(row: dict, delta: int):
    """
    Scarica 'delta' pezzi da magazzino con RPC atomica.
    row: prelievo row (richiede sku, ean, canale, id)
    """
    if delta <= 0:
        return
    args = {
        "p_sku": row["sku"],
        "p_ean": row["ean"],
        "p_canale": row.get("canale") or "Amazon Vendor",
        "p_qty": int(delta),
        "p_motivo": "Impiegato su Prelievo",
        "p_prelievo_id": int(row["id"])
    }
    rpc_res = supabase.rpc("magazzino_scarica", args).execute()
    # SDK può restituire oggetto con .error o dict con "error"
    err = getattr(rpc_res, "error", None) or (rpc_res.get("error") if isinstance(rpc_res, dict) else None)
    if err:
        raise RuntimeError(str(err))

def aggiorna_prelievo(prelievo_id:int, payload:dict):
    _validate_payload(payload)

    row_list = sel_prelievi(ids=[prelievo_id])
    if not row_list:
        raise ValueError(f"Prelievo {prelievo_id} non trovato")
    row = row_list[0]

    fields = {k:v for k,v in payload.items() if k in ("riscontro","plus","note")}
    # stato derivato se mandi un riscontro
    if "riscontro" in fields:
        fields["stato"] = _deriva_stato(qty=int(row["qty"]), riscontro=fields["riscontro"])

    # gestione magazzino_usato (delta)
    if "magazzino_usato" in payload and payload["magazzino_usato"] is not None:
        nuovo = int(payload["magazzino_usato"])
        attuale = int(row.get("magazzino_usato") or 0)
        delta = nuovo - attuale
        if delta > 0:
            _scarica_da_magazzino(row, delta)
        # (se delta < 0 NON ricarichiamo automaticamente in magazzino — operativamente più sicuro)
        fields["magazzino_usato"] = nuovo

    upd_prelievo(prelievo_id, fields)
    sync_produzione_from_prelievo_ids([prelievo_id])

def aggiorna_prelievi_bulk(ids:list[int], fields:dict):
    _validate_payload(fields)

    # se c'è magazzino_usato in bulk, lavoriamo per riga (delta diverso per ciascuno)
    has_mag = "magazzino_usato" in fields and fields["magazzino_usato"] is not None
    bulk_fields = {k:v for k,v in fields.items() if k in ("riscontro","plus","note")}

    if has_mag:
        righe = sel_prelievi(ids=ids)
        for r in righe:
            nuovo = int(fields["magazzino_usato"])
            attuale = int(r.get("magazzino_usato") or 0)
            delta = nuovo - attuale
            per_riga = dict(bulk_fields)
            if "riscontro" in per_riga:
                per_riga["stato"] = _deriva_stato(qty=int(r["qty"]), riscontro=per_riga["riscontro"])
            if delta > 0:
                _scarica_da_magazzino(r, delta)
            per_riga["magazzino_usato"] = nuovo
            upd_prelievo(int(r["id"]), per_riga)
        sync_produzione_from_prelievo_ids(ids)
        return

    # no magazzino_usato in bulk → possiamo clusterizzare per stato se c'è riscontro
    if "riscontro" in bulk_fields:
        righe = sel_prelievi(ids=ids)
        stato_per_id = { r["id"]: _deriva_stato(int(r["qty"]), bulk_fields["riscontro"]) for r in righe }
        for stato in set(stato_per_id.values()):
            ids_cluster = [i for i,s in stato_per_id.items() if s==stato]
            if ids_cluster:
                upd_prelievi_bulk(ids_cluster, {**bulk_fields, "stato": stato})
    else:
        if ids:
            upd_prelievi_bulk(ids, bulk_fields)

    sync_produzione_from_prelievo_ids(ids)

def svuota_prelievi():
    del_prelievi()
