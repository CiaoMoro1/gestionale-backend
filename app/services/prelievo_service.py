# app/services/prelievo_service.py

from typing import Any
from app.repositories.prelievo_repo import (
    sel_date_importabili, sel_prelievi, upd_prelievo, upd_prelievi_bulk,
    del_prelievi, import_da_ordini
)
from app.services.produzione_service import sync_produzione_from_prelievo_ids
from app.supabase_client import supabase
from app.supabase import supa_with_retry  # in testa al file, con gli altri import


STATI = ("manca", "parziale", "completo", "in verifica")

def _movimenta_magazzino_canale(row: dict, canale: str, delta: int, motivo: str):
  """
  delta > 0 => SCARICA dal canale
  delta < 0 => CARICA (reso) al canale
  """
  if delta == 0:
    return
  args = {
    "p_sku": row["sku"],
    "p_ean": row["ean"],
    "p_canale": canale,
    "p_qty": abs(int(delta)),
    "p_motivo": motivo,
    "p_prelievo_id": int(row["id"])
  }
  rpc = "magazzino_scarica" if delta > 0 else "magazzino_carica"
  rpc_res = supabase.rpc(rpc, args).execute()
  err = getattr(rpc_res, "error", None) or (rpc_res.get("error") if isinstance(rpc_res, dict) else None)
  if err:
    raise RuntimeError(str(err))

def _deriva_stato(qty:int, riscontro:int|None)->str:
    r = int(riscontro or 0)
    if r < 0 or r > qty: return "in verifica"
    if r == 0: return "manca"
    if 0 < r < qty: return "parziale"
    return "completo"

def date_importabili():
    return sel_date_importabili()

def importa_prelievi_da_data(data:str):
    del_prelievi(data=data)
    return import_da_ordini(data)

def lista_prelievi(data:str|None, radice:str|None):
    return sel_prelievi(data=data, radice=radice)

def _to_int_or_none(v: Any):
    if v is None:
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        i = int(v)
        return i
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
            return int(s)
    raise ValueError("Valore numerico non valido")

def _validate_payload(p: dict):
    # --- riscontro (TOTALE) ---
    if "riscontro" in p and p["riscontro"] is not None:
        p["riscontro"] = _to_int_or_none(p["riscontro"])
        if p["riscontro"] is None or p["riscontro"] < 0:
            raise ValueError("Riscontro non valido (>=0)")

    # --- plus ---
    if "plus" in p and p["plus"] is not None:
        p["plus"] = _to_int_or_none(p["plus"])
        if p["plus"] is None or p["plus"] < 0:
            raise ValueError("Plus non valido (>=0)")

    # --- breakdown per canale (nuovo) ---
    total_by_canale = None
    if "mag_usato_by_canale" in p and p["mag_usato_by_canale"] is not None:
        if not isinstance(p["mag_usato_by_canale"], dict):
            raise ValueError("mag_usato_by_canale dev'essere un oggetto {canale: qty}")
        norm: dict[str, int] = {}
        s = 0
        for k, v in p["mag_usato_by_canale"].items():
            iv = _to_int_or_none(v)
            if iv is None or iv < 0:
                raise ValueError(f"Quantità non valida per canale '{k}'")
            norm[str(k)] = iv
            s += iv
        p["mag_usato_by_canale"] = norm
        total_by_canale = s

    # --- totale legacy (compatibilità) ---
    if "magazzino_usato" in p and p["magazzino_usato"] is not None:
        p["magazzino_usato"] = _to_int_or_none(p["magazzino_usato"])
        if p["magazzino_usato"] is None or p["magazzino_usato"] < 0:
            raise ValueError("Magazzino usato non valido (>=0)")

    # Se abbiamo il breakdown, forziamo il totale legacy a combaciare con la somma
    if total_by_canale is not None:
        p["magazzino_usato"] = total_by_canale

    # --- Coerenza: riscontro (totale) deve essere >= prenotato ---
    # (solo se entrambi sono presenti nel payload)
    if p.get("riscontro") is not None and p.get("magazzino_usato") is not None:
        if int(p["riscontro"]) < int(p["magazzino_usato"]):
            raise ValueError("Riscontro (totale) deve essere ≥ somma del magazzino usato")
  

def _movimenta_magazzino(row: dict, delta: int):
    if delta == 0:
        return
    args = {
        "p_sku": row["sku"],
        "p_ean": row["ean"],
        "p_canale": (row.get("canale") or "Amazon Vendor"),
        "p_qty": abs(int(delta)),
        "p_motivo": "Regolazione prenotati su Prelievo",
        "p_prelievo_id": int(row["id"]),
    }
    rpc = "magazzino_scarica" if delta > 0 else "magazzino_carica"  # <-- implementa/usa RPC di carico
    supabase.rpc(rpc, args).execute()

def aggiorna_prelievo(prelievo_id:int, payload:dict):
    _validate_payload(payload)

    row_list = sel_prelievi(ids=[prelievo_id])
    if not row_list:
        raise ValueError(f"Prelievo {prelievo_id} non trovato")
    row = row_list[0]

    fields = {k: v for k, v in payload.items() if k in ("riscontro", "plus", "note")}
    if "riscontro" in fields:
        fields["stato"] = _deriva_stato(qty=int(row["qty"]), riscontro=fields["riscontro"])

    channels = ["Amazon Vendor", "Sito", "Amazon Seller"]

    if "mag_usato_by_canale" in payload and payload["mag_usato_by_canale"] is not None:
        # === NUOVO FLUSSO per-canale ===
        incoming: dict[str, int] = payload.get("mag_usato_by_canale") or {}
        prev_breakdown: dict[str, int] = row.get("mag_usato_by_canale") if isinstance(row.get("mag_usato_by_canale"), dict) else {}
        prev_by = {c: int(prev_breakdown.get(c, 0) or 0) for c in channels}
        next_by = {c: int(incoming.get(c, 0) or 0) for c in channels}

        for can in channels:
            delta = next_by[can] - prev_by[can]
            if delta != 0:
                _movimenta_magazzino_canale(row, can, delta, motivo="Impiegato su Prelievo (per canale)")

        fields["mag_usato_by_canale"] = next_by
        fields["magazzino_usato"] = sum(next_by.values())
    elif "magazzino_usato" in payload and payload["magazzino_usato"] is not None:
        # === Fallback legacy: totale unico ===
        nuovo = int(payload["magazzino_usato"])
        attuale = int(row.get("magazzino_usato") or 0)
        delta = nuovo - attuale
        if delta != 0:
            _movimenta_magazzino(row, delta)
        fields["magazzino_usato"] = nuovo
        # Non tocchiamo mag_usato_by_canale se non passato

    upd_prelievo(prelievo_id, fields)
    sync_produzione_from_prelievo_ids([prelievo_id])


def aggiorna_prelievi_bulk(ids: list[int], fields: dict):
    _validate_payload(fields)

    has_breakdown = "mag_usato_by_canale" in fields and fields["mag_usato_by_canale"] is not None
    bulk_fields = {k: v for k, v in fields.items() if k in ("riscontro", "plus", "note")}
    channels = ["Amazon Vendor", "Sito", "Amazon Seller"]

    if has_breakdown:
        righe = sel_prelievi(ids=ids, canale="Amazon Vendor")
        for r in righe:
            prev_breakdown = r.get("mag_usato_by_canale") if isinstance(r.get("mag_usato_by_canale"), dict) else {}
            prev_by = {c: int(prev_breakdown.get(c, 0) or 0) for c in channels}
            next_by = {c: int(fields["mag_usato_by_canale"].get(c, 0) or 0) for c in channels}

            # Delta per canale -> movimenti magazzino
            for can in channels:
                delta = next_by[can] - prev_by[can]
                if delta != 0:
                    _movimenta_magazzino_canale(r, can, delta, motivo="Impiegato su Prelievo (per canale)")

            per_riga = dict(bulk_fields)
            if "riscontro" in per_riga:
                per_riga["stato"] = _deriva_stato(qty=int(r["qty"]), riscontro=per_riga["riscontro"])
            per_riga["mag_usato_by_canale"] = next_by
            per_riga["magazzino_usato"] = sum(next_by.values())

            upd_prelievo(int(r["id"]), per_riga)

        sync_produzione_from_prelievo_ids(ids)
        return

    # --- legacy: niente breakdown per-canale (UNICA versione) ---
    if "riscontro" in bulk_fields:
        righe = sel_prelievi(ids=ids, canale="Amazon Vendor")  # <-- SOLO Vendor

        # ⛑️ guard: se riscontro=0 (completa...), limita ai soli PENDING ("in verifica")
        if int(bulk_fields["riscontro"]) == 0:
            righe = [r for r in righe if r.get("stato") == "in verifica"]
            ids = [int(r["id"]) for r in righe]
            if not ids:
                return  # nessun pending da trattare

        # (opzionale) qui puoi auto-rilasciare prenotati residui se vuoi

        stato_per_id = { r["id"]: _deriva_stato(int(r["qty"]), bulk_fields["riscontro"]) for r in righe }
        for stato in set(stato_per_id.values()):
            ids_cluster = [i for i, s in stato_per_id.items() if s == stato]
            if ids_cluster:
                upd_prelievi_bulk(ids_cluster, {**bulk_fields, "stato": stato})
    else:
        if ids:
            upd_prelievi_bulk(ids, bulk_fields)

    sync_produzione_from_prelievo_ids(ids)


def svuota_prelievi():
    del_prelievi()


# --- Carico magazzino da produzione (bulk) ------------------------------------
from typing import List, Dict, Optional

def carica_magazzino_da_produzione(items: List[Dict]) -> Dict[str, object]:
    """
    items: [{ "id": int, "sku": str, "ean": str|None, "canale": str, "qty": int }]

    Per ogni item esegue la RPC 'magazzino_carica' su Supabase, con motivo
    'Carico da Produzione'. Ritorna un report: { "ok": int, "errors": [ ... ] }.

    NOTE:
    - qty <= 0 viene ignorato (non è errore).
    - ean opzionale (None).
    - canale default 'Amazon Vendor' se omesso/vuoto.
    """
    report: Dict[str, object] = {"ok": 0, "errors": []}
    for it in items:
        try:
            sku = str(it.get("sku") or "").strip()
            if not sku:
                raise ValueError("SKU mancante")

            qty_val = it.get("qty")
            try:
                qty = int(qty_val)
            except Exception:
                raise ValueError(f"Quantità non valida: {qty_val!r}")

            if qty <= 0:
                # Nessun carico per qty <= 0: skip, non è un errore.
                continue

            ean: Optional[str] = (it.get("ean") or None)
            canale = str(it.get("canale") or "Amazon Vendor").strip() or "Amazon Vendor"

            args = {
                "p_sku": sku,
                "p_ean": ean,
                "p_canale": canale,
                "p_qty": qty,
                "p_motivo": "Carico da Produzione",
                "p_prelievo_id": 0,  # non è un prelievo: 0 come placeholder
            }
            # retry + backoff come nel resto del service
            supa_with_retry(lambda: supabase.rpc("magazzino_carica", args).execute())
            report["ok"] = int(report["ok"]) + 1
        except Exception as ex:
            report["errors"].append({"item": it, "error": str(ex)})

    return report