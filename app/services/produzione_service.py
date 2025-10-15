# app/services/produzione_service.py

from __future__ import annotations
import logging

# usa la funzione che hai già in amazon_vendor.py
from app.routes.amazon_vendor import sync_produzione_from_prelievo

def sync_produzione_from_prelievo_ids(ids: list[int]) -> None:
    """
    Bridge: richiama la tua logica esistente per sincronizzare la produzione
    a partire dagli ID dei prelievi modificati.
    """
    if not ids:
        return
    for pid in ids:
        try:
            sync_produzione_from_prelievo(pid)
        except Exception as ex:
            logging.exception(f"[sync_produzione_from_prelievo_ids] errore su prelievo_id={pid}: {ex}")
