# app/supabase.py  ← NUOVO
from app.supabase_client import (
    supabase,
    note_success,
    note_disconnect_and_maybe_reset,
)

# shim per compatibilità con il codice esistente
def sb_table(name: str):
    return supabase.table(name)

def supa_with_retry(fn):
    # Se hai già un tuo retry, mettilo qui.
    # Intanto eseguiamo la funzione direttamente e segnaliamo success/failure al client
    try:
        res = fn()
        note_success()
        return res
    except Exception:
        # opzionale: note_disconnect_and_maybe_reset()
        raise
