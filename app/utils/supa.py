# app/utils/supa.py
import time, logging, httpx

def supa_with_retry(build_query_fn, *, retries=3, base_sleep=0.6):
    """
    build_query_fn: funzione che ritorna il builder supabase (gia' con select/eq/...),
    su cui chiameremo .execute().
    """
    for i in range(retries):
        try:
            return build_query_fn().execute()
        except (httpx.RemoteProtocolError, httpx.ReadTimeout, httpx.ConnectError) as ex:
            logging.warning(f"[supa_with_retry] tentativo {i+1}/{retries}: {ex}")
            if i < retries-1:
                time.sleep(base_sleep * (2 ** i))  # backoff esponenziale
                continue
            raise
