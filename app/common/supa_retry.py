# app/common/supa_retry.py
import time, logging, httpx
from postgrest.exceptions import APIError
from app import supabase_client

_RETRYABLE_EXC = (
    httpx.RemoteProtocolError,
    httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout,
    httpx.ConnectError, httpx.ReadError, httpx.ProtocolError,
    httpx.TransportError, httpx.RequestError,
)

def supa_with_retry(builder_fn, retries: int = 6, delay: float = 0.5, backoff: float = 2.0):
    last_ex = None
    cur_delay = delay

    for attempt in range(1, retries + 1):
        try:
            builder = builder_fn()
            res = builder.execute() if hasattr(builder, "execute") else builder
            if hasattr(supabase_client, "note_success"):
                try: supabase_client.note_success()
                except Exception: pass
            return res

        except APIError as ex:
            msg = getattr(ex, "args", [None])[0]
            code = (msg.get("code") if isinstance(msg, dict) else None)
            details = ((msg.get("details") or "") if isinstance(msg, dict) else "")
            message = ((msg.get("message") or "") if isinstance(msg, dict) else "")

            # NO retry su errori business (PL/pgSQL)
            if code == "P0001":  # 'Quantità oltre il disponibile' / 'Riga origine non trovata' ecc.
                raise ex

            # SI retry su transient CF/edge o 409 JSON/5xx
            transient = (
                "Cloudflare" in details or "Could not find host" in details
                or "JSON could not be generated" in message
                or code in (409, 502, 503, 504)
            )
            last_ex = ex
            if transient:
                logging.warning(f"[supa_with_retry] attempt {attempt}/{retries} — transient APIError: {ex}")
                if hasattr(supabase_client, "note_disconnect_and_maybe_reset"):
                    try: supabase_client.note_disconnect_and_maybe_reset()
                    except Exception: pass
            else:
                logging.warning(f"[supa_with_retry] attempt {attempt}/{retries} — APIError: {ex}")

        except _RETRYABLE_EXC as ex:
            last_ex = ex
            logging.warning(f"[supa_with_retry] attempt {attempt}/{retries} — net/proto: {ex}")
            if hasattr(supabase_client, "note_disconnect_and_maybe_reset"):
                try: supabase_client.note_disconnect_and_maybe_reset()
                except Exception: pass

        except Exception as ex:
            last_ex = ex
            logging.warning(f"[supa_with_retry] attempt {attempt}/{retries} — generic: {ex}")

        if attempt < retries:
            time.sleep(cur_delay * (1.0 + 0.2))
            cur_delay *= backoff

    raise last_ex
