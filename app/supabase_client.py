# app/supabase_client.py

import os
import logging
import threading
from urllib.parse import urlparse

import httpx
from dotenv import load_dotenv
from supabase import create_client
try:
    # supabase-py >= 2.3.x
    from supabase.lib.client_options import ClientOptions
except ImportError:
    # fallback per varianti rare
    from supabase.client_options import ClientOptions  # type: ignore

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
)

# -----------------------------------------------------------------------------
# Env
# -----------------------------------------------------------------------------
load_dotenv()


def _ensure_supabase_url() -> str:
    """
    Ritorna un SUPABASE_URL valido:
    - se manca ma c'è SUPABASE_PROJECT_ID, costruisce https://{id}.supabase.co
    - forza lo schema https:// se assente
    - valida lo schema
    """
    url = os.environ.get("SUPABASE_URL", "").strip()
    project_id = os.environ.get("SUPABASE_PROJECT_ID", "").strip()

    if not url and project_id:
        url = f"https://{project_id}.supabase.co"

    if not url:
        raise EnvironmentError(
            "Supabase URL mancante. Imposta SUPABASE_URL o SUPABASE_PROJECT_ID nel file .env."
        )

    if not urlparse(url).scheme:
        url = f"https://{url}"

    if urlparse(url).scheme not in ("http", "https"):
        raise EnvironmentError(f"SUPABASE_URL non valido: '{url}'.")

    return url


def _get_supabase_key() -> str:
    """
    Preferisci SERVICE_ROLE in ambienti server-side.
    """
    key = (
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("SUPABASE_KEY")
        or os.environ.get("SUPABASE_ANON_KEY")
        or ""
    ).strip()
    if not key:
        raise EnvironmentError(
            "Supabase key mancante. Imposta SUPABASE_SERVICE_ROLE_KEY (consigliato) "
            "o SUPABASE_KEY/SUPABASE_ANON_KEY."
        )
    return key


SUPABASE_URL = _ensure_supabase_url()
SUPABASE_KEY = _get_supabase_key()

# -----------------------------------------------------------------------------
# httpx.Client gestito (riusabile e resettabile)
# -----------------------------------------------------------------------------
_httpx_lock = threading.Lock()
_httpx_client: httpx.Client | None = None


def _make_httpx_client(base_url: str | None = None, headers: dict | None = None) -> httpx.Client:
    return httpx.Client(
        http2=False,
        base_url=base_url,   # <-- importante per i path relativi
        headers=headers,     # <-- conserva apikey/Bearer ecc.
        timeout=httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=60.0),
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=50),
    )


def _get_or_create_httpx() -> httpx.Client:
    global _httpx_client
    with _httpx_lock:
        if _httpx_client is None or _httpx_client.is_closed:
            _httpx_client = _make_httpx_client()
        return _httpx_client


# -----------------------------------------------------------------------------
# Client Supabase (creato una sola volta)
# -----------------------------------------------------------------------------
def _create_supabase():
    """
    Crea il client Supabase ufficiale.
    Nota: NON gli passiamo direttamente httpx.Client; patchiamo la sessione PostgREST 'in place'
    per non rompere i riferimenti negli altri moduli.
    """
    options = ClientOptions(
        auto_refresh_token=False,      # con SERVICE_ROLE non serve refresh
        persist_session=False,
        headers={"X-Client-Info": "gestionale-backend"},
        postgrest_client_timeout=120,  # secondi
        storage_client_timeout=30,
        function_client_timeout=10,
    )
    return create_client(SUPABASE_URL, SUPABASE_KEY, options=options)


supabase = _create_supabase()


def _patch_postgrest_session(client: httpx.Client | None = None) -> bool:
    candidates = [
        ("postgrest", "client"),
        ("postgrest", "_client"),
        ("postgrest", None),
        ("rest", "client"),
        ("rest", "_client"),
        ("rest", None),
    ]

    for base_attr, sub_attr in candidates:
        try:
            base_obj = getattr(supabase, base_attr, None)
            if base_obj is None:
                continue
            target = getattr(base_obj, sub_attr) if sub_attr else base_obj
            old_session = getattr(target, "session", None)
            if old_session is None:
                continue

            # Prendi base_url e headers dalla session corrente (se presenti)
            old_base_url = getattr(old_session, "base_url", None)
            if old_base_url is not None:
                # httpx.BaseURL -> converti in str
                try:
                    old_base_url = str(old_base_url)
                except Exception:
                    old_base_url = None
            old_headers = getattr(old_session, "headers", None)

            # Se non mi hai passato un client pre-costruito, creane uno con i parametri ereditati
            if client is None:
                client = _make_httpx_client(base_url=old_base_url, headers=old_headers)
            else:
                # Se hai passato un client esterno ma senza base_url/headers, rigenera per sicurezza
                if getattr(client, "base_url", None) in (None, httpx._models.URL("")):
                    client = _make_httpx_client(base_url=old_base_url, headers=old_headers)

            setattr(target, "session", client)
            logging.info(
                f"🔧 PostgREST session patched via supabase.{base_attr}"
                f"{('.' + sub_attr) if sub_attr else ''}.session (base_url preserved)"
            )
            return True
        except Exception:
            continue

    logging.warning("⚠️ Impossibile patchare la sessione HTTP di Supabase (struttura non trovata).")
    return False


# Patch iniziale (non bloccare l'avvio se fallisce)
try:
    _patch_postgrest_session(None)  # costruisce client con base_url/headers ereditati
except Exception as ex:
    logging.warning(f"⚠️ Patch PostgREST session fallita: {ex}")


# -----------------------------------------------------------------------------
# Telemetria disconnessioni + reset 'in place'
# -----------------------------------------------------------------------------
_SUPA_LOCK = threading.Lock()
_consecutive_disconnects = 0
_DISCONNECT_THRESHOLD = 3


def reset_supabase_httpx_session():
    """
    Ricrea SOLO httpx.Client con lo stesso base_url/headers della session attuale
    e ripatcha il PostgREST del 'supabase' esistente.
    """
    # Individua la sessione corrente per leggere base_url/headers
    candidates = [
        ("postgrest", "client"), ("postgrest", "_client"), ("postgrest", None),
        ("rest", "client"), ("rest", "_client"), ("rest", None),
    ]
    old_base_url = None
    old_headers = None
    for base_attr, sub_attr in candidates:
        base_obj = getattr(supabase, base_attr, None)
        if base_obj is None:
            continue
        target = getattr(base_obj, sub_attr) if sub_attr else base_obj
        old_session = getattr(target, "session", None)
        if old_session is None:
            continue
        old_base_url = getattr(old_session, "base_url", None)
        if old_base_url is not None:
            try:
                old_base_url = str(old_base_url)
            except Exception:
                old_base_url = None
        old_headers = getattr(old_session, "headers", None)
        break  # trovato

    with _httpx_lock:
        try:
            if _httpx_client:
                _httpx_client.close()
        except Exception:
            pass
        # Ricrea client preservando base_url/headers
        new_client = _make_httpx_client(base_url=old_base_url, headers=old_headers)

    # Ripatch "in place"
    _patch_postgrest_session(new_client)
    logging.warning("♻️ Ricreata la sessione httpx con base_url/headers preservati e ripatchata su PostgREST.")


def note_disconnect_and_maybe_reset():
    """
    Da chiamare su errori rete/protocollo (es. httpx.RemoteProtocolError).
    Dopo N errori consecutivi, forza il reset della sessione httpx.
    """
    global _consecutive_disconnects
    with _SUPA_LOCK:
        _consecutive_disconnects += 1
        if _consecutive_disconnects >= _DISCONNECT_THRESHOLD:
            reset_supabase_httpx_session()
            _consecutive_disconnects = 0


def note_success():
    """
    Da chiamare dopo una chiamata Supabase andata a buon fine: azzera il contatore.
    """
    global _consecutive_disconnects
    with _SUPA_LOCK:
        _consecutive_disconnects = 0


# -----------------------------------------------------------------------------
# Log di bootstrap
# -----------------------------------------------------------------------------
masked_url = SUPABASE_URL.replace("https://", "").replace("http://", "")
logging.info(f"✅ Supabase client initialized (url='{masked_url}').")
