# app/supabase_client.py
import os
import logging
from dotenv import load_dotenv
from urllib.parse import urlparse

import httpx
from supabase import create_client

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
    Ritorna un SUPABASE_URL valido e completo (con schema).
    - Se c'è SUPABASE_URL ma senza schema, antepone https://
    - Se manca SUPABASE_URL ma c'è SUPABASE_PROJECT_ID, costruisce l'URL.
    """
    url = os.environ.get("SUPABASE_URL", "").strip()
    project_id = os.environ.get("SUPABASE_PROJECT_ID", "").strip()

    if not url and project_id:
        url = f"https://{project_id}.supabase.co"

    if not url:
        raise EnvironmentError(
            "Supabase URL mancante. Imposta SUPABASE_URL o SUPABASE_PROJECT_ID nel file .env."
        )

    parsed = urlparse(url)
    if not parsed.scheme:
        # Mancava http/https -> forziamo https
        url = f"https://{url}"

    # Ricontrollo
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise EnvironmentError(
            f"SUPABASE_URL non valido: '{url}'. Deve iniziare con http:// o https://"
        )

    return url

def _get_supabase_key() -> str:
    """
    Se disponibile usa la SERVICE_ROLE; in fallback prova SUPABASE_KEY o SUPABASE_ANON_KEY.
    """
    key = (
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("SUPABASE_KEY")
        or os.environ.get("SUPABASE_ANON_KEY")
        or ""
    ).strip()

    if not key:
        raise EnvironmentError(
            "Supabase key mancante. Imposta SUPABASE_SERVICE_ROLE_KEY (consigliato) o SUPABASE_KEY/SUPABASE_ANON_KEY."
        )
    return key

SUPABASE_URL = _ensure_supabase_url()
SUPABASE_KEY = _get_supabase_key()

# -----------------------------------------------------------------------------
# Client Supabase
# -----------------------------------------------------------------------------
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------------------------------------------------------------
# Patch HTTPX (PostgREST) - forza HTTP/1.1 e timeouts comodi
# -----------------------------------------------------------------------------
httpx_client = httpx.Client(
    http2=False,  # evita alcuni "Server disconnected" / RST_STREAM
    timeout=httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=60.0),
    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
)

# supabase-py v2 espone la sessione httpx su postgrest
try:
    # In alcune versioni: supabase.postgrest.client.session
    # In altre:            supabase.postgrest._client.session
    postgrest_client = getattr(supabase.postgrest, "client", None) or getattr(supabase.postgrest, "_client", None)
    if postgrest_client is None:
        raise AttributeError("PostgREST client non trovato all'interno di supabase.postgrest")

    setattr(postgrest_client, "session", httpx_client)
    logging.info("✅ Supabase PostgREST session patched (HTTP/1.1, timeouts estesi).")
except Exception as ex:
    # Non blocchiamo l'app se il patch fallisce: logghiamo e proseguiamo
    logging.warning(f"⚠️ Impossibile patchare la sessione HTTP di Supabase: {ex}")

# -----------------------------------------------------------------------------
# Log finale di avvio
# -----------------------------------------------------------------------------
masked_url = SUPABASE_URL.replace("https://", "").replace("http://", "")
logging.info(f"✅ Supabase client initialized (url='{masked_url}').")
