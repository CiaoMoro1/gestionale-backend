# app/routes/produzione.py
# -*- coding: utf-8 -*-

from __future__ import annotations

# Flask
from flask import Blueprint, jsonify, request

# Stdlib
import logging
import json
import time
from datetime import datetime, timezone
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple


# Supabase client (come nel tuo progetto)
from app.supabase_client import supabase
from app import supabase_client  # per note_success / reset

# HTTPX per gestione retryable exceptions (come già usavi)
import httpx

bp = Blueprint("produzione", __name__)

# =============================================================================
# Helper & Utilities
# =============================================================================

def _coalesce_logs(logs, window_seconds=3):
    """
    Deduplica i trigger tecnici a ridosso di un'azione umana.
    Regola: se entro 'window_seconds' c'è un log dell'operatore
    che descrive la stessa azione, nascondi il Trigger INSERT/UPDATE.
    """
    out = []
    # indicizza per (sku, ean, stato_nuovo, qty_nuova) entro la finestra
    human_events = []
    for l in logs:
        when = None
        try:
            when = datetime.fromisoformat(str(l.get("created_at")).replace("Z", "+00:00"))
        except Exception:
            pass
        l["_dt"] = when
        is_human = (l.get("utente") or "").strip().lower() not in ("", "postgres", "postgrest", "supabase", "sistema")
        if is_human:
            human_events.append(l)

    for l in logs:
        motivo_low = (l.get("motivo") or "").strip().lower()
        if motivo_low.startswith("trigger"):
            # cerca human match nella finestra temporale
            dt = l.get("_dt")
            if dt:
                for h in human_events:
                    if h.get("sku")==l.get("sku") and h.get("ean")==l.get("ean"):
                        if h.get("stato_nuovo")==l.get("stato_nuovo") and h.get("qty_nuova")==l.get("qty_nuova"):
                            hdt = h.get("_dt")
                            if hdt and abs((hdt - dt).total_seconds()) <= window_seconds:
                                # drop questo trigger
                                break
                else:
                    out.append(l)
            else:
                out.append(l)
        else:
            out.append(l)
    # pulizia
    for l in out:
        l.pop("_dt", None)
    return out


# eccezioni di rete retryable per supabase
_RETRYABLE_EXC = (
    httpx.RemoteProtocolError,
    httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout,
    httpx.ConnectError, httpx.ReadError, httpx.ProtocolError,
    httpx.TransportError, httpx.RequestError,
)

def sb_table(name: str):
    """
    Rende compatibile la tua istanza supabase in caso di differenze di binding.
    """
    tbl_attr_inst = getattr(supabase, "table", None)
    if callable(tbl_attr_inst):
        try:
            return tbl_attr_inst(name)
        except TypeError:
            func = getattr(tbl_attr_inst, "__func__", None)
            if callable(func):
                try:
                    return func(supabase, name)
                except TypeError:
                    return func(name)
    tbl_attr_cls = getattr(supabase.__class__, "table", None)
    if callable(tbl_attr_cls):
        try:
            return tbl_attr_cls(supabase, name)
        except TypeError:
            return tbl_attr_cls(name)
    raise RuntimeError("sb_table: supabase.table() non disponibile")

def supa_with_retry(builder_fn, retries=6, delay=0.35, backoff=1.8):
    """
    Esegue il builder con retry sugli errori di rete. Ritorna l'oggetto
    response di supabase (con .data). Se il builder ha già .execute, la invoca.
    """
    last_ex = None
    cur_delay = delay
    for attempt in range(1, retries + 1):
        try:
            builder = builder_fn()
            res = builder.execute() if hasattr(builder, "execute") else builder
            # hook di salute connessione (come nel tuo progetto)
            if hasattr(supabase_client, "note_success"):
                supabase_client.note_success()
            return res
        except _RETRYABLE_EXC as ex:
            last_ex = ex
            logging.warning(f"[supa_with_retry] attempt {attempt}/{retries} net: {ex}")
            if hasattr(supabase_client, "note_disconnect_and_maybe_reset"):
                supabase_client.note_disconnect_and_maybe_reset()
        except Exception as ex:
            last_ex = ex
            logging.warning(f"[supa_with_retry] attempt {attempt}/{retries} generic: {ex}")
        if attempt < retries:
            time.sleep(cur_delay * (1.0 + 0.15))
            cur_delay *= backoff
    raise last_ex

def _current_user_label() -> str:
    """
    Ricava l'utente dai header. Converte postgres/postgrest/supabase in 'Sistema'.
    """
    who = (request.headers.get("X-USER-NAME") or request.headers.get("X-USER-ID") or "").strip()
    if not who:
        return "Sistema"
    low = who.lower()
    if low in {"postgres", "postgrest", "supabase", "system", "sistema"}:
        return "Sistema"
    return who

def estrai_radice(s: Optional[str]) -> str:
    """
    Radice = primo token prima del primo '-' nello SKU, uppercased.
    Esempi:
      'ACC-MF-FUCSIA-S' -> 'ACC'
      'CFDM-LEGO-X2'    -> 'CFDM'
      'TPCD'            -> 'TPCD'
    """
    if not s:
        return ""
    return s.split("-")[0].strip().upper()

def _norm_null(v):
    """
    Converte '', 'None', 'null' in None (Python) -> NULL in SQL
    """
    if v is None:
        return None
    if isinstance(v, str):
        vs = v.strip()
        if vs == "" or vs.lower() in {"none", "null"}:
            return None
        return vs
    return v

def _eq_or_is_null(query, col: str, value):
    """
    Applica eq oppure is.null in base al valore.
    """
    if value is None:
        return query.is_(col, "null")
    return query.eq(col, value)

def log_movimento_produzione(
    produzione_row: Dict[str, Any],
    *,
    utente: str,
    motivo: str,
    stato_vecchio: Optional[str] = None,
    stato_nuovo: Optional[str] = None,
    qty_vecchia: Optional[int] = None,
    qty_nuova: Optional[int] = None,
    plus_vecchio: Optional[int] = None,
    plus_nuovo: Optional[int] = None,
    dettaglio: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        canale_norm = (produzione_row.get("canale") or "Amazon Vendor").strip() or "Amazon Vendor"
        sku = produzione_row.get("sku")
        payload = {
            "produzione_id": produzione_row.get("id"),
            "sku": sku,
            "ean": produzione_row.get("ean"),
            "start_delivery": produzione_row.get("start_delivery"),
            "canale": canale_norm,  # <- evita NULL
            "stato_vecchio": stato_vecchio,
            "stato_nuovo": stato_nuovo,
            "qty_vecchia": qty_vecchia,
            "qty_nuova": qty_nuova,
            "plus_vecchio": plus_vecchio,
            "plus_nuovo": plus_nuovo,
            "utente": utente or "Sistema",
            "motivo": motivo or "Aggiornamento",
            "dettaglio": (dettaglio or {}),
            "meta": {"canale": canale_norm, "radice": estrai_radice(sku or "")},
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        supa_with_retry(lambda: sb_table("movimenti_produzione_vendor").insert(payload).execute())
    except Exception as ex:
        logging.warning(f"[log_movimento_produzione] errore log: {ex}")

@bp.get("/api/produzione/<int:produzione_id>/log-unified")
def log_unified(produzione_id: int):
    """
    Chiama le funzioni Postgres via Supabase RPC (PostgREST).
    - compatto: produzione_unified_logs_compatti(p_id)
    - raw:      produzione_unified_logs(p_id)
    """
    try:
        compact = request.args.get("compact") in ("1", "true", "yes")

        fn_name = "produzione_unified_logs_compatti" if compact else "produzione_unified_logs"
        # NB: i parametri devono chiamarsi come nella funzione SQL (p_id)
        res = supa_with_retry(lambda: supabase.rpc(fn_name, {"p_id": produzione_id}).execute())
        rows = res.data or []
        return jsonify(rows)
    except Exception as ex:
        logging.exception("[log_unified] errore RPC")
        return jsonify({"error": f"Errore log unified: {str(ex)}"}), 500

@bp.get("/api/produzione/<int:produzione_id>/log-unified/edges")
def log_unified_edges(produzione_id: int):
    try:
        res = supa_with_retry(lambda: supabase.rpc("produzione_unified_edges_compatti", {"p_id": produzione_id}).execute())
        rows = res.data or []
        # normalizzo la forma in {from,to,qty}
        out = [{"from": r["from_stato"], "to": r["to_stato"], "qty": int(r["qty"])} for r in rows]
        return jsonify(out)
    except Exception as ex:
        logging.exception("[log_unified_edges] errore RPC")
        return jsonify({"error": f"Errore edges: {str(ex)}"}), 500

# -----------------------------------------------------------------------------
# Lista produzione + badge
# -----------------------------------------------------------------------------
@bp.route('/api/produzione', methods=['GET'])
def lista_produzione():
    
    try:
        stato = request.args.get("stato_produzione")
        radice = request.args.get("radice")
        search = request.args.get("search", "").strip()
        canale = request.args.get("canale")  # NEW

        query = sb_table("produzione_vendor").select("*")
        if stato:
            query = query.eq("stato_produzione", stato)
        if radice:
            query = query.eq("radice", radice)
        if canale:
            query = query.eq("canale", canale)  # NEW
        if search:
            query = query.or_(f"sku.ilike.%{search}%,ean.ilike.%{search}%")
        query = query.order("start_delivery", desc=False, nullsfirst=True).order("sku")
        rows = supa_with_retry(lambda: query.execute()).data

        all_rows = supa_with_retry(lambda: (
            sb_table("produzione_vendor").select("stato_produzione,radice,canale").execute()
        )).data

        badge_stati, badge_radici, badge_canali = {}, {}, {}
        for r in (all_rows or []):
            s = r.get("stato_produzione", "Da Stampare")
            badge_stati[s] = badge_stati.get(s, 0) + 1
            rd = r.get("radice") or "?"
            badge_radici[rd] = badge_radici.get(rd, 0) + 1
            c = r.get("canale") or "?"
            badge_canali[c] = badge_canali.get(c, 0) + 1

        return jsonify({
            "data": rows or [],
            "badge_stati": badge_stati,
            "badge_radici": badge_radici,
            "badge_canali": badge_canali,
            "all_radici": sorted(set(r.get("radice") for r in (all_rows or []) if r.get("radice")))
        })
    except Exception as ex:
        logging.exception("[lista_produzione] Errore nella GET produzione")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500


# -----------------------------------------------------------------------------
# Patch singola riga produzione (con log)
# -----------------------------------------------------------------------------
@bp.route('/api/produzione/<int:id>', methods=['PATCH'])
def patch_produzione(id):
    try:
        data = request.json or {}
        fields = {}
        utente = _current_user_label()

        old = supa_with_retry(lambda: (
            sb_table("produzione_vendor").select("*").eq("id", id).single().execute()
        )).data
        if not old:
            return jsonify({"error": "Produzione non trovata"}), 404

        log_entries = []

        if "stato_produzione" in data and data["stato_produzione"] != old["stato_produzione"]:
            fields["stato_produzione"] = data["stato_produzione"]
            log_entries.append(dict(
                produzione_row=old,
                utente=utente,
                motivo="Cambio stato",
                stato_vecchio=old["stato_produzione"],
                stato_nuovo=data["stato_produzione"]
            ))
                
        if "da_produrre" in data and data["da_produrre"] != old["da_produrre"]:
            fields["da_produrre"] = data["da_produrre"]
            fields["modificata_manualmente"] = True
            log_entries.append(dict(
                produzione_row=old,
                utente=utente,
                motivo="Modifica quantità",
                qty_vecchia=old["da_produrre"],
                qty_nuova=data["da_produrre"]
            ))
        if "plus" in data and (old.get("plus") or 0) != (data.get("plus") or 0):
            fields["plus"] = data["plus"]
            log_entries.append(dict(
                produzione_row=old,
                utente=utente,
                motivo="Modifica plus",
                plus_vecchio=old.get("plus") or 0,
                plus_nuovo=data["plus"]
            ))
        for f in ["cavallotti", "note"]:
            if f in data:
                fields[f] = data[f]

        if "da_produrre" in data and old["stato_produzione"] != "Da Stampare":
            if data.get("password") != "oreste":
                return jsonify({"error": "Password richiesta per modificare la quantità in questo stato."}), 403

        if not fields:
            return jsonify({"error": "Nessun campo da aggiornare"}), 400

        res = supa_with_retry(lambda: (
        sb_table("produzione_vendor").update(fields).eq("id", id).execute()
            ))

        for entry in log_entries:
            log_movimento_produzione(**entry)

        return jsonify({"ok": True, "updated": res.data})
    except Exception as ex:
        logging.exception("[patch_produzione] Errore patch produzione")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500



# -----------------------------------------------------------------------------
# Patch bulk produzione (con log)
# -----------------------------------------------------------------------------
@bp.route('/api/produzione/bulk', methods=['PATCH'])
def patch_produzione_bulk():
    try:
        ids = request.json.get("ids", [])
        update_fields = request.json.get("fields", {})
        if not ids or not update_fields:
            return jsonify({"error": "Nessun id/campo"}), 400

        utente = _current_user_label()

        # 👉 Aggiornamento bulk “secco”: niente select preventiva, niente log per-riga
        supa_with_retry(lambda: (
            sb_table("produzione_vendor").update(update_fields).in_("id", ids).execute()
        ))

        # 👉 Unico log di riepilogo dell’operazione bulk
        try:
            stub = {
                "id": None,
                "sku": "*",
                "ean": None,
                "start_delivery": None,
                "stato_produzione": None,
                "plus": 0,
                "canale": "Amazon Vendor",
            }
            log_movimento_produzione(
                stub,
                utente=utente,
                motivo="Bulk update produzione",
                dettaglio={
                    # se in futuro distingui scope, cambia questo valore:
                    "scope": "bulk_selected",   # possibili: 'bulk_selected' | 'bulk_filtered' | 'bulk_all'
                    "affected_count": len(ids),
                    "fields": update_fields,
                    "ids": ids,
                },
            )
        except Exception:
            # il bulk rimane valido anche se il log di riepilogo fallisse
            pass

        return jsonify({"ok": True, "updated_count": len(ids)})
    except Exception as ex:
        logging.exception("[patch_produzione_bulk] Errore PATCH bulk produzione")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500

# -----------------------------------------------------------------------------
# GET produzione by ID
# -----------------------------------------------------------------------------
@bp.route('/api/produzione/<int:id>', methods=['GET'])
def get_produzione_by_id(id):
    try:
        res = supa_with_retry(lambda: (
            sb_table("produzione_vendor").select("*").eq("id", id).single().execute()
        ))
        return jsonify(res.data)
    except Exception as ex:
        logging.exception(f"[get_produzione_by_id] Errore GET produzione ID {id}")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500

# -----------------------------------------------------------------------------
# Log storico di una riga produzione
# -----------------------------------------------------------------------------
@bp.route('/api/produzione/<int:id>/log', methods=['GET'])
def get_log_movimenti(id):
    try:
        logs = supa_with_retry(lambda: (
            sb_table("movimenti_produzione_vendor")
            .select("*")
            .eq("produzione_id", id)
            .order("created_at", desc=True)
            .execute()
        )).data or []

        # arricchisci canale / etichette user-friendly
        def _canale_label(l):
            # 1) se già presente in riga
            c = l.get("canale")
            if c: return c
            # 2) prova da meta/dettaglio JSON
            for k in ("meta", "dettaglio"):
                raw = l.get(k)
                if isinstance(raw, dict) and raw.get("canale"):
                    return raw["canale"]
                if isinstance(raw, str):
                    try:
                        j = json.loads(raw)
                        if j.get("canale"):
                            return j["canale"]
                    except Exception:
                        pass
            # 3) fallback: prendo una riga produzione compatibile
            q = sb_table("produzione_vendor").select("canale").eq("sku", l.get("sku"))
            q = _eq_or_is_null(q, "ean", l.get("ean"))
            q = _eq_or_is_null(q, "start_delivery", l.get("start_delivery"))
            r = supa_with_retry(lambda: q.limit(1).execute()).data or []
            return (r[0]["canale"] if r else None)

        def _humanize(l):
            motivo_raw = (l.get("motivo") or "").strip()
            motivo_low = motivo_raw.lower()
            if motivo_low.startswith("trigger insert"):
                motivo = "Creazione riga (sistema)"
            elif motivo_low.startswith("trigger update"):
                motivo = "Aggiornamento automatico (sistema)"
            else:
                motivo = motivo_raw or "Aggiornamento"

            utente = (l.get("utente") or "").strip()
            if not utente or utente.lower() in ("postgres","postgrest","supabase"):
                utente = "Sistema"

            l["motivo"] = motivo
            l["utente"] = utente
            l["canale_label"] = _canale_label(l)
            return l

        logs = [_humanize(l) for l in logs]

        # dedupe: se esiste "Inserimento manuale" nello stesso secondo e stesso stato/qty,
        # nascondi "Creazione riga (sistema)"
        seen_keys = set()
        out = []
        for l in logs:
            ts = l.get("created_at")
            sec = int(datetime.fromisoformat(str(ts).replace("Z","+00:00")).timestamp()) if ts else 0
            key = (sec, l.get("stato_nuovo"), l.get("qty_nuova"))

            if l.get("motivo") == "Inserimento manuale":
                seen_keys.add(key)
                out.append(l)
                continue

            if l.get("motivo") == "Creazione riga (sistema)" and key in seen_keys:
                # salta il trigger duplicato
                continue

            out.append(l)

        return jsonify(_coalesce_logs(out, window_seconds=3))
    except Exception as ex:
        logging.exception(f"[get_log_movimenti] Errore GET log movimenti produzione {id}")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500





# -----------------------------------------------------------------------------
# Bulk delete produzione (+ cancellazione log collegati)
# -----------------------------------------------------------------------------
@bp.route('/api/produzione/bulk', methods=['DELETE'])
def delete_produzione_bulk():
    try:
        ids = request.json.get("ids", [])
        if not ids:
            return jsonify({"error": "Nessun id"}), 400

        BATCH_SIZE = 100
        for i in range(0, len(ids), BATCH_SIZE):
            batch_ids = ids[i:i + BATCH_SIZE]
            supa_with_retry(lambda ids=batch_ids: (
                sb_table("movimenti_produzione_vendor").delete().in_("produzione_id", ids).execute()
            ))
            time.sleep(0.05)

        for i in range(0, len(ids), BATCH_SIZE):
            batch_ids = ids[i:i + BATCH_SIZE]
            supa_with_retry(lambda ids=batch_ids: (
                sb_table("produzione_vendor").delete().in_("id", ids).execute()
            ))
            time.sleep(0.05)

        return jsonify({"ok": True, "deleted_count": len(ids)})
    except Exception as ex:
        logging.exception("[delete_produzione_bulk] Errore DELETE bulk produzione")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500



# -----------------------------------------------------------------------------
# Pulizia produzione "Da Stampare"
# -----------------------------------------------------------------------------
@bp.route('/api/produzione/pulisci-da-stampare', methods=['POST'])
def pulisci_da_stampare_endpoint():
    try:
        def norm(x):
            return (
                (x.get("sku") or "").strip().lower().replace(" ", ""),
                (x.get("ean") or "").strip().lower().replace(" ", "")
            )

        produzione = supa_with_retry(lambda: (
            sb_table("produzione_vendor").select("id,sku,ean,start_delivery").eq("stato_produzione", "Da Stampare").execute()
        )).data
        prelievi = supa_with_retry(lambda: (
            sb_table("prelievi_ordini_amazon").select("sku,ean,start_delivery").execute()
        )).data

        max_data_per_sku_ean = defaultdict(str)
        for p in prelievi:
            chiave = norm(p)
            data = str(p.get("start_delivery") or "")[:10]
            if data and (data > max_data_per_sku_ean[chiave]):
                max_data_per_sku_ean[chiave] = data

        ids_da_eliminare = []
        for r in produzione:
            chiave = norm(r)
            data_riga = str(r.get("start_delivery") or "")[:10]
            if max_data_per_sku_ean.get(chiave) and data_riga != max_data_per_sku_ean[chiave]:
                ids_da_eliminare.append(r["id"])
            elif chiave not in max_data_per_sku_ean:
                ids_da_eliminare.append(r["id"])

        if ids_da_eliminare:
            # Log unico di sintesi (nessun log per singola riga)
            try:
                stub = {
                    "id": None,
                    "sku": "*",
                    "ean": None,
                    "start_delivery": None,
                    "stato_produzione": None,
                    "plus": 0,
                    "canale": "Amazon Vendor",
                }
                log_movimento_produzione(
                    stub,
                    utente=_current_user_label(),
                    motivo="Pulizia Da Stampare",
                    dettaglio={
                        "scope": "globale",
                        "deleted": len(ids_da_eliminare),
                    },
                )
            except Exception:
                pass

            supa_with_retry(lambda: (
                sb_table("produzione_vendor").delete().in_("id", ids_da_eliminare).execute()
            ))

        return jsonify({"ok": True, "deleted": len(ids_da_eliminare)})
    except Exception as ex:
        logging.exception("[pulisci_da_stampare_endpoint] Errore pulizia produzione da stampare")
        return jsonify({"error": f"Errore pulizia: {str(ex)}"}), 500


# -----------------------------------------------------------------------------
# Pulizia parziale "Da Stampare"
# -----------------------------------------------------------------------------
@bp.route('/api/produzione/pulisci-da-stampare-parziale', methods=['POST'])
def pulisci_da_stampare_parziale():
    try:
        data = request.json
        radice = data.get("radice")
        ids = data.get("ids", [])

        def norm(x):
            return (
                (x.get("sku") or "").strip().lower().replace(" ", ""),
                (x.get("ean") or "").strip().lower().replace(" ", "")
            )

        produzione_query = sb_table("produzione_vendor").select("id,sku,ean,start_delivery,prelievo_id")
        if ids:
            produzione_query = produzione_query.in_("prelievo_id", ids)
        elif radice:
            produzione_query = produzione_query.eq("radice", radice)
        produzione = supa_with_retry(lambda: (
            produzione_query.eq("stato_produzione", "Da Stampare").execute()
        )).data

        prelievi_query = sb_table("prelievi_ordini_amazon").select("id,sku,ean,start_delivery")
        if ids:
            prelievi_query = prelievi_query.in_("id", ids)
        elif radice:
            prelievi_query = prelievi_query.eq("radice", radice)
        prelievi = supa_with_retry(lambda: prelievi_query.execute()).data

        max_data_per_sku_ean = defaultdict(str)
        for p in prelievi:
            chiave = norm(p)
            data = str(p.get("start_delivery") or "")[:10]
            if data and (data > max_data_per_sku_ean[chiave]):
                max_data_per_sku_ean[chiave] = data

        ids_da_eliminare = []
        for r in produzione:
            chiave = norm(r)
            data_riga = str(r.get("start_delivery") or "")[:10]
            if max_data_per_sku_ean.get(chiave) and data_riga != max_data_per_sku_ean[chiave]:
                ids_da_eliminare.append(r["id"])
            elif chiave not in max_data_per_sku_ean:
                ids_da_eliminare.append(r["id"])

        if ids_da_eliminare:
            # Log unico di sintesi (nessun log per singola riga)
            try:
                stub = {
                    "id": None,
                    "sku": "*",
                    "ean": None,
                    "start_delivery": None,
                    "stato_produzione": None,
                    "plus": 0,
                    "canale": "Amazon Vendor",
                }
                log_movimento_produzione(
                    stub,
                    utente=_current_user_label(),
                    motivo="Pulizia Da Stampare (parziale)",
                    dettaglio={
                        "scope": "parziale",
                        "deleted": len(ids_da_eliminare),
                        "radice": radice,
                        "prelievo_ids": ids,
                    },
                )
            except Exception:
                pass

            supa_with_retry(lambda: (
                sb_table("produzione_vendor").delete().in_("id", ids_da_eliminare).execute()
            ))

        return jsonify({"ok": True, "deleted": len(ids_da_eliminare)})
    except Exception as ex:
        logging.exception("[pulisci_da_stampare_parziale] Errore pulizia parziale da stampare")
        return jsonify({"error": f"Errore pulizia parziale: {str(ex)}"}), 500

# -----------------------------------------------------------------------------
# Inserimento manuale in produzione (canali: Amazon Seller, Sito)
# -----------------------------------------------------------------------------
@bp.route('/api/produzione/manuale', methods=['POST'])
def crea_produzione_manuale():
    try:
        data = request.json or {}
        canale = (data.get("canale") or "").strip()
        if canale not in ("Amazon Seller", "Sito"):
            return jsonify({"error": "Canale non valido. Usa 'Amazon Seller' o 'Sito'."}), 400

        sku = (data.get("sku") or "").strip()
        ean = (data.get("ean") or "").strip() or None
        qty = data.get("qty")
        start_delivery = (data.get("start_delivery") or "").strip() or None  # opzionale per Sito
        note = (data.get("note") or "").strip()
        plus = int(data.get("plus") or 0)
        cavallotti = bool(data.get("cavallotti") or False)
        radice = estrai_radice(sku)

        if not sku:
            return jsonify({"error": "sku obbligatorio"}), 400
        try:
            qty = int(qty)
        except Exception:
            return jsonify({"error": "qty deve essere un intero >= 1"}), 400
        if qty < 1:
            return jsonify({"error": "qty deve essere >= 1"}), 400
        if len(note) > 255:
            return jsonify({"error": "Nota troppo lunga (max 255)"}), 400

        # tenta aggregazione su riga "Da Stampare" esistente
        # tenta aggregazione su riga "Da Stampare" esistente (stessa chiave logica)
        def _build_existing_query():
            q = (sb_table("produzione_vendor")
                .select("id, da_produrre, qty, plus")
                .eq("sku", sku)
                .eq("stato_produzione", "Da Stampare")
                .eq("canale", canale))
            q = _eq_or_is_null(q, "ean", ean)
            q = _eq_or_is_null(q, "start_delivery", start_delivery)
            return q.limit(1).execute()

        existing = supa_with_retry(_build_existing_query).data or []

        if existing:
            r = existing[0]
            new_qty = int(r.get("da_produrre") or 0) + qty + plus
            # merge
            supa_with_retry(lambda: (
                sb_table("produzione_vendor")
                .update({
                    "da_produrre": new_qty,
                    "qty": new_qty,
                    "plus": 0,
                    "note": note or None,
                    "cavallotti": cavallotti
                }).eq("id", r["id"]).execute()
            ))
            return jsonify({"ok": True, "id": r["id"], "aggregated": True})

        nuovo = {
            "prelievo_id": None,
            "sku": sku,
            "ean": ean,
            "qty": qty,
            "riscontro": 0,
            "plus": plus,
            "start_delivery": start_delivery,
            "stato": "manuale",
            "stato_produzione": "Da Stampare",
            "da_produrre": qty + plus,
            "cavallotti": cavallotti,
            "note": note or None,
            "canale": canale
        }
        inserted = supa_with_retry(lambda: sb_table("produzione_vendor").insert(nuovo).execute()).data or []
        new_id = inserted[0]["id"] if inserted else None

        # ---- NEW: log esplicito “Inserimento manuale” ----
        try:
            user_label = _current_user_label()
            if inserted:
                irow = inserted[0]
                log_movimento_produzione(
                    irow,
                    utente=user_label,
                    motivo="Inserimento manuale",
                    stato_vecchio=None,
                    stato_nuovo="Da Stampare",
                    qty_vecchia=None,
                    qty_nuova=irow.get("da_produrre"),
                    plus_vecchio=None,
                    plus_nuovo=irow.get("plus") or 0,
                    dettaglio={"canale": irow.get("canale")}
                )
        except Exception:
            pass
        # ---------------------------------------------------

        return jsonify({"ok": True, "id": new_id, "aggregated": False})
    except Exception as ex:
        logging.exception("[crea_produzione_manuale] Errore inserimento manuale")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500
    
    # -----------------------------------------------------------------------------
# Sposta parte dei pezzi di una riga produzione in un altro stato
# -----------------------------------------------------------------------------
def _merge_into_target(row_src: dict, to_state: str, qty: int, *, log_merge: bool = True):
    """
    Merge 'qty' sulla destinazione (stessa chiave logica) e RITORNA l'ID della riga target.
    Se log_merge=False, non scrive il log 'Merge in <to_state>' (utile quando stiamo già loggando lo SPOTAMENTO).
    """
    key = {
        "sku": row_src.get("sku"),
        "ean": row_src.get("ean"),
        "start_delivery": row_src.get("start_delivery"),
        "stato_produzione": to_state,
        "canale": row_src.get("canale"),
    }

    def _select_target():
        q = (sb_table("produzione_vendor")
            .select("id, da_produrre, plus, stato_produzione, canale")
            .eq("sku", key["sku"])
            .eq("stato_produzione", to_state)
            .eq("canale", key["canale"]))
        q = _eq_or_is_null(q, "ean", key["ean"])
        q = _eq_or_is_null(q, "start_delivery", key["start_delivery"])
        return q.limit(1).execute()


    found = supa_with_retry(_select_target).data or []

    user_label = _current_user_label()

    if found:
        tgt = found[0]
        tgt_id = tgt["id"]
        new_val = int(tgt.get("da_produrre") or 0) + qty

        supa_with_retry(lambda: (
            sb_table("produzione_vendor").update({"da_produrre": new_val}).eq("id", tgt_id).execute()
        ))

        if log_merge:
            try:
                # log 'merge' associato alla RIGA TARGET (produzione_id = tgt_id)
                tgt_row = {
                    "id": tgt_id,
                    "sku": key["sku"],
                    "ean": key["ean"],
                    "start_delivery": key["start_delivery"],
                    "stato_produzione": to_state,
                    "plus": tgt.get("plus") or 0,
                    "canale": tgt.get("canale") or key["canale"],
                }
                log_movimento_produzione(
                    tgt_row,
                    utente=user_label,
                    motivo=f"Merge in {to_state}",
                    stato_vecchio=to_state,
                    stato_nuovo=to_state,
                    qty_vecchia=None,
                    qty_nuova=qty,  # quantità confluita nel target
                    plus_vecchio=tgt.get("plus") or 0,
                    plus_nuovo=tgt.get("plus") or 0,
                    dettaglio={"merge": True},
                )
            except Exception:
                pass

        return tgt_id

    # target non esiste -> lo creo
    nuovo = {
        "prelievo_id": row_src.get("prelievo_id"),
        "sku": key["sku"],
        "ean": key["ean"],
        "qty": row_src.get("qty"),
        "riscontro": row_src.get("riscontro"),
        "plus": 0,
        "start_delivery": key["start_delivery"],
        "stato": row_src.get("stato"),
        "stato_produzione": to_state,
        "da_produrre": qty,
        "cavallotti": row_src.get("cavallotti"),
        "note": row_src.get("note"),
        "canale": key["canale"],
    }
    inserted = supa_with_retry(lambda: sb_table("produzione_vendor").insert(nuovo).execute()).data or []
    tgt_id = inserted[0]["id"] if inserted else None

    if log_merge and tgt_id:
        try:
            tgt_row = dict(nuovo)
            tgt_row["id"] = tgt_id
            log_movimento_produzione(
                tgt_row,
                utente=user_label,
                motivo=f"Merge in {to_state}",
                stato_vecchio=to_state,
                stato_nuovo=to_state,
                qty_vecchia=None,
                qty_nuova=qty,
                plus_vecchio=0,
                plus_nuovo=0,
                dettaglio={"merge": True},
            )
        except Exception:
            pass

    return tgt_id




@bp.route('/api/produzione/move-qty', methods=['POST'])
def move_qty_endpoint():
    try:
        body = request.json or {}
        from_id = int(body.get("from_id") or 0)
        to_state = body.get("to_state")
        qty = int(body.get("qty") or 0)
        if from_id <= 0 or qty <= 0 or not to_state:
            return jsonify({"error": "Parametri non validi"}), 400

        src = supa_with_retry(lambda: (
            sb_table("produzione_vendor").select("*").eq("id", from_id).single().execute()
        )).data
        if not src:
            return jsonify({"error": "Riga produzione non trovata"}), 404

        canale = (src.get("canale") or "").strip()
        avail = int(src.get("da_produrre") or 0)
        user_label = _current_user_label()

        def _log_spostamento(qty_before: int, qty_after: int, tgt_id: int | None):
            try:
                src_row = dict(src); src_row["id"] = from_id
                log_movimento_produzione(
                    src_row,
                    utente=user_label,
                    motivo=f"Spostamento a {to_state}",
                    stato_vecchio=src.get("stato_produzione"),
                    stato_nuovo=to_state,
                    qty_vecchia=qty_before,
                    qty_nuova=qty_after,
                    plus_vecchio=src.get("plus") or 0,
                    plus_nuovo=src.get("plus") or 0,
                    dettaglio={"target_id": tgt_id}
                )
            except Exception:
                pass

        if qty <= avail:
            new_src_val = avail - qty
            supa_with_retry(lambda: (
                sb_table("produzione_vendor").update({"da_produrre": new_src_val}).eq("id", from_id).execute()
            ))

            tgt_id = _merge_into_target(src, to_state, qty, log_merge=False)  # <— niente log 'Merge in ...' qui
            _log_spostamento(avail, new_src_val, tgt_id)

            if new_src_val <= 0:
                supa_with_retry(lambda: sb_table("produzione_vendor").delete().eq("id", from_id).execute())

            return jsonify({"ok": True})

        # qty > avail
        if canale in ("Sito", "Amazon Seller"):
            if avail > 0:
                supa_with_retry(lambda: sb_table("produzione_vendor").update({"da_produrre": 0}).eq("id", from_id).execute())
                tgt_id_1 = _merge_into_target(src, to_state, avail, log_merge=False)
                _log_spostamento(avail, 0, tgt_id_1)
                supa_with_retry(lambda: sb_table("produzione_vendor").delete().eq("id", from_id).execute())

            extra = qty - avail
            if extra > 0:
                tgt_id_2 = _merge_into_target(src, to_state, extra, log_merge=False)
                _log_spostamento(0, 0, tgt_id_2)

            return jsonify({"ok": True, "over_move": True})

        return jsonify({"error": "Quantità oltre il disponibile"}), 400

    except Exception as ex:
        logging.exception("[move_qty_endpoint] errore")
        return jsonify({"error": f"Errore: {str(ex)}"}), 500
