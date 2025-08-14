# -*- coding: utf-8 -*-
"""
Worker Supabase: 
- import_vendor_orders
- genera_fattura_amazon_vendor
- genera_notecredito_amazon_reso

Compatibile con lo schema che mi hai incollato:
- ordini_vendor_items.start_delivery è TEXT (salvo "YYYY-MM-DD" come stringa)
- ordini_vendor_riepilogo.start_delivery e fatture_amazon_vendor.start_delivery sono DATE (Postgres castera' la stringa ISO)
- aggiunto upsert su upload XML
- gestione NaN/None sicura
- deduplicazione O(1) con set chiave
- nessun cambiamento al “mapping shiftato” per le note di credito (è voluto)
"""

import io
import os
import time
import traceback
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
import html

print("IMPORT OK", flush=True)

# -----------------------
# Helpers
# -----------------------

def safe_str(x: Any) -> Optional[str]:
    """Ritorna None se NaN/None/'nan', altrimenti stringa.strip()."""
    try:
        if x is None:
            return None
        if isinstance(x, float) and pd.isna(x):
            return None
        s = str(x).strip()
        if s.lower() in ("", "none", "nan"):
            return None
        return s
    except Exception:
        return None

def safe_int(x: Any, default: int = 0) -> int:
    """Cast a int; se None/NaN o vuoto torna default."""
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return default
        s = str(x).strip()
        if s == "":
            return default
        # gestisce numeri tipo '12.0' o '12,0'
        s = s.replace(",", ".")
        return int(float(s))
    except Exception:
        return default

def to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return default
        s = str(x).replace(",", ".").replace(" ", "").strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default

def fix_numeric(val: Any) -> Optional[float]:
    """Torna None per vuoti, altrimenti float (gestisce virgole)."""
    if val is None:
        return None
    s = str(val).replace(",", ".").replace(" ", "").strip()
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None

def fix_date(val: Any) -> Optional[str]:
    """Restituisce 'YYYY-MM-DD' (string) oppure None. (Per TEXT nei items.)"""
    if val is None or (hasattr(val, "__len__") and str(val).strip().lower() in ("", "none", "nan")):
        return None
    if hasattr(val, "date"):
        try:
            return val.date().isoformat()
        except Exception:
            pass
    s = str(val).strip()
    # gestisce "YYYY-MM-DDTHH:mm:ss" o simili
    if "T" in s:
        return s.split("T")[0]
    # se è già tipo 2025-08-12 o 12/08/2025
    try:
        # prova ISO
        return datetime.fromisoformat(s).date().isoformat()
    except Exception:
        pass
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            continue
    # se proprio non capisce, torno s (ma meglio None)
    return s if len(s) == 10 and s[4] == "-" else None

def csv_to_xlsx(csv_bytes: bytes) -> bytes:
    df = pd.read_csv(io.BytesIO(csv_bytes), encoding="utf-8-sig", sep=",")
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name="Return_Items")
    return output.getvalue()

# -----------------------
# Setup Supabase
# -----------------------

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Manca SUPABASE_URL o SUPABASE_SERVICE_ROLE_KEY nell'env")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------
# RPC
# -----------------------

def genera_numero_fattura(supabase_client, anno: int) -> str:
    resp = supabase_client.rpc("genera_numero_fattura", {"anno_input": anno}).execute()
    if hasattr(resp, "data") and resp.data:
        return str(resp.data)
    raise Exception("Errore generazione numero fattura")

def genera_numero_nota_credito(supabase_client) -> str:
    resp = supabase_client.rpc("genera_numero_nota_credito").execute()
    if hasattr(resp, "data") and resp.data:
        return str(resp.data)
    raise Exception("Errore generazione numero nota credito")

# -----------------------
# IMPORT VENDOR ORDERS
# -----------------------

def process_import_vendor_orders_job(job: Dict[str, Any]) -> None:
    try:
        supabase.table("jobs").update({
            "status": "in_progress",
            "started_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()

        storage_path = job["payload"]["storage_path"]
        bucket, filename = storage_path.split("/", 1)
        print(f"[worker] Scarico file {storage_path} da storage...", flush=True)
        file_resp = supabase.storage.from_(bucket).download(filename)
        if hasattr(file_resp, 'error') and file_resp.error:
            raise Exception(f"Errore download da storage: {file_resp.error}")
        excel_bytes = file_resp

        # Il file Amazon ha intestazioni a partire dalla terza riga
        df = pd.read_excel(io.BytesIO(excel_bytes), header=2, sheet_name="Articoli")
        df.columns = [str(c).strip().replace('\n', ' ').replace('\r', '').replace('  ', ' ') for c in df.columns]

        required_columns = [
            'Numero ordine/ordine d’acquisto',
            'Codice identificativo esterno',
            'Numero di modello',
            'ASIN',
            'Titolo',
            'Costo',
            'Quantità ordinata',
            'Quantità confermata',
            'Inizio consegna',
            'Termine consegna',
            'Data di consegna prevista',
            'Stato disponibilità',
            'Codice fornitore',
            'Fulfillment Center'
        ]
        for col in required_columns:
            if col not in df.columns:
                raise Exception(f"Colonna mancante: {col}")

        # Pre-carico chiavi esistenti per deduplicazione veloce
        res = supabase.table("ordini_vendor_items").select(
            "po_number,model_number,qty_ordered,start_delivery,fulfillment_center"
        ).execute()
        ordini_esistenti = res.data if hasattr(res, 'data') else res

        def key_tuple(po: str, model: str, qty: int, start: Optional[str], fc: str):
            return (
                (po or "").strip(),
                (model or "").strip(),
                int(qty or 0),
                fix_date(start) or "",
                (fc or "").strip()
            )

        existing_keys = {
            key_tuple(
                o.get("po_number"),
                o.get("model_number"),
                o.get("qty_ordered"),
                o.get("start_delivery"),
                o.get("fulfillment_center"),
            )
            for o in (ordini_esistenti or [])
        }

        importati = 0
        po_numbers = set()
        errors: list[str] = []
        doppioni: list[str] = []

        for _, row in df.iterrows():
            try:
                k = key_tuple(
                    safe_str(row['Numero ordine/ordine d’acquisto']),
                    safe_str(row['Numero di modello']),
                    safe_int(row['Quantità ordinata']),
                    fix_date(row['Inizio consegna']),
                    safe_str(row['Fulfillment Center']),
                )
                if k in existing_keys:
                    doppioni.append(
                        f"Doppione: Ordine={row['Numero ordine/ordine d’acquisto']} | Modello={row['Numero di modello']} | Quantità={row['Quantità ordinata']}"
                    )
                    continue

                ordine = {
                    "po_number": safe_str(row["Numero ordine/ordine d’acquisto"]),
                    "vendor_product_id": safe_str(row["Codice identificativo esterno"]),
                    "model_number": safe_str(row["Numero di modello"]),
                    "asin": safe_str(row["ASIN"]),
                    "title": safe_str(row["Titolo"]),
                    "cost": to_float(row["Costo"], None),  # numeric
                    "qty_ordered": safe_int(row["Quantità ordinata"], 0),
                    "qty_confirmed": safe_int(row["Quantità confermata"], 0),
                    # N.B. in ordini_vendor_items è TEXT
                    "start_delivery": fix_date(row["Inizio consegna"]),
                    "end_delivery": fix_date(row["Termine consegna"]),
                    "delivery_date": fix_date(row["Data di consegna prevista"]),
                    # metto sia status che availability, così non perdi nulla
                    "status": safe_str(row["Stato disponibilità"]),
                    "availability": safe_str(row["Stato disponibilità"]),
                    "vendor_code": safe_str(row["Codice fornitore"]),
                    "fulfillment_center": safe_str(row["Fulfillment Center"]),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }

                supabase.table("ordini_vendor_items").insert(ordine).execute()
                existing_keys.add(k)
                if ordine["po_number"]:
                    po_numbers.add(ordine["po_number"])
                importati += 1

            except Exception as ex:
                errors.append(f"{ex}")

        # --- RIEPILOGO: aggiorna sempre dopo import ---
        ordini = supabase.table("ordini_vendor_items").select(
            "po_number, qty_ordered, fulfillment_center, start_delivery"
        ).execute().data

        gruppi: Dict[tuple, Dict[str, Any]] = defaultdict(lambda: {"po_list": set(), "totale_articoli": 0})
        for o in ordini:
            key = (o["fulfillment_center"], fix_date(o["start_delivery"]))
            gruppi[key]["po_list"].add(o["po_number"])
            gruppi[key]["totale_articoli"] += safe_int(o["qty_ordered"])

        for (fc, data), dati in gruppi.items():
            riepilogo = {
                "fulfillment_center": fc,
                "start_delivery": data,  # la tabella è DATE: Postgres casterà la stringa ISO
                "po_list": list(sorted(dati["po_list"])),
                "totale_articoli": dati["totale_articoli"],
                "stato_ordine": "nuovo",
            }
            res = supabase.table("ordini_vendor_riepilogo") \
                .select("id") \
                .eq("fulfillment_center", fc) \
                .eq("start_delivery", data) \
                .execute()
            if res.data and len(res.data) > 0:
                id_riep = res.data[0]['id']
                supabase.table("ordini_vendor_riepilogo") \
                    .update({
                        "po_list": riepilogo["po_list"],
                        "totale_articoli": riepilogo["totale_articoli"],
                    }) \
                    .eq("id", id_riep) \
                    .execute()
            else:
                supabase.table("ordini_vendor_riepilogo").insert(riepilogo).execute()

        supabase.table("jobs").update({
            "status": "done",
            "result": {
                "importati": importati,
                "doppioni": doppioni,
                "po_unici": len(po_numbers),
                "po_list": list(sorted(po_numbers)),
                "errors": errors,
            },
            "finished_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()

        print(f"[worker] Import terminato! {importati} righe, {len(doppioni)} doppioni.", flush=True)

    except Exception as e:
        print("[worker] ERRORE import!", e, flush=True)
        supabase.table("jobs").update({
            "status": "failed",
            "error": str(e),
            "stacktrace": traceback.format_exc(),
            "finished_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()

# -----------------------
# FATTURE
# -----------------------

def generate_sdi_xml(dati: Dict[str, Any]) -> str:
    """
    Genera XML SDI (FPR12) per fattura.
    dati: centro, start_delivery (YYYY-MM-DD), po_list (list[str]), articoli (rows),
          data_fattura (YYYY-MM-DD), numero_fattura, imponibile, iva, totale
    """
    centro = dati["centro"]
    start_delivery = dati["start_delivery"]
    po_list = dati["po_list"]
    articoli = dati["articoli"]
    data_fattura = dati["data_fattura"]
    numero_fattura = dati["numero_fattura"]
    imponibile = "{:.2f}".format(dati["imponibile"])
    iva = "{:.2f}".format(dati["iva"])
    totale = "{:.2f}".format(dati["totale"])

    intestatario = {
        "denominazione": "AMAZON EU SARL, SUCCURSALE ITALIANA",
        "indirizzo": "VIALE MONTE GRAPPA",
        "numero_civico": "3/5",
        "cap": "20124",
        "comune": "MILANO",
        "provincia": "MI",
        "nazione": "IT",
        "piva": "08973230967",
        "codice_destinatario": "XR6XN0E",
        "pec": "amazoneu@legalmail.it"
    }

    fornitore = {
        "denominazione": "CYBORG",
        "piva": "09780071214",
        "codice_fiscale": "09780071214",
        "indirizzo": "Via G. D' Annunzio 58",
        "cap": "80053",
        "comune": "Castellammare di Stabia",
        "provincia": "NA",
        "nazione": "IT",
        "regime_fiscale": "RF01",
        "cod_eori": "IT09780071214",
        "riferimento_amministrazione": "7401713799"
    }

    causale = f"Ordine Amazon centro {centro} - Data consegna {start_delivery}. Basato su PO: {', '.join(po_list)}."

    # Righe
    dettaglio_linee = ""
    line_num = 0
    for a in articoli:
        qty_conf = safe_int(a.get("qty_confirmed"), None)
        qty_ord = safe_int(a.get("qty_ordered"), 0)
        qty = qty_conf if (qty_conf is not None and qty_conf > 0) else qty_ord
        if qty <= 0:
            continue
        cost = to_float(a.get("cost"), 0.0)
        totale_riga = "{:.2f}".format(cost * qty)
        sku = safe_str(a.get("model_number")) or ""
        asin = safe_str(a.get("asin")) or ""
        raw_descrizione = a.get('title')
        descrizione = html.escape(safe_str(raw_descrizione) or f"Articolo {sku}", quote=True)
        line_num += 1
        dettaglio_linee += f"""
        <DettaglioLinee>
          <NumeroLinea>{line_num}</NumeroLinea>
          <CodiceArticolo>
            <CodiceTipo>SKU</CodiceTipo>
            <CodiceValore>{sku}</CodiceValore>
          </CodiceArticolo>
          {f'''<CodiceArticolo>
            <CodiceTipo>ASIN</CodiceTipo>
            <CodiceValore>{asin}</CodiceValore>
          </CodiceArticolo>''' if asin else ""}
          <Descrizione>{descrizione}</Descrizione>
          <Quantita>{float(qty):.2f}</Quantita>
          <PrezzoUnitario>{cost:.6f}</PrezzoUnitario>
          <PrezzoTotale>{totale_riga}</PrezzoTotale>
          <AliquotaIVA>22.00</AliquotaIVA>
        </DettaglioLinee>
        """

    # Dati Ordine Acquisto
    dati_ordini_xml = "\n".join([
        f"""
        <DatiOrdineAcquisto>
          <RiferimentoNumeroLinea>{i+1}</RiferimentoNumeroLinea>
          <IdDocumento>{html.escape(po, quote=True)}</IdDocumento>
        </DatiOrdineAcquisto>
        """.strip() for i, po in enumerate(po_list)
    ])

    xml = f"""<?xml version="1.0" encoding="utf-8"?>
<p:FatturaElettronica
  xmlns:ds="http://www.w3.org/2000/09/xmldsig#"
  xmlns:p="http://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  versione="FPR12"
  xsi:schemaLocation="http://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2 fatturaordinaria_v1.2.xsd ">
  <FatturaElettronicaHeader>
    <DatiTrasmissione>
      <IdTrasmittente>
        <IdPaese>IT</IdPaese>
        <IdCodice>{fornitore['piva']}</IdCodice>
      </IdTrasmittente>
      <ProgressivoInvio>{numero_fattura}</ProgressivoInvio>
      <FormatoTrasmissione>FPR12</FormatoTrasmissione>
      <CodiceDestinatario>{intestatario['codice_destinatario']}</CodiceDestinatario>
      <PECDestinatario>{intestatario['pec']}</PECDestinatario>
    </DatiTrasmissione>
    <CedentePrestatore>
      <DatiAnagrafici>
        <IdFiscaleIVA>
          <IdPaese>IT</IdPaese>
          <IdCodice>{fornitore['piva']}</IdCodice>
        </IdFiscaleIVA>
        <CodiceFiscale>{fornitore['codice_fiscale']}</CodiceFiscale>
        <Anagrafica>
          <Denominazione>{fornitore['denominazione']}</Denominazione>
          <CodEORI>{fornitore['cod_eori']}</CodEORI>
        </Anagrafica>
        <RegimeFiscale>{fornitore['regime_fiscale']}</RegimeFiscale>
      </DatiAnagrafici>
      <Sede>
        <Indirizzo>{fornitore['indirizzo']}</Indirizzo>
        <CAP>{fornitore['cap']}</CAP>
        <Comune>{fornitore['comune']}</Comune>
        <Provincia>{fornitore['provincia']}</Provincia>
        <Nazione>{fornitore['nazione']}</Nazione>
      </Sede>
      <RiferimentoAmministrazione>{fornitore['riferimento_amministrazione']}</RiferimentoAmministrazione>
    </CedentePrestatore>
    <CessionarioCommittente>
      <DatiAnagrafici>
        <IdFiscaleIVA>
          <IdPaese>IT</IdPaese>
          <IdCodice>{intestatario['piva']}</IdCodice>
        </IdFiscaleIVA>
        <CodiceFiscale>{intestatario['piva']}</CodiceFiscale>
        <Anagrafica>
          <Denominazione>{intestatario['denominazione']}</Denominazione>
        </Anagrafica>
      </DatiAnagrafici>
      <Sede>
        <Indirizzo>{intestatario['indirizzo']}</Indirizzo>
        <NumeroCivico>{intestatario['numero_civico']}</NumeroCivico>
        <CAP>{intestatario['cap']}</CAP>
        <Comune>{intestatario['comune']}</Comune>
        <Provincia>{intestatario['provincia']}</Provincia>
        <Nazione>{intestatario['nazione']}</Nazione>
      </Sede>
    </CessionarioCommittente>
  </FatturaElettronicaHeader>
  <FatturaElettronicaBody>
    <DatiGenerali>
      <DatiGeneraliDocumento>
        <TipoDocumento>TD01</TipoDocumento>
        <Divisa>EUR</Divisa>
        <Data>{data_fattura}</Data>
        <Numero>{numero_fattura}</Numero>
        <ImportoTotaleDocumento>{totale}</ImportoTotaleDocumento>
        <Causale>{html.escape(causale, quote=True)}</Causale>
      </DatiGeneraliDocumento>
      {dati_ordini_xml}
    </DatiGenerali>
    <DatiBeniServizi>
      {dettaglio_linee}
      <DatiRiepilogo>
        <AliquotaIVA>22.00</AliquotaIVA>
        <ImponibileImporto>{imponibile}</ImponibileImporto>
        <Imposta>{iva}</Imposta>
        <EsigibilitaIVA>I</EsigibilitaIVA>
        <RiferimentoNormativo>Iva 22% vendite</RiferimentoNormativo>
      </DatiRiepilogo>
    </DatiBeniServizi>
    <DatiPagamento>
      <CondizioniPagamento>TP02</CondizioniPagamento>
      <DettaglioPagamento>
        <ModalitaPagamento>MP05</ModalitaPagamento>
        <DataScadenzaPagamento>{data_fattura}</DataScadenzaPagamento>
        <ImportoPagamento>{totale}</ImportoPagamento>
      </DettaglioPagamento>
    </DatiPagamento>
  </FatturaElettronicaBody>
</p:FatturaElettronica>
""".strip()

    # normalizzo un po' spazi senza rovinare i contenuti
    return "\n".join(line.strip() for line in xml.splitlines() if line.strip())

def process_genera_fattura_amazon_vendor_job(job: Dict[str, Any]) -> None:
    try:
        supabase.table("jobs").update({
            "status": "in_progress",
            "started_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()

        centro = job["payload"]["centro"]
        start_delivery = job["payload"]["start_delivery"]  # deve essere "YYYY-MM-DD" (string)
        po_list = job["payload"]["po_list"]

        # 1) Articoli di questi PO/centro/data
        res = supabase.table("ordini_vendor_items") \
            .select("*") \
            .in_("po_number", po_list) \
            .eq("fulfillment_center", centro) \
            .eq("start_delivery", start_delivery) \
            .execute()
        articoli = res.data if hasattr(res, 'data') else res
        if not articoli:
            raise Exception("Nessun articolo trovato per questa fattura!")

        # 2) Totali
        def qty_for_sum(a):
            q = safe_int(a.get("qty_confirmed"), None)
            return q if (q is not None and q > 0) else safe_int(a.get("qty_ordered"), 0)

        imponibile = sum(to_float(a.get("cost"), 0.0) * qty_for_sum(a) for a in articoli)
        imponibile = round(imponibile, 2)
        iva = round(imponibile * 0.22, 2)
        totale = round(imponibile + iva, 2)

        articoli_ordinati = sum(safe_int(a.get("qty_ordered"), 0) for a in articoli)
        articoli_confermati = sum(safe_int(a.get("qty_confirmed"), 0) for a in articoli)

        # 3) Numero e data fattura
        data_fattura = datetime.now(timezone.utc).date().isoformat()
        numero_fattura = genera_numero_fattura(supabase, datetime.now().year)

        # 4) XML
        fattura_xml = generate_sdi_xml({
            "centro": centro,
            "start_delivery": start_delivery,
            "po_list": po_list,
            "articoli": articoli,
            "data_fattura": data_fattura,
            "numero_fattura": numero_fattura,
            "imponibile": imponibile,
            "iva": iva,
            "totale": totale
        })

        # 5) Upload XML (upsert=True così non esplode se rigeneri)
        filename = f"fatture/{numero_fattura}_{centro}_{start_delivery}.xml"
        bucket = "fatture"
        upload_resp = supabase.storage.from_(bucket).upload(
            filename,
            fattura_xml.encode("utf-8"),
            {"content-type": "application/xml", "upsert": "true"}
        )
        if hasattr(upload_resp, 'error') and upload_resp.error:
            raise Exception(f"Errore upload XML: {upload_resp.error}")
        xml_url = f"{bucket}/{filename}"

        # 6) Inserisci fattura
        ins = supabase.table("fatture_amazon_vendor").insert({
            "data_fattura": data_fattura,               # DATE
            "numero_fattura": numero_fattura,
            "centro": centro,
            "start_delivery": start_delivery,           # DATE: Postgres casterà
            "po_list": po_list,                         # text[]
            "totale_fattura": totale,
            "imponibile": imponibile,
            "articoli_ordinati": articoli_ordinati,
            "articoli_confermati": articoli_confermati,
            "xml_url": xml_url,
            "stato": "pronta",
            "job_id": job["id"],
            "created_at": datetime.now(timezone.utc).isoformat()
        }).execute()

        # segna riepilogo come fatturato
        supabase.table("ordini_vendor_riepilogo") \
            .update({"fatturato": True}) \
            .eq("fulfillment_center", centro) \
            .eq("start_delivery", start_delivery) \
            .execute()

        supabase.table("jobs").update({
            "status": "done",
            "result": {
                "fattura_id": ins.data[0]["id"] if hasattr(ins, 'data') and ins.data else None,
                "xml_url": xml_url
            },
            "finished_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()

        print(f"[worker] Fattura generata e salvata con successo! {numero_fattura}", flush=True)

    except Exception as e:
        print("[worker] ERRORE fatturazione!", e, flush=True)
        supabase.table("jobs").update({
            "status": "failed",
            "error": str(e),
            "stacktrace": traceback.format_exc(),
            "finished_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()

# -----------------------
# NOTE DI CREDITO
# -----------------------


# -------- helpers invariati --------
def fix_numeric(val):
    if val is None or str(val).strip() == '':
        return None
    return float(str(val).replace(",", ".").replace(" ", ""))

def csv_to_xlsx(csv_bytes):
    df = pd.read_csv(io.BytesIO(csv_bytes), encoding="utf-8-sig", sep=",")
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name="Return_Items")
    return output.getvalue()

def genera_numero_nota_credito(supabase) -> str:
    resp = supabase.rpc("genera_numero_nota_credito").execute()
    if hasattr(resp, "data"):
        return str(resp.data)
    raise Exception("Errore generazione numero nota credito")


# -------- XML NOTE DI CREDITO (TD04) --------
def generate_sdi_notecredito_xml(dati):
    """
    dati = {
      data_nota, numero_nota, vret, dettagli:[{NumeroLinea, asin, ean, descrizione, quantita, prezzo_unitario, prezzo_totale, AliquotaIVA, VRET}],
      imponibile, iva, importo_totale, fattura_collegata (opzionale)
    }
    """
    intestatario = {
        "denominazione": "AMAZON EU SARL, SUCCURSALE ITALIANA",
        "indirizzo": "VIALE MONTE GRAPPA",
        "numero_civico": "3/5",
        "cap": "20124",
        "comune": "MILANO",
        "provincia": "MI",
        "nazione": "IT",
        "piva": "08973230967",
        "codice_destinatario": "XR6XN0E",
        "pec": "amazoneu@legalmail.it"
    }
    fornitore = {
        "denominazione": "CYBORG",
        "piva": "09780071214",
        "codice_fiscale": "09780071214",
        "indirizzo": "Via G. D' Annunzio 58",
        "cap": "80053",
        "comune": "Castellammare di Stabia",
        "provincia": "NA",
        "nazione": "IT",
        "regime_fiscale": "RF01",
        "cod_eori": "IT09780071214",
        # <RiferimentoAmministrazione> QUI **NON** lo mettiamo (Amazon lo rifiuta)
    }

    # Dettaglio linee + VRET in RiferimentoAmministrazione
    dettaglio_linee = []
    for r in dati["dettagli"]:
        dettaglio_linee.append(f"""
        <DettaglioLinee>
          <NumeroLinea>{int(r['NumeroLinea'])}</NumeroLinea>
          <CodiceArticolo>
            <CodiceTipo>EAN</CodiceTipo>
            <CodiceValore>{html.escape(str(r.get('ean','')), quote=True)}</CodiceValore>
          </CodiceArticolo>
          <CodiceArticolo>
            <CodiceTipo>ASIN</CodiceTipo>
            <CodiceValore>{html.escape(str(r.get('asin','')), quote=True)}</CodiceValore>
          </CodiceArticolo>
          <Descrizione>{html.escape(str(r.get('descrizione','')), quote=True)}</Descrizione>
          <Quantita>{float(r.get('quantita') or 0):.6f}</Quantita>
          <PrezzoUnitario>{float(r.get('prezzo_unitario') or 0):.6f}</PrezzoUnitario>
          <PrezzoTotale>{float(r.get('prezzo_totale') or 0):.2f}</PrezzoTotale>
          <AliquotaIVA>{float(r.get('AliquotaIVA') or 22):.2f}</AliquotaIVA>
          <RiferimentoAmministrazione>{html.escape(str(r.get('VRET','')), quote=True)}</RiferimentoAmministrazione>
        </DettaglioLinee>
        """.strip())
    dettaglio_linee_xml = "\n".join(dettaglio_linee)

    # Blocco fattura collegata (se presente)
    fatt_col_xml = ""
    if dati.get("fattura_collegata"):
        fatt_col_xml = f"""
        <DatiFattureCollegate>
          <IdDocumento>{html.escape(str(dati['fattura_collegata']), quote=True)}</IdDocumento>
        </DatiFattureCollegate>
        """.strip()

    # Pagamento
    dati_pagamento = f"""
    <DatiPagamento>
      <CondizioniPagamento>TP02</CondizioniPagamento>
      <DettaglioPagamento>
        <Beneficiario>{fornitore['denominazione']}</Beneficiario>
        <ModalitaPagamento>MP05</ModalitaPagamento>
        <DataRiferimentoTerminiPagamento>{dati['data_nota']}</DataRiferimentoTerminiPagamento>
        <GiorniTerminiPagamento>0</GiorniTerminiPagamento>
        <DataScadenzaPagamento>{dati['data_nota']}</DataScadenzaPagamento>
        <ImportoPagamento>{float(dati['importo_totale']):.2f}</ImportoPagamento>
      </DettaglioPagamento>
    </DatiPagamento>
    """.strip()

    xml = f"""<?xml version="1.0" encoding="utf-8"?>
<p:FatturaElettronica
  xmlns:ds="http://www.w3.org/2000/09/xmldsig#"
  xmlns:p="http://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  versione="FPR12"
  xsi:schemaLocation="http://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2 fatturaordinaria_v1.2.xsd ">
  <FatturaElettronicaHeader>
    <DatiTrasmissione>
      <IdTrasmittente>
        <IdPaese>IT</IdPaese>
        <IdCodice>{fornitore['piva']}</IdCodice>
      </IdTrasmittente>
      <ProgressivoInvio>{dati['numero_nota']}</ProgressivoInvio>
      <FormatoTrasmissione>FPR12</FormatoTrasmissione>
      <CodiceDestinatario>{intestatario['codice_destinatario']}</CodiceDestinatario>
      <PECDestinatario>{intestatario['pec']}</PECDestinatario>
    </DatiTrasmissione>
    <CedentePrestatore>
      <DatiAnagrafici>
        <IdFiscaleIVA>
          <IdPaese>IT</IdPaese>
          <IdCodice>{fornitore['piva']}</IdCodice>
        </IdFiscaleIVA>
        <CodiceFiscale>{fornitore['codice_fiscale']}</CodiceFiscale>
        <Anagrafica>
          <Denominazione>{fornitore['denominazione']}</Denominazione>
          <CodEORI>{fornitore['cod_eori']}</CodEORI>
        </Anagrafica>
        <RegimeFiscale>{fornitore['regime_fiscale']}</RegimeFiscale>
      </DatiAnagrafici>
      <Sede>
        <Indirizzo>{fornitore['indirizzo']}</Indirizzo>
        <CAP>{fornitore['cap']}</CAP>
        <Comune>{fornitore['comune']}</Comune>
        <Provincia>{fornitore['provincia']}</Provincia>
        <Nazione>{fornitore['nazione']}</Nazione>
      </Sede>
    </CedentePrestatore>
    <CessionarioCommittente>
      <DatiAnagrafici>
        <IdFiscaleIVA>
          <IdPaese>IT</IdPaese>
          <IdCodice>{intestatario['piva']}</IdCodice>
        </IdFiscaleIVA>
        <CodiceFiscale>{intestatario['piva']}</CodiceFiscale>
        <Anagrafica>
          <Denominazione>{intestatario['denominazione']}</Denominazione>
        </Anagrafica>
      </DatiAnagrafici>
      <Sede>
        <Indirizzo>{intestatario['indirizzo']}</Indirizzo>
        <NumeroCivico>{intestatario['numero_civico']}</NumeroCivico>
        <CAP>{intestatario['cap']}</CAP>
        <Comune>{intestatario['comune']}</Comune>
        <Provincia>{intestatario['provincia']}</Provincia>
        <Nazione>{intestatario['nazione']}</Nazione>
      </Sede>
    </CessionarioCommittente>
  </FatturaElettronicaHeader>
  <FatturaElettronicaBody>
    <DatiGenerali>
      <DatiGeneraliDocumento>
        <TipoDocumento>TD04</TipoDocumento>
        <Divisa>EUR</Divisa>
        <Data>{dati['data_nota']}</Data>
        <Numero>{dati['numero_nota']}</Numero>
        <ImportoTotaleDocumento>{float(dati['importo_totale']):.2f}</ImportoTotaleDocumento>
        <Causale>VRET</Causale>
      </DatiGeneraliDocumento>
      {fatt_col_xml}
    </DatiGenerali>
    <DatiBeniServizi>
      {dettaglio_linee_xml}
      <DatiRiepilogo>
        <AliquotaIVA>22.00</AliquotaIVA>
        <SpeseAccessorie>0.00</SpeseAccessorie>
        <ImponibileImporto>{float(dati['imponibile']):.2f}</ImponibileImporto>
        <Imposta>{float(dati['iva']):.2f}</Imposta>
        <EsigibilitaIVA>I</EsigibilitaIVA>
        <RiferimentoNormativo>Iva 22% vendite</RiferimentoNormativo>
      </DatiRiepilogo>
    </DatiBeniServizi>
    {dati_pagamento}
  </FatturaElettronicaBody>
</p:FatturaElettronica>""".strip()

    return "\n".join(l for l in xml.splitlines() if l.strip())


# -------- JOB PROCESSOR con mappa SHIFTATA + fattura collegata per VRET --------
def process_genera_notecredito_amazon_reso_job(job):
    try:
        supabase.table("jobs").update({
            "status": "in_progress",
            "started_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()

        # Return_Items
        storage_path = job["payload"]["storage_path"]
        bucket, filename = storage_path.split("/", 1)
        print(f"[worker] Scarico file {storage_path} da storage...", flush=True)
        csv_bytes = supabase.storage.from_(bucket).download(filename)
        if hasattr(csv_bytes, 'error') and csv_bytes.error:
            raise Exception(f"Errore download da storage: {csv_bytes.error}")

        # CSV -> XLSX in RAM
        xlsx_bytes = csv_to_xlsx(csv_bytes)

        # Leggi Return_Items
        df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name="Return_Items")
        df.columns = [c.strip() for c in df.columns]

        # Return_Summary (opzionale) per fattura collegata
        summary_path = job["payload"].get("summary_path")
        fattura_by_vret = {}
        if summary_path:
            try:
                sbucket, sfilename = summary_path.split("/", 1)
                summary_bytes = supabase.storage.from_(sbucket).download(sfilename)
                if summary_bytes:
                    # supporta xls o xlsx
                    df_sum = pd.read_excel(io.BytesIO(summary_bytes))
                    df_sum.columns = [c.strip() for c in df_sum.columns]
                    # cerchiamo colonne tipiche: "ID richiesta spedizione" (VRET) e "Numero fattura"
                    # adatta i nomi se differiscono nel tuo file
                    col_vret = next((c for c in df_sum.columns if c.lower().startswith("id richiesta")), None)
                    col_invoice = next((c for c in df_sum.columns if "fattura" in c.lower()), None)
                    if col_vret and col_invoice:
                        for _, rr in df_sum.iterrows():
                            v = str(rr.get(col_vret, "")).strip()
                            inv = str(rr.get(col_invoice, "")).strip()
                            if v and inv:
                                fattura_by_vret[v] = inv
                    else:
                        print("[worker] Return_Summary: colonne non trovate, salto fatture collegate", flush=True)
            except Exception as ex:
                print(f"[worker] Return_Summary non leggibile: {ex}", flush=True)

        grouped = df.groupby(['ID reso', 'Numero di tracking'])

        risultati = []
        for (vret, _), righe in grouped:
            po = str(righe.iloc[0].get('Numero di tracking', '')).strip()
            print(f"Sto generando nota per PO={po}, VRET={vret}", flush=True)

            oggi = datetime.now(timezone.utc).date().isoformat()
            numero_nota = genera_numero_nota_credito(supabase)

            # --- MAPPATURA SHIFTATA (come volevi)
            dettaglio_linee = []
            imponibile = 0.0
            for idx, r in righe.iterrows():
                qty = fix_numeric(r.get("Linea di prodotti", 1))   # Q.tà
                price = fix_numeric(r.get("Quantità", 0))          # Prezzo unit.
                total_row = (qty or 0.0) * (price or 0.0)
                imponibile += total_row
                dettaglio_linee.append({
                    "NumeroLinea": idx+1,
                    "asin": str(r.get("Corriere", "")),   # ASIN “shiftato”
                    "ean": str(r.get("ASIN", "")),        # EAN “shiftato”
                    "descrizione": str(r.get("UPC", "")),
                    "quantita": qty or 0.0,
                    "prezzo_unitario": price or 0.0,
                    "prezzo_totale": total_row,
                    "AliquotaIVA": 22.00,
                    "VRET": vret
                })

            iva = round(imponibile * 0.22, 2)
            importo_totale = round(imponibile + iva, 2)

            # articoli JSON (puoi lasciare quelli “veri”, non shiftati, se preferisci)
            articoli_json = []
            for idx, r in righe.iterrows():
                qty = fix_numeric(r.get("Linea di prodotti", 1))
                price = fix_numeric(r.get("Quantità", 0))
                total_row = (qty or 0.0) * (price or 0.0)
                articoli_json.append({
                    "numero_linea": idx+1,
                    "ean": str(r.get("EAN", "")),
                    "asin": str(r.get("ASIN", "")),
                    "descrizione": str(r.get("UPC", "")),
                    "quantita": qty or 0.0,
                    "prezzo_unitario": price or 0.0,
                    "prezzo_totale": total_row
                })

            dati_xml = {
                "data_nota": oggi,
                "numero_nota": numero_nota,
                "vret": vret,
                "dettagli": dettaglio_linee,
                "imponibile": imponibile,
                "iva": iva,
                "importo_totale": importo_totale,
                "fattura_collegata": fattura_by_vret.get(vret)  # <- solo per VRET
            }

            xml_str = generate_sdi_notecredito_xml(dati_xml)

            # Upload
            xml_filename = f"xml/{numero_nota}_{vret}.xml"
            xml_bucket = "notecredito"
            upload_resp = supabase.storage.from_(xml_bucket).upload(
                xml_filename,
                xml_str.encode("utf-8"),
                {"content-type": "application/xml"}
            )
            if hasattr(upload_resp, 'error') and upload_resp.error:
                raise Exception(f"Errore upload XML: {upload_resp.error}")
            xml_url = f"{xml_bucket}/{xml_filename}"

            # Inserisci riga
            nota_insert = {
                "data_nota": oggi,
                "numero_nota": numero_nota,
                "po": po,                 # PO = Numero di tracking (come facevi)
                "vret": vret,             # ID reso (VRET)
                "xml_url": xml_url,
                "stato": "pronta",
                "job_id": job["id"],
                "articoli": articoli_json,
                "created_at": datetime.now(timezone.utc).isoformat()
            }
            supabase.table("notecredito_amazon_reso").insert(nota_insert).execute()
            risultati.append(nota_insert)
            print(f"Inserisco nota: PO={po}, VRET={vret}, XML={xml_url}", flush=True)

        supabase.table("jobs").update({
            "status": "done",
            "result": {"note_generate": len(risultati), "note": risultati},
            "finished_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()
        print(f"[worker] Note di credito generate: {len(risultati)}", flush=True)

    except Exception as e:
        print("[worker] ERRORE nota credito!", e, flush=True)
        supabase.table("jobs").update({
            "status": "failed",
            "error": str(e),
            "stacktrace": traceback.format_exc(),
            "finished_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()

# -----------------------
# MAIN LOOP
# -----------------------

def main_loop():
    print("WORKER AVVIATO - SONO IL VERO WORKER!", flush=True)
    while True:
        try:
            jobs = supabase.table("jobs").select("*").eq("status", "pending").execute().data
            print(f"[worker] Trovati {len(jobs)} job pending", flush=True)
            if not jobs:
                time.sleep(5)
                continue

            for job in jobs:
                print(f"[worker] Processo job {job['id']} ({job['type']})...", flush=True)
                jtype = job.get("type")
                if jtype == "import_vendor_orders":
                    process_import_vendor_orders_job(job)
                elif jtype == "genera_fattura_amazon_vendor":
                    process_genera_fattura_amazon_vendor_job(job)
                elif jtype == "genera_notecredito_amazon_reso":
                    process_genera_notecredito_amazon_reso_job(job)
                else:
                    print(f"[worker] Tipo job non gestito: {jtype}", flush=True)
            time.sleep(1)
        except Exception as loop_err:
            print("[worker] ERRORE nel loop principale:", loop_err, flush=True)
            time.sleep(5)

if __name__ == "__main__":
    print("CHIAMO main_loop()", flush=True)
    main_loop()
