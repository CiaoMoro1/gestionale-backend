import io
import os
import time
import traceback
import pandas as pd
from datetime import datetime, timezone
from collections import defaultdict
from supabase import create_client
from dotenv import load_dotenv
load_dotenv()

def fix_date(val):
    if pd.isna(val):
        return None
    if hasattr(val, "date"):
        return val.date().isoformat()
    s = str(val).strip()
    if "T" in s:
        return s.split("T")[0]
    return s

def genera_numero_fattura(supabase, anno: int) -> str:
    resp = supabase.rpc("genera_numero_fattura", {"anno_input": anno}).execute()
    if hasattr(resp, "data"):
        return resp.data
    raise Exception("Errore generazione numero fattura")


# Configura qui il client Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def process_import_vendor_orders_job(job):
    try:
        supabase.table("jobs").update({
            "status": "in_progress",
            "started_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()

        storage_path = job["payload"]["storage_path"]
        bucket, filename = storage_path.split("/", 1)
        print(f"[worker] Scarico file {storage_path} da storage...")
        file_resp = supabase.storage.from_(bucket).download(filename)
        if hasattr(file_resp, 'error') and file_resp.error:
            raise Exception(f"Errore download da storage: {file_resp.error}")
        excel_bytes = file_resp

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

        res = supabase.table("ordini_vendor_items").select(
            "po_number,model_number,qty_ordered,start_delivery,fulfillment_center"
        ).execute()
        ordini_esistenti = res.data if hasattr(res, 'data') else res

        def is_duplicate(row):
            chiave_new = (
                str(row["Numero ordine/ordine d’acquisto"]).strip(),
                str(row["Numero di modello"]).strip(),
                int(row["Quantità ordinata"]),
                fix_date(row["Inizio consegna"]),
                str(row["Fulfillment Center"]).strip()
            )
            for ord_db in ordini_esistenti:
                chiave_db = (
                    str(ord_db.get("po_number", "")).strip(),
                    str(ord_db.get("model_number", "")).strip(),
                    int(ord_db.get("qty_ordered", 0)),
                    fix_date(ord_db.get("start_delivery", "")),
                    str(ord_db.get("fulfillment_center", "")).strip()
                )
                if chiave_new == chiave_db:
                    return True
            return False

        importati = 0
        po_numbers = set()
        errors = []
        doppioni = []

        for _, row in df.iterrows():
            try:
                if is_duplicate(row):
                    doppioni.append(
                        f"Doppione: Ordine={row['Numero ordine/ordine d’acquisto']} | Modello={row['Numero di modello']} | Quantità={row['Quantità ordinata']}"
                    )
                    continue

                ordine = {
                    "po_number": str(row["Numero ordine/ordine d’acquisto"]).strip(),
                    "vendor_product_id": str(row["Codice identificativo esterno"]).strip(),
                    "model_number": str(row["Numero di modello"]).strip(),
                    "asin": str(row["ASIN"]).strip(),
                    "title": str(row["Titolo"]),
                    "cost": row["Costo"],
                    "qty_ordered": row["Quantità ordinata"],
                    "qty_confirmed": row["Quantità confermata"],
                    "start_delivery": fix_date(row["Inizio consegna"]),
                    "end_delivery": fix_date(row["Termine consegna"]),
                    "delivery_date": fix_date(row["Data di consegna prevista"]),
                    "status": row["Stato disponibilità"],
                    "vendor_code": row["Codice fornitore"],
                    "fulfillment_center": row["Fulfillment Center"],
                    "created_at": datetime.now(timezone.utc).isoformat()
                }

                supabase.table("ordini_vendor_items").insert(ordine).execute()
                po_numbers.add(ordine["po_number"])
                importati += 1
            except Exception as ex:
                errors.append(str(ex))

        # --- RIEPILOGO: aggiorna sempre dopo import! ---
        ordini = supabase.table("ordini_vendor_items").select(
            "po_number, qty_ordered, fulfillment_center, start_delivery"
        ).execute().data

        gruppi = defaultdict(lambda: {"po_list": set(), "totale_articoli": 0})
        for o in ordini:
            key = (o["fulfillment_center"], fix_date(o["start_delivery"]))
            gruppi[key]["po_list"].add(o["po_number"])
            gruppi[key]["totale_articoli"] += int(o["qty_ordered"])

        for (fc, data), dati in gruppi.items():
            riepilogo = {
                "fulfillment_center": fc,
                "start_delivery": data,
                "po_list": list(dati["po_list"]),
                "totale_articoli": dati["totale_articoli"],
                "stato_ordine": "nuovo"
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
                        "po_list": list(dati["po_list"]),
                        "totale_articoli": dati["totale_articoli"]
                    }) \
                    .eq("id", id_riep) \
                    .execute()
            else:
                supabase.table("ordini_vendor_riepilogo").insert(riepilogo).execute()

        # Aggiorna stato job DONE
        supabase.table("jobs").update({
            "status": "done",
            "result": {
                "importati": importati,
                "doppioni": doppioni,
                "po_unici": len(po_numbers),
                "po_list": list(po_numbers),
                "errors": errors
            },
            "finished_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()

        print(f"[worker] Import terminato! {importati} righe, {len(doppioni)} doppioni.")

    except Exception as e:
        print("[worker] ERRORE!", e)
        supabase.table("jobs").update({
            "status": "failed",
            "error": str(e),
            "stacktrace": traceback.format_exc(),
            "finished_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()


def process_genera_fattura_amazon_vendor_job(job):
    try:
        supabase.table("jobs").update({
            "status": "in_progress",
            "started_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()

        centro = job["payload"]["centro"]
        start_delivery = job["payload"]["start_delivery"]
        po_list = job["payload"]["po_list"]

        # 1. Carica tutti gli articoli per questi PO/centro/data
        res = supabase.table("ordini_vendor_items") \
            .select("*") \
            .in_("po_number", po_list) \
            .eq("fulfillment_center", centro) \
            .eq("start_delivery", start_delivery) \
            .execute()
        articoli = res.data if hasattr(res, 'data') else res
        if not articoli or len(articoli) == 0:
            raise Exception("Nessun articolo trovato per questa fattura!")

        # 2. Calcola totali (imponibile, iva, totale)
        imponibile = sum(float(a.get("cost", 0)) * int(a.get("qty_confirmed", a.get("qty_ordered", 0))) for a in articoli)
        iva = round(imponibile * 0.22, 2)
        totale = round(imponibile + iva, 2)
        
        articoli_ordinati = sum(int(a.get("qty_ordered", 0)) for a in articoli)
        articoli_confermati = sum(int(a.get("qty_confirmed", 0)) for a in articoli)

        # 3. Genera il numero fattura e la data fattura
        data_fattura = datetime.now(timezone.utc).date().isoformat()
        # Qui puoi implementare la tua logica progressiva per numero fattura
        anno = datetime.now().year
        numero_fattura = genera_numero_fattura(supabase, anno)

        # 4. Genera l’XML SDI (usa la funzione che ti abbiamo dato sopra, o una simile)
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

        # 5. Salva XML su Supabase Storage
        filename = f"fatture/{numero_fattura}_{centro}_{start_delivery}.xml"
        bucket = "fatture"
        upload_resp = supabase.storage.from_(bucket).upload(filename, fattura_xml.encode("utf-8"), {"content-type": "application/xml"})
        if hasattr(upload_resp, 'error') and upload_resp.error:
            raise Exception(f"Errore upload XML: {upload_resp.error}")

        # Ottieni url del file XML
        xml_url = f"{bucket}/{filename}"

        # 6. Salva la fattura su fatture_amazon_vendor
        res = supabase.table("fatture_amazon_vendor").insert({
            "data_fattura": data_fattura,
            "numero_fattura": numero_fattura,
            "centro": centro,
            "start_delivery": start_delivery,
            "po_list": po_list,
            "totale_fattura": totale,
            "imponibile": imponibile,
            "articoli_ordinati": articoli_ordinati,
            "articoli_confermati": articoli_confermati,
            "xml_url": xml_url,
            "stato": "pronta",
            "job_id": job["id"],
            "created_at": datetime.now(timezone.utc).isoformat()
        }).execute()

        # 7. Aggiorna job come DONE
        supabase.table("jobs").update({
            "status": "done",
            "result": {
                "fattura_id": res.data[0]["id"] if hasattr(res, 'data') else None,
                "xml_url": xml_url
            },
            "finished_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()

        print(f"[worker] Fattura generata e salvata con successo! {numero_fattura}")

    except Exception as e:
        print("[worker] ERRORE fatturazione!", e)
        supabase.table("jobs").update({
            "status": "failed",
            "error": str(e),
            "stacktrace": traceback.format_exc(),
            "finished_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()


def generate_sdi_xml(dati):
    """
    Genera una stringa XML SDI pronta da inviare, secondo i dati di input.
    - dati: dict con chiavi centro, start_delivery, po_list, articoli, data_fattura,
      numero_fattura, imponibile, iva, totale
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

    # ==== HEADER E DATI FISSI AMAZON ====
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

    # ==== INTESTATARIO FORNITORE: ADATTA QUI SE SERVE ====
    fornitore = {
        "denominazione": "TUO_NOME_AZIENDA",   # <-- Modifica con la tua ragione sociale
        "piva": "ITXXXXXXXXXXX",               # <-- Modifica con la tua P.IVA
        "codice_fiscale": "XXXXXXXXXXX",       # <-- Modifica se serve
        "indirizzo": "Tuo Indirizzo",
        "cap": "00000",
        "comune": "Tua Citta",
        "provincia": "XX",
        "nazione": "IT",
        "regime_fiscale": "RF01"
    }

    # ==== CAUSALE ====
    causale = f"Ordine Amazon centro {centro} - Data consegna {start_delivery}. Basato su PO: {', '.join(po_list)}."

    # ==== RIGHE XML ====
    dettaglio_linee = ""
    for idx, a in enumerate(articoli, 1):
        qty = int(a.get("qty_confirmed") or a.get("qty_ordered") or 0)
        cost = float(a.get("cost", 0))
        totale_riga = "{:.2f}".format(cost * qty)
        codici_articolo = []
        if a.get("vendor_product_id"):
            codici_articolo.append(f"""
            <CodiceArticolo>
              <CodiceTipo>EAN</CodiceTipo>
              <CodiceValore>{a['vendor_product_id']}</CodiceValore>
            </CodiceArticolo>
            """)
        if a.get("asin"):
            codici_articolo.append(f"""
            <CodiceArticolo>
              <CodiceTipo>ASIN</CodiceTipo>
              <CodiceValore>{a['asin']}</CodiceValore>
            </CodiceArticolo>
            """)
        if a.get("model_number"):
            codici_articolo.append(f"""
            <CodiceArticolo>
              <CodiceTipo>SKU</CodiceTipo>
              <CodiceValore>{a['model_number']}</CodiceValore>
            </CodiceArticolo>
            """)
        dettaglio_linee += f"""
        <DettaglioLinee>
          <NumeroLinea>{idx}</NumeroLinea>
          {''.join(codici_articolo)}
          <Descrizione>{a.get('title', 'Articolo')}</Descrizione>
          <Quantita>{qty:.2f}</Quantita>
          <PrezzoUnitario>{cost:.6f}</PrezzoUnitario>
          <PrezzoTotale>{totale_riga}</PrezzoTotale>
          <AliquotaIVA>22.00</AliquotaIVA>
        </DettaglioLinee>
        """

    # ==== DATI ORDINE ACQUISTO ====
    dati_ordini_xml = "\n".join([
        f"""
        <DatiOrdineAcquisto>
          <RiferimentoNumeroLinea>{idx+1}</RiferimentoNumeroLinea>
          <IdDocumento>{po}</IdDocumento>
        </DatiOrdineAcquisto>
        """ for idx, po in enumerate(po_list)
    ])

    # ==== XML FINALE ====
    xml = f"""<?xml version="1.0" encoding="utf-8"?>
<FatturaElettronica versione="FPR12" xmlns="http://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2">
  <FatturaElettronicaHeader>
    <DatiTrasmissione>
      <IdTrasmittente>
        <IdPaese>IT</IdPaese>
        <IdCodice>{fornitore['piva'].replace('IT','')}</IdCodice>
      </IdTrasmittente>
      <ProgressivoInvio>1</ProgressivoInvio>
      <FormatoTrasmissione>FPR12</FormatoTrasmissione>
      <CodiceDestinatario>{intestatario['codice_destinatario']}</CodiceDestinatario>
      <PECDestinatario>{intestatario['pec']}</PECDestinatario>
    </DatiTrasmissione>
    <CedentePrestatore>
      <DatiAnagrafici>
        <IdFiscaleIVA>
          <IdPaese>IT</IdPaese>
          <IdCodice>{fornitore['piva'].replace('IT','')}</IdCodice>
        </IdFiscaleIVA>
        <CodiceFiscale>{fornitore['codice_fiscale']}</CodiceFiscale>
        <Anagrafica>
          <Denominazione>{fornitore['denominazione']}</Denominazione>
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
        <TipoDocumento>TD01</TipoDocumento>
        <Divisa>EUR</Divisa>
        <Data>{data_fattura}</Data>
        <Numero>{numero_fattura}</Numero>
        <ImportoTotaleDocumento>{totale}</ImportoTotaleDocumento>
        <Causale>{causale}</Causale>
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
</FatturaElettronica>
""".replace("    ", " ").replace("  ", " ").strip()

    return xml



def main_loop():
    while True:
        jobs = supabase.table("jobs").select("*").eq("status", "pending").execute().data
        if not jobs:
            time.sleep(5)
            continue
        for job in jobs:
            print(f"[worker] Processo job {job['id']} ({job['type']})...")
            if job["type"] == "import_vendor_orders":
                process_import_vendor_orders_job(job)
            elif job["type"] == "genera_fattura_amazon_vendor":
                process_genera_fattura_amazon_vendor_job(job)
        time.sleep(1)

if __name__ == "__main__":
    main_loop()
