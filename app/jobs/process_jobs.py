import io
import os
import time
import traceback
import pandas as pd
from datetime import datetime, timezone
from collections import defaultdict
from supabase import create_client
from dotenv import load_dotenv
import html


print("IMPORT OK", flush=True)



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

        df = pd.read_excel(
            io.BytesIO(excel_bytes),
            header=2,
            sheet_name="Articoli",
            dtype={"Codice identificativo esterno": str}
        )
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

        supabase.table("ordini_vendor_riepilogo") \
            .update({"fatturato": True}) \
            .eq("fulfillment_center", centro) \
            .eq("start_delivery", start_delivery) \
            .execute()

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


import html

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

    # ==== DATI AMAZON (fissi) ====
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

    # ==== I TUOI DATI (REALISTICI E CONFORMI SDI) ====
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

    causale = "VRET"

    # ==== RIGHE XML (SKU + ASIN, descrizione sempre valorizzata e escapata) ====
    dettaglio_linee = ""
    for idx, a in enumerate(articoli, 1):
        qty = float(a.get("qty_confirmed") or 0)
        if qty == 0:
            continue  # Salta righe non confermate
        cost = float(a.get("cost", 0))
        totale_riga = "{:.2f}".format(cost * qty)
        sku = a.get("model_number", "")
        asin = a.get("asin", "")
        raw_descrizione = a['title'] if a.get('title') and str(a['title']).lower() != "none" else f"Articolo {sku}"
        descrizione = html.escape(raw_descrizione, quote=True)  # <-- escapato!
        dettaglio_linee += f"""
        <DettaglioLinee>
          <NumeroLinea>{idx}</NumeroLinea>
          <CodiceArticolo>
            <CodiceTipo>SKU</CodiceTipo>
            <CodiceValore>{sku}</CodiceValore>
          </CodiceArticolo>
          {f'''<CodiceArticolo>
            <CodiceTipo>ASIN</CodiceTipo>
            <CodiceValore>{asin}</CodiceValore>
          </CodiceArticolo>''' if asin else ""}
          <Descrizione>{descrizione}</Descrizione>
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
</p:FatturaElettronica>
""".replace("    ", " ").replace("  ", " ").strip()

    return xml




def fix_numeric(val):
    # Torna None per valori vuoti, oppure float con il punto decimale
    if val is None or str(val).strip() == '':
        return None
    return float(str(val).replace(",", ".").replace(" ", ""))

def to_float(val):
    try:
        if pd.isna(val):
            return 0.0
        return float(str(val).replace(",", ".").replace(" ", "").strip())
    except Exception:
        return 0.0

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

def process_genera_notecredito_amazon_reso_job(job):
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
        csv_bytes = file_resp

        # Conversione CSV -> XLSX (in RAM)
        xlsx_bytes = csv_to_xlsx(csv_bytes)

        # Leggi XLSX
        df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name="Return_Items")
        df.columns = [c.strip() for c in df.columns]

        grouped = df.groupby(['ID reso', 'Numero di tracking'])

        risultati = []
        for (vret, _), righe in grouped:
            po = righe.iloc[0]['Numero di tracking'].strip()
            print(f"Sto generando nota per PO={po}, VRET={vret}")

            oggi = datetime.now(timezone.utc).date().isoformat()
            numero_nota = genera_numero_nota_credito(supabase)

            # DETTAGLIO ARTICOLI PER L'XML
            dettaglio_linee = []
            imponibile = 0.0
            for idx, r in righe.iterrows():
                qty = fix_numeric(r.get("Linea di prodotti", 1))
                price = fix_numeric(r.get("Quantità", 0))
                total_row = qty * price if qty and price else 0.0
                imponibile += total_row
                dettaglio_linee.append({
                    "NumeroLinea": idx+1,
                    "asin": str(r.get("Corriere", "")),
                    "ean": str(r.get("ASIN", "")),
                    "descrizione": str(r.get("UPC", "")),
                    "quantita": qty,
                    "prezzo_unitario": price,
                    "prezzo_totale": total_row,
                    "AliquotaIVA": 22.00,
                    "VRET": vret  # <--- ID reso!
                })

            iva = round(imponibile * 0.22, 2)
            importo_totale = round(imponibile + iva, 2)


            articoli_json = []
            for idx, r in righe.iterrows():
                qty = fix_numeric(r.get("Linea di prodotti", 1))
                price = fix_numeric(r.get("Quantità", 0))
                total_row = qty * price if qty and price else 0.0
                articoli_json.append({
                    "numero_linea": idx+1,
                    "ean": str(r.get("EAN", "")),
                    "asin": str(r.get("ASIN", "")),   # <-- prendi ASIN vero!
                    "descrizione": str(r.get("UPC", "")),
                    "quantita": qty,
                    "prezzo_unitario": price,
                    "prezzo_totale": total_row
                })


            dati_xml = {
                "data_nota": oggi,
                "numero_nota": numero_nota,
                "po": po,
                "vret": vret,
                "dettagli": dettaglio_linee,
                "imponibile": imponibile,
                "iva": iva,
                "importo_totale": importo_totale
            }

            # --- GENERA XML NOTA DI CREDITO ---
            xml_str = generate_sdi_notecredito_xml(dati_xml)

            # Salva XML su Supabase Storage
            xml_filename = f"xml/{numero_nota}_{vret}.xml"
            xml_bucket = "notecredito"
            upload_resp = supabase.storage.from_(xml_bucket).upload(xml_filename, xml_str.encode("utf-8"), {"content-type": "application/xml"})
            if hasattr(upload_resp, 'error') and upload_resp.error:
                raise Exception(f"Errore upload XML: {upload_resp.error}")
            xml_url = f"{xml_bucket}/{xml_filename}"

            # Inserisci record in tabella notecredito_amazon_reso
            nota_insert = {
                "data_nota": oggi,
                "numero_nota": numero_nota,
                "po": po,
                "vret": vret,
                "xml_url": xml_url,
                "stato": "pronta",
                "job_id": job["id"],
                "articoli": articoli_json,
                "created_at": datetime.now(timezone.utc).isoformat()
            }
            supabase.table("notecredito_amazon_reso").insert(nota_insert).execute()
            risultati.append(nota_insert)
            print(f"Inserisco nota: PO={po}, VRET={vret}, XML={xml_url}", flush=True)

        # Aggiorna job come DONE
        supabase.table("jobs").update({
            "status": "done",
            "result": {
                "note_generate": len(risultati),
                "note": risultati
            },
            "finished_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()
        print(f"[worker] Note di credito generate: {len(risultati)}")

    except Exception as e:
        print("[worker] ERRORE nota credito!", e)
        supabase.table("jobs").update({
            "status": "failed",
            "error": str(e),
            "stacktrace": traceback.format_exc(),
            "finished_at": datetime.now(timezone.utc).isoformat()
        }).eq("id", job["id"]).execute()



def generate_sdi_notecredito_xml(dati):
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

    # DETTAGLIO ARTICOLI
    dettaglio_linee = ""
    for r in dati["dettagli"]:
        dettaglio_linee += f"""
        <DettaglioLinee>
            <NumeroLinea>{r['NumeroLinea']}</NumeroLinea>
            <CodiceArticolo>
                <CodiceTipo>EAN</CodiceTipo>
                <CodiceValore>{html.escape(str(r['ean']), quote=True)}</CodiceValore>
            </CodiceArticolo>
            <CodiceArticolo>
                <CodiceTipo>ASIN</CodiceTipo>
                <CodiceValore>{html.escape(str(r['asin']), quote=True)}</CodiceValore>
            </CodiceArticolo>
            <Descrizione>{html.escape(str(r['descrizione']), quote=True)}</Descrizione>
            <Quantita>{float(r['quantita']):.6f}</Quantita>
            <PrezzoUnitario>{float(r['prezzo_unitario']):.6f}</PrezzoUnitario>
            <PrezzoTotale>{float(r['prezzo_totale']):.2f}</PrezzoTotale>
            <AliquotaIVA>22.00</AliquotaIVA>
            <RiferimentoAmministrazione>{r['VRET']}</RiferimentoAmministrazione>
            </DettaglioLinee>
        """

    # Dati pagamento (sezione obbligatoria per Amazon, puoi parametrizzare IBAN o altro)
    dati_pagamento = f"""
    <DatiPagamento>
      <CondizioniPagamento>TP02</CondizioniPagamento>
      <DettaglioPagamento>
        <Beneficiario>{fornitore['denominazione']}</Beneficiario>
        <ModalitaPagamento>MP05</ModalitaPagamento>
        <DataRiferimentoTerminiPagamento>{dati['data_nota']}</DataRiferimentoTerminiPagamento>
        <GiorniTerminiPagamento>0</GiorniTerminiPagamento>
        <DataScadenzaPagamento>{dati['data_nota']}</DataScadenzaPagamento>
        <ImportoPagamento>{dati['importo_totale']:.2f}</ImportoPagamento>
      </DettaglioPagamento>
    </DatiPagamento>
    """

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
        <TipoDocumento>TD04</TipoDocumento>
        <Divisa>EUR</Divisa>
        <Data>{dati['data_nota']}</Data>
        <Numero>{dati['numero_nota']}</Numero>
        <ImportoTotaleDocumento>{dati['importo_totale']:.2f}</ImportoTotaleDocumento>
        <Causale>VRET</Causale>
      </DatiGeneraliDocumento>
      <DatiOrdineAcquisto>
        <IdDocumento>{dati['po']}</IdDocumento>
      </DatiOrdineAcquisto>
    </DatiGenerali>
    <DatiBeniServizi>
      {dettaglio_linee}
      <DatiRiepilogo>
        <AliquotaIVA>22.00</AliquotaIVA>
        <SpeseAccessorie>0.00</SpeseAccessorie>
        <ImponibileImporto>{dati['imponibile']:.2f}</ImponibileImporto>
        <Imposta>{dati['iva']:.2f}</Imposta>
        <EsigibilitaIVA>I</EsigibilitaIVA>
        <RiferimentoNormativo>Iva 22% vendite</RiferimentoNormativo>
      </DatiRiepilogo>
    </DatiBeniServizi>
    {dati_pagamento}
  </FatturaElettronicaBody>
</p:FatturaElettronica>
"""
    return xml

def main_loop():
    print("WORKER AVVIATO - SONO IL VERO WORKER 3!")
    while True:
        jobs = supabase.table("jobs").select("*").eq("status", "pending").execute().data
        print(f"[worker] Trovati {len(jobs)} job pending", flush=True)
        if not jobs:
            time.sleep(5)
            continue
        for job in jobs:
            print(f"[worker] Processo job {job['id']} ({job['type']})...")
            if job["type"] == "import_vendor_orders":
                process_import_vendor_orders_job(job)
            elif job["type"] == "genera_fattura_amazon_vendor":
                process_genera_fattura_amazon_vendor_job(job)
            elif job["type"] == "genera_notecredito_amazon_reso":   # <--- AGGIUNGI QUESTA!
                process_genera_notecredito_amazon_reso_job(job)     # <--- E QUESTA!
        time.sleep(1)

if __name__ == "__main__":
    print("CHIAMO main_loop()", flush=True)
    main_loop()