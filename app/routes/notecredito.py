from flask import Blueprint, request, jsonify, send_file
from app.supabase_client import supabase
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime
import io
import traceback
import html

bp = Blueprint("notecredito_tools", __name__)

def get_next_nc_number():
    res = supabase.rpc("genera_numero_nota_credito_fattura").execute()
    return str(res.data) if hasattr(res, "data") and res.data else "2025700001"

@bp.route('/api/notecredito/genera-da-xml', methods=['POST'])
def genera_nc_da_xml():
    if 'file' not in request.files:
        return jsonify({"error": "File XML mancante"}), 400
    file = request.files['file']
    xml_bytes = file.read()
    try:
        SDI_NS = "{http://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2}"

        def findtext_flessibile(root, ns_path, no_ns_path):
            el = root.find(ns_path)
            if el is not None and el.text:
                return el.text
            el = root.find(no_ns_path)
            return el.text if el is not None else None

        root = ET.fromstring(xml_bytes)
        ns = {'p': 'http://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2'}

        fattura_numero = findtext_flessibile(
            root,
            f".//{SDI_NS}FatturaElettronicaBody/{SDI_NS}DatiGenerali/{SDI_NS}DatiGeneraliDocumento/{SDI_NS}Numero",
            ".//FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Numero"
        )
        fattura_data = findtext_flessibile(
            root,
            f".//{SDI_NS}FatturaElettronicaBody/{SDI_NS}DatiGenerali/{SDI_NS}DatiGeneraliDocumento/{SDI_NS}Data",
            ".//FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Data"
        )

        causale_el = root.find(f".//{SDI_NS}FatturaElettronicaBody/{SDI_NS}DatiGenerali/{SDI_NS}DatiGeneraliDocumento/{SDI_NS}Causale")
        if causale_el is None:
            causale_el = root.find(".//FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/Causale")
        centro = ""
        if causale_el is not None and "centro" in causale_el.text:
            centro = causale_el.text.split('centro')[-1].split('-')[0].strip()
        else:
            centro = "???"
        start_delivery = fattura_data

        if not fattura_numero or not fattura_data:
            print("DEBUG: Fattura_numero/data non trovati")
            return jsonify({"error": "Numero o data fattura non trovati nell'XML. Carica un XML SDI corretto!"}), 400

        # --- PO mapping
        po_map = defaultdict(list)
        for doacq in root.findall(f'.//{SDI_NS}DatiOrdineAcquisto'):
            po = doacq.find(f'{SDI_NS}IdDocumento').text
            riferimenti = [int(n.text) for n in doacq.findall(f'{SDI_NS}RiferimentoNumeroLinea')]
            for n in riferimenti:
                po_map[po].append(n)
        if not po_map:
            for doacq in root.findall('.//DatiOrdineAcquisto'):
                po = doacq.find('IdDocumento').text
                riferimenti = [int(n.text) for n in doacq.findall('RiferimentoNumeroLinea')]
                for n in riferimenti:
                    po_map[po].append(n)
        po_list = list(po_map.keys())

        # --- Linee prodotti
        all_lines = []
        for det in root.findall(f'.//{SDI_NS}DettaglioLinee'):
            num = int(det.find(f'{SDI_NS}NumeroLinea').text)
            descrizione_raw = det.find(f'{SDI_NS}Descrizione').text or ''
            descrizione = html.escape(descrizione_raw, quote=True)
            line = {
                'numero': num,
                'descrizione': descrizione,
                'quantita': det.find(f'{SDI_NS}Quantita').text,
                'prezzo': det.find(f'{SDI_NS}PrezzoUnitario').text,
                'totale': det.find(f'{SDI_NS}PrezzoTotale').text,
                'aliquota': det.find(f'{SDI_NS}AliquotaIVA').text,
                'codici': [(
                    ca.find(f'{SDI_NS}CodiceTipo').text,
                    ca.find(f'{SDI_NS}CodiceValore').text
                ) for ca in det.findall(f'{SDI_NS}CodiceArticolo')]
            }
            all_lines.append(line)
        if not all_lines:
            for det in root.findall('.//DettaglioLinee'):
                num = int(det.find('NumeroLinea').text)
                descrizione_raw = det.find('Descrizione').text or ''
                descrizione = html.escape(descrizione_raw, quote=True)
                line = {
                    'numero': num,
                    'descrizione': descrizione,
                    'quantita': det.find('Quantita').text,
                    'prezzo': det.find('PrezzoUnitario').text,
                    'totale': det.find('PrezzoTotale').text,
                    'aliquota': det.find('AliquotaIVA').text,
                    'codici': [(
                        ca.find('CodiceTipo').text,
                        ca.find('CodiceValore').text
                    ) for ca in det.findall('CodiceArticolo')]
                }
                all_lines.append(line)
        po_to_lines = {po: [l for l in all_lines if l['numero'] in nums] for po, nums in po_map.items()}

        # --- Totali: prendi da DatiRiepilogo (supporto multi-aliquota) e ImportoTotaleDocumento
        # ImportoTotaleDocumento (da DatiGeneraliDocumento)
        totale_doc = findtext_flessibile(
            root,
            f".//{SDI_NS}FatturaElettronicaBody/{SDI_NS}DatiGenerali/{SDI_NS}DatiGeneraliDocumento/{SDI_NS}ImportoTotaleDocumento",
            ".//FatturaElettronicaBody/DatiGenerali/DatiGeneraliDocumento/ImportoTotaleDocumento"
        )
        totale_doc = float(totale_doc) if totale_doc else sum(float(l['totale']) for l in all_lines)

        # Tutti i DatiRiepilogo (multi aliquota)
        dati_riepilogo = []
        for r in root.findall('.//p:DatiRiepilogo', ns):
            dati_riepilogo.append({
                "aliquota": float(r.find('p:AliquotaIVA', ns).text),
                "imponibile": float(r.find('p:ImponibileImporto', ns).text),
                "imposta": float(r.find('p:Imposta', ns).text),
                "esigibilita": r.find('p:EsigibilitaIVA', ns).text if r.find('p:EsigibilitaIVA', ns) is not None else "",
                "riferimento": r.find('p:RiferimentoNormativo', ns).text if r.find('p:RiferimentoNormativo', ns) is not None else ""
            })
        # fallback senza ns
        if not dati_riepilogo:
            for r in root.findall('.//DatiRiepilogo'):
                dati_riepilogo.append({
                    "aliquota": float(r.find('AliquotaIVA').text),
                    "imponibile": float(r.find('ImponibileImporto').text),
                    "imposta": float(r.find('Imposta').text),
                    "esigibilita": r.find('EsigibilitaIVA').text if r.find('EsigibilitaIVA') is not None else "",
                    "riferimento": r.find('RiferimentoNormativo').text if r.find('RiferimentoNormativo') is not None else ""
                })

        today = datetime.now().strftime("%Y-%m-%d")
        numero_nc = get_next_nc_number()

        # --- XML TD04 generato
        out = io.StringIO()
        out.write(f"""<?xml version="1.0" encoding="utf-8"?>
<p:FatturaElettronica
  xmlns:p="http://ivaservizi.agenziaentrate.gov.it/docs/xsd/fatture/v1.2">
  <FatturaElettronicaHeader>
    <DatiTrasmissione>
      <IdTrasmittente>
        <IdPaese>IT</IdPaese>
        <IdCodice>09780071214</IdCodice>
      </IdTrasmittente>
      <ProgressivoInvio>{numero_nc}</ProgressivoInvio>
      <FormatoTrasmissione>FPR12</FormatoTrasmissione>
      <CodiceDestinatario>XR6XN0E</CodiceDestinatario>
      <PECDestinatario>amazoneu@legalmail.it</PECDestinatario>
    </DatiTrasmissione>
    <CedentePrestatore>
      <DatiAnagrafici>
        <IdFiscaleIVA>
          <IdPaese>IT</IdPaese>
          <IdCodice>09780071214</IdCodice>
        </IdFiscaleIVA>
        <CodiceFiscale>09780071214</CodiceFiscale>
        <Anagrafica>
          <Denominazione>CYBORG</Denominazione>
          <CodEORI>IT09780071214</CodEORI>
        </Anagrafica>
        <RegimeFiscale>RF01</RegimeFiscale>
      </DatiAnagrafici>
      <Sede>
        <Indirizzo>Via G. D' Annunzio 58</Indirizzo>
        <CAP>80053</CAP>
        <Comune>Castellammare di Stabia</Comune>
        <Provincia>NA</Provincia>
        <Nazione>IT</Nazione>
      </Sede>
      <RiferimentoAmministrazione>7401713799</RiferimentoAmministrazione>
    </CedentePrestatore>
    <CessionarioCommittente>
      <DatiAnagrafici>
        <IdFiscaleIVA>
          <IdPaese>IT</IdPaese>
          <IdCodice>08973230967</IdCodice>
        </IdFiscaleIVA>
        <CodiceFiscale>08973230967</CodiceFiscale>
        <Anagrafica>
          <Denominazione>AMAZON EU SARL, SUCCURSALE ITALIANA</Denominazione>
        </Anagrafica>
      </DatiAnagrafici>
      <Sede>
        <Indirizzo>VIALE MONTE GRAPPA</Indirizzo>
        <NumeroCivico>3/5</NumeroCivico>
        <CAP>20124</CAP>
        <Comune>MILANO</Comune>
        <Provincia>MI</Provincia>
        <Nazione>IT</Nazione>
      </Sede>
    </CessionarioCommittente>
  </FatturaElettronicaHeader>
  <FatturaElettronicaBody>
    <DatiGenerali>
      <DatiGeneraliDocumento>
        <TipoDocumento>TD04</TipoDocumento>
        <Divisa>EUR</Divisa>
        <Data>{today}</Data>
        <Numero>{numero_nc}</Numero>
        <ImportoTotaleDocumento>{totale_doc:.2f}</ImportoTotaleDocumento>
        <Causale>Nota di credito per storno fattura {fattura_numero} del {fattura_data}</Causale>
      </DatiGeneraliDocumento>
""")
        # PO → riferimenti numeri linea
        for po, lines in po_to_lines.items():
            out.write('      <DatiOrdineAcquisto>\n')
            for l in lines:
                out.write(f'        <RiferimentoNumeroLinea>{l["numero"]}</RiferimentoNumeroLinea>\n')
            out.write(f'        <IdDocumento>{po}</IdDocumento>\n')
            out.write('      </DatiOrdineAcquisto>\n')
        out.write(f"""      <DatiFattureCollegate>
        <IdDocumento>{fattura_numero}</IdDocumento>
      </DatiFattureCollegate>
    </DatiGenerali>
    <DatiBeniServizi>
""")
        for l in all_lines:
            out.write(f"""      <DettaglioLinee>
        <NumeroLinea>{l['numero']}</NumeroLinea>\n""")
            for tipo, valore in l['codici']:
                out.write(f"""        <CodiceArticolo><CodiceTipo>{tipo}</CodiceTipo><CodiceValore>{valore}</CodiceValore></CodiceArticolo>\n""")
            out.write(f"""        <Descrizione>{l['descrizione']}</Descrizione>
        <Quantita>{l['quantita']}</Quantita>
        <PrezzoUnitario>{l['prezzo']}</PrezzoUnitario>
        <PrezzoTotale>{l['totale']}</PrezzoTotale>
        <AliquotaIVA>{l['aliquota']}</AliquotaIVA>
      </DettaglioLinee>
""")
        for r in dati_riepilogo:
            out.write(f"""      <DatiRiepilogo>
        <AliquotaIVA>{r["aliquota"]:.2f}</AliquotaIVA>
        <ImponibileImporto>{r["imponibile"]:.2f}</ImponibileImporto>
        <Imposta>{r["imposta"]:.2f}</Imposta>
        <EsigibilitaIVA>{r["esigibilita"]}</EsigibilitaIVA>
        <RiferimentoNormativo>{r["riferimento"]}</RiferimentoNormativo>
      </DatiRiepilogo>
""")
        out.write(f"""    </DatiBeniServizi>
    <DatiPagamento>
      <CondizioniPagamento>TP02</CondizioniPagamento>
      <DettaglioPagamento>
        <ModalitaPagamento>MP05</ModalitaPagamento>
        <DataScadenzaPagamento>{today}</DataScadenzaPagamento>
        <ImportoPagamento>{totale_doc:.2f}</ImportoPagamento>
      </DettaglioPagamento>
    </DatiPagamento>
  </FatturaElettronicaBody>
</p:FatturaElettronica>
""")
        xml_out = out.getvalue()
        out.close()

        # Upload su Supabase Storage
        bucket = "notecredito"
        filename_storage = f"nc_uploads/NC_{numero_nc}_{centro}_{today}.xml"
        supabase.storage.from_(bucket).upload(
            filename_storage, xml_out.encode("utf-8"),
            {"content-type": "application/xml", "upsert": "true"}
        )
        xml_url = f"{bucket}/{filename_storage}"

        # Insert tabella
        supabase.table("notecredito_amazon_fattura").insert({
            "data_nota": today,
            "numero_nota": numero_nc,
            "centro": centro,
            "start_delivery": start_delivery,
            "po_list": po_list,
            "totale": totale_doc,
            "imponibile": sum(r["imponibile"] for r in dati_riepilogo),
            "xml_url": xml_url,
            "stato": "pronta",
            "fattura_id": None,
            "fattura_numero": fattura_numero,
            "created_at": datetime.now().isoformat()
        }).execute()

        filename = f"NC_{numero_nc}_DA_{fattura_numero}_{today}.xml"
        return send_file(
            io.BytesIO(xml_out.encode("utf-8")),
            download_name=filename,
            as_attachment=True,
            mimetype="application/xml"
        )
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
