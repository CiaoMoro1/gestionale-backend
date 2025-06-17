from flask import Blueprint, request, jsonify
import os
import requests
import json
from dotenv import load_dotenv
from app.supabase_client import supabase
from app.utils.auth import require_auth

load_dotenv()

brt = Blueprint("brt", __name__)

# ----------------------------------------
# CREAZIONE SPEDIZIONE (PROVVISORIA - DA CONFERMARE)
# ----------------------------------------
@brt.route("/api/brt/create-label", methods=["POST"])
@require_auth
def create_brt_label():
    """
    Crea una spedizione BRT (stato provvisorio, in attesa di conferma).
    Salva numericSenderReference nell'ordine per la conferma/cancellazione.
    Usa i dati normalizzati dal frontend se presenti.
    """
    data = request.json
    order_id = data.get("orderId")
    if not order_id:
        return jsonify({"error": "orderId mancante"}), 400

    # Recupera ordine da Supabase
    order_resp = supabase.table("orders").select("*").eq("id", order_id).single().execute()
    order = order_resp.data
    if not order:
        return jsonify({"error": "Ordine non trovato"}), 404

    # --- Qui usiamo i dati normalizzati se inviati dal frontend ---
    consignee_address = data.get("shipping_address") or order.get("shipping_address")
    consignee_zip = data.get("shipping_zip") or order.get("shipping_zip")
    consignee_city = data.get("shipping_city") or order.get("shipping_city")
    consignee_province = data.get("shipping_province") or order.get("shipping_province")
    consignee_country = data.get("shipping_country") or order.get("shipping_country") or "IT"
    consignee_email = data.get("shipping_email") or order.get("customer_email") or ""
    consignee_mobile = data.get("shipping_phone") or order.get("customer_phone") or ""
    consignee_contact = data.get("shipping_contact_name") or order.get("customer_name") or ""
    consignee_type = data.get("shipping_contact_type") or ("COMPANY" if order.get("company_name") else "PRIVATE")
    delivery_notes = data.get("delivery_notes") or order.get("delivery_notes") or ""
    is_alert_required = "1"
    is_saturday_delivery = data.get("is_saturday_delivery") or "0"
    taxid_code = data.get("customer_taxid") or order.get("customer_taxid") or ""
    vat_number = data.get("customer_vatnumber") or order.get("customer_vatnumber") or ""

    number_of_parcels = int(data.get("parcel_count") or order.get("parcel_count") or 1)
    peso_totale = 1.00  # Puoi personalizzare (ad es: 1.0 * number_of_parcels)

    # numericSenderReference: solo cifre dal numero ordine
    number = order.get("number", "")
    try:
        numeric_reference = int(''.join(filter(str.isdigit, str(number))))
    except Exception:
        numeric_reference = 0

    # Contrassegno block
    is_cod = order.get("payment_status", "").lower() == "contrassegno"
    cod_block = {"isCODMandatory": "1" if is_cod else "0"}
    if is_cod:
        cod_block.update({
            "cashOnDelivery": float(order.get("total", 0)),
            "codCurrency": "EUR"
        })

    # Province/nazione: 2 lettere uppercase
    prov = (consignee_province or "")[:2].upper()
    country = (consignee_country or "IT")[:2].upper()

    # Costruzione payload BRT
    create_data = {
        "network": " ",
        "departureDepot": int(os.getenv("BRT_DEPARTURE_DEPOT")),
        "senderCustomerCode": int(os.getenv("BRT_CODICE_CLIENTE")),
        "deliveryFreightTypeCode": "DAP",
        "consigneeCompanyName": order.get("customer_name"),
        "consigneeAddress": consignee_address,
        "consigneeZIPCode": consignee_zip,
        "consigneeCity": consignee_city,
        "consigneeProvinceAbbreviation": prov,
        "consigneeCountryAbbreviationISOAlpha2": country,
        "consigneeEMail": consignee_email,
        "consigneeTelephone": "",  # opzionale, puoi aggiungere se hai numero fisso
        "consigneeMobilePhoneNumber": consignee_mobile,
        "isAlertRequired": is_alert_required,
        "deliveryNotes": delivery_notes,
        "consigneeTaxIDCode": taxid_code,
        "consigneeVATNumber": vat_number,
        "pricingConditionCode": "000",  # Tariffa test/contratto
        "serviceType": "",
        "numberOfParcels": number_of_parcels,
        "weightKG": peso_totale,
        "numericSenderReference": numeric_reference,
        **cod_block
    }
    # Elimina campi vuoti per non sporcare il payload
    create_data = {k: v for k, v in create_data.items() if v not in (None, "", [])}

    payload = {
        "account": {
            "userID": os.getenv("BRT_USER_ID"),
            "password": os.getenv("BRT_PASSWORD"),
        },
        "createData": create_data,
        "isLabelRequired": "1",
        "labelParameters": {
            "outputType": os.getenv("BRT_LABEL_FORMAT", "PDF"),
            "offsetX": "0",
            "offsetY": "0",
            "isBorderRequired": "1",
            "isLogoRequired": "1",
            "isBarcodeControlRowRequired": "0"
        }
    }

    print("=== BRT PAYLOAD ===")
    print(json.dumps(payload, indent=2, ensure_ascii=False))

    # --- Chiamata API BRT ---
    try:
        res = requests.post(
            os.getenv("BRT_API_URL"),
            json=payload,
            timeout=20
        )
        print("BRT RAW RESPONSE TEXT:", res.text)  # <--- AGGIUNGI QUESTA RIGA
        brt_data = res.json()
    except Exception as exc:
        print("BRT EXCEPTION:", exc)
        return jsonify({"error": "Errore connessione BRT", "details": str(exc)}), 502

    print("=== BRT RAW RESPONSE ===")
    print(json.dumps(brt_data, indent=2, ensure_ascii=False))

    # Gestione errori business
    def parse_brt_error(exec_msg):
        code = exec_msg.get("codeDesc", "")
        msg = exec_msg.get("message", "")
        if code == "ROUTING CALCULATION ERROR":
            return "Provincia e CAP non corrispondono: verifica che i dati di spedizione siano corretti."
        if "codPaymentType" in msg:
            return "Tipo pagamento contrassegno non valido: lascia il campo vuoto oppure chiedi all’amministratore."
        # ...altri if per altri codici...
        return msg or "Errore generico dal corriere."

    execution_msg = brt_data.get("createResponse", {}).get("executionMessage")
    if execution_msg and execution_msg.get("severity", "").upper() == "ERROR":
        user_friendly = parse_brt_error(execution_msg)
        return jsonify({"error": user_friendly, "details": execution_msg}), 400

    # Estrai label e dati spedizione (gestione robusta di tutti i casi possibili)
    label = None
    parcel_id = None
    parcel_number = None

    # ...parte iniziale invariata...

    # Estrai TUTTE le etichette
    brt_create = brt_data.get("createResponse", {})
    labels_raw = brt_create.get("labels", {}).get("label", [])
    # labels_raw può essere un dict (singola), una lista di dict o una lista di stringhe
    labels_stream = []
    parcel_ids = []
    if labels_raw:
        if isinstance(labels_raw, list):
            for label_obj in labels_raw:
                if isinstance(label_obj, dict):
                    if label_obj.get("stream"):
                        labels_stream.append(label_obj.get("stream"))
                        parcel_ids.append(label_obj.get("parcelID"))
                elif isinstance(label_obj, str):
                    labels_stream.append(label_obj)
        elif isinstance(labels_raw, dict):
            if labels_raw.get("stream"):
                labels_stream.append(labels_raw.get("stream"))
                parcel_ids.append(labels_raw.get("parcelID"))
        elif isinstance(labels_raw, str):
            labels_stream.append(labels_raw)

    # (mantieni compatibilità con la vecchia struttura, ma ora puoi inviare tutte)
    # Salva solo la prima come preview (vecchia compatibilità)
    label = labels_stream[0] if labels_stream else None
    parcel_id = parcel_ids[0] if parcel_ids else None
    parcel_number = brt_create.get("parcelNumberFrom")

    supabase.table("orders").update({
        "stato_ordine": "etichetta_generata",
        "parcel_id": json.dumps(parcel_ids),  # <-- Salva TUTTI i parcelID (come array)
        "parcel_number": parcel_number,
        "numeric_sender_reference": numeric_reference,
        "label_pdf_base64": json.dumps(labels_stream),
        "parcel_count": number_of_parcels
    }).eq("id", order_id).execute()

    # Ritorna TUTTE le etichette al frontend (base64)
    return jsonify({
        "labels": labels_stream,
        "parcel_ids": parcel_ids,
        "parcel_number": parcel_number,
        "numeric_sender_reference": numeric_reference
    })


# ----------------------------------------
# CONFERMA SPEDIZIONE (Necessario in "conferma esplicita")
# ----------------------------------------
@brt.route("/api/brt/confirm-shipment", methods=["PUT"])
@require_auth
def confirm_brt_shipment():
    """
    Conferma una spedizione già creata (Modalità Conferma Esplicita).
    Serve numericSenderReference e, se usato, alphanumericSenderReference.
    """
    data = request.json
    numeric_ref = data.get("numericSenderReference")
    sender_code = int(os.getenv("BRT_CODICE_CLIENTE"))
    alpha_ref = data.get("alphanumericSenderReference", "")
    if not numeric_ref:
        return jsonify({"error": "numericSenderReference mancante"}), 400

    payload = {
        "account": {
            "userID": os.getenv("BRT_USER_ID"),
            "password": os.getenv("BRT_PASSWORD"),
        },
        "confirmData": {
            "senderCustomerCode": sender_code,
            "numericSenderReference": int(numeric_ref)
        }
    }
    if alpha_ref:
        payload["confirmData"]["alphanumericSenderReference"] = alpha_ref

    try:
        res = requests.put(
            "https://api.brt.it/rest/v1/shipments/shipment",
            json=payload,
            timeout=20
        )
        print("BRT RAW CONFIRM RESPONSE:", res.text)
        try:
            brt_data = res.json()
        except Exception:
            brt_data = {"raw": res.text, "status_code": res.status_code}
    except Exception as exc:
        print("BRT EXCEPTION:", exc)
        return jsonify({"error": "Errore connessione BRT", "details": str(exc)}), 502

    print("BRT CONFIRM RESPONSE:", brt_data)
    execution_msg = brt_data.get("confirmResponse", {}).get("executionMessage")
    if execution_msg and execution_msg.get("severity", "").upper() == "ERROR":
        return jsonify({"error": "BRT confirm error", "details": execution_msg}), 400

    # Cerca label e tracking nella risposta conferma
    label = None
    parcel_id = None
    parcel_number = None

    if "confirmResponse" in brt_data:
        label_list = brt_data["confirmResponse"].get("labels", {}).get("label", [])
        if label_list and isinstance(label_list, list) and label_list:
            label_obj = label_list[0]
            label = label_obj.get("stream")
            parcel_id = label_obj.get("parcelID")
            parcel_number = label_obj.get("parcelNumberGeoPost") or brt_data["confirmResponse"].get("parcelNumberFrom")
        else:
            parcel_number = brt_data["confirmResponse"].get("parcelNumberFrom")

    # Aggiorna ordine su Supabase: stato, tracking ecc.
    supabase.table("orders").update({
        "stato_ordine": "etichetta",
        "parcel_id": parcel_id,
        "parcel_number": parcel_number
    }).eq("numeric_sender_reference", numeric_ref).execute()

    return jsonify({
        "label": label,
        "parcel_id": parcel_id,
        "parcel_number": parcel_number,
        "trackingUrl": f"https://vas.brt.it/vas/sped_det_show.hsm?chisono={parcel_number}" if parcel_number else None,
        "confirmRaw": brt_data
    })

# ----------------------------------------
# CANCELLAZIONE SPEDIZIONE (PUT)
# ----------------------------------------
@brt.route("/api/brt/delete-shipment", methods=["PUT"])
@require_auth
def delete_brt_shipment():
    """
    Cancella una spedizione (prima che sia presa in carico dalla filiale BRT).
    Rimette lo stato ordine a "prelievo" e cancella dati etichetta.
    """
    data = request.json
    numeric_ref = data.get("numericSenderReference")
    sender_code = int(os.getenv("BRT_CODICE_CLIENTE"))
    alpha_ref = data.get("alphanumericSenderReference", "")
    if not numeric_ref:
        return jsonify({"error": "numericSenderReference mancante"}), 400

    payload = {
        "account": {
            "userID": os.getenv("BRT_USER_ID"),
            "password": os.getenv("BRT_PASSWORD"),
        },
        "deleteData": {
            "senderCustomerCode": sender_code,
            "numericSenderReference": int(numeric_ref)
        }
    }
    if alpha_ref:
        payload["deleteData"]["alphanumericSenderReference"] = alpha_ref

    try:
        res = requests.put(
            "https://api.brt.it/rest/v1/shipments/delete",
            json=payload,
            timeout=20
        )
        print("BRT RAW DELETE RESPONSE:", res.text)
        try:
            brt_data = res.json()
        except Exception:
            brt_data = {"raw": res.text, "status_code": res.status_code}
    except Exception as exc:
        print("BRT EXCEPTION:", exc)
        return jsonify({"error": "Errore connessione BRT", "details": str(exc)}), 502

    print("BRT DELETE RESPONSE:", brt_data)

    # Se cancellazione ok, resetta lo stato ordine e svuota tracking
    execution_msg = (
        brt_data.get("deleteResponse", {})
        .get("executionMessage")
        if "deleteResponse" in brt_data
        else brt_data.get("executionMessage")
    )
    if execution_msg and execution_msg.get("severity", "").upper() in ("OK", "INFO"):
        supabase.table("orders").update({
            "stato_ordine": "prelievo",
            "parcel_id": None,
            "parcel_number": None,
            "numeric_sender_reference": None,
            "label_pdf_base64": None
        }).eq("numeric_sender_reference", numeric_ref).execute()
        return jsonify({"ok": True})

    # Se non cancellata, ritorna errore
    return jsonify({"error": "Eliminazione fallita", "details": execution_msg}), 400



# ----------------------------------------
# TRACKING SPEDIZIONE BRT (MULTIPARCEL)
# ----------------------------------------
@brt.route("/api/brt/tracking", methods=["GET"])
@require_auth
def brt_tracking_multi():
    """
    Tracking multiplo BRT: accetta uno o più parcelId tramite query string.
    Esempio: /api/brt/tracking?parcelIds=123,456,789
    """
    import os
    import requests

    parcel_ids = request.args.get("parcelIds", "")
    if not parcel_ids:
        return jsonify({"error": "Nessun parcelId fornito"}), 400

    BRT_USER_ID = os.getenv("BRT_USER_ID")
    BRT_PASSWORD = os.getenv("BRT_PASSWORD")

    ids = [pid.strip() for pid in parcel_ids.split(",") if pid.strip()]
    results = []
    errors = []

    for pid in ids:
        url = f"https://api.brt.it/rest/v1/tracking/parcelID/{pid}"
        headers = {
            "userID": BRT_USER_ID,
            "password": BRT_PASSWORD,
        }
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 200:
                results.append({"parcelId": pid, "tracking": resp.json()})
            else:
                errors.append({"parcelId": pid, "error": resp.text, "status_code": resp.status_code})
        except Exception as exc:
            errors.append({"parcelId": pid, "error": str(exc), "status_code": None})

    return jsonify({"results": results, "errors": errors})
