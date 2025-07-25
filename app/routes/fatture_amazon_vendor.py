from flask import Blueprint, request, jsonify, send_file
from app.supabase_client import supabase  # importa il tuo client Supabase
import uuid

bp = Blueprint('fatture_amazon_vendor', __name__)

# 1. CREA UN NUOVO JOB DI GENERAZIONE FATTURA
@bp.route('/api/fatture_amazon_vendor/genera', methods=['POST'])
def crea_fattura_amazon_vendor():
    data = request.get_json()
    centro = data.get("centro")
    start_delivery = data.get("start_delivery")
    po_list = data.get("po_list")
    user_id = data.get("user_id")  # opzionale

    # Validazione veloce
    if not centro or not start_delivery or not po_list:
        return jsonify({"error": "Dati mancanti"}), 400

    job_id = str(uuid.uuid4())
    job_payload = {
        "centro": centro,
        "start_delivery": start_delivery,
        "po_list": po_list
    }

    res = supabase.table("jobs").insert({
        "id": job_id,
        "type": "genera_fattura_amazon_vendor",
        "payload": job_payload,
        "status": "pending",
        "user_id": user_id
    }).execute()

    return jsonify({"job_id": job_id, "status": "pending"})

# 2. LISTA FATTURE (per tabella frontend)
@bp.route('/api/fatture_amazon_vendor/list', methods=['GET'])
def lista_fatture_amazon_vendor():
    # Puoi aggiungere filtri tramite query params (es: centro, data, stato)
    query = supabase.table("fatture_amazon_vendor").select("*")
    centro = request.args.get("centro")
    stato = request.args.get("stato")
    if centro:
        query = query.eq("centro", centro)
    if stato:
        query = query.eq("stato", stato)
    res = query.order("data_fattura", desc=True).limit(100).execute()
    return jsonify(res.data)

# 3. DOWNLOAD FATTURA (link diretto o via Flask)
@bp.route('/api/fatture_amazon_vendor/download/<int:fattura_id>', methods=['GET'])
def download_fattura_amazon_vendor(fattura_id):
    fattura = supabase.table("fatture_amazon_vendor").select("*").eq("id", fattura_id).single().execute().data
    if not fattura:
        return jsonify({"error": "Fattura non trovata"}), 404
    xml_url = fattura.get("xml_url")
    if not xml_url:
        return jsonify({"error": "Fattura senza XML"}), 404

    # Se vuoi restituire direttamente il link al frontend (consigliato)
    return jsonify({"download_url": xml_url})

    # --- oppure, se vuoi servire il file come allegato tramite Flask ---
    # bucket, file_path = xml_url.split("/", 1)
    # file_resp = supabase.storage.from_(bucket).download(file_path)
    # return send_file(io.BytesIO(file_resp), download_name=f"Fattura_{fattura['numero_fattura']}.xml", as_attachment=True, mimetype="application/xml")

# 4. DETTAGLIO FATTURA (facoltativo, per anteprima o info)
@bp.route('/api/fatture_amazon_vendor/<int:fattura_id>', methods=['GET'])
def dettaglio_fattura_amazon_vendor(fattura_id):
    fattura = supabase.table("fatture_amazon_vendor").select("*").eq("id", fattura_id).single().execute().data
    if not fattura:
        return jsonify({"error": "Fattura non trovata"}), 404
    return jsonify(fattura)


