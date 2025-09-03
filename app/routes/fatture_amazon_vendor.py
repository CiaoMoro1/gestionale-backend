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
    try:
        query = supabase.table("fatture_amazon_vendor").select("*")
        centro = request.args.get("centro")
        stato = request.args.get("stato")
        if centro:
            query = query.eq("centro", centro)
        if stato:
            query = query.eq("stato", stato)
        res = query.order("data_fattura", desc=True).limit(100).execute()
        return jsonify(res.data)
    except Exception as e:
        # logga errore dettagliato su console/server
        print(f"ERRORE su /api/fatture_amazon_vendor/list: {e}")
        return jsonify({"error": "Errore nel recupero delle fatture", "details": str(e)}), 500


# 3. DOWNLOAD FATTURA (link diretto o via Flask)
@bp.route('/api/fatture_amazon_vendor/download/<int:fattura_id>', methods=['GET'])
def download_fattura_amazon_vendor(fattura_id):
    try:
        fattura = supabase.table("fatture_amazon_vendor").select("*").eq("id", fattura_id).single().execute().data
        if not fattura:
            return jsonify({"error": "Fattura non trovata"}), 404
        xml_url = fattura.get("xml_url")
        if not xml_url:
            return jsonify({"error": "Fattura senza XML"}), 404

        # Serve il file XML come download:
        bucket, *file_path_parts = xml_url.split('/', 1)
        file_path = file_path_parts[0] if file_path_parts else ""
        file_resp = supabase.storage.from_(bucket).download(file_path)
        if not file_resp:
            return jsonify({"error": "File non trovato"}), 404

        import io
        return send_file(
            io.BytesIO(file_resp),
            download_name=f"Fattura_{fattura['numero_fattura']}_{fattura['centro']}_{fattura['data_fattura'].replace('-', '')}.xml",
            as_attachment=True,
            mimetype="application/xml"
        )
    except Exception as e:
        print(f"ERRORE su /api/fatture_amazon_vendor/download: {e}")
        return jsonify({"error": "Errore durante il download", "details": str(e)}), 500

# 4. DETTAGLIO FATTURA (facoltativo, per anteprima o info)
@bp.route('/api/fatture_amazon_vendor/<int:fattura_id>', methods=['GET'])
def dettaglio_fattura_amazon_vendor(fattura_id):
    try:
        fattura = supabase.table("fatture_amazon_vendor").select("*").eq("id", fattura_id).single().execute().data
        if not fattura:
            return jsonify({"error": "Fattura non trovata"}), 404
        return jsonify(fattura)
    except Exception as e:
        print(f"ERRORE su /api/fatture_amazon_vendor/{fattura_id}: {e}")
        return jsonify({"error": "Errore nel recupero della fattura", "details": str(e)}), 500



# 5. CREA JOB DI NOTA DI CREDITO DA FATTURA
@bp.route('/api/fatture_amazon_vendor/nota-credito', methods=['POST'])
def crea_nota_credito_da_fattura():
    data = request.get_json()
    fattura_id = data.get("fattura_id")
    motive = data.get("motive")  # opzionale (descrizione/causale)
    user_id = data.get("user_id")  # opzionale

    if not fattura_id:
        return jsonify({"error": "fattura_id mancante"}), 400

    # carico la fattura per estrarre centro, data, po_list e (facoltativo) lines da rigenerare
    fatt = supabase.table("fatture_amazon_vendor").select("*").eq("id", fattura_id).single().execute().data
    if not fatt:
        return jsonify({"error": "Fattura non trovata"}), 404

    job_id = str(uuid.uuid4())
    job_payload = {
        "fattura_id": fattura_id,
        "centro": fatt.get("centro"),
        "start_delivery": fatt.get("start_delivery"),
        "po_list": fatt.get("po_list") or [],
        "numero_fattura_collegata": fatt.get("numero_fattura"),
        "motive": motive or "Storno/variazione Amazon"
    }

    supabase.table("jobs").insert({
        "id": job_id,
        "type": "genera_nota_credito_da_fattura",
        "payload": job_payload,
        "status": "pending",
        "user_id": user_id
    }).execute()

    return jsonify({"job_id": job_id, "status": "pending"})