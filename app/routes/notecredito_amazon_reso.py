from flask import Blueprint, request, jsonify, send_file
from app.supabase_client import supabase
import uuid
import io
import zipfile

bp = Blueprint('notecredito_amazon_reso', __name__)

BUCKET = "notecredito"  # Bucket separato per le note di credito

# 1. ENDPOINT UPLOAD RETURN_ITEMS.CSV
@bp.route('/api/notecredito_amazon_reso/upload', methods=['POST'])
def upload_notecredito_amazon_reso():
    if 'file' not in request.files:
        return jsonify({'error': 'Nessun file inviato'}), 400

    file = request.files['file']
    filename = f"return_items/{uuid.uuid4()}_{file.filename}"
    file_bytes = file.read()
    res = supabase.storage.from_(BUCKET).upload(filename, file_bytes, {"content-type": "text/csv"})
    if hasattr(res, 'error') and res.error:
        return jsonify({'error': 'Errore upload su storage'}), 500

    # Crea job asincrono
    job_id = str(uuid.uuid4())
    job_payload = {
        "storage_path": f"{BUCKET}/{filename}"
    }
    supabase.table("jobs").insert({
        "id": job_id,
        "type": "genera_notecredito_amazon_reso",
        "payload": job_payload,
        "status": "pending"
    }).execute()

    return jsonify({"job_id": job_id, "status": "pending"})


# 2. LISTA NOTE DI CREDITO
@bp.route('/api/notecredito_amazon_reso/list', methods=['GET'])
def lista_notecredito_amazon_reso():
    try:
        query = supabase.table("notecredito_amazon_reso").select("*")
        po = request.args.get("po")
        vret = request.args.get("vret")
        stato = request.args.get("stato")
        if po:
            query = query.eq("po", po)
        if vret:
            query = query.eq("vret", vret)
        if stato:
            query = query.eq("stato", stato)
        res = query.order("data_nota", desc=True).limit(100).execute()
        return jsonify(res.data)
    except Exception as e:
        print(f"ERRORE su /api/notecredito_amazon_reso/list: {e}")
        return jsonify({"error": "Errore nel recupero note credito", "details": str(e)}), 500


# 3. DOWNLOAD SINGOLA NOTA DI CREDITO
@bp.route('/api/notecredito_amazon_reso/download/<int:nota_id>', methods=['GET'])
def download_notecredito_amazon_reso(nota_id):
    try:
        nota = supabase.table("notecredito_amazon_reso").select("*").eq("id", nota_id).single().execute().data
        if not nota:
            return jsonify({"error": "Nota di credito non trovata"}), 404
        xml_url = nota.get("xml_url")
        if not xml_url:
            return jsonify({"error": "Nota senza XML"}), 404

        bucket, file_path = xml_url.split('/', 1)
        file_resp = supabase.storage.from_(bucket).download(file_path)
        if not file_resp:
            return jsonify({"error": "File non trovato"}), 404

        return send_file(
            io.BytesIO(file_resp),
            download_name=f"NotaCredito_{nota['numero_nota']}_{nota['po']}_{nota['data_nota'].replace('-', '')}.xml",
            as_attachment=True,
            mimetype="application/xml"
        )
    except Exception as e:
        print(f"ERRORE su /api/notecredito_amazon_reso/download: {e}")
        return jsonify({"error": "Errore durante il download", "details": str(e)}), 500


# 4. DETTAGLIO SINGOLA NOTA (opzionale)
@bp.route('/api/notecredito_amazon_reso/<int:nota_id>', methods=['GET'])
def dettaglio_notecredito_amazon_reso(nota_id):
    try:
        nota = supabase.table("notecredito_amazon_reso").select("*").eq("id", nota_id).single().execute().data
        if not nota:
            return jsonify({"error": "Nota di credito non trovata"}), 404
        return jsonify(nota)
    except Exception as e:
        print(f"ERRORE su /api/notecredito_amazon_reso/{nota_id}: {e}")
        return jsonify({"error": "Errore nel recupero della nota", "details": str(e)}), 500


# 5. DOWNLOAD ZIP MASSIVO
@bp.route('/api/notecredito_amazon_reso/download_zip', methods=['POST'])
def download_zip_notecredito_amazon_reso():
    """
    Riceve lista di ID nota di credito via POST JSON: {"ids": [1,2,3]}
    Restituisce un unico ZIP con tutti gli XML delle note richieste.
    """
    try:
        ids = request.json.get('ids')
        if not ids or not isinstance(ids, list):
            return jsonify({"error": "IDs non forniti"}), 400

        # Scarica tutte le note richieste
        note = supabase.table("notecredito_amazon_reso").select("*").in_("id", ids).execute().data
        if not note:
            return jsonify({"error": "Nessuna nota trovata"}), 404

        memory_file = io.BytesIO()
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for n in note:
                xml_url = n.get("xml_url")
                if not xml_url:
                    continue
                bucket, file_path = xml_url.split('/', 1)
                file_resp = supabase.storage.from_(bucket).download(file_path)
                if not file_resp:
                    continue
                filename = f"NotaCredito_{n['numero_nota']}_{n['po']}_{n['data_nota'].replace('-', '')}.xml"
                zipf.writestr(filename, file_resp)
        memory_file.seek(0)
        return send_file(
            memory_file,
            download_name="NoteCredito_AmazonReso.zip",
            as_attachment=True,
            mimetype="application/zip"
        )

    except Exception as e:
        print(f"ERRORE su /api/notecredito_amazon_reso/download_zip: {e}")
        return jsonify({"error": "Errore durante il download ZIP", "details": str(e)}), 500
