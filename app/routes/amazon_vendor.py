from flask import Blueprint, jsonify, request
import pandas as pd
import io
from app.supabase_client import supabase
from datetime import datetime

bp = Blueprint('amazon_vendor', __name__)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in {"xls", "xlsx"}

@bp.route('/api/amazon/vendor/orders/upload', methods=['POST'])
def upload_vendor_orders():
    if 'file' not in request.files:
        return jsonify({"error": "Nessun file fornito"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "Nessun file selezionato"}), 400

    if not allowed_file(file.filename):
        return jsonify({"error": "Formato file non valido"}), 400

    try:
        excel_bytes = file.read()
        df = pd.read_excel(io.BytesIO(excel_bytes), header=2)  # Intestazione alla riga 3 (indice 2)

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
                return jsonify({"error": f"Colonna mancante: {col}"}), 400

        importati = 0
        po_numbers = set()
        errors = []

        for _, row in df.iterrows():
            try:
                ordine = {
                    "po_number": str(row["Numero ordine/ordine d’acquisto"]).strip(),
                    "external_code": str(row["Codice identificativo esterno"]).strip(),
                    "sku": str(row["Numero di modello"]).strip(),
                    "asin": str(row["ASIN"]).strip(),
                    "title": str(row["Titolo"]).strip(),
                    "cost": float(str(row["Costo"]).replace("€", "").replace(",", ".").strip()),
                    "ordered_quantity": int(row["Quantità ordinata"]),
                    "confirmed_quantity": int(row["Quantità confermata"]),
                    "delivery_start": str(row["Inizio consegna"]).strip(),
                    "delivery_end": str(row["Termine consegna"]).strip(),
                    "expected_delivery": str(row["Data di consegna prevista"]).strip(),
                    "availability": str(row["Stato disponibilità"]).strip(),
                    "vendor_code": str(row["Codice fornitore"]).strip(),
                    "fulfillment_center": str(row["Fulfillment Center"]).strip(),
                    "created_at": datetime.utcnow().isoformat(),
                    "raw_data": row.to_dict()
                }
                supabase.table("ordini_vendor_items").upsert(ordine, on_conflict="po_number,sku,asin").execute()
                po_numbers.add(ordine["po_number"])
                importati += 1
            except Exception as ex:
                errors.append(f"Errore riga {row.to_dict()}: {ex}")

        return jsonify({
            "status": "ok",
            "importati": importati,
            "po_unici": len(po_numbers),
            "po_list": list(po_numbers),
            "errors": errors
        })

    except Exception as e:
        return jsonify({"error": f"Errore durante l'importazione: {e}"}), 500
