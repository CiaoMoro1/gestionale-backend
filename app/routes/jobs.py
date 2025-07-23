# app/routes/jobs.py
from flask import Blueprint, jsonify
from app.supabase_client import supabase  # O importa come nel resto del tuo progetto

bp = Blueprint('jobs', __name__)

@bp.route('/api/jobs/<job_id>/status', methods=['GET'])
def get_job_status(job_id):
    job = supabase.table("jobs").select("*").eq("id", job_id).single().execute().data
    if not job:
        return jsonify({"error": "Job non trovato"}), 404
    return jsonify({
        "status": job["status"],
        "result": job.get("result"),
        "error": job.get("error"),
        "created_at": job.get("created_at"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at")
    })
