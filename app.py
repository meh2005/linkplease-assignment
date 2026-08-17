import os
import json
import hmac
import hashlib
import threading

from dotenv import load_dotenv
from flask import Flask, request, jsonify

from database import get_db, init_db
from worker import worker_loop


load_dotenv()

app = Flask(__name__)

init_db()

API_KEY = os.getenv("PSEUDOGRAM_API_KEY")

def verify_webhook_signature(raw_body, signature):
    if not API_KEY:
        print("HMAC DEBUG: API_KEY is missing")
        return False

    if not signature:
        print("HMAC DEBUG: signature header is missing")
        return False

    print(
        "HMAC DEBUG:",
        "signature_prefix=", signature[:15],
        "signature_length=", len(signature),
        "body_length=", len(raw_body)
    )

    if not signature.startswith("sha256="):
        print("HMAC DEBUG: invalid signature prefix")
        return False

    received_signature = signature[7:]

    expected_signature = hmac.new(
        API_KEY.encode("utf-8"),
        raw_body,
        hashlib.sha256
    ).hexdigest()

    print(
        "HMAC DEBUG:",
        "received_length=", len(received_signature),
        "expected_length=", len(expected_signature),
        "match=", hmac.compare_digest(
            received_signature,
            expected_signature
        )
    )

    return hmac.compare_digest(
        received_signature,
        expected_signature
    )

@app.route("/rules", methods=["POST"])
def create_rule():

    data = request.get_json()

    if not data:
        return jsonify({
            "error": "JSON body is required"
        }), 400

    keyword = data.get("keyword")
    dm_message = data.get("dm_message")

    if not keyword or not dm_message:
        return jsonify({
            "error": "keyword and dm_message are required"
        }), 400

    keyword = keyword.strip().lower()

    if not keyword:
        return jsonify({
            "error": "keyword cannot be empty"
        }), 400

    with get_db() as db:

        cursor = db.execute("""
            INSERT INTO rules (
                keyword,
                dm_message
            )
            VALUES (?, ?)
        """, (
            keyword,
            dm_message
        ))

        rule_id = cursor.lastrowid

    return jsonify({
        "rule_id": str(rule_id),
        "keyword": keyword,
        "dm_message": dm_message
    }), 201


@app.route("/webhook", methods=["POST"])
def webhook():

    # IMPORTANT:
    # Read the exact raw request body before parsing JSON.
    raw_body = request.get_data(cache=True)

    signature = request.headers.get(
        "X-PseudoGram-Signature"
    )

    if not verify_webhook_signature(
        raw_body,
        signature
    ):
        return jsonify({
            "error": "invalid_signature"
        }), 401

    try:
        data = json.loads(raw_body)
    except json.JSONDecodeError:

        return jsonify({
            "error": "Invalid JSON"
        }), 400

    event_id = data.get("event_id")
    event_type = data.get("event_type")

    if not event_id or not event_type:
        return jsonify({
            "error": "event_id and event_type are required"
        }), 400

    with get_db() as db:

        existing = db.execute("""
            SELECT event_id
            FROM events
            WHERE event_id = ?
        """, (event_id,)).fetchone()

        if existing:

            return jsonify({
                "status": "already_received"
            }), 200

        db.execute("""
            INSERT INTO events (
                event_id,
                event_type,
                payload
            )
            VALUES (?, ?, ?)
        """, (
            event_id,
            event_type,
            raw_body.decode("utf-8")
        ))

    return jsonify({
        "status": "accepted"
    }), 200


@app.route("/health", methods=["GET"])
def health():

    return jsonify({
        "status": "ok"
    }), 200


@app.route("/stats", methods=["GET"])
def stats():

    with get_db() as db:

        sent = db.execute("""
            SELECT COUNT(*)
            FROM dm_jobs
            WHERE status = 'delivered'
        """).fetchone()[0]

        failed = db.execute("""
            SELECT COUNT(*)
            FROM dm_jobs
            WHERE status = 'failed'
        """).fetchone()[0]

        queued = db.execute("""
            SELECT COUNT(*)
            FROM dm_jobs
            WHERE status IN (
                'queued',
                'sending',
                'waiting'
            )
        """).fetchone()[0]

        duplicates = db.execute("""
            SELECT duplicates_blocked
            FROM stats
            WHERE id = 1
        """).fetchone()[0]

    return jsonify({
        "sent": sent,
        "failed": failed,
        "queued": queued,
        "duplicates_blocked": duplicates
    }), 200


def start_worker():

    thread = threading.Thread(
        target=worker_loop,
        daemon=True
    )

    thread.start()

start_worker()
if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=True,
        use_reloader=False
    )