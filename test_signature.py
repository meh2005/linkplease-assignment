import os
import json
import hmac
import hashlib
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("PSEUDOGRAM_API_KEY")

payload = {
    "event_id": "manual_signature_test_001",
    "event_type": "comment.created",
    "sent_at": "2026-08-17T13:15:00.000Z",
    "data": {
        "comment_id": "test_comment_001",
        "post_id": "test_post_001",
        "text": "PRICE please",
        "created_at": "2026-08-17T13:14:59.000Z",
        "from": {
            "user_id": "test_user_001",
            "username": "testuser"
        }
    }
}

# IMPORTANT: these exact bytes are what we sign
raw_body = json.dumps(
    payload,
    separators=(",", ":")
).encode("utf-8")

signature = hmac.new(
    API_KEY.encode("utf-8"),
    raw_body,
    hashlib.sha256
).hexdigest()

headers = {
    "Content-Type": "application/json",
    "X-PseudoGram-Signature": f"sha256={signature}"
}

response = requests.post(
    "https://linkplease-assignment-nclt.onrender.com/webhook",
    data=raw_body,
    headers=headers
)

print("Status:", response.status_code)
print("Response:", response.text)