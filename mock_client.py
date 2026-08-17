import os

import requests
from dotenv import load_dotenv


load_dotenv()


BASE_URL = os.getenv(
    "PSEUDOGRAM_BASE_URL",
    "https://pseudogram-api.onrender.com"
)

API_KEY = os.getenv("PSEUDOGRAM_API_KEY")


def get_headers(idempotency_key=None):

    if not API_KEY:
        raise RuntimeError(
            "PSEUDOGRAM_API_KEY is not configured"
        )

    headers = {
        "X-API-Key": API_KEY,
        "Content-Type": "application/json"
    }

    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key

    return headers


def send_dm(
    user_id,
    message,
    comment_id,
    idempotency_key
):

    url = f"{BASE_URL}/v1/dm/send"

    payload = {
        "recipient_user_id": user_id,
        "message": message,
        "comment_id": comment_id
    }

    return requests.post(
        url,
        headers=get_headers(idempotency_key),
        json=payload,
        timeout=10
    )


def get_dm_status(dm_id):

    url = f"{BASE_URL}/v1/dm/{dm_id}"

    return requests.get(
        url,
        headers=get_headers(),
        timeout=10
    )