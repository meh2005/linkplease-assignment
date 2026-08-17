# LinkPlease Assignment

A Flask-based webhook processing service that receives PseudoGram
events, matches comments against configured keyword rules, creates
direct-message jobs, processes them asynchronously, enforces
idempotency, and tracks delivery statistics.

## Features

-   Create keyword-based DM rules.
-   Receive webhook events through `POST /webhook`.
-   Verify webhook requests using HMAC-SHA256.
-   Prevent duplicate webhook processing using `event_id`.
-   Match incoming comments against configured rules.
-   Queue DM jobs for background processing.
-   Send DMs through the PseudoGram API.
-   Use idempotency keys for outgoing DM requests.
-   Track delivery states and retry attempts.
-   Apply the outgoing DM rate limit using a persistent SQLite request
    log.
-   Provide health and statistics endpoints.
-   SQLite database with WAL mode and indexes for the main lookup
    fields.
-   Production deployment using Gunicorn/Render.

## Project Structure

``` text
Linkplease/
│
├── app.py
├── database.py
├── worker.py
├── mock_client.py
├── requirements.txt
├── .python-version
├── .gitignore
├── FAILURES.md
└── README.md
```

### `app.py`

Flask API containing the webhook, rule-management, health, and
statistics endpoints. It also performs HMAC-SHA256 webhook verification
and starts the background worker.

### `database.py`

Creates and manages the SQLite database, tables, indexes, WAL
configuration, and database connections.

### `worker.py`

Background processing loop responsible for processing webhook events,
creating DM jobs, sending DMs, handling retries, checking delivery
status, and updating job state.

### `mock_client.py`

Client used for communicating with the mock PseudoGram API during local
development/testing.

### `FAILURES.md`

Documents known test failures and observations from the assignment
testing process.

## API Endpoints

### Health Check

``` http
GET /health
```

Response:

``` json
{
  "status": "ok"
}
```

### Create Rule

``` http
POST /rules
Content-Type: application/json
```

Example:

``` json
{
  "keyword": "hello",
  "dm_message": "Thanks for your comment!"
}
```

Successful response:

``` json
{
  "rule_id": "1",
  "keyword": "hello",
  "dm_message": "Thanks for your comment!"
}
```

### Webhook

``` http
POST /webhook
Content-Type: application/json
X-PseudoGram-Signature: sha256=<HMAC-SHA256>
```

The service validates the signature against the exact raw request body
before processing the JSON payload.

A new event returns:

``` json
{
  "status": "accepted"
}
```

A previously received `event_id` returns:

``` json
{
  "status": "already_received"
}
```

Invalid signatures return HTTP `401`.

### Statistics

``` http
GET /stats
```

Example response:

``` json
{
  "sent": 0,
  "failed": 0,
  "queued": 0,
  "duplicates_blocked": 0
}
```

## Database Design

The SQLite database contains the following main tables:

-   `rules` --- configured keyword and DM-message rules.
-   `events` --- received webhook events and their processing state.
-   `dm_jobs` --- queued and processed DM jobs.
-   `sent_dms` --- delivery records.
-   `stats` --- duplicate-blocking statistics.
-   `dm_send_log` --- outgoing DM request timestamps used for rate
    limiting.

Important uniqueness constraints prevent repeated processing of the same
rule/user combination and duplicate DM records.

## Event Processing Flow

``` text
PseudoGram Webhook
       │
       ▼
HMAC Signature Verification
       │
       ▼
Duplicate event_id check
       │
       ▼
Store event in SQLite
       │
       ▼
Return HTTP 200
       │
       ▼
Background Worker
       │
       ├── Find matching rule
       │
       ├── Create DM job
       │
       ├── Enforce rate limit
       │
       ├── Send DM
       │
       ├── Poll delivery status
       │
       └── Retry failed requests
```

The webhook endpoint does not perform the external DM operation
synchronously. It stores the event and returns quickly so that the
background worker can process it.

## Security

The webhook uses HMAC-SHA256 verification.

The expected signature is calculated using:

``` text
HMAC-SHA256(
    PSEUDOGRAM_API_KEY,
    raw_request_body
)
```

The received signature must use the format:

``` text
sha256=<hex digest>
```

API credentials are supplied through environment variables and are not
stored in source code.

## Environment Variables

Create a `.env` file for local development:

``` env
PSEUDOGRAM_API_KEY=your_api_key
PSEUDOGRAM_BASE_URL=https://pseudogram-api.onrender.com
```

Do not commit `.env` or API keys to GitHub.

For Render, configure the same values in the service's Environment
Variables section.

## Installation

Create a virtual environment:

``` bash
python -m venv .venv
```

Activate it on Windows:

``` powershell
.venv\Scripts\Activate.ps1
```

Install dependencies:

``` bash
pip install -r requirements.txt
```

Initialize the database:

``` bash
python database.py
```

Run locally:

``` bash
python app.py
```

The Flask application runs on:

``` text
http://127.0.0.1:5000
```

## Local Testing

Health:

``` bash
curl.exe http://127.0.0.1:5000/health
```

Statistics:

``` bash
curl.exe http://127.0.0.1:5000/stats
```

The expected initial statistics are:

``` json
{
  "duplicates_blocked": 0,
  "failed": 0,
  "queued": 0,
  "sent": 0
}
```

Rule creation can be tested using Thunder Client, Postman, or curl.

For signed webhook testing, generate the HMAC from the exact request
body and send it in the `X-PseudoGram-Signature` header.

## Production Deployment

The application is configured to run with Gunicorn:

``` bash
gunicorn --worker-class gthread --threads 4 --timeout 120 app:app
```

The deployed service exposes:

``` text
/health
/stats
/rules
/webhook
```

## Reliability and Idempotency

Webhook events are identified using `event_id`. If the same event is
received again, it is not inserted a second time.

DM jobs also use a unique `(rule_id, user_id)` constraint so that a user
is not repeatedly sent the same rule's DM.

Outgoing requests use an idempotency key so retries do not
unintentionally create duplicate DMs.

## Rate Limiting

Outgoing DM requests are recorded in `dm_send_log`. The worker uses this
persistent log to enforce the assignment's rolling request limit across
application restarts.

## Testing Result

The local implementation was tested for:

-   Rule creation.
-   Invalid rule input.
-   Webhook acceptance.
-   Duplicate webhook handling.
-   HMAC signature validation.
-   Background DM processing.
-   Successful DM delivery.
-   Delivery status polling.
-   Statistics endpoint.
-   Production deployment on Render.

Example successful local flow:

``` text
POST /rules              → 201
POST /webhook            → 200 accepted
Background worker        → DM job created
DM API                   → accepted
Delivery status          → delivered
GET /stats               → statistics returned
Duplicate webhook       → already handled
```

## Known Deployment Note

The application uses SQLite, which is appropriate for this assignment
and local/single-instance testing. SQLite storage on ephemeral cloud
service instances should not be treated as durable production storage
unless persistent storage is configured.

The Render deployment was successfully started with Gunicorn and the
`/health` endpoint was verified as operational.

## Technologies

-   Python
-   Flask
-   SQLite
-   Requests
-   Gunicorn
-   python-dotenv
-   HMAC-SHA256
-   REST/HTTP APIs
-   Render
