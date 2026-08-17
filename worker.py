import json
import time
import threading
import sqlite3

from database import get_db
from mock_client import send_dm, get_dm_status


MAX_RETRIES = 5

RATE_LIMIT = 10
RATE_WINDOW_SECONDS = 60


def process_event(event):

    event_type = event["event_type"]

    if event_type != "comment.created":
        mark_event_processed(event["event_id"])
        return

    data = event["payload"]
    comment = data.get("data", {})

    comment_text = comment.get("text", "")
    comment_id = comment.get("comment_id")

    user_data = comment.get("from", {})
    user_id = user_data.get("user_id")

    if not comment_text or not comment_id or not user_id:
        mark_event_processed(event["event_id"])
        return

    comment_text_lower = comment_text.lower()

    with get_db() as db:

        rules = db.execute("""
            SELECT id, keyword, dm_message
            FROM rules
        """).fetchall()

        for rule in rules:

            if rule["keyword"].lower() not in comment_text_lower:
                continue

            try:

                db.execute("""
                    INSERT INTO dm_jobs (
                        rule_id,
                        user_id,
                        comment_id,
                        message,
                        status
                    )
                    VALUES (?, ?, ?, ?, 'queued')
                """, (
                    rule["id"],
                    user_id,
                    comment_id,
                    rule["dm_message"]
                ))

                print(
                    f"DM job created: "
                    f"user={user_id}, "
                    f"rule={rule['id']}"
                )

            except sqlite3.IntegrityError:

                db.execute("""
                    UPDATE stats
                    SET duplicates_blocked =
                        duplicates_blocked + 1
                    WHERE id = 1
                """)

                print(
                    f"Duplicate blocked: "
                    f"user={user_id}, "
                    f"rule={rule['id']}"
                )

    mark_event_processed(event["event_id"])


def mark_event_processed(event_id):

    with get_db() as db:

        db.execute("""
            UPDATE events
            SET processed = 1
            WHERE event_id = ?
        """, (event_id,))


def get_pending_events():

    with get_db() as db:

        rows = db.execute("""
            SELECT event_id, event_type, payload
            FROM events
            WHERE processed = 0
            ORDER BY received_at
        """).fetchall()

    events = []

    for row in rows:

        events.append({
            "event_id": row["event_id"],
            "event_type": row["event_type"],
            "payload": json.loads(row["payload"])
        })

    return events


def send_pending_jobs():

    with get_db() as db:

        jobs = db.execute("""
            SELECT *
            FROM dm_jobs
            WHERE status = 'queued'
            AND (
                next_attempt_at IS NULL
                OR next_attempt_at <= datetime('now')
            )
            ORDER BY created_at
        """).fetchall()

    for job in jobs:
        send_job(job)


def wait_for_rate_limit():

    while True:

        with get_db() as db:

            row = db.execute("""
                SELECT COUNT(*) AS request_count
                FROM dm_send_log
                WHERE requested_at >= datetime(
                    'now',
                    '-60 seconds'
                )
            """).fetchone()

            request_count = row["request_count"]

            if request_count < RATE_LIMIT:
                return

            oldest = db.execute("""
                SELECT requested_at
                FROM dm_send_log
                WHERE requested_at >= datetime(
                    'now',
                    '-60 seconds'
                )
                ORDER BY requested_at ASC
                LIMIT 1
            """).fetchone()

        if not oldest:
            return

        # SQLite timestamps have second precision.
        # Add a small safety margin.
        try:

            with get_db() as db:

                now = db.execute("""
                    SELECT strftime(
                        '%s',
                        'now'
                    )
                """).fetchone()[0]

                oldest_timestamp = db.execute("""
                    SELECT strftime(
                        '%s',
                        ?
                    )
                """, (oldest["requested_at"],)).fetchone()[0]

            wait_seconds = (
                int(oldest_timestamp)
                + RATE_WINDOW_SECONDS
                - int(now)
                + 1
            )

        except Exception:
            wait_seconds = 2

        wait_seconds = max(wait_seconds, 1)

        print(
            f"DM rate limit reached. "
            f"Waiting {wait_seconds}s..."
        )

        time.sleep(wait_seconds)


def record_dm_request(job_id):

    with get_db() as db:

        db.execute("""
            INSERT INTO dm_send_log (
                job_id
            )
            VALUES (?)
        """, (job_id,))


def send_job(job):

    job_id = job["id"]

    # Claim job.
    with get_db() as db:

        result = db.execute("""
            UPDATE dm_jobs
            SET status = 'sending'
            WHERE id = ?
            AND status = 'queued'
        """, (job_id,))

        if result.rowcount == 0:
            return

    # Wait until our rolling window allows another request.
    wait_for_rate_limit()

    # Record the request BEFORE sending it.
    # The API rate limit counts the request even if
    # the response is 500 or 429.
    record_dm_request(job_id)

    idempotency_key = f"dm-job-{job_id}"

    try:

        response = send_dm(
            user_id=job["user_id"],
            message=job["message"],
            comment_id=job["comment_id"],
            idempotency_key=idempotency_key
        )

    except Exception as error:

        print(
            f"Network error for job {job_id}: {error}"
        )

        schedule_retry(job_id)
        return

    print(
        f"DM API response for job {job_id}: "
        f"{response.status_code}"
    )

    # Rate limited
    if response.status_code == 429:

        retry_after = response.headers.get(
            "Retry-After",
            "5"
        )

        try:
            retry_after = int(retry_after)
        except ValueError:
            retry_after = 5

        schedule_retry(
            job_id,
            delay=retry_after
        )

        return

    # Temporary server error
    if response.status_code >= 500:

        schedule_retry(job_id)
        return

    # Invalid request
    if response.status_code == 400:

        mark_failed(job_id)

        print(
            f"Permanent failure for job {job_id}: "
            f"invalid request"
        )

        return

    # Successful acceptance
    if response.status_code in (200, 202):

        try:

            result = response.json()
            dm_id = result["dm_id"]

        except Exception:

            schedule_retry(job_id)
            return

        with get_db() as db:

            db.execute("""
                UPDATE dm_jobs
                SET
                    status = 'waiting',
                    dm_id = ?,
                    attempts = attempts + 1,
                    next_attempt_at = NULL
                WHERE id = ?
            """, (
                dm_id,
                job_id
            ))

        print(
            f"DM accepted: "
            f"job={job_id}, "
            f"dm_id={dm_id}"
        )

        threading.Thread(
            target=check_delivery,
            args=(job_id, dm_id),
            daemon=True
        ).start()

        return

    schedule_retry(job_id)


def check_delivery(job_id, dm_id):

    max_checks = 10

    for _ in range(max_checks):

        time.sleep(2)

        try:

            response = get_dm_status(dm_id)

        except Exception as error:

            print(
                f"Delivery check error "
                f"for {dm_id}: {error}"
            )

            continue

        if response.status_code != 200:
            continue

        try:

            result = response.json()
            status = result.get("status")

        except Exception:
            continue

        print(
            f"Delivery status: "
            f"dm={dm_id}, "
            f"status={status}"
        )

        if status == "delivered":

            mark_delivered(
                job_id,
                dm_id
            )

            return

        if status == "failed":

            handle_delivery_failure(job_id)

            return

        if status == "queued":
            continue

    # If it never reaches a terminal state,
    # retry the operation.
    schedule_retry(job_id)


def handle_delivery_failure(job_id):

    with get_db() as db:

        job = db.execute("""
            SELECT attempts
            FROM dm_jobs
            WHERE id = ?
        """, (job_id,)).fetchone()

    if not job:
        return

    if job["attempts"] >= MAX_RETRIES:

        mark_failed(job_id)

        print(
            f"DM permanently failed: "
            f"job={job_id}"
        )

    else:

        schedule_retry(job_id)


def schedule_retry(job_id, delay=5):

    with get_db() as db:

        job = db.execute("""
            SELECT attempts
            FROM dm_jobs
            WHERE id = ?
        """, (job_id,)).fetchone()

        if not job:
            return

        attempts = job["attempts"] + 1

        if attempts >= MAX_RETRIES:

            db.execute("""
                UPDATE dm_jobs
                SET
                    status = 'failed',
                    attempts = ?,
                    next_attempt_at = NULL
                WHERE id = ?
            """, (
                attempts,
                job_id
            ))

            print(
                f"Job permanently failed: "
                f"{job_id}"
            )

            return

        db.execute("""
            UPDATE dm_jobs
            SET
                status = 'queued',
                attempts = ?,
                next_attempt_at = datetime(
                    'now',
                    ? || ' seconds'
                )
            WHERE id = ?
        """, (
            attempts,
            delay,
            job_id
        ))

    print(
        f"Retry scheduled: "
        f"job={job_id}, "
        f"attempt={attempts}, "
        f"delay={delay}s"
    )


def mark_delivered(job_id, dm_id):

    with get_db() as db:

        db.execute("""
            UPDATE dm_jobs
            SET
                status = 'delivered',
                dm_id = ?,
                next_attempt_at = NULL
            WHERE id = ?
        """, (
            dm_id,
            job_id
        ))

    print(
        f"DM delivered successfully: "
        f"job={job_id}"
    )


def mark_failed(job_id):

    with get_db() as db:

        db.execute("""
            UPDATE dm_jobs
            SET
                status = 'failed',
                next_attempt_at = NULL
            WHERE id = ?
        """, (job_id,))


def worker_loop():

    print("Background worker started.")

    while True:

        try:

            events = get_pending_events()

            for event in events:

                try:
                    process_event(event)

                except Exception as error:

                    print(
                        f"Error processing event "
                        f"{event['event_id']}: "
                        f"{error}"
                    )

            send_pending_jobs()

        except Exception as error:

            print(
                f"Worker error: {error}"
            )

        time.sleep(0.5)


if __name__ == "__main__":
    worker_loop()