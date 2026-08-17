import sqlite3
from contextlib import contextmanager

DATABASE = "linkplease.db"


@contextmanager
def get_db():
    conn = sqlite3.connect(DATABASE, timeout=30)
    conn.row_factory = sqlite3.Row

    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_db() as db:

        # Rules
        db.execute("""
            CREATE TABLE IF NOT EXISTS rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT NOT NULL,
                dm_message TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Webhook events
        db.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                processed INTEGER NOT NULL DEFAULT 0,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # DM jobs
        db.execute("""
            CREATE TABLE IF NOT EXISTS dm_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id INTEGER NOT NULL,
                user_id TEXT NOT NULL,
                comment_id TEXT NOT NULL,
                message TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                attempts INTEGER NOT NULL DEFAULT 0,
                dm_id TEXT,
                next_attempt_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (rule_id) REFERENCES rules(id),

                UNIQUE(rule_id, user_id)
            )
        """)

        # Delivery records
        db.execute("""
            CREATE TABLE IF NOT EXISTS sent_dms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id INTEGER NOT NULL,
                user_id TEXT NOT NULL,
                comment_id TEXT NOT NULL,
                dm_id TEXT UNIQUE,
                status TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (rule_id) REFERENCES rules(id),

                UNIQUE(rule_id, user_id)
            )
        """)

        # Statistics
        db.execute("""
            CREATE TABLE IF NOT EXISTS stats (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                duplicates_blocked INTEGER NOT NULL DEFAULT 0
            )
        """)

        db.execute("""
            INSERT OR IGNORE INTO stats (
                id,
                duplicates_blocked
            )
            VALUES (1, 0)
        """)

        # Every outgoing DM API request is recorded here.
        # This allows us to enforce the rolling 60-second limit
        # even after an application restart.
        db.execute("""
            CREATE TABLE IF NOT EXISTS dm_send_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Indexes
        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_processed
            ON events(processed)
        """)

        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_dm_jobs_status
            ON dm_jobs(status)
        """)

        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_dm_jobs_next_attempt
            ON dm_jobs(next_attempt_at)
        """)

        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_dm_send_log_requested_at
            ON dm_send_log(requested_at)
        """)


if __name__ == "__main__":
    init_db()
    print("Database initialized successfully.")