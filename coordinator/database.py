import json
import sqlite3
import threading


class Database:
    def __init__(self, path="data/sparecycle.db"):
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS jobs("
            "id TEXT PRIMARY KEY, name TEXT, type TEXT, "
            "status TEXT, created REAL, data TEXT)"
        )
        self.conn.commit()

    def save_job(self, job):
        with self.lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO jobs VALUES(?,?,?,?,?,?)",
                (
                    job["id"],
                    job["name"],
                    job["type"],
                    job["status"],
                    job["created"],
                    json.dumps(job),
                ),
            )
            self.conn.commit()

    def jobs(self):
        with self.lock:
            rows = self.conn.execute(
                "SELECT data FROM jobs ORDER BY created DESC"
            )
            return [json.loads(row[0]) for row in rows]
