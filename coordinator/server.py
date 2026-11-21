import base64
import json
import socket
import socketserver
import threading
import time
import uuid
from pathlib import Path

from coordinator.database import Database
from coordinator.scheduler import choose
from jobs.csv_processing import combine_csv_results, split_csv
from shared.config import Limits, secret
from shared.protocol import checksum, message, receive, send


class Coordinator:
    def __init__(self):
        self.workers = {}
        self.jobs = {}
        self.lock = threading.RLock()
        self.limits = Limits()
        self.db = Database()
        self.round_robin = 0

        for job in self.db.jobs():
            self.jobs[job["id"]] = job

    def worker_list(self):
        with self.lock:
            return [dict(worker, socket=None) for worker in self.workers.values()]

    def register(self, payload, client_socket):
        if payload.get("secret") != secret():
            raise PermissionError("authentication failed")

        worker_id = payload.get("worker_id") or str(uuid.uuid4())
        now = time.time()

        with self.lock:
            self.workers[worker_id] = {
                "id": worker_id,
                "name": payload.get("name", worker_id),
                "metrics": payload.get("metrics", {}),
                "connected": True,
                "last_heartbeat": now,
                "socket": client_socket,
                "active_tasks": 0,
                "uptime": payload.get("uptime", 0),
            }

        return worker_id

    def heartbeat(self, worker_id, payload):
        with self.lock:
            if worker_id in self.workers:
                self.workers[worker_id].update(
                    metrics=payload.get("metrics", {}),
                    last_heartbeat=time.time(),
                    connected=True,
                )

    def create_csv(self, name, data, strategy):
        with self.lock:
            job = {
                "id": str(uuid.uuid4()),
                "name": name,
                "type": "csv",
                "status": "queued",
                "created": time.time(),
                "strategy": strategy,
                "tasks": {},
                "result": None,
                "started": None,
                "finished": None,
            }

            chunks = split_csv(data, max(1, len(self.workers) * 2))
            job["total"] = len(chunks)

            for chunk in chunks:
                task_id = str(uuid.uuid4())
                job["tasks"][task_id] = {
                    "id": task_id,
                    "status": "queued",
                    "data": base64.b64encode(chunk).decode(),
                    "attempts": 0,
                    "worker": None,
                    "result": None,
                }

            self.jobs[job["id"]] = job
            self.db.save_job(job)

        self.dispatch(job["id"])
        return job

    def dispatch(self, job_id):
        with self.lock:
            job = self.jobs[job_id]

            if job["status"] == "cancelled":
                return

            job["status"] = "running"
            job["started"] = job["started"] or time.time()

            for task in job["tasks"].values():
                if task["status"] != "queued":
                    continue

                worker = choose(
                    list(self.workers.values()),
                    job["strategy"],
                    self.round_robin,
                    self.limits,
                )

                if not worker:
                    break

                self.round_robin += 1

                task.update(
                    status="assigned",
                    worker=worker["id"],
                    attempts=task["attempts"] + 1,
                    assigned=time.time(),
                )
                worker["active_tasks"] += 1

                try:
                    raw_data = base64.b64decode(task["data"])

                    send(
                        worker["socket"],
                        message(
                            "JOB_ASSIGN",
                            job_id=job_id,
                            task_id=task["id"],
                            job_type="csv",
                            data=task["data"],
                            checksum=checksum(raw_data),
                            params={},
                        ),
                    )
                except OSError:
                    worker["connected"] = False
                    task["status"] = "queued"
                    worker["active_tasks"] -= 1

            self.db.save_job(job)

    def result(self, worker_id, payload):
        with self.lock:
            job = self.jobs.get(payload.get("job_id"))
            task = job and job["tasks"].get(payload.get("task_id"))

            if (
                not task
                or task["status"] == "completed"
                or task.get("worker") != worker_id
            ):
                return

            task.update(
                status="completed",
                result=payload["result"],
                process=payload.get("process"),
            )

            if worker_id in self.workers:
                self.workers[worker_id]["active_tasks"] = max(
                    0,
                    self.workers[worker_id]["active_tasks"] - 1,
                )

            if all(task["status"] == "completed" for task in job["tasks"].values()):
                job["result"] = combine_csv_results(
                    [task["result"] for task in job["tasks"].values()]
                )
                job["status"] = "completed"
                job["finished"] = time.time()

            self.db.save_job(job)

        self.dispatch(job["id"])

    def failed(self, worker_id, payload):
        with self.lock:
            job = self.jobs.get(payload.get("job_id"))
            task = job and job["tasks"].get(payload.get("task_id"))

            if task and task["status"] not in ("completed", "cancelled"):
                worker = self.workers.get(worker_id, {})
                worker["active_tasks"] = max(0, worker.get("active_tasks", 1) - 1)
                task["process"] = payload.get("process")

                if task["attempts"] < self.limits.max_retries:
                    task["status"] = "queued"
                else:
                    task["status"] = "failed"

            if job:
                self.db.save_job(job)

        if job:
            self.dispatch(payload["job_id"])

    def sweep(self):
        while True:
            time.sleep(2)
            now = time.time()

            with self.lock:
                for worker in self.workers.values():
                    if (
                        worker["connected"]
                        and now - worker["last_heartbeat"]
                        > self.limits.worker_timeout_seconds
                    ):
                        worker["connected"] = False

                        for job in self.jobs.values():
                            for task in job["tasks"].values():
                                if (
                                    task.get("worker") == worker["id"]
                                    and task["status"] in ("assigned", "running")
                                ):
                                    task["status"] = "queued"
                                    worker["active_tasks"] = max(
                                        0,
                                        worker["active_tasks"] - 1,
                                    )

                job_ids = list(self.jobs)

            for job_id in job_ids:
                self.dispatch(job_id)


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        if self.request.recv(4, socket.MSG_PEEK).startswith((b"GET ", b"POST")):
            return self.http()

        worker_id = None

        try:
            while True:
                payload = receive(self.request)
                message_type = payload.get("type")

                if message_type == "REGISTER":
                    worker_id = self.server.app.register(payload, self.request)

                    send(
                        self.request,
                        message(
                            "REGISTER_ACK",
                            worker_id=worker_id,
                            limits=self.server.app.limits.__dict__,
                        ),
                    )

                elif message_type in ("HEARTBEAT", "RESOURCE_UPDATE"):
                    self.server.app.heartbeat(worker_id, payload)

                elif message_type == "JOB_RESULT":
                    self.server.app.result(worker_id, payload)

                elif message_type == "JOB_FAILED":
                    self.server.app.failed(worker_id, payload)

        except Exception:
            if worker_id and worker_id in self.server.app.workers:
                self.server.app.workers[worker_id]["connected"] = False

    def http(self):
        raw = self.request.recv(65536)
        headers, _, request_body = raw.partition(b"\r\n\r\n")

        request_line = headers.decode(errors="ignore").split("\r\n", 1)[0].split()
        method = request_line[0] if request_line else "GET"
        path = request_line[1] if len(request_line) > 1 else "/"

        content_length = next(
            (
                int(line.split(":", 1)[1])
                for line in headers.decode(errors="ignore").split("\r\n")
                if line.lower().startswith("content-length:")
            ),
            0,
        )

        while len(request_body) < content_length:
            request_body += self.request.recv(content_length - len(request_body))

        app = self.server.app

        if method == "POST" and path == "/api/jobs/csv":
            try:
                request_data = json.loads(request_body)
                job = app.create_csv(
                    request_data.get("name", "dataset.csv"),
                    base64.b64decode(request_data["data"]),
                    request_data.get("strategy", "weighted"),
                )
                body = json.dumps({"id": job["id"]}).encode()
                status = "201 Created"

            except Exception as error:
                body = json.dumps({"error": str(error)}).encode()
                status = "400 Bad Request"

            content_type = "application/json"

        elif path == "/api/state":
            body = json.dumps(
                {
                    "workers": app.worker_list(),
                    "jobs": list(app.jobs.values()),
                    "limits": app.limits.__dict__,
                },
                default=str,
            ).encode()

            content_type = "application/json"
            status = "200 OK"

        else:
            body = Path("dashboard/templates/index.html").read_bytes()
            content_type = "text/html; charset=utf-8"
            status = "200 OK"

        response_headers = (
            f"HTTP/1.1 {status}\r\n"
            f"Content-Type: {content_type}\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode()

        self.request.sendall(response_headers + body)


class ReusableThreadingTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def serve(host, port):
    app = Coordinator()

    threading.Thread(
        target=app.sweep,
        daemon=True,
    ).start()

    server = ReusableThreadingTCPServer((host, port), Handler)
    server.app = app
    server.serve_forever()