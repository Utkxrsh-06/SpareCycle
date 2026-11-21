import base64
import socket
import threading
import time
import uuid

from shared.config import Limits, secret
from shared.protocol import checksum, message, receive, send
from worker.hardware_monitor import metrics
from worker.process_manager import ProcessManager


class WorkerClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.id = str(uuid.uuid4())
        self.sock = None
        self.running = True
        self.limits = Limits()
        self.started = time.time()

    def run(self):
        delay = 1
        while self.running:
            try:
                self.sock = socket.create_connection(
                    (self.host, self.port),
                    timeout=8,
                )
                self.sock.settimeout(None)
                send(
                    self.sock,
                    message(
                        "REGISTER",
                        worker_id=self.id,
                        name=metrics()["hostname"],
                        metrics=metrics(),
                        uptime=time.time() - self.started,
                        secret=secret(),
                    ),
                )
                ack = receive(self.sock)
                if ack.get("type") != "REGISTER_ACK":
                    raise ConnectionError("registration rejected")

                self.limits = Limits(**ack.get("limits", {}))
                threading.Thread(
                    target=self._heartbeats,
                    daemon=True,
                ).start()
                delay = 1
                while self.running:
                    self._handle(receive(self.sock))
            except Exception:
                try:
                    self.sock.close()
                except Exception:
                    pass
                time.sleep(delay)
                delay = min(delay * 2, 15)

    def _heartbeats(self):
        while self.running and self.sock:
            try:
                send(self.sock, message("HEARTBEAT", metrics=metrics()))
            except OSError:
                return
            time.sleep(self.limits.heartbeat_seconds)

    def _handle(self, payload):
        if payload.get("type") != "JOB_ASSIGN":
            return
        threading.Thread(
            target=self._execute,
            args=(payload,),
            daemon=True,
        ).start()

    def _execute(self, payload):
        execution = None
        try:
            current = metrics()
            if (
                current["cpu_percent"]
                >= self.limits.max_cpu_percent
                - self.limits.cpu_safety_margin
                or current["ram_percent"] >= self.limits.max_ram_percent
            ):
                raise RuntimeError("resource safety ceiling reached")

            data = base64.b64decode(payload["data"])
            if checksum(data) != payload["checksum"]:
                raise ValueError("input checksum mismatch")

            process_payload = {
                "job_type": payload["job_type"],
                "data": payload["data"],
                "params": payload.get("params", {}),
                "job_id": payload["task_id"],
            }
            execution = ProcessManager(
                self.limits.task_timeout_seconds
            ).run(process_payload)
            if not execution.get("ok"):
                raise RuntimeError(
                    execution.get("error", "job process failed")
                )

            send(
                self.sock,
                message(
                    "JOB_RESULT",
                    job_id=payload["job_id"],
                    task_id=payload["task_id"],
                    result=execution["result"],
                    process=execution.get("process"),
                ),
            )
        except Exception as error:
            try:
                send(
                    self.sock,
                    message(
                        "JOB_FAILED",
                        job_id=payload.get("job_id"),
                        task_id=payload.get("task_id"),
                        error=str(error),
                        process=(
                            execution.get("process")
                            if execution
                            else None
                        ),
                    ),
                )
            except OSError:
                pass
