import base64
import socket
import threading
import time
import uuid

from shared.config import Limits, secret
from shared.machine_identity import get_machine_id
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
        self.machine_id = get_machine_id()
        self.hostname = metrics()["hostname"]
        self.shutdown_requested = False
        self.active_managers = []
        self.execution_lock = threading.Lock()

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
                        machine_id=self.machine_id,
                        hostname=self.hostname,
                        name=self.hostname,
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
                if self.shutdown_requested:
                    break
                try:
                    self.sock.close()
                except Exception:
                    pass
                time.sleep(delay)
                delay = min(delay * 2, 15)
        self.stop()

    def _heartbeats(self):
        while self.running and self.sock and not self.shutdown_requested:
            try:
                send(
                    self.sock,
                    message(
                        "HEARTBEAT",
                        machine_id=self.machine_id,
                        hostname=self.hostname,
                        metrics=metrics(),
                    ),
                )
            except OSError:
                return
            time.sleep(self.limits.heartbeat_seconds)

    def stop(self):
        self.shutdown_requested = True
        self.running = False
        with self.execution_lock:
            managers = list(self.active_managers)
        for manager in managers:
            try:
                manager.cancel()
            except Exception:
                pass
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.sock.close()
            except OSError:
                pass

    def _handle(self, payload):
        message_type = payload.get("type")
        if message_type == "WORKER_SHUTDOWN":
            self.shutdown_requested = True
            self.running = False
            self.stop()
            return
        if message_type != "JOB_ASSIGN":
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
            manager = ProcessManager(self.limits.task_timeout_seconds)
            with self.execution_lock:
                if not self.running or self.shutdown_requested:
                    return
                self.active_managers.append(manager)
            try:
                execution = manager.run(process_payload)
            finally:
                with self.execution_lock:
                    if manager in self.active_managers:
                        self.active_managers.remove(manager)
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
