import base64
import socket
import threading
import time
import uuid
import random

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
        self.shutdown_event = threading.Event()
        self.active_managers = []
        self.execution_lock = threading.Lock()
        self.cleanup_lock = threading.Lock()
        self.cleanup_complete = False
        self.heartbeat_thread = None
        self.execution_threads = set()
        self.write_lock = threading.Lock()

    def _send(self, payload):
        if not self.sock:
            raise ConnectionError("worker socket is not connected")
        with self.write_lock:
            send(self.sock, payload)

    def run(self):
        delay = 1
        try:
            while self.running and not self.shutdown_event.is_set():
                try:
                    self.sock = socket.create_connection(
                        (self.host, self.port),
                        timeout=1,
                    )
                    self.sock.settimeout(0.5)
                    self._send(
                        message(
                            "REGISTER",
                            worker_id=self.id,
                            machine_id=self.machine_id,
                            hostname=self.hostname,
                            name=self.hostname,
                            metrics=metrics(),
                            uptime=time.time() - self.started,
                            secret=secret(),
                        )
                    )
                    ack = receive(self.sock)
                    if ack.get("type") != "REGISTER_ACK":
                        raise ConnectionError("registration rejected")

                    self.limits = Limits(**ack.get("limits", {}))
                    self.heartbeat_thread = threading.Thread(
                        target=self._heartbeats,
                        daemon=True,
                        name="sparecycle-heartbeat",
                    )
                    self.heartbeat_thread.start()
                    delay = 1

                    while self.running and not self.shutdown_event.is_set():
                        try:
                            payload = receive(self.sock)
                        except socket.timeout:
                            continue
                        except (ConnectionError, OSError):
                            break
                        self._handle(payload)
                except socket.timeout:
                    if self.shutdown_event.is_set():
                        break
                except (ConnectionError, OSError):
                    if self.shutdown_event.is_set():
                        break
                except Exception:
                    if self.shutdown_event.is_set():
                        break

                if self.shutdown_event.is_set():
                    break
                try:
                    self.sock.close()
                except Exception:
                    pass
                if self.shutdown_event.wait(delay):
                    break
                delay = min(delay * 2, 15) + random.uniform(0, 0.25)
        finally:
            self.stop()

    def _heartbeats(self):
        while self.running and self.sock and not self.shutdown_event.is_set():
            try:
                self._send(
                    message(
                        "HEARTBEAT",
                        machine_id=self.machine_id,
                        hostname=self.hostname,
                        metrics=metrics(),
                    )
                )
            except OSError:
                return
            if self.shutdown_event.wait(self.limits.heartbeat_seconds):
                return

    def stop(self):
        with self.cleanup_lock:
            if self.cleanup_complete:
                return
            self.shutdown_requested = True
            self.running = False
            self.shutdown_event.set()

            with self.execution_lock:
                managers = list(self.active_managers)
            for manager in managers:
                try:
                    manager.cancel()
                except Exception:
                    pass

            if self.sock:
                try:
                    self.sock.settimeout(0.2)
                    self.sock.shutdown(socket.SHUT_RDWR)
                except (OSError, socket.error):
                    pass
                try:
                    self.sock.close()
                except OSError:
                    pass
                self.sock = None

            heartbeat_thread = self.heartbeat_thread
            if (
                heartbeat_thread
                and heartbeat_thread is not threading.current_thread()
                and heartbeat_thread.is_alive()
            ):
                heartbeat_thread.join(timeout=1)

            with self.execution_lock:
                execution_threads = [
                    thread
                    for thread in self.execution_threads
                    if thread is not threading.current_thread()
                ]
            for execution_thread in execution_threads:
                if execution_thread.is_alive():
                    execution_thread.join(timeout=2)

            self.cleanup_complete = True

    def request_shutdown(self):
        self.shutdown_requested = True
        self.running = False
        self.shutdown_event.set()

    def _handle(self, payload):
        message_type = payload.get("type")
        if message_type == "WORKER_SHUTDOWN":
            self.request_shutdown()
            return
        if message_type != "JOB_ASSIGN":
            return
        execution_thread = threading.Thread(
            target=self._execute,
            args=(payload,),
            daemon=True,
            name="sparecycle-job",
        )
        with self.execution_lock:
            self.execution_threads.add(execution_thread)
        execution_thread.start()

    def _execute(self, payload):
        execution = None
        current_thread = threading.current_thread()
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

            self._send(
                message(
                    "JOB_RESULT",
                    job_id=payload["job_id"],
                    task_id=payload["task_id"],
                    result=execution["result"],
                    process=execution.get("process"),
                )
            )
        except Exception as error:
            try:
                self._send(
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
                    )
                )
            except OSError:
                pass
        finally:
            with self.execution_lock:
                self.execution_threads.discard(current_thread)
