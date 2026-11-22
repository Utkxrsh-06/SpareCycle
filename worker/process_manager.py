import os
import signal
import subprocess
import sys
import time
import json
import uuid
from pathlib import Path

import psutil


class ProcessManager:
    def __init__(self, timeout):
        self.timeout = float(timeout)
        self.native = self._native_backend()
        self._process = None
        self._cancelled = False

    def run(self, payload):
        payload = self._validate_payload(payload)
        if self.native:
            return self._run_native(payload)
        return self._run_python(payload)

    def _native_backend(self):
        root = Path(__file__).resolve().parent.parent
        if os.name == "nt":
            candidate = root / "native" / "build" / "sparecycle-process-manager.exe"
        else:
            candidate = root / "native" / "build" / "sparecycle-process-manager"
        return candidate if candidate.is_file() else None

    def cancel(self):
        self._cancelled = True
        process = self._process
        if process is not None:
            self._terminate_process_tree(process)

    def _validate_payload(self, payload):
        allowed = {"csv", "image", "benchmark"}
        if not isinstance(payload, dict):
            raise ValueError("execution payload must be a JSON object")
        job_type = payload.get("job_type")
        if job_type not in allowed:
            raise ValueError("job type is not allowlisted")
        params = payload.get("params", {})
        if not isinstance(params, dict):
            raise ValueError("job parameters must be a JSON object")

        if job_type == "benchmark":
            start = params.get("start")
            end = params.get("end")
            if (
                not isinstance(start, int)
                or isinstance(start, bool)
                or not isinstance(end, int)
                or isinstance(end, bool)
                or start < 0
                or end < start
                or end - start > 100_000_000
            ):
                raise ValueError("benchmark requires 0 <= start <= end and a bounded range")
        elif job_type == "csv":
            numeric_columns = params.get("numeric_columns")
            if numeric_columns is not None:
                if not isinstance(numeric_columns, list) or not all(
                    isinstance(name, str) for name in numeric_columns
                ):
                    raise ValueError("numeric_columns must be a list of names")
        elif job_type == "image":
            operation = params.get("operation")
            if operation not in {"thumbnail", "resize", "convert"}:
                raise ValueError("unsupported image operation")
            for name in ("width", "height"):
                if name in params:
                    value = params[name]
                    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                        raise ValueError("{} must be a positive integer".format(name))
            if "format" in params and str(params["format"]).upper() not in {
                "PNG",
                "JPEG",
                "WEBP",
            }:
                raise ValueError("unsupported image format")

        data = payload.get("data")
        if not isinstance(data, str):
            raise ValueError("job data must be base64 text")
        if len(data) > 64 * 1024 * 1024:
            raise ValueError("job data exceeds the message limit")

        validated = dict(payload)
        validated["job_id"] = str(validated.get("job_id") or uuid.uuid4())
        return validated

    def _run_native(self, payload):
        root = Path(__file__).resolve().parent.parent
        script = Path(__file__).with_name("job_process.py")
        memory_mb = payload.get("memory_mb")
        cpu_seconds = payload.get("cpu_seconds")
        if memory_mb is not None:
            if not isinstance(memory_mb, int) or isinstance(memory_mb, bool) or memory_mb < 0:
                raise ValueError("memory_mb must be a non-negative integer")
        if cpu_seconds is not None:
            if not isinstance(cpu_seconds, int) or isinstance(cpu_seconds, bool) or cpu_seconds < 0:
                raise ValueError("cpu_seconds must be a non-negative integer")

        arguments = [
            str(self.native),
            "--python={}".format(sys.executable),
            "--handler={}".format(script),
            "--cwd={}".format(root),
            "--job-id={}".format(payload["job_id"]),
            "--timeout={}".format(self.timeout),
            "--memory-mb={}".format(int(memory_mb or 0)),
            "--cpu-seconds={}".format(int(cpu_seconds or 0)),
        ]
        try:
            self._process = psutil.Popen(
                arguments,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=str(root),
            )
            try:
                self._process.stdin.write(
                    json.dumps(payload, separators=(",", ":")).encode("utf-8")
                )
            except (BrokenPipeError, OSError):
                pass
            finally:
                self._process.stdin.close()
            stdout, stderr = self._process.communicate(
                timeout=max(5, self.timeout + 5)
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            if self._cancelled:
                return {
                    "ok": False,
                    "error": "job process cancelled during worker shutdown",
                    "process": self._unavailable_metadata(payload["job_id"]),
                }
            return {
                "ok": False,
                "error": "native process manager backend failed: {}".format(error),
                "process": self._unavailable_metadata(payload["job_id"]),
            }
        completed_stdout = stdout
        completed_stderr = stderr

        try:
            native = json.loads(completed_stdout.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            return {
                "ok": False,
                "error": "invalid native process-manager response: {}".format(error),
                "process": self._unavailable_metadata(payload["job_id"]),
            }

        process = {
            "pid": native.get("pid"),
            "exit_code": native.get("exit_code"),
            "signal": native.get("signal"),
            "timed_out": native.get("execution_status") == "timed_out",
            "elapsed_seconds": native.get("elapsed_seconds"),
            "cpu_seconds": native.get("cpu_seconds"),
            "peak_rss_bytes": native.get("peak_memory_bytes"),
            "peak_memory_bytes": native.get("peak_memory_bytes"),
            "termination_reason": native.get("termination_reason"),
            "resource_limit_status": native.get("resource_limit_status"),
            "platform_metadata": native.get("platform_metadata"),
            "job_id": native.get("job_id"),
        }

        child_stdout = native.get("child_stdout")
        child_stderr = native.get("child_stderr")
        if child_stdout:
            try:
                response = json.loads(child_stdout)
            except json.JSONDecodeError:
                response = None
        else:
            response = None

        if process["timed_out"]:
            return {"ok": False, "error": "job process timed out", "process": process}
        if response is not None:
            response["process"] = process
            return response

        error = child_stderr.strip() or completed_stderr.decode("utf-8", "replace").strip()
        if not error:
            error = "job process produced no structured response"
        return {"ok": False, "error": error, "process": process}

    def _unavailable_metadata(self, job_id):
        return {
            "job_id": job_id,
            "pid": None,
            "exit_code": None,
            "signal": None,
            "timed_out": False,
            "elapsed_seconds": None,
            "cpu_seconds": None,
            "peak_rss_bytes": None,
            "peak_memory_bytes": None,
            "termination_reason": "backend_error",
            "resource_limit_status": "unavailable",
            "platform_metadata": {},
        }

    def _run_python(self, payload):
        script = Path(__file__).with_name("job_process.py")
        stdin = (
            __import__("json")
            .dumps(payload, separators=(",", ":"))
            .encode("utf-8")
        )
        started = time.monotonic()
        popen_kwargs = {
            "stdin": subprocess.PIPE,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "cwd": str(script.resolve().parent.parent),
        }
        if os.name == "posix":
            popen_kwargs["start_new_session"] = True

        process = psutil.Popen(
            [sys.executable, "-u", str(script)],
            **popen_kwargs,
        )
        self._process = process
        peak_rss = 0
        timed_out = False
        exit_code = None
        signal_name = None

        try:
            try:
                process.stdin.write(stdin)
            except (BrokenPipeError, OSError):
                pass
            finally:
                process.stdin.close()

            while process.poll() is None:
                elapsed = time.monotonic() - started
                if elapsed >= self.timeout:
                    timed_out = True
                    self._terminate_process_tree(process)
                    break

                try:
                    peak_rss = max(
                        peak_rss,
                        process.memory_info().rss,
                    )
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
                time.sleep(0.02)

            try:
                peak_rss = max(peak_rss, process.memory_info().rss)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

            stdout, stderr = process.communicate(timeout=5)
            exit_code = process.returncode
        except subprocess.TimeoutExpired:
            timed_out = True
            self._terminate_process_tree(process)
            try:
                stdout, stderr = process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    process.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
                stdout, stderr = process.communicate(timeout=2)
            exit_code = process.returncode

        elapsed = time.monotonic() - started

        if timed_out:
            signal_name = "SIGTERM"
            if exit_code is not None:
                if os.name == "nt":
                    signal_name = "TERMINATED"
                elif exit_code < 0:
                    signal_name = signal.Signals(-exit_code).name

        try:
            cpu_times = process.cpu_times()
            cpu_seconds = cpu_times.user + cpu_times.system
        except (psutil.NoSuchProcess, psutil.AccessDenied, AttributeError):
            cpu_seconds = None

        metadata = {
            "pid": process.pid,
            "exit_code": exit_code,
            "signal": signal_name,
            "timed_out": timed_out,
            "elapsed_seconds": round(elapsed, 6),
            "cpu_seconds": (
                round(cpu_seconds, 6) if cpu_seconds is not None else None
            ),
            "peak_rss_bytes": peak_rss,
        }

        if timed_out:
            return {
                "ok": False,
                "error": "job process timed out",
                "process": metadata,
            }

        try:
            response = __import__("json").loads(
                stdout.decode("utf-8")
            )
        except Exception as error:
            try:
                error_output = stderr.decode("utf-8", errors="replace").strip()
            except AttributeError:
                error_output = str(stderr)
            return {
                "ok": False,
                "error": (
                    "invalid job process response: {}".format(error)
                    if not error_output
                    else error_output
                ),
                "process": metadata,
            }

        response["process"] = metadata
        return response

    def _terminate_process_tree(self, process):
        if os.name == "posix":
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass

        try:
            children = process.children(recursive=True)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            children = []

        for child in children:
            try:
                child.terminate()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        try:
            process.terminate()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

        try:
            process.wait(timeout=2)
        except (subprocess.TimeoutExpired, psutil.TimeoutExpired):
            for child in children:
                try:
                    child.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
            try:
                process.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
