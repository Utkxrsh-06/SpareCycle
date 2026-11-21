import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import psutil


class ProcessManager:
    def __init__(self, timeout):
        self.timeout = timeout

    def run(self, payload):
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
