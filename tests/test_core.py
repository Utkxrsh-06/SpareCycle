import io
import base64
import os
import time
import tempfile
from pathlib import Path
import threading

from PIL import Image

from coordinator.scheduler import allocations
from jobs.csv_processing import process_csv_chunk, combine_csv_results, split_csv
from jobs.image_processing import process_image
from worker.hardware_monitor import metrics
from worker.process_manager import ProcessManager
from worker.client import WorkerClient
from shared.config import Limits
from shared.machine_identity import (
    get_machine_id,
    group_workers_by_machine,
    valid_machine_id,
)
from shared.config import secret
from coordinator.server import Coordinator


def test_metrics_are_real_shape():
    hardware = metrics()
    assert hardware["logical_cores"] >= 1
    assert hardware["total_ram"] > 0
    assert 0 <= hardware["cpu_percent"] <= 100


def test_csv_partition_and_combine():
    data = b"a,b\n1,x\n2,\n3,y\n"
    parts = [
        process_csv_chunk(part)
        for part in split_csv(data, 2)
    ]
    result = combine_csv_results(parts)
    assert result["rows"] == 3
    assert result["columns"]["a"]["sum"] == 6
    assert result["columns"]["a"]["mean"] == 2
    assert result["missing"]["b"] == 1


def test_image_resize():
    source = io.BytesIO()
    Image.new("RGB", (40, 30), "red").save(source, format="PNG")
    data, image_format = process_image(
        source.getvalue(),
        "resize",
        {"width": 10, "height": 8, "format": "PNG"},
    )
    assert image_format == "png"
    assert Image.open(io.BytesIO(data)).size == (10, 8)


def test_weighted_prefers_idle_machine():
    workers = [
        {
            "id": "busy",
            "connected": True,
            "active_tasks": 0,
            "metrics": {
                "cpu_percent": 45,
                "ram_percent": 60,
                "available_ram": 3000,
                "total_ram": 10000,
                "logical_cores": 4,
            },
        },
        {
            "id": "idle",
            "connected": True,
            "active_tasks": 0,
            "metrics": {
                "cpu_percent": 5,
                "ram_percent": 10,
                "available_ram": 8000,
                "total_ram": 10000,
                "logical_cores": 8,
            },
        },
    ]
    allocations_by_worker = allocations(workers, "weighted")
    assert allocations_by_worker["idle"] > allocations_by_worker["busy"]
    assert round(sum(allocations_by_worker.values()), 6) == 1

def test_allowlisted_job_runs_in_child_process():
    payload = {
        "job_type": "benchmark",
        "data": base64.b64encode(b"").decode(),
        "params": {"start": 1, "end": 1000},
    }
    result = ProcessManager(timeout=5).run(payload)
    assert result["ok"] is True
    assert result["result"]["value"] >= 0
    assert result["process"]["pid"] > 0
    assert result["process"]["exit_code"] == 0
    assert result["process"]["elapsed_seconds"] >= 0

def test_job_process_timeout_terminates_child():
    payload = {
        "job_type": "benchmark",
        "data": base64.b64encode(b"").decode(),
        "params": {"start": 1, "end": 20_000_000},
    }
    started = time.monotonic()
    result = ProcessManager(timeout=0.05).run(payload)
    elapsed = time.monotonic() - started
    assert result["ok"] is False
    assert result["process"]["timed_out"] is True
    assert elapsed < 3

def test_invalid_allowlisted_job_parameter_returns_error():
    payload = {
        "job_type": "benchmark",
        "data": base64.b64encode(b"").decode(),
        "params": {"start": "invalid", "end": 10},
    }
    try:
        ProcessManager(timeout=5).run(payload)
    except ValueError as error:
        assert "bounded range" in str(error)
    else:
        raise AssertionError("invalid benchmark parameters were accepted")

def test_process_manager_validates_job_payload():
    payload = {
        "job_type": "benchmark",
        "data": base64.b64encode(b"").decode(),
        "params": {"start": True, "end": 10},
    }
    try:
        ProcessManager(timeout=5).run(payload)
    except ValueError as error:
        assert "bounded range" in str(error)
    else:
        raise AssertionError("invalid benchmark parameters were accepted")

def test_native_backend_is_platform_specific_or_falls_back_cleanly():
    manager = ProcessManager(timeout=5)
    if os.name == "nt":
        expected = "sparecycle-process-manager.exe"
    else:
        expected = "sparecycle-process-manager"
    if manager.native is not None:
        assert manager.native.name == expected


def test_machine_id_is_generated_and_reused():
    previous_override = os.environ.pop("SPARECYCLE_MACHINE_ID", None)
    try:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "SpareCycle" / "machine_id"
            first = get_machine_id(path)
            second = get_machine_id(path)
            assert valid_machine_id(first)
            assert first == second
    finally:
        if previous_override is not None:
            os.environ["SPARECYCLE_MACHINE_ID"] = previous_override


def test_machine_id_environment_override():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "machine_id"
        old_override = os.environ.get("SPARECYCLE_MACHINE_ID")
        try:
            os.environ["SPARECYCLE_MACHINE_ID"] = "12345678-1234-4234-8234-123456789abc"
            assert get_machine_id(path) == "12345678-1234-4234-8234-123456789abc"
        finally:
            if old_override is None:
                os.environ.pop("SPARECYCLE_MACHINE_ID", None)
            else:
                os.environ["SPARECYCLE_MACHINE_ID"] = old_override


def test_different_storage_locations_generate_different_ids():
    previous_override = os.environ.pop("SPARECYCLE_MACHINE_ID", None)
    try:
        with tempfile.TemporaryDirectory() as directory:
            first_path = Path(directory) / "first" / "machine_id"
            second_path = Path(directory) / "second" / "machine_id"
            first = get_machine_id(first_path)
            second = get_machine_id(second_path)
            assert first != second
    finally:
        if previous_override is not None:
            os.environ["SPARECYCLE_MACHINE_ID"] = previous_override


def test_malformed_machine_id_file_is_replaced():
    previous_override = os.environ.pop("SPARECYCLE_MACHINE_ID", None)
    try:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "machine_id"
            path.write_text("not-a-uuid", encoding="utf-8")
            generated = get_machine_id(path)
            assert valid_machine_id(generated)
            assert get_machine_id(path) == generated
    finally:
        if previous_override is not None:
            os.environ["SPARECYCLE_MACHINE_ID"] = previous_override


def test_machine_grouping_and_legacy_workers():
    machine_a = "12345678-1234-4234-8234-123456789abc"
    machine_b = "87654321-4321-4432-8432-cba987654321"
    workers = [
        {"id": "worker-a1", "machine_id": machine_a, "hostname": "PC-A"},
        {"id": "worker-a2", "machine_id": machine_a, "hostname": "PC-A"},
        {"id": "worker-b1", "machine_id": machine_b, "hostname": "PC-B"},
        {"id": "legacy-a", "hostname": "PC-A"},
        {"id": "legacy-b", "hostname": "PC-A"},
    ]

    groups = group_workers_by_machine(workers)
    assert len(groups) == 4
    machine_a_group = next(
        group for group in groups
        if group["machine_id"] == machine_a
    )
    assert [worker["id"] for worker in machine_a_group["workers"]] == [
        "worker-a1",
        "worker-a2",
    ]
    assert sum(bool(group["legacy"]) for group in groups) == 2


def test_coordinator_stores_worker_machine_identity():
    coordinator = Coordinator.__new__(Coordinator)
    import threading
    coordinator.workers = {}
    coordinator.lock = threading.RLock()

    machine_id = "12345678-1234-4234-8234-123456789abc"
    worker_id = coordinator.register(
        {
            "secret": secret(),
            "worker_id": "worker-process-1",
            "machine_id": machine_id,
            "hostname": "LAB-PC",
            "name": "LAB-PC",
            "metrics": {"cpu_percent": 10, "ram_percent": 20},
        },
        object(),
    )
    coordinator.heartbeat(
        worker_id,
        {
            "machine_id": machine_id,
            "hostname": "LAB-PC",
            "metrics": {"cpu_percent": 15, "ram_percent": 22},
        },
    )

    worker = coordinator.worker_list()[0]
    assert worker["id"] == "worker-process-1"
    assert worker["machine_id"] == machine_id
    assert worker["hostname"] == "LAB-PC"


def make_state_coordinator():
    coordinator = Coordinator.__new__(Coordinator)
    coordinator.workers = {}
    coordinator.jobs = {}
    coordinator.lock = threading.RLock()
    coordinator.limits = Limits()
    coordinator.db = type(
        "MemoryDatabase",
        (),
        {"save_job": lambda self, job: None},
    )()
    coordinator.round_robin = 0
    coordinator.accepting_jobs = True
    coordinator.running = True
    coordinator.shutdown_requested = False
    return coordinator


def test_all_completed_tasks_complete_job():
    coordinator = make_state_coordinator()
    job = {
        "id": "job-complete",
        "name": "complete",
        "type": "csv",
        "status": "running",
        "started": time.time() - 2,
        "finished": None,
        "result": None,
        "tasks": {
            "task-1": {
                "id": "task-1",
                "status": "completed",
                "worker": "worker-1",
                "result": {
                    "rows": 1,
                    "columns": {},
                    "missing": {},
                },
            },
            "task-2": {
                "id": "task-2",
                "status": "completed",
                "worker": "worker-1",
                "result": {
                    "rows": 1,
                    "columns": {},
                    "missing": {},
                },
            },
        },
    }
    coordinator.jobs[job["id"]] = job
    coordinator._recalculate_job(job)
    assert job["status"] == "completed"
    assert job["finished"] is not None


def test_task_failure_marks_failed_job_without_active_tasks():
    coordinator = make_state_coordinator()
    job = {
        "id": "job-failed",
        "name": "failed",
        "type": "csv",
        "status": "running",
        "started": time.time() - 1,
        "finished": None,
        "result": None,
        "tasks": {
            "task-1": {
                "id": "task-1",
                "status": "failed",
                "worker": "worker-1",
                "result": None,
            },
        },
    }
    coordinator.jobs[job["id"]] = job
    coordinator._recalculate_job(job)
    assert job["status"] == "failed"
    assert job["finished"] is not None


def test_duplicate_completed_result_is_ignored():
    coordinator = make_state_coordinator()
    coordinator.workers["worker-1"] = {
        "id": "worker-1",
        "connected": True,
        "active_tasks": 1,
        "socket": None,
        "metrics": {},
    }
    job = {
        "id": "job-duplicate",
        "name": "duplicate",
        "type": "csv",
        "status": "running",
        "started": time.time(),
        "finished": None,
        "result": None,
        "tasks": {
            "task-1": {
                "id": "task-1",
                "status": "assigned",
                "worker": "worker-1",
                "result": None,
            },
        },
    }
    coordinator.jobs[job["id"]] = job
    payload = {
        "job_id": "job-duplicate",
        "task_id": "task-1",
        "result": {"rows": 1, "columns": {}, "missing": {}},
    }
    coordinator.result("worker-1", payload)
    first_finished = job["finished"]
    coordinator.result("worker-1", payload)
    assert job["status"] == "completed"
    assert job["finished"] == first_finished
    assert coordinator.workers["worker-1"]["active_tasks"] == 0


def test_coordinator_restart_marks_running_tasks_interrupted():
    coordinator = make_state_coordinator()
    job = {
        "id": "job-restart",
        "name": "restart",
        "type": "csv",
        "status": "running",
        "started": time.time(),
        "finished": None,
        "result": None,
        "tasks": {
            "task-1": {
                "id": "task-1",
                "status": "completed",
                "result": {"rows": 1, "columns": {}, "missing": {}},
            },
            "task-2": {
                "id": "task-2",
                "status": "assigned",
                "worker": "worker-1",
                "result": None,
            },
        },
    }
    coordinator.jobs[job["id"]] = job
    coordinator._reconcile_after_restart()
    assert job["tasks"]["task-1"]["status"] == "completed"
    assert job["tasks"]["task-2"]["status"] == "interrupted"
    assert job["status"] == "interrupted"


def test_shutdown_marks_workers_offline_and_jobs_interrupted():
    coordinator = make_state_coordinator()
    coordinator.limits.shutdown_grace_seconds = 0

    class Socket:
        def __init__(self):
            self.closed = False

        def sendall(self, data):
            return None

        def shutdown(self, how):
            return None

        def close(self):
            self.closed = True

    worker_socket = Socket()
    coordinator.workers["worker-1"] = {
        "id": "worker-1",
        "connected": True,
        "active_tasks": 1,
        "socket": worker_socket,
        "metrics": {},
    }
    job = {
        "id": "job-shutdown",
        "name": "shutdown",
        "type": "csv",
        "status": "running",
        "started": time.time(),
        "finished": None,
        "result": None,
        "tasks": {
            "task-1": {
                "id": "task-1",
                "status": "assigned",
                "worker": "worker-1",
                "result": None,
            },
        },
    }
    coordinator.jobs[job["id"]] = job
    coordinator.shutdown()

    assert coordinator.accepting_jobs is False
    assert coordinator.workers["worker-1"]["connected"] is False
    assert job["status"] == "interrupted"
    assert job["tasks"]["task-1"]["status"] == "interrupted"


def test_worker_explicit_shutdown_stops_reconnect_loop():
    worker = WorkerClient.__new__(WorkerClient)
    worker.running = True
    worker.shutdown_requested = False
    worker.execution_lock = threading.Lock()
    worker.active_managers = []
    worker.sock = None
    worker.stop()
    worker._handle({"type": "WORKER_SHUTDOWN"})
    assert worker.running is False
    assert worker.shutdown_requested is True


def test_active_job_process_can_be_cancelled():
    payload = {
        "job_type": "benchmark",
        "data": base64.b64encode(b"").decode(),
        "params": {"start": 1, "end": 20_000_000},
    }
    manager = ProcessManager(timeout=30)
    thread = threading.Thread(
        target=lambda: manager.run(payload),
        daemon=True,
    )
    thread.start()
    time.sleep(0.05)
    manager.cancel()
    thread.join(timeout=3)
    assert not thread.is_alive()

