import io
import base64
import os
import time

from PIL import Image

from coordinator.scheduler import allocations
from jobs.csv_processing import process_csv_chunk, combine_csv_results, split_csv
from jobs.image_processing import process_image
from worker.hardware_monitor import metrics
from worker.process_manager import ProcessManager


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

