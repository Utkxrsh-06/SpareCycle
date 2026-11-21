# SpareCycle

SpareCycle is a LAN-only distributed processing pool. The coordinator hosts the dashboard and accepts authenticated workers over one TCP port; workers report real `psutil` metrics and only run allowlisted CSV, image, and benchmark tasks.

## Setup and launch

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
$env:SPARECYCLE_SECRET='replace-with-the-same-secret-on-every-laptop'
python coordinator.py --host 0.0.0.0 --port 8765
```

Open `http://localhost:8765`; use the displayed LAN IP from another laptop. Allow inbound TCP 8765 through the coordinator firewall. On each worker (same environment and secret):

```powershell
python worker.py --host 192.168.1.10 --port 8765
```

For Linux replace activation with `source .venv/bin/activate`. The dashboard shows genuine telemetry. Upload a CSV, select Weighted, Least Loaded, or Round Robin, and submit. CSV chunks are transferred with SHA-256 checksums; results are idempotently merged by the coordinator.

## Architecture and safety

`coordinator/` owns scheduling, SQLite job persistence, failure requeueing and the shared-port HTTP/TCP server. `worker/` handles reconnection, heartbeats, limits and execution. `jobs/` contains deterministic operations. No remote shell commands or arbitrary scripts are accepted.

Job execution is isolated in an OS child process for each assigned task. The worker launches only the fixed allowlisted job handler, applies the configured task timeout, terminates timed-out process trees, and records exit code, signal, elapsed time, CPU time, peak resident memory, and PID in the worker result. These process details are persisted with the corresponding task in SQLite.

The scheduler refuses overloaded/offline workers and uses 70% CPU/RAM safety ceilings by default. Workers lower process priority; portable hard CPU/RAM quotas are not available in pure Python (use Windows Job Objects or Linux cgroups for stricter enforcement). This is for a trusted LAN: the shared secret authenticates peers but traffic is not encrypted. Image and benchmark functions are implemented in the worker allowlist; CSV is the dashboard submission workflow.

## Testing

```powershell
pytest -q
```

For development you may run several worker processes on one machine; this tests networking but is not a substitute for physical LAN machines.
