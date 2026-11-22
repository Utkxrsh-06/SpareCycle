from dataclasses import dataclass
import os

@dataclass
class Limits:
    max_cpu_percent: float = float(os.getenv("SPARECYCLE_MAX_CPU", "70"))
    max_ram_percent: float = float(os.getenv("SPARECYCLE_MAX_RAM", "70"))
    cpu_safety_margin: float = float(os.getenv("SPARECYCLE_CPU_MARGIN", "10"))
    heartbeat_seconds: float = float(os.getenv("SPARECYCLE_HEARTBEAT", "2"))
    worker_timeout_seconds: float = float(os.getenv("SPARECYCLE_WORKER_TIMEOUT", "10"))
    task_timeout_seconds: float = float(os.getenv("SPARECYCLE_TASK_TIMEOUT", "120"))
    max_retries: int = int(os.getenv("SPARECYCLE_MAX_RETRIES", "2"))
    shutdown_grace_seconds: float = float(os.getenv("SPARECYCLE_SHUTDOWN_GRACE", "5"))

def secret() -> str:
    return os.getenv("SPARECYCLE_SECRET", "change-me-for-a-real-lan")
