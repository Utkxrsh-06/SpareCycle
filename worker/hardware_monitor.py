import platform
import socket
import time

import psutil


def metrics():
    memory = psutil.virtual_memory()
    frequency = psutil.cpu_freq()
    hostname = socket.gethostname()
    try:
        load = list(psutil.getloadavg())
    except (AttributeError, OSError, RuntimeError):
        load = []
    try:
        ip_address = socket.gethostbyname(hostname)
    except socket.gaierror:
        ip_address = "127.0.0.1"
    return {
        "hostname": hostname,
        "os": platform.platform(),
        "ip": ip_address,
        "cpu_percent": psutil.cpu_percent(interval=None),
        "physical_cores": psutil.cpu_count(logical=False) or 0,
        "logical_cores": psutil.cpu_count() or 1,
        "cpu_mhz": frequency.current if frequency else None,
        "load": load,
        "total_ram": memory.total,
        "used_ram": memory.used,
        "available_ram": memory.available,
        "ram_percent": memory.percent,
        "timestamp": time.time(),
    }
