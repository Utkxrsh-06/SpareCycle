import time

from shared.config import Limits


def worker_eligibility(worker, limits=Limits(), now=None):
    now = time.time() if now is None else now
    metrics = worker.get("metrics") or {}

    if not worker.get("connected"):
        return False, "worker is disconnected"
    if not metrics:
        return False, "worker has no metrics"

    timestamp = metrics.get("timestamp")
    if not isinstance(timestamp, (int, float)):
        return False, "metrics timestamp is missing"
    metric_age = now - timestamp
    metric_age_limit = getattr(limits, "worker_timeout_seconds", 10)
    if metric_age < -60 or metric_age > metric_age_limit:
        return False, "metrics are stale ({:.1f}s old)".format(metric_age)

    try:
        cpu = float(metrics["cpu_percent"])
        ram = float(metrics["ram_percent"])
    except (KeyError, TypeError, ValueError):
        return False, "metrics contain invalid CPU or RAM values"

    cpu_limit = limits.max_cpu_percent - limits.cpu_safety_margin
    if not 0 <= cpu <= 100:
        return False, "CPU metric is outside 0-100%"
    if not 0 <= ram <= 100:
        return False, "RAM metric is outside 0-100%"
    if cpu >= cpu_limit:
        return False, "CPU {:.1f}% is at/above effective limit {:.1f}%".format(cpu, cpu_limit)
    if ram >= limits.max_ram_percent:
        return False, "RAM {:.1f}% is at/above limit {:.1f}%".format(ram, limits.max_ram_percent)
    return True, "eligible: CPU {:.1f}%/{:.1f}%, RAM {:.1f}%/{:.1f}%".format(
        cpu, cpu_limit, ram, limits.max_ram_percent
    )


def eligible(workers, limits=Limits(), now=None):
    return [
        worker
        for worker in workers
        if worker_eligibility(worker, limits, now)[0]
    ]

def score(worker, limits=Limits(), max_cores=None):
    m=worker["metrics"]; cpu=max(0, limits.max_cpu_percent-limits.cpu_safety_margin-m["cpu_percent"])/(limits.max_cpu_percent-limits.cpu_safety_margin)
    ram=max(0, m["available_ram"]-m["total_ram"]*(1-limits.max_ram_percent/100))/max(m["total_ram"],1)
    cores=m.get("logical_cores",1)/max(1,max_cores or m.get("logical_cores",1))
    return max(0, .5*cpu + .4*ram + .1*cores) * (1/(1+worker.get("active_tasks",0)))

def allocations(workers, strategy, limits=Limits()):
    active=eligible(workers, limits)
    if not active: return {}
    if strategy == "round_robin": return {w["id"]: 1/len(active) for w in active}
    if strategy == "least_loaded":
        maximum=max(w['metrics'].get('logical_cores',1) for w in active); best=max(active, key=lambda w: score(w,limits,maximum)); return {w["id"]: 1.0 if w is best else 0.0 for w in active}
    maximum=max(w['metrics'].get('logical_cores',1) for w in active); raw={w["id"]:score(w,limits,maximum) for w in active}; total=sum(raw.values())
    return {key:(value/total if total else 1/len(active)) for key,value in raw.items()}

def choose(workers, strategy, counter=0, limits=Limits()):
    active=eligible(workers,limits)
    if not active: return None
    if strategy == "round_robin": return active[counter % len(active)]
    maximum=max(w['metrics'].get('logical_cores',1) for w in active)
    return max(active, key=lambda w: score(w,limits,maximum))
