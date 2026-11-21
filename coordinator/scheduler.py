from shared.config import Limits

def eligible(workers, limits=Limits()):
    return [
        worker
        for worker in workers
        if worker["connected"]
        and worker.get("metrics")
        and worker["metrics"].get("cpu_percent", 100)
        < limits.max_cpu_percent - limits.cpu_safety_margin
        and worker["metrics"].get("ram_percent", 100)
        < limits.max_ram_percent
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
