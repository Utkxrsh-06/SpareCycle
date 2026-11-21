"""Best-effort process priority adjustment."""

import os


def lower_priority():
    try:
        import psutil

        process = psutil.Process()
        priority = (
            psutil.BELOW_NORMAL_PRIORITY_CLASS
            if os.name == "nt"
            else 10
        )
        process.nice(priority)
    except Exception:
        pass
