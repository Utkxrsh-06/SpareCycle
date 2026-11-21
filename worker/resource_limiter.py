"""Best-effort limits. Windows priority is supported; hard CPU quotas need OS policy."""
import os
def lower_priority():
 try:
  import psutil
  p=psutil.Process()
  p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS if os.name=='nt' else 10)
 except Exception: pass
