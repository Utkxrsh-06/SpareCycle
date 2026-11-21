import base64, socket, threading, time, uuid
from shared.protocol import send, receive, message, checksum
from shared.config import Limits, secret
from worker.hardware_monitor import metrics
from worker.process_manager import ProcessManager

class WorkerClient:
 def __init__(self,host,port): self.host=host; self.port=port; self.id=str(uuid.uuid4()); self.sock=None; self.running=True; self.limits=Limits(); self.started=time.time()
 def run(self):
  delay=1
  while self.running:
   try:
    self.sock=socket.create_connection((self.host,self.port),timeout=8); self.sock.settimeout(None)
    send(self.sock,message('REGISTER',worker_id=self.id,name=metrics()['hostname'],metrics=metrics(),uptime=time.time()-self.started,secret=secret()))
    ack=receive(self.sock)
    if ack.get('type')!='REGISTER_ACK': raise ConnectionError('registration rejected')
    self.limits=Limits(**ack.get('limits',{})); threading.Thread(target=self._heartbeats,daemon=True).start(); delay=1
    while self.running: self._handle(receive(self.sock))
   except Exception:
    try: self.sock.close()
    except Exception: pass
    time.sleep(delay); delay=min(delay*2,15)
 def _heartbeats(self):
  while self.running and self.sock:
   try: send(self.sock,message('HEARTBEAT',metrics=metrics()))
   except OSError: return
   time.sleep(self.limits.heartbeat_seconds)
 def _handle(self,p):
  if p.get('type')!='JOB_ASSIGN': return
  threading.Thread(target=self._execute,args=(p,),daemon=True).start()
 def _execute(self,p):
  execution=None
  try:
   current=metrics()
   if current['cpu_percent'] >= self.limits.max_cpu_percent-self.limits.cpu_safety_margin or current['ram_percent'] >= self.limits.max_ram_percent: raise RuntimeError('resource safety ceiling reached')
   data=base64.b64decode(p['data'])
   if checksum(data)!=p['checksum']: raise ValueError('input checksum mismatch')
   process_payload={
    'job_type':p['job_type'],
    'data':p['data'],
    'params':p.get('params',{}),
   }
   execution=ProcessManager(self.limits.task_timeout_seconds).run(process_payload)
   if not execution.get('ok'):
    raise RuntimeError(execution.get('error','job process failed'))
   result=execution['result']
   send(self.sock,message('JOB_RESULT',job_id=p['job_id'],task_id=p['task_id'],result=result,process=execution.get('process')))
  except Exception as e:
   try: send(self.sock,message('JOB_FAILED',job_id=p.get('job_id'),task_id=p.get('task_id'),error=str(e),process=execution.get('process') if execution else None))
   except OSError: pass
