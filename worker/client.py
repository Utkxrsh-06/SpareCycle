import base64, socket, threading, time, uuid
from shared.protocol import send, receive, message, checksum
from shared.config import Limits, secret
from worker.hardware_monitor import metrics
from worker.resource_limiter import lower_priority
from jobs.csv_processing import process_csv_chunk
from jobs.image_processing import process_image
from jobs.benchmark import benchmark

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
  try:
   current=metrics()
   if current['cpu_percent'] >= self.limits.max_cpu_percent-self.limits.cpu_safety_margin or current['ram_percent'] >= self.limits.max_ram_percent: raise RuntimeError('resource safety ceiling reached')
   data=base64.b64decode(p['data'])
   if checksum(data)!=p['checksum']: raise ValueError('input checksum mismatch')
   lower_priority()
   if p['job_type']=='csv': result=process_csv_chunk(data,p.get('params',{}).get('numeric_columns'))
   elif p['job_type']=='image':
    out,fmt=process_image(data,p['params']['operation'],p['params']); result={'data':base64.b64encode(out).decode(),'format':fmt,'checksum':checksum(out)}
   elif p['job_type']=='benchmark': result={'value':benchmark(**p['params'])}
   else: raise ValueError('job type is not allowlisted')
   send(self.sock,message('JOB_RESULT',job_id=p['job_id'],task_id=p['task_id'],result=result))
  except Exception as e:
   try: send(self.sock,message('JOB_FAILED',job_id=p.get('job_id'),task_id=p.get('task_id'),error=str(e)))
   except OSError: pass
