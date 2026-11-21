import base64, json, socket, socketserver, threading, time, uuid
from pathlib import Path
from shared.protocol import receive, send, message, checksum
from shared.config import Limits, secret
from coordinator.scheduler import allocations, choose
from coordinator.database import Database
from jobs.csv_processing import split_csv, combine_csv_results

class Coordinator:
 def __init__(self):
  self.workers={}; self.jobs={}; self.lock=threading.RLock(); self.limits=Limits(); self.db=Database(); self.round_robin=0
  for job in self.db.jobs(): self.jobs[job['id']]=job
 def worker_list(self):
  with self.lock: return [dict(w, socket=None) for w in self.workers.values()]
 def register(self, payload, sock):
  if payload.get('secret') != secret(): raise PermissionError('authentication failed')
  wid=payload.get('worker_id') or str(uuid.uuid4()); now=time.time()
  with self.lock:
   self.workers[wid]={"id":wid,"name":payload.get('name',wid),"metrics":payload.get('metrics',{}),"connected":True,"last_heartbeat":now,"socket":sock,"active_tasks":0,"uptime":payload.get('uptime',0)}
  return wid
 def heartbeat(self,wid,payload):
  with self.lock:
   if wid in self.workers: self.workers[wid].update(metrics=payload.get('metrics',{}),last_heartbeat=time.time(),connected=True)
 def create_csv(self,name,data,strategy):
  with self.lock:
   job={"id":str(uuid.uuid4()),"name":name,"type":"csv","status":"queued","created":time.time(),"strategy":strategy,"tasks":{},"result":None,"started":None,"finished":None}
   chunks=split_csv(data,max(1,len(self.workers)*2)); job['total']=len(chunks)
   for chunk in chunks:
    tid=str(uuid.uuid4()); job['tasks'][tid]={"id":tid,"status":"queued","data":base64.b64encode(chunk).decode(),"attempts":0,"worker":None,"result":None}
   self.jobs[job['id']]=job; self.db.save_job(job)
  self.dispatch(job['id']); return job
 def dispatch(self,jid):
  with self.lock:
   job=self.jobs[jid]
   if job['status'] == 'cancelled': return
   job['status']='running'; job['started']=job['started'] or time.time()
   for task in job['tasks'].values():
    if task['status']!='queued': continue
    worker=choose(list(self.workers.values()),job['strategy'],self.round_robin,self.limits)
    if not worker: break
    self.round_robin+=1; task.update(status='assigned',worker=worker['id'],attempts=task['attempts']+1,assigned=time.time()); worker['active_tasks']+=1
    try: send(worker['socket'],message('JOB_ASSIGN',job_id=jid,task_id=task['id'],job_type='csv',data=task['data'],checksum=checksum(base64.b64decode(task['data'])),params={}))
    except OSError: worker['connected']=False; task['status']='queued'; worker['active_tasks']-=1
   self.db.save_job(job)
 def result(self,wid,p):
  with self.lock:
   job=self.jobs.get(p.get('job_id')); task=job and job['tasks'].get(p.get('task_id'))
   if not task or task['status']=='completed' or task.get('worker')!=wid: return
   task.update(status='completed',result=p['result']); self.workers[wid]['active_tasks']=max(0,self.workers[wid]['active_tasks']-1)
   if all(t['status']=='completed' for t in job['tasks'].values()):
    job['result']=combine_csv_results([t['result'] for t in job['tasks'].values()]); job['status']='completed'; job['finished']=time.time()
   self.db.save_job(job)
  self.dispatch(job['id'])
 def failed(self,wid,p):
  with self.lock:
   job=self.jobs.get(p.get('job_id')); task=job and job['tasks'].get(p.get('task_id'))
   if task and task['status'] not in ('completed','cancelled'):
    self.workers.get(wid,{}).update(active_tasks=max(0,self.workers.get(wid,{}).get('active_tasks',1)-1)); task['status']='queued' if task['attempts']<self.limits.max_retries else 'failed'
   self.db.save_job(job)
  self.dispatch(p['job_id'])
 def sweep(self):
  while True:
   time.sleep(2); now=time.time()
   with self.lock:
    for w in self.workers.values():
     if w['connected'] and now-w['last_heartbeat']>self.limits.worker_timeout_seconds:
      w['connected']=False
      for job in self.jobs.values():
       for task in job['tasks'].values():
        if task.get('worker')==w['id'] and task['status'] in ('assigned','running'): task['status']='queued'; w['active_tasks']=max(0,w['active_tasks']-1)
    ids=list(self.jobs)
   for jid in ids: self.dispatch(jid)

class Handler(socketserver.BaseRequestHandler):
 def handle(self):
  if self.request.recv(4,socket.MSG_PEEK).startswith((b'GET ',b'POST')): return self.http()
  wid=None
  try:
   while True:
    p=receive(self.request); typ=p.get('type')
    if typ=='REGISTER': wid=self.server.app.register(p,self.request); send(self.request,message('REGISTER_ACK',worker_id=wid,limits=self.server.app.limits.__dict__))
    elif typ in ('HEARTBEAT','RESOURCE_UPDATE'): self.server.app.heartbeat(wid,p)
    elif typ=='JOB_RESULT': self.server.app.result(wid,p)
    elif typ=='JOB_FAILED': self.server.app.failed(wid,p)
  except Exception:
   if wid and wid in self.server.app.workers: self.server.app.workers[wid]['connected']=False
 def http(self):
  raw=self.request.recv(65536); head,_,bodyraw=raw.partition(b'\r\n\r\n'); first=head.decode(errors='ignore').split('\r\n',1)[0].split(); method=first[0] if first else 'GET'; path=first[1] if len(first)>1 else '/'
  length=next((int(x.split(':',1)[1]) for x in head.decode(errors='ignore').split('\r\n') if x.lower().startswith('content-length:')),0)
  while len(bodyraw)<length: bodyraw+=self.request.recv(length-len(bodyraw))
  app=self.server.app
  if method=='POST' and path=='/api/jobs/csv':
   try:
    req=json.loads(bodyraw); job=app.create_csv(req.get('name','dataset.csv'),base64.b64decode(req['data']),req.get('strategy','weighted')); body=json.dumps({'id':job['id']}).encode(); status='201 Created'
   except Exception as exc: body=json.dumps({'error':str(exc)}).encode(); status='400 Bad Request'
   ctype='application/json'
  elif path=='/api/state': body=json.dumps({"workers":app.worker_list(),"jobs":list(app.jobs.values()),"limits":app.limits.__dict__},default=str).encode(); ctype='application/json'; status='200 OK'
  else: body=Path('dashboard/templates/index.html').read_bytes(); ctype='text/html; charset=utf-8'; status='200 OK'
  self.request.sendall(f'HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n'.encode()+body)

def serve(host,port):
 app=Coordinator(); threading.Thread(target=app.sweep,daemon=True).start(); server=socketserver.ThreadingTCPServer((host,port),Handler); server.daemon_threads=True; server.allow_reuse_address=True; server.app=app; server.serve_forever()
