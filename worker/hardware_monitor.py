import platform, socket, time, psutil
def metrics():
 mem=psutil.virtual_memory(); freq=psutil.cpu_freq()
 try: load=list(psutil.getloadavg())
 except (AttributeError, OSError, RuntimeError): load=[]
 try: ip=socket.gethostbyname(socket.gethostname())
 except socket.gaierror: ip='127.0.0.1'
 return {"hostname":socket.gethostname(),"os":platform.platform(),"ip":ip,"cpu_percent":psutil.cpu_percent(interval=None),"physical_cores":psutil.cpu_count(logical=False) or 0,"logical_cores":psutil.cpu_count() or 1,"cpu_mhz":freq.current if freq else None,"load":load,"total_ram":mem.total,"used_ram":mem.used,"available_ram":mem.available,"ram_percent":mem.percent,"timestamp":time.time()}
