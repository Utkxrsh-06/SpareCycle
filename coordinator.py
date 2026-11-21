import argparse, socket
from coordinator.server import serve
def main():
 p=argparse.ArgumentParser(description='SpareCycle LAN coordinator'); p.add_argument('--host',default='0.0.0.0'); p.add_argument('--port',type=int,default=8765); args=p.parse_args()
 print('Coordinator IPs:', ', '.join(socket.gethostbyname_ex(socket.gethostname())[2])); print(f'Dashboard: http://localhost:{args.port}'); serve(args.host,args.port)
if __name__=='__main__': main()
