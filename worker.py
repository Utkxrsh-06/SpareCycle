import argparse
from worker.client import WorkerClient
def main():
 p=argparse.ArgumentParser(description='SpareCycle worker agent'); p.add_argument('--host',required=True); p.add_argument('--port',type=int,default=8765); args=p.parse_args(); WorkerClient(args.host,args.port).run()
if __name__=='__main__': main()
