import argparse
import atexit
import signal

from worker.client import WorkerClient


def main():
    parser = argparse.ArgumentParser(description="SpareCycle worker agent")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    worker = WorkerClient(args.host, args.port)

    def stop_worker(_signum=None, _frame=None):
        worker.request_shutdown()

    for signal_name in ("SIGINT", "SIGTERM", "SIGHUP", "SIGBREAK"):
        if hasattr(signal, signal_name):
            signal.signal(getattr(signal, signal_name), stop_worker)
    atexit.register(worker.stop)

    try:
        worker.run()
    except (KeyboardInterrupt, SystemExit):
        worker.request_shutdown()


if __name__ == "__main__":
    main()
