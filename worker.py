import argparse

from worker.client import WorkerClient


def main():
    parser = argparse.ArgumentParser(description="SpareCycle worker agent")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    WorkerClient(args.host, args.port).run()


if __name__ == "__main__":
    main()
