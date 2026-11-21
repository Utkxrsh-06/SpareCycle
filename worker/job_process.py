import base64
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from jobs.benchmark import benchmark
from jobs.csv_processing import process_csv_chunk
from jobs.image_processing import process_image
from shared.protocol import checksum
from worker.resource_limiter import lower_priority


def main():
    try:
        request = json.load(sys.stdin)
        job_type = request.get("job_type")
        data = base64.b64decode(request.get("data", ""), validate=True)

        lower_priority()

        if job_type == "csv":
            params = request.get("params", {})
            result = process_csv_chunk(data, params.get("numeric_columns"))
        elif job_type == "image":
            params = request.get("params", {})
            operation = params["operation"]
            out, fmt = process_image(data, operation, params)
            result = {
                "data": base64.b64encode(out).decode(),
                "format": fmt,
                "checksum": checksum(out),
            }
        elif job_type == "benchmark":
            result = {"value": benchmark(**request.get("params", {}))}
        else:
            raise ValueError("job type is not allowlisted")

        json.dump({"ok": True, "result": result}, sys.stdout)
    except Exception as error:
        json.dump({"ok": False, "error": str(error)}, sys.stdout)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
