import csv
import io
import math


def process_csv_chunk(data: bytes, numeric_columns=None):
    """Return associative statistics for a CSV fragment including its header."""
    rows = csv.DictReader(io.StringIO(data.decode("utf-8-sig")))
    result = {"rows": 0, "columns": {}, "missing": {}}
    for row in rows:
        result["rows"] += 1
        for key, value in row.items():
            if value is None or not str(value).strip():
                result["missing"][key] = result["missing"].get(key, 0) + 1
                continue
            if numeric_columns is not None and key not in numeric_columns:
                continue
            try: number = float(value)
            except (ValueError, TypeError):
                continue
            stat = result["columns"].setdefault(
                key,
                {"sum": 0.0, "count": 0, "min": number, "max": number},
            )
            stat["sum"] += number
            stat["count"] += 1
            stat["min"] = min(stat["min"], number)
            stat["max"] = max(stat["max"], number)
    return result


def combine_csv_results(parts):
    merged = {"rows": 0, "columns": {}, "missing": {}}
    for part in parts:
        merged["rows"] += part["rows"]
        for name, count in part.get("missing", {}).items():
            merged["missing"][name] = merged["missing"].get(name, 0) + count
        for name, stat in part.get("columns", {}).items():
            target = merged["columns"].setdefault(
                name,
                {
                    "sum": 0.0,
                    "count": 0,
                    "min": stat["min"],
                    "max": stat["max"],
                },
            )
            target["sum"] += stat["sum"]
            target["count"] += stat["count"]
            target["min"] = min(target["min"], stat["min"])
            target["max"] = max(target["max"], stat["max"])
    for stat in merged["columns"].values():
        stat["mean"] = stat["sum"] / stat["count"] if stat["count"] else None
    return merged


def split_csv(data: bytes, chunks: int):
    lines = data.splitlines(keepends=True)
    if len(lines) < 2:
        raise ValueError("CSV needs a header and at least one row")
    header, rows = lines[0], lines[1:]
    width = max(1, math.ceil(len(rows) / max(1, chunks)))
    return [
        header + b"".join(rows[i:i + width])
        for i in range(0, len(rows), width)
    ]
