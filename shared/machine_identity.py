import os
import uuid
from pathlib import Path


def machine_id_path():
    if os.name == "nt":
        base = os.getenv("LOCALAPPDATA")
        if not base:
            base = str(Path.home() / "AppData" / "Local")
        return Path(base) / "SpareCycle" / "machine_id"

    return Path.home() / ".config" / "sparecycle" / "machine_id"


def valid_machine_id(value):
    if not isinstance(value, str):
        return False
    try:
        return str(uuid.UUID(value.strip())) == value.strip().lower()
    except (AttributeError, TypeError, ValueError):
        return False


def get_machine_id(path=None):
    override = os.getenv("SPARECYCLE_MACHINE_ID")
    if override:
        if valid_machine_id(override):
            return override.strip().lower()

    identity_path = Path(path) if path is not None else machine_id_path()
    identity_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        value = identity_path.read_text(encoding="utf-8").strip()
        if valid_machine_id(value):
            return value.lower()
    except (OSError, UnicodeDecodeError):
        pass

    generated = str(uuid.uuid4())
    try:
        descriptor = os.open(
            identity_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as identity_file:
            identity_file.write(generated)
    except FileExistsError:
        try:
            value = identity_path.read_text(encoding="utf-8").strip()
            if valid_machine_id(value):
                return value.lower()
        except (OSError, UnicodeDecodeError):
            pass
        try:
            identity_path.write_text(generated, encoding="utf-8")
        except OSError:
            return generated
    except OSError:
        try:
            identity_path.parent.mkdir(parents=True, exist_ok=True)
            with identity_path.open("x", encoding="utf-8") as identity_file:
                identity_file.write(generated)
        except FileExistsError:
            try:
                value = identity_path.read_text(encoding="utf-8").strip()
                if valid_machine_id(value):
                    return value.lower()
            except (OSError, UnicodeDecodeError):
                pass
        except OSError:
            return generated
    return generated


def group_workers_by_machine(workers):
    groups = {}
    for worker in workers:
        machine_id = worker.get("machine_id")
        if isinstance(machine_id, str) and valid_machine_id(machine_id):
            key = ("machine", machine_id.lower())
        else:
            key = ("legacy", worker.get("id") or str(id(worker)))

        if key not in groups:
            groups[key] = {
                "machine_id": machine_id.lower() if key[0] == "machine" else None,
                "legacy": key[0] == "legacy",
                "workers": [],
            }
        groups[key]["workers"].append(worker)
    return list(groups.values())
