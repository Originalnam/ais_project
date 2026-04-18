import csv
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
READ_DIR   = str(_PROJECT_ROOT / "data" / "raw")
WRITE_DIR  = str(_PROJECT_ROOT / "data" / "raw" / "unzipped")
STATUS_CSV = _PROJECT_ROOT / "data" / "pipeline_status.csv"

print(f"Reading zip files from: {READ_DIR}")
print(f"Extracting zip files to: {WRITE_DIR}")


# ── CSV update ────────────────────────────────────────────────────────────────

def _update_status(date_str: str, **fields):
    """Set column values for one date row in pipeline_status.csv."""
    if not STATUS_CSV.exists():
        return
    with open(STATUS_CSV, newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)
    for row in rows:
        if row["date"] == date_str:
            row.update({k: str(v) for k, v in fields.items()})
            break
    with open(STATUS_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _dir_size(path: str) -> int:
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for fname in filenames:
            try:
                total += os.path.getsize(os.path.join(dirpath, fname))
            except OSError:
                pass
    return total


# ── extraction ────────────────────────────────────────────────────────────────

def extract_zip(file_name):
    date_str = file_name.replace("aisdk-", "").replace(".zip", "")
    if date_str in _done:
        print(f"Skipping (unzipped=True in status): {file_name}")
        return

    zip_path   = os.path.join(READ_DIR, file_name)
    target_dir = os.path.join(WRITE_DIR, file_name.replace(".zip", ""))

    print(f"Starting extraction of {file_name} to {target_dir}")
    os.makedirs(target_dir, exist_ok=True)

    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(target_dir)
        print(f"Successfully extracted {file_name}")

        # Update pipeline_status.csv
        date_str = file_name.replace("aisdk-", "").replace(".zip", "")
        size = _dir_size(target_dir)
        _update_status(date_str, unzipped=True, unzipped_size=size)

    except zipfile.BadZipFile:
        print(f"Error: {file_name} is not a valid zip file")
    except Exception as e:
        print(f"Error extracting {file_name}: {e}")


# ── read status to know what's already done ───────────────────────────────────

_done: set[str] = set()
if STATUS_CSV.exists():
    import csv as _csv_mod
    with open(STATUS_CSV, newline="") as _f:
        for _row in _csv_mod.DictReader(_f):
            if _row.get("unzipped") == "True":
                _done.add(_row["date"])

zip_files = [f for f in os.listdir(READ_DIR) if f.endswith(".zip")]
print(f"Found {len(zip_files)} zip files: {zip_files}")

with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(extract_zip, zip_files)

print("All extractions completed.")
