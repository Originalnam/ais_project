import os
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "http://aisdata.ais.dk"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "raw")  # folder next to the script
MAX_WORKERS = 4  # adjust depending on bandwidth

os.makedirs(OUTPUT_DIR, exist_ok=True)


def generate_dates(year, month):
    """Generate all dates for a given month."""
    start = datetime(year, month, 1)
    if month == 12:
        end = datetime(year + 1, 1, 1)
    else:
        end = datetime(year, month + 1, 1)

    current = start
    while current < end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def download_file(date_str):
    filename = f"aisdk-{date_str}.zip"
    url = f"{BASE_URL}/{filename}"
    output_path = os.path.join(OUTPUT_DIR, filename)

    if os.path.exists(output_path):
        print(f"✔ Skipping (exists): {filename}")
        return

    try:
        with requests.get(url, stream=True, timeout=30) as r:
            if r.status_code != 200:
                print(f"✘ Not found: {filename}")
                return

            with open(output_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        print(f"⬇ Downloaded: {filename}")

    except Exception as e:
        print(f"⚠ Error downloading {filename}: {e}")


def download_month(year, month):
    dates = list(generate_dates(year, month))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(download_file, dates)


if __name__ == "__main__":
    download_month(2026, 2)