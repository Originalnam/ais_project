import os
import zipfile
from concurrent.futures import ThreadPoolExecutor

READ_DIR = os.path.join(os.path.dirname(__file__), "raw")
WRITE_DIR = os.path.join(os.path.dirname(__file__), "raw\unzipped")

print(f"Reading zip files from: {READ_DIR}")
print(f"Extracting zip files to: {WRITE_DIR}")

def extract_zip(file_name):
    zip_path = os.path.join(READ_DIR, file_name)
    target_dir = os.path.join(WRITE_DIR, file_name.replace(".zip", ""))
    
    print(f"Starting extraction of {file_name} to {target_dir}")
    os.makedirs(target_dir, exist_ok=True)
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(target_dir)
        print(f"Successfully extracted {file_name}")
    except zipfile.BadZipFile:
        print(f"Error: {file_name} is not a valid zip file")
    except Exception as e:
        print(f"Error extracting {file_name}: {e}")

zip_files = [f for f in os.listdir(READ_DIR) if f.endswith(".zip")]
print(f"Found {len(zip_files)} zip files: {zip_files}")

with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(extract_zip, zip_files)

print("All extractions completed.")