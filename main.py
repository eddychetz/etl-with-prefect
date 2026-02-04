# Install Prefect ----
# * pip install prefect
# * prefect cloud login and choose to login with a web browser

# * run `prefect deploy etl-with-prefect/main.py:main_flow`
# Installing libraries
# File System & Utilities
# ----------------------
import os
import zipfile
import getpass
from glob import glob
from pathlib import Path
from dotenv import load_dotenv
from utils.processors import get_latest_zip # utils.py
from typing import Tuple, Optional, Callable
# import ftplib
# import tempfile
# from io import BytesIO

# Data Manipulation & time
# -------------------------
import numpy as np
import pandas as pd
from datetime import timedelta, datetime

# FTP server communication
# -------------------------
import paramiko

# Data Validation
# ----------------
import pandera as pa
from pandera.typing.pandas import Series

# For workflow automation
# -----------------------
from prefect import task, flow, get_run_logger # type: ignore

# Ignore warnings
# ----------------
import warnings
warnings.simplefilter('ignore')

# Load environment variables
load_dotenv()

# Print current working directory
print(os.getcwd()) # os.getcwd()
# Download data ----
@task(name='Download data from FTP server', retries=3)
def download_data():
    """
    Connects to SFTP and downloads Vilbev-{YYYYMMDD}.zip after removing
    any existing Vilbev-*.zip files in ./data/raw.
    """
    logger = get_run_logger()
    current_date = datetime.now().strftime('%Y%m%d')

    # ---- PATHS ----
    data_dir = Path("./data/raw")
    data_dir.mkdir(parents=True, exist_ok=True)

    local_file = data_dir / f"Vilbev-{current_date}.zip"
    remote_file = f"/home/viljoenbev/Vilbev-{current_date}.zip"

    # ---- DELETE LOCAL Vilbev FILES FIRST ----
    logger.info("🧹 Cleaning up existing Vilbev-*.zip files in ./data/raw ...")
    deleted_any = False
    for p in data_dir.glob("Vilbev-*.zip"):
        try:
            p.unlink()
            deleted_any = True
            logger.info(f"🗑️ Deleted: {p.name}")
        except Exception as e:
            logger.error(f"❌ Error deleting {p.name}: {e}")
    if not deleted_any:
        logger.info("ℹ️  No existing Vilbev-*.zip files found to delete.")

    # (Optional) ensure target file does not exist—even if name pattern changes in future
    if local_file.exists():
        try:
            local_file.unlink()
            logger.info(f"🗑️ Removed pre-existing target file: {local_file.name}")
        except Exception as e:
            logger.error(f"❌ Error deleting pre-existing target file {local_file.name}: {e}")

    # ---- PARAMIKO CLIENT SETUP ----
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    sftp = None
    try:
        # ---- CONNECT ----
        host = os.getenv('ftp_host')
        port = int(os.getenv('ftp_port'))  # ensure integer
        user = os.getenv('ftp_user')
        pwd  = os.getenv('ftp_pass')

        client.connect(
            hostname=host,
            port=port,
            username=user,
            password=pwd,
            allow_agent=False,
            look_for_keys=False,
            timeout=30,
        )
        sftp = client.open_sftp()
        logger.info("🔐 Connected to SFTP server")

        # ---- DOWNLOAD ----
        logger.info(f"📥 Downloading: {remote_file} → {local_file}")
        sftp.get(
            remotepath=remote_file,
            localpath=str(local_file),
            callback=None  # add progress callback if you need it
        )
        logger.info("✅ Download complete!")

        logger.info(f"📁 File saved on {str(local_file)}")

    except FileNotFoundError:
        logger.error(f"❌ Remote file not found: {remote_file}")
        return None
    except Exception as e:
        logger.error(f"❌ Error during SFTP operation: {e}")
        return None
    finally:
        # ---- CLEAN UP ----
        try:
            if sftp is not None:
                sftp.close()
        except Exception:
            pass
        try:
            client.close()
        except Exception:
            pass
# 2.0 Extract ----
@task(name='')
def extract_data() -> pd.DataFrame:
    """
    Extract the first CSV file from a ZIP archive and load it into a pandas DataFrame.
    Handles:
    - file existence checks
    - multiple CSV files (selects first match)
    - safe extraction into a temp folder
    - consistent return behavior
    """
    zip_file_path = get_latest_zip(os.getenv('BASE_DIR'))
    logger = get_run_logger()
    if not os.path.exists(zip_file_path):
        raise FileNotFoundError(f"❌ ZIP file does not exist: {zip_file_path}")

    logger.info("📦 Reading ZIP archive!")

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:

        # List all files
        file_list = zip_ref.namelist()
        logger.info("📁 Files inside ZIP:", file_list)

        # find CSV file(s)
        csv_files = [f for f in file_list if f.lower().endswith(".csv")]

        if not csv_files:
            raise ValueError("❌ No CSV file found inside ZIP.")

        # Use the first CSV file found
        csv_file_name = csv_files[0]
        logger.info(f"📄 Found CSV file: {csv_file_name}")

        # Ensure extraction directory exists
        extract_dir = "data"
        os.makedirs(extract_dir, exist_ok=True)

        # Extract file (optional but useful for debugging)
        extracted_path = zip_ref.extract(csv_file_name, path=extract_dir)
        logger.info(f"📤 Extracted to: {extracted_path}")

        # Load CSV into pandas directly from ZIP
        with zip_ref.open(csv_file_name) as csv_file:
            try:
                df = pd.read_csv(csv_file)
                logger.info(f"✅ Loaded CSV: {csv_file_name}")
            except Exception as e:
                raise ValueError(f"❌ Failed to read CSV inside ZIP: {e}")

    return df
# 3.0 Transform ----
@task
def transform():
    print('Hello, step 2 is running!!')

# 4.0 Load ----
@task
def load():
    print('Hello, step 3 is running!!')

# 5.0 Main Flow ----
@flow(name='DAILY MASTER IMPORT', log_prints=True)
def master_flow():
    logger = get_run_logger()
    logger.info("🚀 Starting Viljoen Pipeline")
    download_data()
    raw = extract_data()
    print(raw.head())
    step2 = transform()
    print(step2)
    step3 = load()
    print(step3)
    logger.info("🏁 Viljoen Pipeline Finished Successfully")
# 6.0 Run ----
if __name__ == '__main__':
    master_flow()


# 7.0 Deployment
# * prefect deploy [OPTIONS] [ENTRYPOINT] e.g ./etl-with-prefect/main.py:main_flow
# * run `prefect deploy -name test-deployment ./etl-with-prefect/main.py:main_flow`
# * run `prefect dashboard`