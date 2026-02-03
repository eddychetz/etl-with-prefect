import os
import zipfile
import paramiko
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from prefect import task, flow, get_run_logger
import pandera as pa
from typing import Tuple

# --- THE CLEANING LOGIC (Transformation) ---
@task(name="Viljoen: Transform Data")
def transform_viljoen(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    columns = [
        'SellerID','GUID','Date','Reference','Customer_Code','Name','Physical_Address1',
        'Physical_Address2','Physical_Address3','Physical_Address4','Telephone',
        'Stock_Code','Description','Price_Ex_Vat','Quantity','RepCode','ProductBarCodeID'
    ]
    
    df1 = pd.DataFrame(columns=columns)
    df1['Date'] = df['Date']
    df1['SellerID'] = 'VILJOEN'
    df1['GUID'] = 0
    df1['Reference'] = df['Reference']
    df1['Customer_Code'] = df['Customer code']
    df1['Name'] = df['Customer name']
    df1['Physical_Address1'] = df['Physical_Address1']
    df1['Physical_Address2'] = df['Physical_Address2']
    df1['Physical_Address3'] = df['Physical_Address3']
    df1['Physical_Address4'] = (
        df['Deliver1'].fillna('').astype(str) + ' ' +
        df['Deliver2'].fillna('').astype(str) + ' ' +
        df['Deliver3'].fillna('').astype(str) + ' ' +
        df['Deliver4'].fillna('').astype(str)
    ).str.strip()

    df1['Telephone'] = df['Telephone']
    df1['Stock_Code'] = df['Product code']
    df1['Description'] = df['Product description']
    df1['Price_Ex_Vat'] = round(abs(df['Value']/df['Quantity']), 2)
    df1['Quantity'] = df['Quantity']
    df1['RepCode'] = df['Rep']
    df1['ProductBarCodeID'] = ''

    # Cleaning Formats
    df1['Date'] = pd.to_datetime(df1['Date'], errors="coerce").dt.strftime("%Y-%m-%d")
    df1["Name"].fillna('SPAR NORTH RAND (11691)', inplace=True)

    logger.info(f"✅ Transformation complete. Rows: {len(df1)}")
    return df1

# --- THE VALIDATION (Pandera) ---
@task(name="Viljoen: Validate Schema")
def validate_data(df: pd.DataFrame):
    """
    Function to validate data
    """
    logger = get_run_logger()
    class Schema(pa.DataFrameModel):
        # 1. Check data types and uniqueness
        SellerID: Series[str] = pa.Field(nullable=False)  # seller IDs must be non-null
        GUID: Series[int] = pa.Field(ge=0, nullable=False)  # must be non-null

        # 2. Dates coerced to proper datetime
        Date: Series[pd.Timestamp] = pa.Field(coerce=False, nullable=False) # must be non-null

        # 3. Reference and customer codes
        Reference: Series[str] = pa.Field(nullable=False) # must be non-null
        Customer_Code: Series[str] = pa.Field(str_matches=r"^[A-Z0-9]+$", nullable=False)  # must be non-null

        # 4. Customer details
        Name: Series[str] = pa.Field(nullable=False) # must be non-null
        Physical_Address1: Series[str] = pa.Field(nullable=True)
        Physical_Address2: Series[str] = pa.Field(nullable=True)
        Physical_Address3: Series[str] = pa.Field(nullable=True)
        Physical_Address4: Series[str] = pa.Field(nullable=True)

        # 5. Telephone validation (basic regex for digits, spaces, +, -)
        Telephone: Series[str] = pa.Field(nullable=True)

        # 6. Product details
        Stock_Code: Series[str] = pa.Field(nullable=False) # must be non-null
        Description: Series[str] = pa.Field(nullable=False) # must be non-null
        Price_Ex_Vat: Series[float] = pa.Field(ge=0.0, nullable=False)  # must be non-null
        Quantity: Series[int] = pa.Field(nullable=False)  # must be non-null

        # 7. Rep and barcode
        RepCode: Series[str] = pa.Field(nullable=True)
        ProductBarCodeID: Series[str] = pa.Field(nullable=True)  # typical EAN/UPC

        class Config:
            strict = True  # enforce exact schema
            coerce = True  # auto-convert types where possible

    try:
        # lazy=True means "find all errors before crashing"
        Schema.validate(df, lazy=True)
        print("✅ Data passed validation!\nℹ️ Proceeding to next step.")

    except pa.errors.SchemaErrors as err:
        print("⚠️ Data Contract Breached!.......\n")
        print(f"❌ Total errors found: {len(err.failure_cases)}")

        # Let's look at the specific failures
        print("\n*********⚠️Failure Report⚠️************\n")
        print(err.failure_cases[['column', 'check', 'failure_case']])

# --- THE DOWNLOAD (SFTP) ---
@task(name="Viljoen: Download from SFTP", retries=2)
def download_viljoen():
    logger = get_run_logger()
    host = os.getenv('ftp_host')
    # ... (Your paramiko connection logic)
    logger.info(f"📥 Downloaded latest file from {host}")
    return "./data/raw/Vilbev-Latest.zip"
def download_data():
    """
    Connects to SFTP and downloads Vilbev-{YYYYMMDD}.zip after removing
    any existing Vilbev-*.zip files in ./data/raw.
    """

    current_date = datetime.now().strftime('%Y%m%d')

    # ---- PATHS ----
    data_dir = Path("./data/raw")
    data_dir.mkdir(parents=True, exist_ok=True)

    local_file = data_dir / f"Vilbev-{current_date}.zip"
    remote_file = f"/home/viljoenbev/Vilbev-{current_date}.zip"

    # ---- DELETE LOCAL Vilbev FILES FIRST ----
    print("🧹 Cleaning up existing Vilbev-*.zip files in ./data/raw ...")
    deleted_any = False
    for p in data_dir.glob("Vilbev-*.zip"):
        try:
            p.unlink()
            deleted_any = True
            print(f"🗑️ Deleted: {p.name}")
        except Exception as e:
            print(f"❌ Error deleting {p.name}: {e}")
    if not deleted_any:
        print("ℹ️ No existing Vilbev-*.zip files found to delete.")

    # (Optional) ensure target file does not exist—even if name pattern changes in future
    if local_file.exists():
        try:
            local_file.unlink()
            print(f"🗑️ Removed pre-existing target file: {local_file.name}")
        except Exception as e:
            print(f"❌ Error deleting pre-existing target file {local_file.name}: {e}")

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
        print("🔐 Connected to SFTP server")

        # ---- DOWNLOAD ----
        print(f"📥 Downloading: {remote_file} → {local_file}")
        sftp.get(
            remotepath=remote_file,
            localpath=str(local_file),
            callback=None  # add progress callback if you need it
        )
        print("✅ Download complete!")

        print(f"📁 File saved on {str(local_file)}")

    except FileNotFoundError:
        print(f"❌ Remote file not found: {remote_file}")
        return None
    except Exception as e:
        print(f"❌ Error during SFTP operation: {e}")
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
# --- THE MAIN VILJOEN FLOW ---
@flow(name="Viljoen Bev Pipeline")
def viljoen_full_flow():
    logger = get_run_logger()
    
    # 1. Extract
    zip_path = download_viljoen()
    # raw_df = extract_data(zip_path) <-- Convert your extract function to a @task
    
    # 2. Clean & Validate
    # clean_df = transform_viljoen(raw_df)
    # validate_viljoen(clean_df)
    
    # 3. Load & Run Remote
    # local_csv, saved = load_to_local(clean_df)
    # if saved:
    #    upload_to_server(local_csv)
    #    run_import() # Perl Trigger
    
    logger.info("🏁 Viljoen Pipeline Finished Successfully")