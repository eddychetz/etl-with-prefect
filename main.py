# Install Prefect ----
# *********************************************************
# * pip install prefect
# * prefect cloud login and choose to login with a web browser
 
# * run `prefect deploy etl-with-prefect/main.py:main_flow`
 
# *********************************************************
# Installing libraries ----
# *********************************************************
# File System & Utilities
# ----------------------
import os
import zipfile
from glob import glob
from pathlib import Path
from dotenv import load_dotenv
from utils.processors import get_latest_zip, validate_data, validate_dates
from typing import Dict, List, Tuple, Optional
 
# Data Manipulation & time
# -------------------------
import numpy as np
import pandas as pd
from datetime import datetime
 
# FTP server communication
# -------------------------
import paramiko

# For workflow automation
# -----------------------
from prefect import task, flow, get_run_logger # type: ignore
from prefect.blocks.system import JSON, Secret

# Ignore warnings
# ----------------
import warnings
warnings.simplefilter('ignore')

# Load environment variables
# ------------------------------
load_dotenv()

# Print current working directory
# ----------------------------------
print(os.getcwd())
 
# *********************************************************
# 1. Download data ----
# ********************************************************* 
@task(name='Download data from FTP server', retries=3)
def download_data():
    """
    Connects to SFTP and downloads Vilbev-{YYYYMMDD}.zip after removing
    any existing Vilbev-*.zip files in ./data/raw.
    """
    # Fetch from Cloud Blocks
    # --------------------------------
    config = JSON.load('ftp-config').value
    config_paths = JSON.load("project-paths").value
    password = Secret.load('ftp-pass').get()
 
    logger = get_run_logger()
    current_date = datetime.now().strftime('%Y%m%d')

    # ---- PATHS ----
    # ---------------------
    data_dir = config_paths['BASE_DIR'] # Path("./data/raw")
    data_dir.mkdir(parents=True, exist_ok=True)

    local_file = data_dir / f"Vilbev-{current_date}.zip"
    remote_file = f"/home/viljoenbev/Vilbev-{current_date}.zip"

    # ---- DELETE LOCAL Vilbev FILES FIRST ----
    # -----------------------------------------------
    logger.info(f"🧹 Cleaning up existing Vilbev-*.zip files in {data_dir} ...")
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
    # -----------------------------------------------------------------------------------------
    if local_file.exists():
        try:
            local_file.unlink()
            logger.info(f"🗑️ Removed pre-existing target file: {local_file.name}")
        except Exception as e:
            logger.error(f"❌ Error deleting pre-existing target file {local_file.name}: {e}")

    # ---- PARAMIKO CLIENT SETUP ----
    # ---------------------------------------
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    sftp = None
    try:
        # ---- CONNECT ----
        # ----------------------------------
        client.connect(
            hostname=config['host',
            port=config['port'],
            username=config['user'],
            password=password,
            allow_agent=False,
            look_for_keys=False,
            timeout=30,
        )
        sftp = client.open_sftp()
        logger.info("🔐 Connected to SFTP server")

        # ---- DOWNLOAD ----
        # -----------------------------
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
        # ----------------------
        try:
            if sftp is not None:
                sftp.close()
        except Exception:
            pass
        try:
            client.close()
        except Exception:
            pass

# *********************************************************
# 2.0 Extract ----
# *********************************************************
@task(name='Data Extraction')
def extract_data() -> pd.DataFrame:
    """
    Extract the first CSV file from a ZIP archive and load it into a pandas DataFrame.
    Handles:
    - file existence checks
    - multiple CSV files (selects first match)
    - safe extraction into a temp folder
    - consistent return behavior
    """
    zip_file_path = get_latest_zip(directory=str(os.getenv('zip_file_path')), pattern='Vilbev-*.zip')
    logger = get_run_logger()
    if not os.path.exists(zip_file_path):
        raise FileNotFoundError(f"❌ ZIP file does not exist: {zip_file_path}")

    logger.info("📦 Reading ZIP archive!")

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:

        # List all files
        # ------------------
        file_list = zip_ref.namelist()
        logger.info("📁 Files inside ZIP:", file_list)

        # find CSV file(s)
        # ---------------------
        csv_files = [f for f in file_list if f.lower().endswith(".csv")]

        if not csv_files:
            raise ValueError("❌ No CSV file found inside ZIP.")

        # Use the first CSV file found
        # ---------------------------------
        csv_file_name = csv_files[0]
        logger.info(f"📄 Found CSV file: {csv_file_name}")

        # Ensure extraction directory exists
        # -----------------------------------
        extract_dir = os.getenv('BASE_DIR')
        os.makedirs(extract_dir, exist_ok=True)

        # Extract file (optional but useful for debugging)
        # --------------------------------------------------
        extracted_path = zip_ref.extract(csv_file_name, path=extract_dir)
        logger.info(f"📤 Extracted to: {extracted_path}")

        # Load CSV into pandas directly from ZIP
        # ------------------------------------------
        with zip_ref.open(csv_file_name) as csv_file:
            try:
                df = pd.read_csv(csv_file)
                logger.info(f"✅ Loaded CSV: {csv_file_name}")
            except Exception as e:
                raise ValueError(f"❌ Failed to read CSV inside ZIP: {e}")

    return df

# *********************************************************
# 3.0 Transform ---- 
# *********************************************************
@task(name='Data transformation')
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to transform Viljoen Beverages data

    Args:
        df: Input dataframe to transform
        returns: Transformed dataframe

    """
    logger = get_run_logger()
 
    # Standard column layout
    # ---------------------------
    columns=[
        'SellerID','GUID','Date','Reference','Customer_Code','Name','Physical_Address1',\
        'Physical_Address2','Physical_Address3','Physical_Address4','Telephone',\
        'Stock_Code','Description','Price_Ex_Vat','Quantity','RepCode','ProductBarCodeID'
        ]
    # Create an empty dataframe
    # ------------------------------
    df1=pd.DataFrame(columns=columns)

    # Build the dataframe
    # -----------------------
    df1['Date']=df['Date']
    df1['SellerID']='VILJOEN'
    df1['GUID']=0
    df1['Reference']=df['Reference']
    df1['Customer_Code']=df['Customer code']
    df1['Name']=df['Customer name']
    df1['Physical_Address1']=df['Physical_Address1']
    df1['Physical_Address2']=df['Physical_Address2']
    df1['Physical_Address3']=df['Physical_Address3']
    df1['Physical_Address4']=(
        df['Deliver1'].fillna('').astype(str)+' '+df['Deliver2'].fillna('').astype(str)+' '+df['Deliver3'].fillna('').astype(str)+' '+df['Deliver4'].fillna('').astype(str)
        ).str.strip()

    df1['Telephone']=df['Telephone']
    df1['Stock_Code']=df['Product code']
    df1['Description']=df['Product description']
    df1['Price_Ex_Vat']=round(abs(df['Value']/df['Quantity']),2)
    df1['Quantity']=df['Quantity']
    df1['RepCode']=df['Rep']
    df1['ProductBarCodeID']=''
    
    logger.info(f"ℹ️  Total quantity: {np.sum(df1['Quantity']):.0f}\n")

    df2=df1.copy()
    df2['Date']=pd.to_datetime(df2['Date'])
    # filling up missing Name for Spar Northrand
    # ------------------------------------------
    df1["Name"].fillna('SPAR NORTH RAND (11691)', inplace=True)
    
    #   DATE FORMAT CLEANING - Always monitor on the date format when transforming data
    # -------------------------------------------------------------------------------------
    logger.info("✅ Date fomat cleaned")
    df1['Date'] = pd.to_datetime(df1['Date'], errors="coerce").dt.strftime("%Y-%d-%m")
    logger.info("✅ Data transformation complete!")

    return df1

# *********************************************************
# 4.0 Load ----
# *********************************************************
@task(name='Loading data to local repo')
def load_to_local(
    df: pd.DataFrame,
    create_dir_if_missing: bool = True,
    delete_existing_csvs: bool = True,        # ← new flag to control cleanup
    restrict_delete_to_prefix: str | None = None  # e.g., "Viljoenbev_" to only delete those CSVs
) -> Tuple[str, bool]:
    """
    Save cleaned data to a CSV inside the folder specified by OUTPUT_DIR in .env,
    only if:
    - the DataFrame's date range is entirely within the last 3 days, and
    - the latest date's month is the current month or the previous month.
    Skips save if a file with the same name already exists.

    Returns
    -------
    (full_path, saved) : Tuple[str, bool]
        full_path -> absolute path to the intended CSV
        saved     -> True if file was written, False if skipped (already existed)
    """


    # --- Resolve OUTPUT_DIR ---
    # ----------------------------
    output_dir = os.getenv("OUTPUT_DIR")
    if not output_dir:
        raise ValueError("Environment variable 'OUTPUT_DIR' is not set in your environment or .env file.")
    logger = get_run_logger()
    output_dir_path = Path(os.path.abspath(os.path.expanduser(output_dir)))
    if not output_dir_path.is_dir():
        if create_dir_if_missing:
            output_dir_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"📁 Created output directory: {output_dir_path}")
        else:
            raise FileNotFoundError(f"Output directory does not exist: {output_dir_path}")

    # ---- DELETE EXISTING CSVs IN CLEANED FOLDER (before saving) ----
    # --------------------------------------------------------------------
    if delete_existing_csvs:
        # Choose the pattern:
        #   "*.csv" to delete ALL CSVs in the folder
        #   f"{restrict_delete_to_prefix}*.csv" to only delete those starting with a prefix
        if restrict_delete_to_prefix:
            pattern = f"{restrict_delete_to_prefix}*.csv"
        else:
            pattern = "*.csv"

        logger.info(f"🧹 Cleaning up existing CSV file in:\n📁 {output_dir_path}.")
        deleted_any = False
        for p in output_dir_path.glob(pattern):
            try:
                p.unlink()
                deleted_any = True
                logger.info(f"🗑️ Deleted CSV: {p.name}")
            except Exception as e:
                logger.error(f"❌ Error deleting {p.name}: {e}")
        if not deleted_any:
            logger.info("ℹ️  No matching CSV files found to delete.")

    # --- Prepare and validate dates ---
    # ---------------------------------------
    if "Date" not in df.columns:
        raise KeyError("Input DataFrame must contain a 'Date' column.")

    data = df.copy()
    data["Date"] = pd.to_datetime(data["Date"], errors="coerce")

    if data["Date"].isna().all():
        raise ValueError("All values in 'Date' are NaT after parsing. Check your input data.")

    min_date = data["Date"].dropna().min()
    max_date = data["Date"].dropna().max()

    # Validation per your rule:
    # -------------------------
    validate_dates(min_date, max_date, lookback_days=3)

    # --- Build deterministic filename and check for existence ---
    # -------------------------------------------------------------
    min_str = min_date.strftime("%Y-%m-%d")
    max_str = max_date.strftime("%Y-%m-%d")
    filename = f"Viljoenbev_{min_str}_to_{max_str}.csv"
    full_path = output_dir_path / filename

    if full_path.exists():
        logger.info(f"🛑 File already exists, skipping save:\n📁 {full_path}")
        return str(full_path), False

    # --- Finalize and save ---
    # ----------------------------
    data["Date"] = data["Date"].dt.strftime("%Y-%m-%d")
    data.to_csv(full_path, index=False)
    logger.info(f"\n✅ Data saved to:\n📁 {full_path}")
    return str(full_path), True
 
 
# *********************************************************
# 5.0 Load to FTP server ----
# *********************************************************
@task(name='Uploading data to FTP server')
def upload_to_server(csv_file_path: str = None,) -> Optional[str]:
    """
    Upload a specific CSV file to an SFTP server using only paramiko.

    Parameters:
    -----------
    csv_file_path : str
        Path to the local CSV file to upload. If None, it will pick the first
        file matching 'Viljoenbev_*.csv' in `output_dir`.

    Returns:
    --------
    str or None
        Remote path where the file was uploaded, or None if the upload failed.
    """
    logger = get_run_logger()
 
    # ---- CONNECT ----
    # 1. Fetch from Cloud Blocks
    # ------------------------------
    config = JSON.load("ftp-config").value
    config_paths = JSON.load("project-paths").value
   
    sftp_host=config['host']
    sftp_port=config['port']
    sftp_user=config['user']
    password = Secret.load("ftp-pass").get()
    remote_dir = config['remote_dir']

    # Fallback to discover a file if not provided (mirrors your original default idea)
    # --------------------------------------------------------------------------------
    if csv_file_path is None:
        # Adjust `output_dir` to your actual variable/scope if needed
        # ------------------------------------------------------------
        output_dir = config_paths['OUTPUT_DIR']
        matches = glob(os.path.join(output_dir, 'Viljoenbev_*.csv'))
        if not matches:
            logger.warning("⚠️ No CSV file found matching 'Viljoenbev_*.csv'.")
            return None
        csv_file_path = matches[0]

    try:
        # Verify the local file exists
        # ----------------------------
        if not os.path.exists(csv_file_path):
            logger.warning(f"⚠️ Local file not found: {csv_file_path}")
            return None

        filename = os.path.basename(csv_file_path)
        # Normalize remote path to POSIX style for SFTP
        # ------------------------------------------------
        remote_dir_posix = remote_dir.replace('\\', '/').rstrip('/') + '/'
        remote_path = (remote_dir_posix + filename)

        # Create SSH client and connect
        # -------------------------------
        ssh = paramiko.SSHClient()

        # WARNING: Auto-adding host keys reduces security. Prefer loading known hosts in production.
        # -------------------------------------------------------------------------------------------
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        logger.info(f"🔐 Connecting to FTP server as {sftp_user}.")
        ssh.connect(
            hostname=config['host'],
            port=config['port'],
            username=config['user'],
            password=password,
            look_for_keys=False,
            allow_agent=False,
            timeout=30
        )

        try:
            # Open SFTP session
            # ----------------------
            sftp = ssh.open_sftp()

            # Ensure remote directory exists (create recursively if missing)
            # --------------------------------------------------------------
            _ensure_remote_dir(sftp, remote_dir_posix)

            # Upload with confirmation via file size/stat check
            # ---------------------------------------------------
            logger.info(f"🚀 Uploading {filename} to {remote_path}...")
            sftp.put(csv_file_path, remote_path)

            # Optional: verify upload completed by checking remote file size
            # -----------------------------------------------------------------
            local_size = os.path.getsize(csv_file_path)
            remote_stat = sftp.stat(remote_path)
            if remote_stat.st_size != local_size:
                logger.warning("⚠️ Size mismatch after upload. Upload may be incomplete.")
                return None

            logger.info("✅ Upload completed successfully!")
            return None

        finally:
            try:
                sftp.close()
            except Exception:
                pass
            ssh.close()

    except paramiko.AuthenticationException:
        logger.warning("⚠️ Authentication failed. Please verify username/password (or key).")
        return None
    except paramiko.SSHException as e:
        logger.error(f"❌ SSH/SFTP error: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Error uploading file: {e}")
        return None

# Remote Directory Helpers
# ------------------------------
def _ensure_remote_dir(sftp: paramiko.SFTPClient, remote_dir_posix: str) -> None:
    """
    Recursively create remote directories if they do not exist.
    `remote_dir_posix` must be a POSIX-style path ending with '/'.
    """
    # Split path into components and build progressively
    # Handle absolute paths like '/home/user/data/'
    parts = [p for p in remote_dir_posix.split('/') if p]
    prefix = '/' if remote_dir_posix.startswith('/') else ''

    current = prefix
    for part in parts:
        current = (current.rstrip('/') + '/' + part)
        try:
            sftp.stat(current)  # Exists
        except FileNotFoundError:
            sftp.mkdir(current)

 
# *********************************************************
# 6.0 Running importtxns.pl on CLI ----
# *********************************************************
@task(name='Running `importtxns.pl` file on CLI')
def run_import(timeout: int = 120):
    """
    Connect to a remote server via SSH and execute commands, first testing then running.

    Args:
        timeout (int): Timeout for command execution in seconds

    Returns:
        tuple: (stdout, stderr, exit_code) from the last command executed
    """
    logger = get_run_logger()
    
    # Access login credentials
    # ---------------------------------
    config = JSON.load('toby-config').value
    password = Secret.load('toby-pass').get()
    hostname = config['host']
    port = config['port']
    username = config['user']

    # Define commands
    # ----------------------
    commands = {
        'test': "/usr/local/eroute2market/supply_chain/scripts/importtxns.pl /home/viljoenbev/data 1",
        'run': "/usr/local/eroute2market/supply_chain/scripts/importtxns.pl /home/viljoenbev/data 1"
    }

    # If no password provided, prompt for it securely
    # -----------------------------------------------
    if password is None:
        password = getpass.getpass(f"🔐 Enter SSH password for {username}@{hostname}: ")

    # Create an SSH client instance
    # ---------------------------------
    client = paramiko.SSHClient()

    try:
        # Automatically add the server's host key
        # ------------------------------------------
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to the remote server
        # -----------------------------
        logger.info(f"🔐 Connecting to {hostname} on port {port}...")
        client.connect(
            hostname=hostname,
            port=port,
            username=username,
            password=password
        )

        # First execute the test command
        # ----------------------------------
        logger.info(f"🚀 Executing test command:>>>>>>>> {commands['test']}")
        stdin, stdout, stderr = client.exec_command(commands['test'], timeout=timeout)
        exit_status = stdout.channel.recv_exit_status()
        stdout_str = stdout.read().decode('utf-8')
        stderr_str = stderr.read().decode('utf-8')

        if exit_status != 0:
            logger.info(f"❌ Test command failed with exit code: {exit_status}")
            logger.info("🛑 Aborting - not running the main command.")
            return stdout_str, stderr_str, exit_status

        logger.info("✅ Test command succeeded. \n🚀  Now executing run command...")

        # If test succeeded, execute the run command
        # ---------------------------------------------
        logger.info(f"🚀 Executing run command:>>>>>>>> {commands['run']}")
        stdin, stdout, stderr = client.exec_command(commands['run'], timeout=timeout)
        exit_status = stdout.channel.recv_exit_status()
        stdout_str = stdout.read().decode('utf-8')
        stderr_str = stderr.read().decode('utf-8')

        # Print status
        # ------------------
        if exit_status == 0:
            logger.info("✅ Run command executed successfully.")
        else:
            logger.info(f"❌ Run command failed with exit code: {exit_status}")

        # Return results from the run command
        # ----------------------------------------
        return stdout_str, stderr_str, exit_status

    except Exception as e:
        logger.info(f"❌ Error: {str(e)}")
        return "", str(e), -1

    finally:
        # Always close the connection
        # ----------------------------
        client.close()
        logger.info("🔐 SSH connection closed.")

# Helper Function
# -----------------
def parse_perl_output(stdout: str, stderr: str, exit_code: int) -> dict:
    working_messages = [line for line in stdout.splitlines() if line.strip()]

    stderr_lines = [line for line in stderr.splitlines() if line.strip()]
    warnings = [warn for warn in stderr_lines if "warning" in warn.lower()]
    errors = [err for err in stderr_lines if "error" in err.lower()]
    other_stderr = [serr for serr in stderr_lines if serr not in warnings + errors]

    return {
        "working_on": working_messages,
        "warnings": warnings,
        "errors": errors,
        "other_stderr": other_stderr,
        "exit_code": exit_code
    } 
 
  
# *********************************************************
# 5.0 Main Flow ----
# *********************************************************
@flow(name='DAILY MASTER IMPORT', log_prints=True)
def master_flow():
    logger = get_run_logger()
    logger.info("🚀 STARTING VILJOEN PIPELINE")
 
    logger.info(">>>>>>>>>>>>>> ⭐ 1. DOWNLOADING PROCESS..>>>>>>>>>>>>>>>>>>>")
    download_data()
 
    logger.info(">>>>>>>>>>>>>> ⭐ 2. EXTRACTION PROCESS.....>>>>>>>>>>>>>>>>>>>")
    raw_data = extract_data()
 
    logger.info(">>>>>>>>>>>>>> ⭐ 3. TRANSFORMATION PROCESS...>>>>>>>>>>>>>>>>>>>")
    clean_df = transform_data(raw_data)
 
    logger.info(">>>>>>>>>>>>>>⭐ 4. VALIDATION PROCESS...>>>>>>>>>>>>>>>>>>>")
    validate_data(clean_df)
 
    logger.info(">>>>>>>>>>>>>> ⭐ 5. DATA LOADING PROCESS...>>>>>>>>>>>>>>>>>>>")
    # load_to_local(clean_df)
    upload_to_server()
 
    logger.info(">>>>>>>>>>>>>> ⭐ 6. RUNNING IMPORTATION SCRIPT...>>>>>>>>>>>>>>>>>>>")
    out, err, cod = run_import()
    result = parse_perl_output(out, err, cod)
    for key, value in result.items():
        logger.warning(f"❌ {key}: {value}")
 
    logger.info("🏁 Viljoen Pipeline Finished Successfully")

 
 
# *********************************************************
# 6.0 Run ----
# *********************************************************
if __name__ == '__main__':
    master_flow.serve(
        name="daily-viljoen-deployment",
        cron="20 8 * * *"
    )
 
 
 
# *********************************************************
# 7.0 Deployment
# *********************************************************
 
# Your flow 'DAILY MASTER IMPORT' is being served and polling for scheduled runs!

# To trigger a run for this flow, use the following command:

#         $ prefect deployment run 'DAILY MASTER IMPORT/daily-viljoen-deployment'
