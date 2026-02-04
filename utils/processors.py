# # LIBRARIES
import os
import sys
from glob import glob
import importlib
import importlib.util
from datetime import timedelta, datetime
import subprocess
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd
GROUPS: Dict[str, List[Tuple[str, Optional[str]]]] = {
    # (import_name, pip_name or None if same)
    "stdlib": [
        ("os", None),
        ("sys", None),
        ("subprocess", None),
        ("warnings", None),
        ("glob", None),
        ("zipfile", None),
        ("tempfile", None),
        ("time", None),
        ("getpass", None),
        # You also use these below via from-imports
        ("pathlib", None),
        ("io", None),
        ("datetime", None),
        ("typing", None),
    ],
    "core_data": [
        ("pandas", None),
        ("numpy", None),
    ],
    "sftp": [
        ("paramiko", None)  # note: unmaintained; keep only if you must
    ],
    "orchestration": [
        ("prefect", None),
    ],
    "validation": [
        ("pandera", None),
        ("pandera.typing.pandas", None),
    ],
    "ux": [
        ("tqdm", None), 
        ("tqdm.notebook", None),
        ("ipywidgets", None)
    ]
}

# ---------------------------
# 2) Utils
# ---------------------------
def _is_installed(import_name: str) -> bool:
    """Return True if the top-level module is importable."""
    top = import_name.split(".", 1)[0]
    return importlib.util.find_spec(top) is not None

def _pip_install(pkg_name: str):
    """Install a package via pip for the current interpreter."""
    print(f"📦 Installing via pip: {pkg_name}")
    subprocess.check_call([sys.executable, "-m", "pip", "install", pkg_name])

def _resolve_pip_name(import_name: str, pip_name: Optional[str]) -> str:
    """
    Given an import_name and optional pip_name, determine which pip package to install.
    """
    if pip_name:
        return pip_name
    # If import_name maps from an alias, find the pip name
    for pip_key, import_val in PIP_IMPORT_ALIASES.items():
        if import_val == import_name:
            return pip_key
    # Default fallback: assume pip name == import name
    return import_name

def import_module_dotted(import_name: str):
    """
    Import a (possibly dotted) module and return it.
    Example: import_module_dotted("tqdm.notebook")
    """
    return importlib.import_module(import_name)

# ---------------------------
# 3) Ensure one dependency: import if present; otherwise install then import
# ---------------------------
def ensure_one(import_name: str, pip_name: Optional[str] = None):
    top = import_name.split(".", 1)[0]

    # stdlib? (heuristic: if it's importable already, just import)
    if _is_installed(top):
        return import_module_dotted(import_name)

    # Not installed → attempt pip install for third-party
    to_install = _resolve_pip_name(import_name, pip_name)
    try:
        _pip_install(to_install)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"❌ pip install failed for '{to_install}': {e}") from e

    # Retry import
    return import_module_dotted(import_name)

# ---------------------------
# 4) Ensure whole groups (returns dict of imported modules)
# ---------------------------
def ensure_grouped_dependencies(
    groups: Dict[str, List[Tuple[str, Optional[str]]]] = GROUPS,
    *,
    install_missing_from_requirements: bool = False,
    requirements_file: str = "requirements.txt"
    ) -> Dict[str, object]:
    """
    Ensure all grouped dependencies are available.
    If install_missing_from_requirements=True, will first try to import all,
    and if any missing occurs, install everything from requirements.txt once, then import again.
    Otherwise, installs missing packages individually.
    Returns dict of imported modules keyed by their import names (top-level names for dotted imports).
    """
    imported: Dict[str, object] = {}
    missing: List[Tuple[str, Optional[str]]] = []

    # First pass: attempt imports
    for group_name, items in groups.items():
        print(f"🔹 Checking group: {group_name}")
        for import_name, pip_name in items:
            top = import_name.split(".", 1)[0]
            try:
                mod = import_module_dotted(import_name)
                imported[top] = mod if top == import_name else importlib.import_module(top)
                print(f" ✔ {import_name} Imported")
            except ImportError:
                print(f"⚠️ Missing {import_name}")
                missing.append((import_name, pip_name))

    # If nothing missing, return early
    if not missing:
        return imported

    # Strategy A: bulk install from requirements once
    if install_missing_from_requirements:
        print(f"📦 Installing all missing from {requirements_file}...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", requirements_file])
        except subprocess.CalledProcessError as e:
            print("❌ Bulk installation from requirements failed:", e)
            print("↪ Switching to per-package installation for missing items...")
            # Fall back to per package
            for import_name, pip_name in missing:
                to_install = _resolve_pip_name(import_name, pip_name)
                _pip_install(to_install)
    else:
        # Strategy B: install missing individually
        for import_name, pip_name in missing:
            to_install = _resolve_pip_name(import_name, pip_name)
            _pip_install(to_install)

    # Second pass: import again
    for import_name, _ in missing:
        top = import_name.split(".", 1)[0]
        mod = import_module_dotted(import_name)
        imported[top] = mod if top == import_name else importlib.import_module(top)
        print(f"✅ Imported after install: {import_name}")

    return imported

# ---------------------------
# 5) Zipped file helpers
# ---------------------------

def get_latest_zip(directory: str, pattern: str = "Vilbev-*.zip") -> str:
    """
    Return the most recent ZIP matching the pattern in the given directory.
    Ensures reliable access to the latest Vilbev file.
    """
    directory = Path(directory).expanduser().resolve()

    # Use glob
    files = list(directory.glob(pattern))

    if not files:
        raise FileNotFoundError(f"❌ No files found matching pattern {pattern} in {directory}")

    # Sort by modification time (latest first)
    files_sorted = sorted(files, key=os.path.getmtime, reverse=True)

    latest = str(files_sorted[0])
    print(f"📦 Latest ZIP selected: {latest}")

    return latest

# 6) Function to unzip files
# ------------------------------------
def unzip_file() -> pd.DataFrame:
    """
    Extract the first CSV file from a ZIP archive and load it into a pandas DataFrame.
    Handles:
    - file existence checks
    - multiple CSV files (selects first match)
    - safe extraction into a temp folder
    - consistent return behavior
    """
    zip_file_path = get_latest_zip(os.getenv('BASE_DIR'))

    if not os.path.exists(zip_file_path):
        raise FileNotFoundError(f"❌ ZIP file does not exist: {zip_file_path}")

    print("📦 Reading ZIP archive!")

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:

        # List all files
        file_list = zip_ref.namelist()
        print("📁 Files inside ZIP:", file_list)

        # find CSV file(s)
        csv_files = [f for f in file_list if f.lower().endswith(".csv")]

        if not csv_files:
            raise ValueError("❌ No CSV file found inside ZIP.")

        # Use the first CSV file found
        csv_file_name = csv_files[0]
        print(f"📄 Found CSV file: {csv_file_name}")

        # Ensure extraction directory exists
        extract_dir = "data"
        os.makedirs(extract_dir, exist_ok=True)

        # Extract file (optional but useful for debugging)
        extracted_path = zip_ref.extract(csv_file_name, path=extract_dir)
        print(f"📤 Extracted to: {extracted_path}")

        # Load CSV into pandas directly from ZIP
        with zip_ref.open(csv_file_name) as csv_file:
            try:
                df = pd.read_csv(csv_file)
                print(f"✅ Loaded CSV: {csv_file_name}")
            except Exception as e:
                raise ValueError(f"❌ Failed to read CSV inside ZIP: {e}")

    return df
# 7) Function to load clean data to clean folder
# ------------------------------------
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
    output_dir = os.getenv("OUTPUT_DIR")
    if not output_dir:
        raise ValueError("Environment variable 'OUTPUT_DIR' is not set in your environment or .env file.")

    output_dir_path = Path(os.path.abspath(os.path.expanduser(output_dir)))
    if not output_dir_path.is_dir():
        if create_dir_if_missing:
            output_dir_path.mkdir(parents=True, exist_ok=True)
            print(f"📁 Created output directory: {output_dir_path}")
        else:
            raise FileNotFoundError(f"Output directory does not exist: {output_dir_path}")

    # ---- DELETE EXISTING CSVs IN CLEANED FOLDER (before saving) ----
    if delete_existing_csvs:
        # Choose the pattern:
        #   "*.csv" to delete ALL CSVs in the folder
        #   f"{restrict_delete_to_prefix}*.csv" to only delete those starting with a prefix
        if restrict_delete_to_prefix:
            pattern = f"{restrict_delete_to_prefix}*.csv"
        else:
            pattern = "*.csv"

        print(f"🧹 Cleaning up existing CSV file in:\n📁 {output_dir_path}.")
        deleted_any = False
        for p in output_dir_path.glob(pattern):
            try:
                p.unlink()
                deleted_any = True
                print(f"🗑️ Deleted CSV: {p.name}")
            except Exception as e:
                print(f"❌ Error deleting {p.name}: {e}")
        if not deleted_any:
            print("ℹ️ No matching CSV files found to delete.")

    # --- Prepare and validate dates ---
    if "Date" not in df.columns:
        raise KeyError("Input DataFrame must contain a 'Date' column.")

    data = df.copy()
    data["Date"] = pd.to_datetime(data["Date"], errors="coerce")

    if data["Date"].isna().all():
        raise ValueError("All values in 'Date' are NaT after parsing. Check your input data.")

    min_date = data["Date"].dropna().min()
    max_date = data["Date"].dropna().max()

    # Validation per your rule:
    validate_dates(min_date, max_date, lookback_days=3)

    # --- Build deterministic filename and check for existence ---
    min_str = min_date.strftime("%Y-%m-%d")
    max_str = max_date.strftime("%Y-%m-%d")
    filename = f"Viljoenbev_{min_str}_to_{max_str}.csv"
    full_path = output_dir_path / filename

    if full_path.exists():
        print(f"🛑 File already exists, skipping save:\n📁 {full_path}")
        return str(full_path), False

    # --- Finalize and save ---
    data["Date"] = data["Date"].dt.strftime("%Y-%m-%d")
    data.to_csv(full_path, index=False)
    print(f"\n✅ Data saved to:\n📁 {full_path}")
    return str(full_path), True

# -------------------------------------------------------------
# Validate date in cleaned file
# -------------------------------------------------------------
def validate_dates(
    min_date: pd.Timestamp,
    max_date: pd.Timestamp,
    today: datetime = None,
    lookback_days: int = 3) -> None:

    """
    Raises ValueError if the date range is not entirely within the last `lookback_days`
    and if the latest month is neither the current month nor the previous month.
    """

    if today is None:
        today = datetime.now()

    # Normalize to date (drop time)
    today_d = today.date()
    window_start = today_d - timedelta(days=lookback_days)

    min_d = min_date.date()
    max_d = max_date.date()

    # 1) Entire range must be within the last `lookback_days` days (inclusive)
    if not (window_start <= min_d <= today_d and window_start <= max_d <= today_d):
        raise ValueError(
            f"❌ Date range {min_d} to {max_d} is not fully within the last {lookback_days} days "
            f"({window_start}..{today_d})."
        )

    # 2) Month check on the latest date in the file (max_d)
    cur_month = today_d.month
    prev_month = 12 if cur_month == 1 else cur_month - 1
    file_month = max_d.month

    if file_month not in (cur_month, prev_month):
        raise ValueError(
            f"❌ Latest file month ({file_month}) is not the current month ({cur_month}) "
            f"or previous month ({prev_month})."
        )
        
# Data validation function
# -----------------------------
def validate_data(df: pd.DataFrame):
    """
    Function to validate data
    """
    # logger = get_run_logger()
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

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to transform Viljoen Beverages data

    Args:
        df: Input dataframe to transform
        returns: Transformed dataframe

    """

    # Standard column layout
    columns=[
        'SellerID','GUID','Date','Reference','Customer_Code','Name','Physical_Address1',\
        'Physical_Address2','Physical_Address3','Physical_Address4','Telephone',\
        'Stock_Code','Description','Price_Ex_Vat','Quantity','RepCode','ProductBarCodeID'
        ]
    # Create an empty dataframe
    df1=pd.DataFrame(columns=columns)

    # Build the dataframe
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
        df['Deliver1'].fillna('').astype(str) +' '+
        df['Deliver2'].fillna('').astype(str) +' '+
        df['Deliver3'].fillna('').astype(str) +' '+
        df['Deliver4'].fillna('').astype(str)
        ).str.strip()

    df1['Telephone']=df['Telephone']
    df1['Stock_Code']=df['Product code']
    df1['Description']=df['Product description']
    df1['Price_Ex_Vat']=round(abs(df['Value']/df['Quantity']),2)
    df1['Quantity']=df['Quantity']
    df1['RepCode']=df['Rep']
    df1['ProductBarCodeID']=''

    print(f"ℹ️ Total quantity: {np.sum(df1['Quantity']):.0f}\n")

    df2=df1.copy()
    df2['Date']=pd.to_datetime(df2['Date'])
    df2['Date']=df2['Date'].apply(lambda x: x.strftime("%Y-%m-%d"))

    df1["Name"].fillna('SPAR NORTH RAND (11691)', inplace=True)
    #   DATE FORMAT CLEANING
    # -----------------------------
    print("✅ Date fomat cleaned")
    df1['Date'] = pd.to_datetime(df1['Date'], errors="coerce").dt.strftime("%Y-%m-%d")
    print("✅ Data transformation complete!")

    return df1