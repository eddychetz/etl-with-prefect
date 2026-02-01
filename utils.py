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

# ---------------------------
# 1) Pip name ↔ import name aliases (extend as needed)
# ---------------------------
PIP_IMPORT_ALIASES = {
    # pip-name              : import-name
    "python-dotenv": "dotenv",
    "mysql-connector-python": "mysql.connector",
    # examples you might add later:
    # "pillow": "PIL",
}
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
        ("paramiko", None) # note: unmaintained; keep only if you must
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
    ],
}
# ---------------------------
# 3) Utils
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
# 4) Ensure one dependency: import if present; otherwise install then import
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
# 5) Ensure whole groups (returns dict of imported modules)
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
# 6) Zipped file helpers
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


# # Function to import libraries
# def import_libraries(modules: list[str]):
#     imported = {}
#     for module in modules:
#         try:
#             imported[module] = importlib.import_module(module)
#             print(f"✔ Imported {module}")
#         except ImportError:
#             print(f"❌ Could not import {module}")
#     return imported

# # Function to install requirements
# # - try to import packages if it exist or else install package using pip
# def install_requirements(requirements_file="requirements.txt"):
#     """
#     Install packages from a requirements.txt file using pip.
#     Safe, works inside Jupyter and normal Python scripts.
#     """
#     try:
#         print(f"📦 Installing packages from {requirements_file}...")
#         subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", requirements_file])
#         print("✅ Installation complete")
#     except subprocess.CalledProcessError as e:
#         print("❌ Error during installation:", e)