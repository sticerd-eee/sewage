import subprocess
import sys
import os
from pathlib import Path
import venv

# === Setup paths ===
project_dir = Path(__file__).parent.resolve()
venv_dir = project_dir / ".venv"
requirements_file = project_dir / "requirements.txt"
main_script = project_dir / "RiverNetworks.py"

# === Step 1: Create virtual environment if it doesn't exist ===
if not venv_dir.exists():
    print("Creating virtual environment...")
    builder = venv.EnvBuilder(with_pip=True)
    builder.create(venv_dir)
else:
    print("Virtual environment already exists.")

# === Step 2: Define pip/python paths ===
if sys.platform == "win32":
    pip_path = venv_dir / "Scripts" / "pip"
    python_path = venv_dir / "Scripts" / "python"
else:
    pip_path = venv_dir / "bin" / "pip"
    python_path = venv_dir / "bin" / "python"

# === Step 3: Install dependencies ===
print("Installing dependencies from requirements.txt...")
subprocess.check_call([str(pip_path), "install", "-r", str(requirements_file)])