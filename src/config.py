"""Configuration module for SKZ PACE project paths and logging setup.

Initializes directory structures required for data pipelines and configures
the loguru logger to integrate smoothly with CLI progress bars.
"""

from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

# Load environment variables from .env file if present
load_dotenv()

# =============================================================================
# PROJECT PATHS
# =============================================================================
PROJ_ROOT = Path(__file__).resolve().parents[1]

# Data Directories
DATA_DIR = PROJ_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
INTERIM_DATA_DIR = DATA_DIR / "interim"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
EXTERNAL_DATA_DIR = DATA_DIR / "external"

# Artifact Directories
MODELS_DIR = PROJ_ROOT / "models"
REPORTS_DIR = PROJ_ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"

# =============================================================================
# LOGGER CONFIGURATION
# =============================================================================
# Redirect loguru output through tqdm to prevent progress bar corruption
try:
    from tqdm import tqdm

    logger.remove(0)
    logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)
except ModuleNotFoundError:
    pass

# =============================================================================
# INFRASTRUCTURE SETUP
# =============================================================================
_project_dirs = [
    DATA_DIR,
    RAW_DATA_DIR,
    INTERIM_DATA_DIR,
    PROCESSED_DATA_DIR,
    EXTERNAL_DATA_DIR,
    MODELS_DIR,
    REPORTS_DIR,
    FIGURES_DIR,
]

# Ensure required directory structure exists on module import
for d in _project_dirs:
    d.mkdir(parents=True, exist_ok=True)
