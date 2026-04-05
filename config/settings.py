import os
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

# API Settings
USER_AGENT = os.getenv("USER_AGENT", "SWIIAS-Research-Project")
DOT_API_KEY = os.getenv("DOT_API_KEY")

# Directory Settings
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR = os.path.join(BASE_DIR, os.getenv("RAW_DATA_PATH", "data/raw"))
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, os.getenv("PROCESSED_DATA_PATH", "data/processed"))

# Spatial Bounds (Continental US)
BOUNDS = {
    "lat_min": float(os.getenv("LAT_MIN", 24.396308)),
    "lat_max": float(os.getenv("LAT_MAX", 49.384358)),
    "lon_min": float(os.getenv("LON_MIN", -125.000000)),
    "lon_max": float(os.getenv("LON_MAX", -66.934570)),
}
