import requests
import json
import os
import logging
from datetime import datetime
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))

sys.path.insert(0, BASE_DIR)

# Import our externalized settings
from config.settings import USER_AGENT, RAW_DATA_DIR

# Set up professional logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class NOAAIngestor:
    def __init__(self):
        self.url = "https://api.weather.gov/alerts/active"
        self.headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/geo+json"
        }
        self.params = {
            "status": "actual"
        }

    def fetch_alerts(self):
        logging.info("Fetching national active alerts from NOAA API...")
        try:
            # Added a timeout for robust error handling
            response = requests.get(self.url, headers=self.headers, params=self.params, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            features = data.get("features", [])
            logging.info(f"Successfully retrieved {len(features)} alerts.")
            return features
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch data from NOAA API: {e}")
            return None

    def save_data(self, data):
        if not data:
            logging.warning("No data retrieved. Skipping save operation.")
            return

        # Ensure the Bronze layer directory exists using our externalized settings
        os.makedirs(RAW_DATA_DIR, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"noaa_alerts_{timestamp}.json"
        filepath = os.path.join(RAW_DATA_DIR, filename)
        
        try:
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=4)
            logging.info(f"SUCCESS: Saved {len(data)} records to {filepath}")
        except IOError as e:
            logging.error(f"Failed to write data to file: {e}")

if __name__ == "__main__":
    ingestor = NOAAIngestor()
    alerts_data = ingestor.fetch_alerts()
    ingestor.save_data(alerts_data)
