import requests
import json
import os
import logging
from datetime import datetime

# Setup logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataIngestor:
    def __init__(self, base_dir="data/raw"):
        self.base_dir = base_dir
        # Creates the persistent storage directory 
        os.makedirs(self.base_dir, exist_ok=True)

    def save_json(self, data, filename):
        path = os.path.join(self.base_dir, filename)
        with open(path, 'w') as f:
            json.dump(data, f, indent=4)
        logging.info(f"SUCCESS: Saved {len(data.get('features', []))} records to {path}")

    def fetch_noaa_alerts(self):
        # NOAA NWS API 
        url = "https://api.weather.gov/alerts/active?area=GA,AL,FL"
        headers = {'User-Agent': '(SWIIAS_Project, aalvarez@student.ksu.edu)'}
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            self.save_json(data, f"noaa_alerts_{datetime.now().strftime('%Y%m%d_%H%M')}.json")
            return data
        except Exception as e:
            logging.error(f"Ingestion Failed: {e}")
            return None

if __name__ == "__main__":
    ingestor = DataIngestor()
    print("--- Running Ingestion Layer ---")
    ingestor.fetch_noaa_alerts()
