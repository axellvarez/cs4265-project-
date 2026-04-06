import requests
import json
import os
import logging
import time
from datetime import datetime
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
sys.path.insert(0, BASE_DIR)

from config.settings import DOT_API_KEY, RAW_DATA_DIR

# Set up professional logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DOTIngestor:
    def __init__(self):
        self.url = "https://api.tomtom.com/traffic/services/5/incidentDetails"
        
        # TomTom restricts bounding boxes to a very small size.
        # To get a large volume, we target 10 of the busiest US metro areas.
        self.metros = {
            "Atlanta": "-84.6,33.5,-84.1,34.0",
            "New York": "-74.3,40.5,-73.7,41.0",
            "Los Angeles": "-118.6,33.7,-118.0,34.3",
            "Chicago": "-88.0,41.6,-87.4,42.1",
            "Houston": "-95.7,29.5,-95.1,30.1",
            "Dallas": "-97.2,32.5,-96.5,33.1",
            "Miami": "-80.4,25.5,-80.0,26.1",
            "Seattle": "-122.5,47.4,-122.0,47.8",
            "Washington DC": "-77.3,38.7,-76.8,39.1",
            "Boston": "-71.3,42.2,-70.9,42.5"
        }

    def fetch_traffic(self):
        logging.info("Fetching traffic incidents across major US Metro areas...")
        
        if not DOT_API_KEY:
            logging.error("DOT_API_KEY is missing! Check your .env file.")
            return None

        all_incidents = []
        
        for city, bbox in self.metros.items():
            logging.info(f"Pulling data for {city}...")
            params = {
                "key": DOT_API_KEY,
                "bbox": bbox,
                "fields": "{incidents{type,geometry{type,coordinates},properties{iconCategory}}}"
            }
            
            try:
                response = requests.get(self.url, params=params, timeout=15)
                response.raise_for_status()
                
                data = response.json()
                incidents = data.get("incidents", [])
                all_incidents.extend(incidents)
                logging.info(f"Retrieved {len(incidents)} incidents from {city}.")
                
                # Sleep to respect API rate limits
                time.sleep(1)
                
            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to fetch {city}: {e}")

        logging.info(f"Total traffic incidents retrieved: {len(all_incidents)}")
        return all_incidents

    def save_data(self, data):
        if not data:
            logging.warning("No data retrieved. Skipping save operation.")
            return

        os.makedirs(RAW_DATA_DIR, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"dot_traffic_{timestamp}.json"
        filepath = os.path.join(RAW_DATA_DIR, filename)
        
        try:
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=4)
            logging.info(f"SUCCESS: Saved {len(data)} records to {filepath}")
        except IOError as e:
            logging.error(f"Failed to write data to file: {e}")

if __name__ == "__main__":
    ingestor = DOTIngestor()
    traffic_data = ingestor.fetch_traffic()
    ingestor.save_data(traffic_data)
