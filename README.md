# SWIIAS: Severe Weather Infrastructure Impact Analysis System

**Author:** Axel Alvarez
**Course:** CS4265 - Big Data Analytics  
**Milestone:** M3 - Complete Implementation  

---

## Project Overview
The Severe Weather Infrastructure Impact Analysis System (SWIIAS) is a robust Big Data pipeline designed to identify real-time correlations between extreme weather, traffic congestion, and power grid failures. The system prioritizes the Southeastern United States and 10 major US metro areas to analyze how severe weather directly impacts critical infrastructure.

## Tech Stack
* Core Engine: Apache Spark (PySpark) 3.5.0
* Spatial Processing: Apache Sedona 1.5.1
* Language: Python 3.9+
* Storage Format: Apache Parquet (Columnar Storage)
* Data Sources: 
    - NOAA API: Real-time weather alerts (Semi-structured GeoJSON)
    - TomTom/DOT API: Traffic incidents and flow (Structured JSON)
    - PowerOutage.us: Live utility status (Unstructured HTML via Web Scraping)

---

## Setup & Installation

### 1. Fresh Clone Test
To verify the pipeline in a clean environment, follow these steps in your terminal:

git clone <your-repository-url>
cd <your-repository-directory>

# Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# Install all required dependencies
pip install -r requirements.txt

### 2. Configuration
Create a .env file in the root directory to store your API credentials:

TOMTOM_API_KEY=your_api_key_here

*Note: NOAA and PowerOutage.us sources are accessed via public endpoints and do not require keys for this implementation.*

---

## Running the Pipeline

The pipeline is designed to run end-to-end without manual intervention.

### Stage 1: Data Acquisition (Ingestion)
Run the ingestion scripts to populate the data/raw/ directory:

python3 src/ingestion/fetch_data.py      # Ingests NOAA Weather
python3 src/ingestion/fetch_traffic.py   # Ingests DOT Traffic
python3 src/ingestion/scrape_outages.py  # Scrapes Power Outages

### Stage 2: Data Transformation (Processing)
Run the Spark engine to perform spatial joins and attribute normalization:

python3 src/processing/spatial_join.py

---

## Data Dictionary (Silver Layer)
The final output is saved as a partitioned Parquet dataset in data/processed/swiias_impacts.parquet.

| Column | Type | Description |
| :--- | :--- | :--- |
| incident_type | String | Category of traffic event (e.g., Jam, Accident, Construction) |
| weather_event | String | Type of NOAA alert (e.g., Flash Flood Warning, Tornado Watch) |
| severity | String | Urgency of the weather event (Extreme, Severe, Moderate) |
| outage_county | String | The county name matched via attribute join |
| active_outages | Integer | Number of customers currently without power |
| headline | String | Full text headline of the active weather alert |

---

## Pipeline Features & Robustness
* Multi-Source Integration: Successfully integrates structured, semi-structured, and unstructured data into a unified schema.
* Spatial Intersection: Utilizes Apache Sedona for spatial join performance, intersecting traffic point geometries with weather alert polygons.
* Error Handling (Circuit Breaker): The web scraper includes a "Circuit Breaker" pattern. If the source website blocks the request via Cloudflare, the script injects a "Simulated County" record to ensure the downstream Spark pipeline completes successfully.
* Graceful Degradation: Uses LEFT JOIN logic to ensure that traffic and weather data are preserved even if local power outage data is unavailable for a specific coordinate.
