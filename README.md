# Severe Weather Infrastructure Impact Analysis System (SWIIAS)

**Course:** CS 4265 - Big Data Analytics  
**Semester:** Spring 2026  
**Author:** Axel Alvarez  
**Status:** Milestone 2 - Initial Implementation 

---

## 1. Project Overview
SWIIAS is a distributed Big Data pipeline designed to correlate real-time severe weather alerts with ground-level infrastructure impact. By ingesting heterogeneous data streams from the **NOAA National Weather Service** (weather polygons) and **Department of Transportation** (traffic incidents), the system performs spatial joins at scale to attribute traffic disruptions to specific weather events.

## 2. Milestone 2: Current Status
Milestone 2 serves as the "proof of concept" for the data acquisition and storage layers.
* **Data Acquisition:** Automated ingestion from the NOAA NWS API is fully operational.
* **Persistent Storage:** Raw data is successfully persisted to a local "Bronze" layer in JSON format, ensuring data exists between runs.
* **Pipeline Framework:** A modular directory structure has been established to separate ingestion, storage, and future distributed processing stages.


## 3. Setup and Execution
To run this proof of concept, follow the instructions below to ensure all dependencies are met.

### Prerequisites
* **Python 3.9+**
* **Internet connection** (for real-time API access)

### Installation
Clone the repository and install the required libraries listed in `requirements.txt`:
```bash
pip3 install -r requirements.txt
