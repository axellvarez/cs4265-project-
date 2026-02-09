# Severe Weather Infrastructure Impact Analysis System (SWIIAS)

**Course:** CS 4265 - Big Data Analytics  
**Semester:** Spring 2026  
**Author:** Axel Alvarez  

## 1. Project Overview
SWIIAS is a distributed Big Data pipeline designed to correlate real-time severe weather alerts with ground-level infrastructure impact. By ingesting heterogeneous data streams from the **NOAA National Weather Service** (weather polygons) and **Department of Transportation** (traffic incidents), the system performs spatial joins at scale to attribute traffic disruptions to specific weather events.

### Core Objectives
* **Ingest** high-velocity data streams from NOAA and DOT APIs.
* **Process** spatial data using **Apache Spark** and **Apache Sedona** (GeoSpark) to handle the $O(N \times M)$ complexity of Point-in-Polygon joins.
* **Analyze** infrastructure impact by aggregating traffic incidents within active storm cells.
* **Serve** insights via a **PostGIS** spatial database for downstream visualization in QGIS.
