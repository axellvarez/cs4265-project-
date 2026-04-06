import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, to_json
from sedona.spark import *

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
sys.path.insert(0, BASE_DIR)

from config.settings import RAW_DATA_DIR

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_spark_session():
    logging.info("Igniting Apache Spark Engine with Sedona Spatial capabilities...")
    config = SedonaContext.builder() \
        .appName("SWIIAS-Spatial-Processing") \
        .config("spark.jars.packages", 
                "org.apache.sedona:sedona-spark-3.5_2.12:1.5.1,"
                "org.datasyslab:geotools-wrapper:1.5.1-28.2") \
        .getOrCreate()
    sedona = SedonaContext.create(config)
    return sedona

def load_raw_data(spark):
    logging.info("Loading raw JSON files into Spark DataFrames...")
    noaa_path = os.path.join(RAW_DATA_DIR, "noaa_alerts_*.json")
    dot_path = os.path.join(RAW_DATA_DIR, "dot_traffic_*.json")
    outage_path = os.path.join(RAW_DATA_DIR, "power_outages_*.json")
    
    try:
        noaa_df = spark.read.option("multiline", "true").json(noaa_path)
        dot_df = spark.read.option("multiline", "true").json(dot_path)
        
        try:
            outage_df = spark.read.option("multiline", "true").json(outage_path)
        except Exception:
            outage_df = None
            logging.warning("No power outage data found in the raw directory.")
            
        return noaa_df, dot_df, outage_df
    except Exception as e:
        logging.error(f"Failed to load data: {e}")
        return None, None, None

def process_data(noaa_df, dot_df, outage_df):
    logging.info("Processing data and building spatial/attribute objects...")
    
    noaa_processed = noaa_df.filter(col("geometry").isNotNull()).select(
        col("properties.event").alias("weather_event"),
        col("properties.severity").alias("severity"),
        col("properties.areaDesc").alias("affected_areas"),
        col("properties.headline").alias("headline"),
        expr("ST_GeomFromGeoJSON(to_json(geometry))").alias("weather_geom")
    )

    dot_processed = dot_df.filter(col("geometry").isNotNull()).select(
        col("properties.iconCategory").alias("incident_type"),
        expr("ST_GeomFromGeoJSON(to_json(geometry))").alias("crash_geom")
    )

    outage_processed = None
    if outage_df is not None:
        outage_processed = outage_df.select(
            col("county"),
            col("state"),
            col("outages"),
            col("tracked")
        )

    return noaa_processed, dot_processed, outage_processed

def perform_unified_join(noaa_processed, dot_processed, outage_processed):
    logging.info("Executing Phase 1: Spatial Join (Traffic intersecting Storms)...")
    
    spatial_join_df = dot_processed.alias("traffic").join(
        noaa_processed.alias("weather"),
        expr("ST_Intersects(traffic.crash_geom, weather.weather_geom)")
    )
    
    if outage_processed is not None:
        logging.info("Executing Phase 2: Attribute Join (Correlating Outages via Text Parsing)...")
        final_df = spatial_join_df.join(
            outage_processed.alias("outage"),
            expr("weather.affected_areas LIKE CONCAT('%', outage.county, '%')"),
            "left"  
        )
        
        final_output = final_df.select(
            col("traffic.incident_type"),
            col("weather.weather_event"),
            col("weather.severity"),
            col("outage.county").alias("outage_county"),
            col("outage.outages").alias("active_outages"),
            col("weather.headline")
        )
    else:
        final_output = spatial_join_df.select(
            col("traffic.incident_type"),
            col("weather.weather_event"),
            col("weather.severity"),
            col("weather.headline")
        )
    
    return final_output

def save_processed_data(df):
    logging.info("Saving final impacts to Parquet format...")
    
    # Create the processed data directory path
    processed_dir = os.path.join(BASE_DIR, "data", "processed")
    os.makedirs(processed_dir, exist_ok=True)
    
    output_path = os.path.join(processed_dir, "swiias_impacts.parquet")
    
    try:
        # Save as Parquet, overwriting any previous runs
        df.write.mode("overwrite").parquet(output_path)
        logging.info(f"SUCCESS: Pipeline output securely saved to {output_path}")
    except Exception as e:
        logging.error(f"Failed to save Parquet file: {e}")

if __name__ == "__main__":
    spark = create_spark_session()
    noaa_df, dot_df, outage_df = load_raw_data(spark)
    
    if noaa_df and dot_df:
        noaa_processed, dot_processed, outage_processed = process_data(noaa_df, dot_df, outage_df)
        final_results = perform_unified_join(noaa_processed, dot_processed, outage_processed)
        
        logging.info("Multi-source pipeline complete! Displaying unified impacts:")
        print("\n--- SEVERE WEATHER INFRASTRUCTURE IMPACTS ---")
        final_results.show(20, truncate=False)
        
        # Call the new save function
        save_processed_data(final_results)
        
    spark.stop()
