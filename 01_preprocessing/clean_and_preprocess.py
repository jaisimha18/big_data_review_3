"""
SafeRoads Data Cleaning & Preprocessing Pipeline
Course: 23CSE352-BIG DATA ANALYTICS (Project Review 3)
Dataset: US Accidents (3.86M records, 1.4 GB)
"""

import os
import time
import json
import duckdb

RAW_CSV = "dataset/US_Accidents_1.4GB.csv"
CLEAN_PARQUET = "dataset/cleaned_accidents.parquet"
CLEAN_SAMPLE_CSV = "dataset/cleaned_accidents_sample.csv"
SUMMARY_JSON = "01_preprocessing/data_profile_summary.json"

def run_cleaning_pipeline():
    print("=" * 70)
    print("STARTING DATA CLEANING & PREPROCESSING PIPELINE (BIG DATA ANALYTICS)")
    print("=" * 70)
    start_total = time.time()
    
    con = duckdb.connect(database=":memory:")
    
    # Enable multiple threads for DuckDB
    con.execute("PRAGMA threads=8;")
    con.execute("PRAGMA memory_limit='6GB';")
    
    print(f"1. Reading and profiling raw dataset from {RAW_CSV}...")
    t0 = time.time()
    raw_count = con.execute(f"SELECT COUNT(*) FROM '{RAW_CSV}'").fetchone()[0]
    print(f"   Raw record count: {raw_count:,} (took {time.time()-t0:.2f}s)")
    
    print("\n2. Executing cleaning, transformation, and feature engineering SQL...")
    t0 = time.time()
    
    # We clean, filter anomalies, impute nulls, and create new features
    clean_sql = f"""
    CREATE TABLE cleaned_accidents AS
    SELECT
        ID,
        CAST(Severity AS INTEGER) AS Severity,
        CASE WHEN Severity >= 3 THEN 1 ELSE 0 END AS Is_Severe,
        TRY_CAST(Start_Time AS TIMESTAMP) AS Start_Time,
        TRY_CAST(End_Time AS TIMESTAMP) AS End_Time,
        CAST(Start_Lat AS DOUBLE) AS Start_Lat,
        CAST(Start_Lng AS DOUBLE) AS Start_Lng,
        COALESCE(CAST("Distance(mi)" AS DOUBLE), 0.0) AS Distance_mi,
        
        -- Temporal Features
        EXTRACT(HOUR FROM TRY_CAST(Start_Time AS TIMESTAMP)) AS Hour,
        EXTRACT(DOW FROM TRY_CAST(Start_Time AS TIMESTAMP)) AS DayOfWeek, -- 0=Sun, 6=Sat
        EXTRACT(MONTH FROM TRY_CAST(Start_Time AS TIMESTAMP)) AS Month,
        EXTRACT(YEAR FROM TRY_CAST(Start_Time AS TIMESTAMP)) AS Year,
        
        -- Duration in minutes (clipped between 5 and 720 mins for realism)
        GREATEST(5.0, LEAST(720.0, 
            ROUND(EXTRACT(EPOCH FROM (TRY_CAST(End_Time AS TIMESTAMP) - TRY_CAST(Start_Time AS TIMESTAMP))) / 60.0, 2)
        )) AS Duration_Minutes,
        
        -- Rush Hour Flag (7-9 AM, 4-6 PM on weekdays)
        CASE 
            WHEN EXTRACT(DOW FROM TRY_CAST(Start_Time AS TIMESTAMP)) NOT IN (0, 6)
                 AND EXTRACT(HOUR FROM TRY_CAST(Start_Time AS TIMESTAMP)) IN (7, 8, 9, 16, 17, 18)
            THEN 1 ELSE 0 
        END AS Is_Rush_Hour,
        
        -- Weekend Flag
        CASE 
            WHEN EXTRACT(DOW FROM TRY_CAST(Start_Time AS TIMESTAMP)) IN (0, 6) 
            THEN 1 ELSE 0 
        END AS Is_Weekend,
        
        -- Spatial Attributes
        COALESCE(City, 'Unknown') AS City,
        COALESCE(County, 'Unknown') AS County,
        COALESCE(State, 'Unknown') AS State,
        COALESCE(Timezone, 'US/Eastern') AS Timezone,
        
        -- Weather Features (imputed with domain-standard medians)
        COALESCE(CAST("Temperature(F)" AS DOUBLE), 61.0) AS Temperature_F,
        COALESCE(CAST("Humidity(%)" AS DOUBLE), 65.0) AS Humidity_Pct,
        COALESCE(CAST("Pressure(in)" AS DOUBLE), 29.92) AS Pressure_in,
        COALESCE(CAST("Visibility(mi)" AS DOUBLE), 10.0) AS Visibility_mi,
        COALESCE(CAST("Wind_Speed(mph)" AS DOUBLE), 7.0) AS Wind_Speed_mph,
        COALESCE(CAST("Precipitation(in)" AS DOUBLE), 0.0) AS Precipitation_in,
        
        -- Weather Condition Grouping
        CASE
            WHEN LOWER(Weather_Condition) LIKE '%rain%' 
                 OR LOWER(Weather_Condition) LIKE '%drizzle%' 
                 OR LOWER(Weather_Condition) LIKE '%shower%' THEN 'Rain'
            WHEN LOWER(Weather_Condition) LIKE '%snow%' 
                 OR LOWER(Weather_Condition) LIKE '%ice%' 
                 OR LOWER(Weather_Condition) LIKE '%wintry%' 
                 OR LOWER(Weather_Condition) LIKE '%sleet%' THEN 'Snow/Ice'
            WHEN LOWER(Weather_Condition) LIKE '%fog%' 
                 OR LOWER(Weather_Condition) LIKE '%mist%' 
                 OR LOWER(Weather_Condition) LIKE '%haze%' 
                 OR LOWER(Weather_Condition) LIKE '%smoke%' THEN 'Fog/Haze'
            WHEN LOWER(Weather_Condition) LIKE '%cloud%' 
                 OR LOWER(Weather_Condition) LIKE '%overcast%' THEN 'Cloudy'
            WHEN LOWER(Weather_Condition) LIKE '%clear%' 
                 OR LOWER(Weather_Condition) LIKE '%fair%' THEN 'Clear'
            WHEN LOWER(Weather_Condition) LIKE '%thunder%' 
                 OR LOWER(Weather_Condition) LIKE '%storm%' 
                 OR LOWER(Weather_Condition) LIKE '%tornado%' THEN 'Storm'
            ELSE 'Other'
        END AS Weather_Category,
        
        -- Road Infrastructure Flags (converted to 0 or 1 integer)
        CASE WHEN Amenity = 'True' THEN 1 ELSE 0 END AS Amenity,
        CASE WHEN Bump = 'True' THEN 1 ELSE 0 END AS Bump,
        CASE WHEN Crossing = 'True' THEN 1 ELSE 0 END AS Crossing,
        CASE WHEN Give_Way = 'True' THEN 1 ELSE 0 END AS Give_Way,
        CASE WHEN Junction = 'True' THEN 1 ELSE 0 END AS Junction,
        CASE WHEN No_Exit = 'True' THEN 1 ELSE 0 END AS No_Exit,
        CASE WHEN Railway = 'True' THEN 1 ELSE 0 END AS Railway,
        CASE WHEN Roundabout = 'True' THEN 1 ELSE 0 END AS Roundabout,
        CASE WHEN Station = 'True' THEN 1 ELSE 0 END AS Station,
        CASE WHEN Stop = 'True' THEN 1 ELSE 0 END AS Stop,
        CASE WHEN Traffic_Calming = 'True' THEN 1 ELSE 0 END AS Traffic_Calming,
        CASE WHEN Traffic_Signal = 'True' THEN 1 ELSE 0 END AS Traffic_Signal,
        
        -- Day/Night illumination indicator
        CASE 
            WHEN Sunrise_Sunset = 'Day' THEN 'Day'
            WHEN Sunrise_Sunset = 'Night' THEN 'Night'
            WHEN EXTRACT(HOUR FROM TRY_CAST(Start_Time AS TIMESTAMP)) BETWEEN 6 AND 18 THEN 'Day'
            ELSE 'Night'
        END AS Day_Night
        
    FROM '{RAW_CSV}'
    WHERE 
        ID IS NOT NULL
        AND Severity BETWEEN 1 AND 4
        AND TRY_CAST(Start_Time AS TIMESTAMP) IS NOT NULL
        AND TRY_CAST(End_Time AS TIMESTAMP) IS NOT NULL
        AND TRY_CAST(End_Time AS TIMESTAMP) >= TRY_CAST(Start_Time AS TIMESTAMP)
        AND CAST(Start_Lat AS DOUBLE) BETWEEN 24.0 AND 50.0       -- Valid US Continental Latitude
        AND CAST(Start_Lng AS DOUBLE) BETWEEN -125.0 AND -66.0   -- Valid US Continental Longitude
        AND COALESCE(CAST("Distance(mi)" AS DOUBLE), 0.0) >= 0.0
        AND COALESCE(CAST("Distance(mi)" AS DOUBLE), 0.0) <= 100.0 -- Outlier filter
    """
    
    con.execute(clean_sql)
    clean_count = con.execute("SELECT COUNT(*) FROM cleaned_accidents").fetchone()[0]
    print(f"   Cleaned record count: {clean_count:,} (Removed {raw_count - clean_count:,} invalid/outlier rows)")
    print(f"   Cleaning and feature creation completed in {time.time()-t0:.2f}s")
    
    print("\n3. Exporting cleaned data to Parquet format (optimized columnar storage)...")
    t0 = time.time()
    con.execute(f"COPY cleaned_accidents TO '{CLEAN_PARQUET}' (FORMAT PARQUET, COMPRESSION SNAPPY);")
    parquet_size_mb = os.path.getsize(CLEAN_PARQUET) / (1024 * 1024)
    print(f"   Exported {CLEAN_PARQUET} ({parquet_size_mb:.2f} MB in {time.time()-t0:.2f}s)")
    
    print("\n4. Exporting representative sample CSV (150,000 rows) for fast RDD benchmarking...")
    t0 = time.time()
    con.execute(f"""
        COPY (SELECT * FROM cleaned_accidents USING SAMPLE 150000) 
        TO '{CLEAN_SAMPLE_CSV}' (HEADER, DELIMITER ',');
    """)
    sample_size_mb = os.path.getsize(CLEAN_SAMPLE_CSV) / (1024 * 1024)
    print(f"   Exported {CLEAN_SAMPLE_CSV} ({sample_size_mb:.2f} MB in {time.time()-t0:.2f}s)")
    
    # Statistical summaries
    print("\n5. Computing summary statistics for reporting...")
    sev_stats = con.execute("""
        SELECT Severity, COUNT(*) as count, ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM cleaned_accidents), 2) as pct
        FROM cleaned_accidents
        GROUP BY Severity
        ORDER BY Severity
    """).fetchall()
    
    weather_stats = con.execute("""
        SELECT Weather_Category, COUNT(*) as count, ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM cleaned_accidents), 2) as pct
        FROM cleaned_accidents
        GROUP BY Weather_Category
        ORDER BY count DESC
    """).fetchall()
    
    state_stats = con.execute("""
        SELECT State, COUNT(*) as count
        FROM cleaned_accidents
        GROUP BY State
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()
    
    infra_stats = con.execute("""
        SELECT 
            SUM(Traffic_Signal) AS Traffic_Signals,
            SUM(Junction) AS Junctions,
            SUM(Crossing) AS Crossings,
            SUM(Stop) AS Stop_Signs,
            SUM(Railway) AS Railways
        FROM cleaned_accidents
    """).fetchone()

    total_time = round(time.time() - start_total, 2)
    
    summary = {
        "pipeline_name": "SafeRoads US Accidents Data Cleaning & Preprocessing",
        "raw_csv_path": RAW_CSV,
        "raw_record_count": raw_count,
        "cleaned_record_count": clean_count,
        "dropped_invalid_rows": raw_count - clean_count,
        "retention_rate_pct": round(clean_count * 100.0 / raw_count, 2),
        "parquet_output_path": CLEAN_PARQUET,
        "parquet_size_mb": round(parquet_size_mb, 2),
        "sample_csv_path": CLEAN_SAMPLE_CSV,
        "sample_size_mb": round(sample_size_mb, 2),
        "execution_time_seconds": total_time,
        "severity_distribution": [{"severity": s, "count": c, "pct": p} for s, c, p in sev_stats],
        "weather_category_distribution": [{"category": w, "count": c, "pct": p} for w, c, p in weather_stats],
        "top_10_states": [{"state": s, "count": c} for s, c in state_stats],
        "road_infrastructure_presence": {
            "traffic_signals": infra_stats[0],
            "junctions": infra_stats[1],
            "crossings": infra_stats[2],
            "stop_signs": infra_stats[3],
            "railways": infra_stats[4]
        }
    }
    
    with open(SUMMARY_JSON, "w") as f:
        json.dump(summary, f, indent=2)
        
    print(f"\nSaved comprehensive summary to {SUMMARY_JSON}")
    print(f"PIPELINE COMPLETED SUCCESSFULLY IN {total_time}s!")
    print("=" * 70)

if __name__ == "__main__":
    run_cleaning_pipeline()
