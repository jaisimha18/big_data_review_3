import duckdb
import json
import time

CSV_PATH = "dataset/US_Accidents_1.4GB.csv"

def profile():
    print(f"Connecting to DuckDB to inspect {CSV_PATH}...")
    start_time = time.time()
    con = duckdb.connect(database=":memory:")
    
    # Get row count
    total_rows = con.execute(f"SELECT COUNT(*) FROM '{CSV_PATH}'").fetchone()[0]
    print(f"Total records in dataset: {total_rows:,}")
    
    # Describe columns
    schema_info = con.execute(f"DESCRIBE SELECT * FROM '{CSV_PATH}' LIMIT 1").fetchall()
    columns = [col[0] for col in schema_info]
    print(f"Total columns: {len(columns)}")
    print("Columns:", columns)
    
    # Severity distribution
    sev_dist = con.execute(f"""
        SELECT Severity, COUNT(*) as count, ROUND(COUNT(*) * 100.0 / {total_rows}, 2) as pct
        FROM '{CSV_PATH}'
        GROUP BY Severity
        ORDER BY Severity
    """).fetchall()
    print("\n--- Severity Distribution ---")
    for sev, cnt, pct in sev_dist:
        print(f"Severity {sev}: {cnt:,} ({pct}%)")
        
    # Top states
    top_states = con.execute(f"""
        SELECT State, COUNT(*) as count
        FROM '{CSV_PATH}'
        WHERE State IS NOT NULL
        GROUP BY State
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()
    print("\n--- Top 10 States by Accidents ---")
    for st, cnt in top_states:
        print(f"{st}: {cnt:,}")

    # Top Weather Conditions
    top_weather = con.execute(f"""
        SELECT Weather_Condition, COUNT(*) as count
        FROM '{CSV_PATH}'
        WHERE Weather_Condition IS NOT NULL
        GROUP BY Weather_Condition
        ORDER BY count DESC
        LIMIT 10
    """).fetchall()
    print("\n--- Top 10 Weather Conditions ---")
    for wc, cnt in top_weather:
        print(f"{wc}: {cnt:,}")

    # Null analysis for key columns
    key_cols = [
        "Severity", "Start_Time", "End_Time", "Start_Lat", "Start_Lng", 
        "Distance(mi)", "State", "City", "Temperature(F)", "Humidity(%)", 
        "Pressure(in)", "Visibility(mi)", "Wind_Speed(mph)", "Precipitation(in)",
        "Weather_Condition", "Crossing", "Junction", "Traffic_Signal", "Sunrise_Sunset"
    ]
    null_queries = [f"SUM(CASE WHEN \"{col}\" IS NULL THEN 1 ELSE 0 END) as null_{i}" for i, col in enumerate(key_cols)]
    query = f"SELECT {', '.join(null_queries)} FROM '{CSV_PATH}'"
    null_counts = con.execute(query).fetchone()
    
    print("\n--- Null Counts in Key Columns ---")
    null_report = {}
    for col, null_val in zip(key_cols, null_counts):
        null_report[col] = {
            "null_count": int(null_val),
            "null_pct": round(null_val * 100.0 / total_rows, 2)
        }
        print(f"{col}: {null_val:,} ({null_report[col]['null_pct']}%)")
        
    elapsed = round(time.time() - start_time, 2)
    print(f"\nProfiling completed in {elapsed} seconds.")
    
    output_profile = {
        "dataset_name": "US_Accidents_1.4GB.csv",
        "total_rows": total_rows,
        "total_columns": len(columns),
        "columns": columns,
        "severity_distribution": [{"severity": s, "count": c, "percentage": p} for s, c, p in sev_dist],
        "top_states": [{"state": s, "count": c} for s, c in top_states],
        "top_weather": [{"weather": w, "count": c} for w, c in top_weather],
        "null_report": null_report,
        "profiling_time_seconds": elapsed
    }
    
    with open("01_preprocessing/data_profile_raw.json", "w") as f:
        json.dump(output_profile, f, indent=2)
    print("Saved raw profile to 01_preprocessing/data_profile_raw.json")

if __name__ == "__main__":
    profile()
