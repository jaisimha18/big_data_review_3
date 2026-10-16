# SafeRoads: Scalable Traffic Accident Severity Prediction & Analytics
## Course: 23CSE352 - Big Data Analytics | Project Review 3 (Apache Spark)

---

### Slide 1 – Title
- **Project Title:** SafeRoads: Scalable Traffic Accident Severity Prediction & Analytics
- **Course Name:** 23CSE352 - Big Data Analytics (Project Review 3 - 15 Marks)
- **Technology Stack:** Apache Spark, Scala RDDs, PySpark MLlib, Snappy Parquet, Python
- **Team Details:** [Student Name / Register Numbers]
- **Department & Institution:** Department of Computer Science & Engineering, Amrita Vishwa Vidyapeetham

---

### Slide 2 – Problem Statement
- **Real-World Problem:**
  - Road traffic accidents represent a premier public safety crisis, resulting in over 40,000 fatalities, millions of debilitating injuries, and over $870 Billion in annual economic damage across the United States.
  - Emergency medical response systems (EMS) frequently suffer from delayed triage and misallocated critical-care units due to insufficient situational awareness in the immediate aftermath of collisions.
  - Real-world traffic telemetry generates millions of records continuously from IoT road sensors, law enforcement logs, and weather stations, exceeding the processing capabilities of traditional relational databases.
- **Why the Problem is Important:**
  - **Golden Hour Survival:** Timely prediction of high-severity accidents enables immediate dispatch of trauma response units, directly saving lives.
  - **Proactive Traffic Management:** Identifies high-risk corridors and weather-infrastructure friction points for state Departments of Transportation (DOTs).
  - **Big Data Scale:** Requires distributed, fault-tolerant processing using Apache Spark across multi-node clusters.

---

### Slide 3 – Dataset
- **Dataset Name & Source:** US Accidents (Nationwide Traffic Accident Dataset, 2016–2023) curated from real-time road sensors, law enforcement reports, and DOT traffic cameras (Moosavi et al., arXiv/Kaggle).
- **Total Records:**
  - Raw Records: 3,864,197 records (~1.4 GB raw CSV).
  - Cleaned & Validated: 3,864,157 records (99.999% retention, 40 corrupted rows dropped).
  - Parquet Optimization: Converted to Snappy-compressed columnar Parquet (183.45 MB, 8x storage reduction).
- **Important Attributes:**
  - *Target Variable:* `Severity` (Scale 1 to 4) & `Is_Severe` (Binary: 0=Minor [1-2], 1=Severe [3-4]).
  - *Spatio-Temporal:* `Start_Time`, `End_Time`, `Start_Lat`, `Start_Lng`, `State`, `City`, `County`, `Hour`, `DayOfWeek`, `Month`.
  - *Environmental/Weather:* `Temperature(F)`, `Humidity(%)`, `Pressure(in)`, `Visibility(mi)`, `Wind_Speed(mph)`, `Precipitation(in)`, `Weather_Category`.
  - *Road Infrastructure:* `Traffic_Signal`, `Junction`, `Crossing`, `Stop`, `Amenity`, `Railway`.
  - *Engineered Indicators:* `Duration_Minutes` (delay duration), `Is_Rush_Hour` (7-9 AM, 4-6 PM), `Is_Weekend`.

---

### Slide 4 – Project Workflow
```
[Raw Dataset: US_Accidents (1.4 GB CSV, 3.86M rows)]
                      │
                      ▼
[Data Cleaning & Preprocessing (DuckDB / SQL Engine)]
- Outlier filtering, null imputation, timestamp parsing
- Feature engineering (Rush Hour, Duration, Weather Grouping)
- Snappy Parquet (183 MB) & Benchmarking Sample (150K rows)
                      │
                      ▼
[Distributed RDD Processing (Scala + Apache Spark)]
- sc.textFile() RDD Creation & Partition Inspection
- Narrow Transformations: filter(), map(), flatMap()
- Wide Key-Value Transformations: reduceByKey(), mapValues(), sortByKey()
- Spark Actions: count(), take(), reduce(), collect()
- Caching & Persistence: persist(StorageLevel.MEMORY_AND_DISK)
- Lineage Graph & DAG execution analysis (toDebugString)
                      │
                      ▼
[Machine Learning & Modeling (PySpark MLlib)]
- StringIndexer & VectorAssembler dense feature pipeline
- 80/20 Stratified Train-Test Split (96K train, 24K test)
- Model Training: Random Forest Classifier (100 trees, depth 12) & Logistic Regression
                      │
                      ▼
[Evaluation & Benchmarking]
- Accuracy (72.57%), Weighted Precision (82.62%), F1-Score (75.22%)
- ROC-AUC: 0.8136 | Feature Importance Rankings
                      │
                      ▼
[Visualization & Decision Support System]
- 6 Publication-quality charts (Severity distribution, Rush Hour Heatmap, Weather Impact, Confusion Matrix)
- Actionable insights for DOTs and Emergency Dispatchers
```

---

### Slide 5 – Scala + Spark Implementation
- **RDD Creation:** Initialized via `sc.textFile("dataset/cleaned_accidents_sample.csv")` with 4 default executor partitions.
- **Transformations Demonstrated:**
  1. `filter()`: Removes header line, filters empty strings, and subsets severe crashes (`severity >= 3`).
  2. `map()`: Tokenizes CSV records into strongly-typed `AccidentRecord` case classes.
  3. `flatMap()`: Emits analytical risk factor tokens (`Risk_Factor:Rush_Hour`, `Risk_Factor:Night_Time`, `Adverse_Weather:Snow`, `Infrastructure:Near_Traffic_Signal`).
  4. `reduceByKey()`: Computes total accidents per state and hourly traffic density.
  5. `mapValues()`: Calculates average accident severity alongside total frequency per weather category.
- **Actions Demonstrated:**
  1. `count()`: Evaluates total dataset instances across partitions.
  2. `take(n)`: Retrieves sample parsed records into the driver console.
  3. `reduce()`: Sums total road disruption impact mileage (>2.4M miles).
  4. `collect()`: Retrieves full state aggregation summaries to driver memory.
- **Key-Value Pair RDD Operations:**
  - `(State, 1).reduceByKey(_ + _)`: State-level frequency ranking.
  - `(Weather, (Severity, 1)).reduceByKey(...).mapValues(...)`: Dual-metric aggregation.

---

### Slide 6 – Spark Concepts
- **Partitions:**
  - Spark partitions data across cluster nodes based on file blocks (typically 128 MB chunks).
  - In our run, `getNumPartitions` shows parallel partition slices executing across available CPU cores.
- **Directed Acyclic Graph (DAG) & Stages:**
  - Spark translates transformations into a logical DAG before execution.
  - The DAG Scheduler splits the plan into **Stages** based on shuffle dependencies.
- **RDD Lineage & Fault Tolerance:**
  - Tracked via `toDebugString`. RDDs are immutable and record their historical lineage.
  - If a worker node crashes mid-computation, Spark recomputes only the lost partition from lineage rather than restarting the entire job.
- **Narrow vs. Wide Dependencies:**
  - *Narrow (PipelinedRDD):* `map()` and `filter()` require data only from a single parent partition, executing in-memory without network transfer.
  - *Wide (ShuffledRDD):* `reduceByKey()` and `sortBy()` require data shuffling across cluster nodes, creating stage boundaries.
- **cache() vs. persist():**
  - Storing parsed records using `StorageLevel.MEMORY_AND_DISK` eliminated redundant CSV tokenization, yielding immediate speedup across successive actions.

---

### Slide 7 – Data Processing Results
- **Geographic Clustering (Top States):**
  - **California (CA):** 871,217 accidents (22.5% of total nationwide volume).
  - **Florida (FL):** 439,903 accidents (11.4%).
  - **Texas (TX):** 291,520 accidents (7.5%).
  - Top 3 states represent over 41.4% of total recorded US accidents due to high vehicle-miles-traveled (VMT) and dense interstate networks.
- **Temporal Peaks:**
  - Morning rush peak at 08:00 AM (sharp commuter convergence).
  - Evening rush peak at 05:00 PM (17:00) with sustained high congestion until 07:00 PM.
- **Environmental & Infrastructure Risk:**
  - Clear/Fair weather accounts for 58.2% of raw occurrences, but adverse weather (Snow/Ice and Fog) exhibits a 35% higher severe accident ratio.
  - Over 28% of accidents occurred within 50 meters of traffic signals or major highway junctions.

---

### Slide 8 – PySpark Machine Learning
- **Features Used:**
  - Continuous: `Duration_Minutes`, `Distance_mi`, `Temperature_F`, `Humidity_Pct`, `Pressure_in`, `Visibility_mi`, `Wind_Speed_mph`, `Hour`, `Month`.
  - Categorical: `Weather_Category`, `Day_Night`.
  - Binary Indicators: `Traffic_Signal`, `Junction`, `Crossing`, `Stop`, `Amenity`, `Railway`, `Is_Rush_Hour`, `Is_Weekend`.
- **Pipeline Architecture:**
  - `StringIndexer` -> `OneHotEncoder` -> `VectorAssembler` -> `RandomForestClassifier`.
- **Train/Test Data Split:**
  - 80% Training set (96,000 samples) / 20% Testing set (24,000 samples) with seed=42 and stratified target distribution.
- **Algorithms Implemented:**
  - Primary: Random Forest Classifier (100 estimators, max depth 12, balanced class weighting).
  - Baseline: Logistic Regression for linear decision boundary benchmark.

---

### Slide 9 – Model Evaluation
- **Comparative Performance Table:**

| Evaluation Metric | PySpark Random Forest | Logistic Regression (Baseline) | Performance Lift |
| :--- | :---: | :---: | :---: |
| **Accuracy** | **72.57%** | 51.29% | **+21.28%** |
| **Weighted Precision** | **82.62%** | 76.39% | **+6.23%** |
| **Weighted Recall** | **72.57%** | 51.29% | **+21.28%** |
| **Weighted F1-Score** | **75.22%** | 55.63% | **+19.59%** |
| **ROC-AUC Score** | **0.8136** | 0.6649 | **+0.1487** |

- **Key Feature Importances:**
  1. `Duration_Minutes`: **31.11%** (Severity strongly correlates with road clearance duration).
  2. `Distance_mi`: **25.32%** (Spatial extent of physical lane obstruction).
  3. `Traffic_Signal`: **7.74%** (High-energy perpendicular collision risk at intersections).
  4. `Crossing`: **5.86%** (Pedestrian and cyclist transition zones).
  5. `Pressure_in`: **5.02%** (Atmospheric pressure drops preceding severe precipitation).

---

### Slide 10 – Visualizations
*(Includes all 4 embedded charts)*
- **Figure 1:** Accident Severity Distribution (Bar chart and proportional pie chart).
- **Figure 2:** Hourly & Day-of-Week Density Heatmap (Clear visualization of rush hour clusters).
- **Figure 3:** Impact of Weather Conditions on Accident Frequency & Severity.
- **Figure 4:** Top 10 US States with Severity Breakdown.
- **Figure 5:** Feature Importance Rankings from Random Forest.
- **Figure 6:** Normalized Confusion Matrix showing balanced classification performance.

---

### Slide 11 – Results & Discussion
- **Analytical Insights:**
  - Non-linear tree ensemble algorithms vastly outperform linear models on traffic accident data due to complex multi-variable interactions (e.g., Rain + Night + Traffic Signal dramatically elevates severity beyond additive linear effects).
  - Traffic congestion (rush hour) increases accident frequency, while open highway conditions combined with adverse weather increase accident severity.
- **Real-World Usefulness:**
  - **EMS Trauma Triage:** Automatically routes severe crash alerts to level-1 trauma centers within the "golden hour".
  - **Smart Infrastructure:** Department of Transportation can dynamically adjust electronic variable speed limit (VSL) signs during predicted high-risk conditions.
  - **Patrol Staging:** Law enforcement can preposition highway patrol units at high-risk junctions prior to peak evening rush hours.

---

### Slide 12 – Conclusion & Future Enhancements
- **Summary of Achievements:**
  - Successfully ingested, cleaned, and preprocessed 3.86M records (~1.4 GB) with 99.999% retention.
  - Implemented complete Scala + Spark RDD analytics with transformations (`map`, `filter`, `flatMap`, `reduceByKey`), actions (`count`, `take`, `reduce`, `collect`), caching, and DAG lineage explanation.
  - Built an end-to-end PySpark Machine Learning pipeline achieving 82.62% precision and 0.8136 ROC-AUC.
  - Delivered 6 publication-ready visualizations and comprehensive project documentation.
- **Future Enhancements:**
  - **Streaming Analytics:** Ingest real-time telemetry via Apache Kafka and Spark Structured Streaming.
  - **Graph Analytics:** Model regional road networks using GraphFrames to predict cascading traffic congestion.
  - **Cloud Deployment:** Deploy on AWS EMR or Databricks for multi-node cluster scaling.
