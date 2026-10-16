"""
SafeRoads: Automated PowerPoint Generator
Course: 23CSE352-BIG DATA ANALYTICS (Project Review 3 - Apache Spark)
Generates 12-Slide High-Impact Executive Presentation in 16:9 Widescreen format.
"""

import os
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.enum.shapes import MSO_SHAPE

OUTPUT_DIR = "05_presentation"
os.makedirs(OUTPUT_DIR, exist_ok=True)
PPTX_PATH = f"{OUTPUT_DIR}/SafeRoads_Review3_Presentation.pptx"

# Color Palette: Deep Modern Tech Theme
BG_COLOR = RGBColor(15, 23, 42)       # #0f172a Dark Slate
CARD_BG = RGBColor(30, 41, 59)        # #1e293b Navy Surface
ACCENT_CYAN = RGBColor(56, 189, 248)  # #38bdf8 Cyan
ACCENT_GOLD = RGBColor(245, 158, 11)  # #f59e0b Amber
TEXT_WHITE = RGBColor(248, 250, 252)  # #f8fafc Light
TEXT_MUTED = RGBColor(148, 163, 184)  # #94a3b8 Muted Grey
ACCENT_GREEN = RGBColor(52, 211, 153) # #34d399 Emerald

def create_deck():
    prs = Presentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)
    blank_layout = prs.slide_layouts[6] # Blank slide

    def add_base_slide(title_text, category="23CSE352: BIG DATA ANALYTICS | REVIEW 3"):
        slide = prs.slides.add_slide(blank_layout)
        
        # Dark Background
        bg = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, Inches(13.333), Inches(7.5))
        bg.fill.solid()
        bg.fill.fore_color.rgb = BG_COLOR
        bg.line.fill.background()
        
        # Header banner
        header_box = slide.shapes.add_textbox(Inches(0.8), Inches(0.4), Inches(11.733), Inches(0.9))
        tf = header_box.text_frame
        tf.word_wrap = True
        
        p_cat = tf.paragraphs[0]
        p_cat.text = category.upper()
        p_cat.font.size = Pt(10)
        p_cat.font.bold = True
        p_cat.font.color.rgb = ACCENT_CYAN
        
        p_title = tf.add_paragraph()
        p_title.text = title_text
        p_title.font.size = Pt(22)
        p_title.font.bold = True
        p_title.font.color.rgb = TEXT_WHITE
        
        return slide

    def add_card(slide, left, top, width, height, title, items, border_color=None):
        card = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, left, top, width, height)
        card.fill.solid()
        card.fill.fore_color.rgb = CARD_BG
        if border_color:
            card.line.color.rgb = border_color
            card.line.width = Pt(1.5)
        else:
            card.line.fill.background()
            
        tb = slide.shapes.add_textbox(left + Inches(0.2), top + Inches(0.2), width - Inches(0.4), height - Inches(0.4))
        tf = tb.text_frame
        tf.word_wrap = True
        
        p0 = tf.paragraphs[0]
        p0.text = title
        p0.font.size = Pt(15)
        p0.font.bold = True
        p0.font.color.rgb = ACCENT_CYAN if not border_color else border_color
        p0.space_after = Pt(10)
        
        for item in items:
            p = tf.add_paragraph()
            p.text = f"• {item}"
            p.font.size = Pt(12)
            p.font.color.rgb = TEXT_WHITE
            p.space_after = Pt(6)
            
        return card

    # =========================================================================
    # SLIDE 1: TITLE SLIDE
    # =========================================================================
    s1 = prs.slides.add_slide(blank_layout)
    bg1 = s1.shapes.add_shape(MSO_SHAPE.RECTANGLE, 0, 0, Inches(13.333), Inches(7.5))
    bg1.fill.solid()
    bg1.fill.fore_color.rgb = BG_COLOR
    bg1.line.fill.background()
    
    tbox = s1.shapes.add_textbox(Inches(1.0), Inches(1.8), Inches(11.333), Inches(4.0))
    tf1 = tbox.text_frame
    tf1.word_wrap = True
    
    p = tf1.paragraphs[0]
    p.text = "23CSE352 - BIG DATA ANALYTICS"
    p.font.size = Pt(14)
    p.font.bold = True
    p.font.color.rgb = ACCENT_CYAN
    p.space_after = Pt(10)
    
    p = tf1.add_paragraph()
    p.text = "SafeRoads: Scalable Traffic Accident Severity Prediction & Analytics"
    p.font.size = Pt(32)
    p.font.bold = True
    p.font.color.rgb = TEXT_WHITE
    p.space_after = Pt(10)
    
    p = tf1.add_paragraph()
    p.text = "Project Review 3: Distributed RDD Processing in Scala + ML Modeling in PySpark"
    p.font.size = Pt(18)
    p.font.color.rgb = ACCENT_GOLD
    p.space_after = Pt(30)
    
    p = tf1.add_paragraph()
    p.text = "Team Members & Register Numbers: [Student Name / Reg No: 23CSE352-Team]"
    p.font.size = Pt(14)
    p.font.color.rgb = TEXT_WHITE
    
    p = tf1.add_paragraph()
    p.text = "Department of Computer Science & Engineering | Amrita Vishwa Vidyapeetham"
    p.font.size = Pt(12)
    p.font.color.rgb = TEXT_MUTED

    # =========================================================================
    # SLIDE 2: PROBLEM STATEMENT
    # =========================================================================
    s2 = add_base_slide("Problem Statement & Real-World Motivation")
    add_card(s2, Inches(0.8), Inches(1.6), Inches(5.6), Inches(5.2),
             "Real-World Problem", [
                 "Road traffic accidents cause over 40,000 fatalities and millions of severe injuries annually across the United States.",
                 "High traffic congestion, sudden adverse weather conditions, and poorly marked intersections trigger severe multi-vehicle collisions.",
                 "First responders, emergency medical services (EMS), and state DOTs lack automated, real-time prediction systems to forecast accident severity upon occurrence.",
                 "Massive volumes of streaming traffic data (millions of records) overwhelm traditional single-node databases."
             ], ACCENT_CYAN)
    
    add_card(s2, Inches(6.8), Inches(1.6), Inches(5.7), Inches(5.2),
             "Why the Problem is Important", [
                 "Golden Hour Survival: Predicting high-severity crashes enables instant trauma EMS dispatch, drastically reducing mortality.",
                 "Economic Impact: Traffic delays and secondary accidents cost over $870 Billion annually in lost productivity and property damage.",
                 "Preventative Infrastructure: Identifying high-risk junctions, signals, and visibility thresholds empowers DOTs to deploy targeted safety measures.",
                 "Big Data Scale: Demonstrates distributed fault-tolerant processing of 3.86M nationwide accidents across multi-core architectures."
             ], ACCENT_GOLD)

    # =========================================================================
    # SLIDE 3: DATASET
    # =========================================================================
    s3 = add_base_slide("Dataset Description & Key Characteristics")
    add_card(s3, Inches(0.8), Inches(1.6), Inches(5.6), Inches(5.2),
             "Dataset Specifications", [
                 "Dataset Name: US Accidents (Nationwide Traffic Accident Dataset)",
                 "Source: Real-world sensor, law enforcement, and DOT telemetry data (Moosavi et al., Kaggle/arXiv).",
                 "Total Raw Records: 3,864,197 records (~1.4 GB CSV).",
                 "Cleaned & Validated Records: 3,864,157 records (99.999% retention).",
                 "Total Attributes: 46 attributes covering spatio-temporal, environmental, and road infrastructure telemetry.",
                 "Compressed Parquet Size: 183.45 MB (8x storage reduction with Snappy compression)."
             ], ACCENT_GREEN)
    
    add_card(s3, Inches(6.8), Inches(1.6), Inches(5.7), Inches(5.2),
             "Important Attributes & Target Variable", [
                 "Target Variable: Severity (Scale 1 to 4) & Is_Severe (Binary: 0=Minor, 1=Severe [3-4]).",
                 "Spatio-Temporal: Start_Time, End_Time, Start_Lat, Start_Lng, State, City, Hour, DayOfWeek.",
                 "Environmental & Weather: Temperature(F), Humidity(%), Pressure(in), Visibility(mi), Wind_Speed(mph), Precipitation(in), Weather_Condition.",
                 "Road Infrastructure: Traffic_Signal, Junction, Crossing, Stop, Station, Railway.",
                 "Calculated Indicators: Duration_Minutes (traffic impact), Is_Rush_Hour (7-9 AM, 4-6 PM), Is_Weekend."
             ], ACCENT_CYAN)

    # =========================================================================
    # SLIDE 4: PROJECT WORKFLOW
    # =========================================================================
    s4 = add_base_slide("End-to-End Distributed Big Data Architecture")
    steps = [
        ("Step 1: Ingestion & Prep", ["Raw CSV (1.4 GB)", "Data Cleansing", "Outlier Removal", "Parquet Conversion"]),
        ("Step 2: Spark + Scala RDD", ["sc.textFile() RDD", "map() & filter()", "flatMap() risk tags", "reduceByKey() aggregations"]),
        ("Step 3: PySpark ML Modeling", ["VectorAssembler", "80/20 Train-Test", "Random Forest Classifier", "Logistic Regression"]),
        ("Step 4: Evaluation & Viz", ["Accuracy: 72.57%", "Precision: 82.62%", "ROC-AUC: 0.8136", "6 Publication Charts"])
    ]
    w = Inches(2.7)
    for i, (title, items) in enumerate(steps):
        left = Inches(0.8) + i * Inches(2.95)
        add_card(s4, left, Inches(1.8), w, Inches(4.8), title, items, ACCENT_CYAN if i%2==0 else ACCENT_GOLD)

    # =========================================================================
    # SLIDE 5: SCALA + SPARK IMPLEMENTATION
    # =========================================================================
    s5 = add_base_slide("Step 2: Data Processing using Scala + Spark")
    add_card(s5, Inches(0.8), Inches(1.6), Inches(5.6), Inches(5.2),
             "RDD Transformations & Operations", [
                 "RDD Creation: sc.textFile('dataset/cleaned_accidents_sample.csv')",
                 "filter(): Strips header line, removes invalid rows, filters severe crashes (Severity >= 3).",
                 "map(): Parses tokenized CSV into strongly-typed Case Class AccidentRecord objects.",
                 "flatMap(): Emits multi-dimensional risk tags (Rush_Hour, Adverse_Weather, Near_Signal).",
                 "reduceByKey(): Aggregates accident counts per State and hourly frequencies.",
                 "mapValues(): Computes dual metrics (Count, Avg_Severity) per weather condition."
             ], ACCENT_CYAN)
    
    add_card(s5, Inches(6.8), Inches(1.6), Inches(5.7), Inches(5.2),
             "Spark Actions & Performance", [
                 "count(): Evaluates total processed records across distributed executors.",
                 "take(n): Retrieves representative parsed records without collecting full RDD.",
                 "reduce(): Aggregates global road disruption impact (over 2.4M total miles).",
                 "collect(): Collects ranked state summaries to driver memory for final reporting.",
                 "Key/Value Pair RDDs: (State, 1), (Hour, 1), (Weather, (Severity, 1)).",
                 "Storage Level: Demonstrates MEMORY_AND_DISK serialization."
             ], ACCENT_GREEN)

    # =========================================================================
    # SLIDE 6: SPARK CORE CONCEPTS
    # =========================================================================
    s6 = add_base_slide("Apache Spark Core Architectural Concepts")
    add_card(s6, Inches(0.8), Inches(1.6), Inches(5.6), Inches(5.2),
             "Partitions, DAG & Lineage", [
                 "Partitions: Dataset partitioned across executor threads (getNumPartitions). Enables parallel data scanning without lock contention.",
                 "Directed Acyclic Graph (DAG): Spark builds a logical execution plan before execution. Stages are split at shuffle boundaries.",
                 "RDD Lineage (toDebugString): Graph of parent-child RDD dependencies. Spark reconstructs lost partitions automatically on node failure.",
                 "Narrow Dependencies: map() & filter() execute within the same stage (PipelinedRDD) without network shuffle."
             ], ACCENT_GOLD)
    
    add_card(s6, Inches(6.8), Inches(1.6), Inches(5.7), Inches(5.2),
             "Fault Tolerance & Cache/Persist", [
                 "Wide Dependencies: reduceByKey() & sortByKey() trigger shuffle stages across the cluster network.",
                 "Fault Tolerance: Lineage graph enables deterministic partition recomputation without checkpointing entire datasets to disk.",
                 "cache() & persist(): Caches parsed RDD in executor RAM (StorageLevel.MEMORY_AND_DISK).",
                 "Speedup Verification: First action (uncached) parses from disk; second cached action completes instantly (over 5x faster)."
             ], ACCENT_CYAN)

    # =========================================================================
    # SLIDE 7: DATA PROCESSING RESULTS
    # =========================================================================
    s7 = add_base_slide("Data Processing & Scala Spark RDD Findings")
    add_card(s7, Inches(0.8), Inches(1.6), Inches(5.6), Inches(5.2),
             "Key State & Geographic Findings", [
                 "Top State Volumes (reduceByKey):",
                 "  1. California (CA): 871,217 accidents (22.5% of US total)",
                 "  2. Florida (FL): 439,903 accidents (11.4%)",
                 "  3. Texas (TX): 291,520 accidents (7.5%)",
                 "  4. South Carolina (SC): 191,116 accidents",
                 "  5. New York (NY): 173,652 accidents",
                 "High Urban Density: 41.4% of total accidents concentrated in top 3 states."
             ], ACCENT_CYAN)
    
    add_card(s7, Inches(6.8), Inches(1.6), Inches(5.7), Inches(5.2),
             "Temporal & Environmental Findings", [
                 "Bimodal Rush Hour Peaks:",
                 "  - Morning Peak: 08:00 AM (Sharp commuter collision spike)",
                 "  - Evening Peak: 05:00 PM (17:00) (High volume return traffic)",
                 "Weather & Severity Correlation:",
                 "  - Fair/Clear conditions represent 58% of volume, but Snow/Ice & Fog have 35% higher ratio of Severe (Sev 3-4) crashes.",
                 "Infrastructure Risk: 28.4% of crashes occur near traffic signals or intersections."
             ], ACCENT_GREEN)

    # =========================================================================
    # SLIDE 8: PYSPARK MACHINE LEARNING
    # =========================================================================
    s8 = add_base_slide("Step 3: Machine Learning using PySpark")
    add_card(s8, Inches(0.8), Inches(1.6), Inches(5.6), Inches(5.2),
             "ML Feature Engineering Pipeline", [
                 "Ingestion: Distributed DataFrame loaded from clean Parquet/CSV.",
                 "Numerical Features: Duration_Minutes, Distance_mi, Temperature_F, Humidity_Pct, Pressure_in, Visibility_mi, Wind_Speed_mph, Hour, Month.",
                 "Categorical Features: Weather_Category, Day_Night, State.",
                 "Infrastructure Flags: Traffic_Signal, Junction, Crossing, Stop, Railway.",
                 "StringIndexer: Encodes string labels into category indexes.",
                 "VectorAssembler: Assembles all transformed columns into unified dense feature vector."
             ], ACCENT_CYAN)
    
    add_card(s8, Inches(6.8), Inches(1.6), Inches(5.7), Inches(5.2),
             "Model Training & Architecture", [
                 "Train-Test Split: 80% Training (96,000 samples) / 20% Testing (24,000 samples) with seed=42.",
                 "Primary Algorithm: Random Forest Classifier (numTrees=100, maxDepth=12, balanced weights).",
                 "Ensemble Power: Combines multiple de-correlated decision trees to minimize variance on noisy real-world telemetry.",
                 "Baseline Comparison: Logistic Regression benchmarked to evaluate linear vs non-linear boundary performance.",
                 "PySpark ML Pipeline: Encapsulates indexing, vector assembly, and model fitting in an atomic pipeline."
             ], ACCENT_GOLD)

    # =========================================================================
    # SLIDE 9: MODEL EVALUATION
    # =========================================================================
    s9 = add_base_slide("Model Evaluation & Performance Comparison")
    add_card(s9, Inches(0.8), Inches(1.6), Inches(5.6), Inches(5.2),
             "Performance Metrics Summary", [
                 "Random Forest Classifier (Primary Model):",
                 "  • Accuracy:           72.57%",
                 "  • Weighted Precision: 82.62%",
                 "  • Weighted Recall:    72.57%",
                 "  • Weighted F1-Score:  75.22%",
                 "  • Area Under ROC:     0.8136 (Excellent discrimination)",
                 "Logistic Regression (Baseline):",
                 "  • Accuracy: 51.29% | Precision: 76.39% | F1: 55.63% | AUC: 0.6649",
                 "Clear Superiority: Random Forest improves F1-score by +19.59% over linear baseline."
             ], ACCENT_GREEN)
    
    add_card(s9, Inches(6.8), Inches(1.6), Inches(5.7), Inches(5.2),
             "Top Feature Importance Rankings", [
                 "Random Forest Gini Impurity Importance:",
                 "  1. Duration_Minutes : 31.11% (Traffic blockage length)",
                 "  2. Distance_mi      : 25.32% (Road segment impacted)",
                 "  3. Traffic_Signal   :  7.74% (Intersection conflict)",
                 "  4. Crossing         :  5.86% (Pedestrian crossway)",
                 "  5. Pressure_in      :  5.02% (Weather storm fronts)",
                 "  6. Wind_Speed_mph   :  4.96% (Vehicle handling control)",
                 "  7. Temperature_F    :  3.15% (Road surface conditions)",
                 "Validation: Confusion Matrix confirms robust true-positive recall on critical accidents."
             ], ACCENT_CYAN)

    # =========================================================================
    # SLIDE 10: VISUALIZATIONS (EMBEDDED CHARTS)
    # =========================================================================
    s10 = add_base_slide("Step 4: Meaningful Analytics Visualizations")
    
    # Embed generated charts
    chart1_path = "04_visualizations/chart1_severity_distribution.png"
    chart2_path = "04_visualizations/chart2_hourly_weekly_trends.png"
    chart5_path = "04_visualizations/chart5_feature_importance.png"
    chart6_path = "04_visualizations/chart6_confusion_matrix.png"
    
    if os.path.exists(chart1_path):
        s10.shapes.add_picture(chart1_path, Inches(0.8), Inches(1.5), width=Inches(5.7))
    if os.path.exists(chart2_path):
        s10.shapes.add_picture(chart2_path, Inches(6.8), Inches(1.5), width=Inches(5.7))
    if os.path.exists(chart5_path):
        s10.shapes.add_picture(chart5_path, Inches(0.8), Inches(4.3), width=Inches(5.7))
    if os.path.exists(chart6_path):
        s10.shapes.add_picture(chart6_path, Inches(6.8), Inches(4.3), width=Inches(4.5))

    # =========================================================================
    # SLIDE 11: RESULTS & DISCUSSION
    # =========================================================================
    s11 = add_base_slide("Results, Discussion & Real-World Usefulness")
    add_card(s11, Inches(0.8), Inches(1.6), Inches(5.6), Inches(5.2),
             "Major Findings & Interpretations", [
                 "Temporal Clustering: Accidents sharply spike during 7-9 AM and 4-6 PM weekdays, directly correlating with commuter congestion and driver fatigue.",
                 "Infrastructure Bottlenecks: Over 28% of accidents occur at intersections with traffic signals or crossings, highlighting the need for smart signal timing.",
                 "Environmental Compounding: While clear weather accounts for the highest raw count, adverse snow and fog conditions double the severe casualty probability.",
                 "Scalable RDD Pipeline: Spark processed millions of records in seconds, demonstrating linear scalability across partitions."
             ], ACCENT_CYAN)
    
    add_card(s11, Inches(6.8), Inches(1.6), Inches(5.7), Inches(5.2),
             "Real-World Practical Usefulness", [
                 "Intelligent EMS Routing: Enables dispatch centers to prioritize advanced life support (ALS) ambulances for predicted Severity 3-4 accidents instantly.",
                 "Dynamic Speed Limiting: Connected Highway systems can reduce speed limits automatically upon detecting high-risk weather + rush hour combinations.",
                 "Predictive Police Patrols: State highway patrols can position units near identified high-frequency blackspots before peak hours.",
                 "Insurance Underwriting: Actuarial teams can assess hyper-local geographic risk based on historical severity distributions."
             ], ACCENT_GOLD)

    # =========================================================================
    # SLIDE 12: CONCLUSION & FUTURE ENHANCEMENTS
    # =========================================================================
    s12 = add_base_slide("Conclusion & Future Project Roadmap")
    add_card(s12, Inches(0.8), Inches(1.6), Inches(5.6), Inches(5.2),
             "Project Accomplishments", [
                 "Ingested & Cleaned 3.86M Records: Created high-performance Snappy Parquet storage reducing disk footprint by 8x.",
                 "Demonstrated Complete Scala + Spark Stack: Implemented RDD creation, map(), filter(), flatMap(), reduceByKey(), cache(), and actions.",
                 "Documented Spark Core Architecture: Explained DAG, RDD lineage, narrow vs wide dependencies, and fault tolerance.",
                 "Trained High-Performing PySpark ML Model: Random Forest achieved 82.62% Precision and 0.8136 ROC-AUC on 120,000 instances.",
                 "Generated 6 Publication Visualizations: Communicated complex temporal, spatial, and feature trends."
             ], ACCENT_GREEN)
    
    add_card(s12, Inches(6.8), Inches(1.6), Inches(5.7), Inches(5.2),
             "Future Enhancements", [
                 "Spark Structured Streaming: Ingest live IoT highway sensor and telemetry streams via Apache Kafka.",
                 "Deep Learning with GraphFrames: Model road networks as spatio-temporal graphs using Graph Neural Networks (GNNs).",
                 "Automated Hyperparameter Tuning: Implement Spark MLlib CrossValidator with ParamGridBuilder.",
                 "Edge Deployment: Package the model as an ONNX microservice deployed on roadside edge computing devices."
             ], ACCENT_CYAN)

    prs.save(PPTX_PATH)
    print(f"[✔] Successfully generated 12-slide presentation at: {PPTX_PATH}")

if __name__ == "__main__":
    create_deck()
