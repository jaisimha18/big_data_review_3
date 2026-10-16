package com.saferoads.analytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

/**
 * SafeRoads: US Traffic Accident RDD Analytics Pipeline
 * Course: 23CSE352-BIG DATA ANALYTICS (Project Review 3)
 *
 * Demonstrates:
 * 1. RDD creation from large-scale real-world dataset
 * 2. Transformations: filter(), map(), flatMap(), reduceByKey(), mapValues(), sortByKey()
 * 3. Actions: count(), collect(), take(), reduce()
 * 4. Key-Value RDD operations (aggregation and statistics)
 * 5. Partition inspection and tuning
 * 6. Cache() & Persist() with runtime benchmarking
 * 7. RDD Lineage and DAG explanation
 */

case class AccidentRecord(
  id: String,
  severity: Int,
  isSevere: Int,
  startTime: String,
  endTime: String,
  startLat: Double,
  startLng: Double,
  distanceMi: Double,
  hour: Int,
  dayOfWeek: Int,
  month: Int,
  year: Int,
  durationMinutes: Double,
  isRushHour: Int,
  isWeekend: Int,
  city: String,
  county: String,
  state: String,
  timezone: String,
  temperatureF: Double,
  humidityPct: Double,
  pressureIn: Double,
  visibilityMi: Double,
  windSpeedMph: Double,
  precipitationIn: Double,
  weatherCategory: String,
  trafficSignal: Int,
  dayNight: String
)

object TrafficAccidentAnalytics {

  def main(args: Array[String]): Unit = {
    println("================================================================================")
    println("  SAFEROADS: APACHE SPARK & SCALA RDD BIG DATA ANALYTICS PIPELINE")
    println("  Course: 23CSE352 - Big Data Analytics | Project Review 3 (Apache Spark)")
    println("================================================================================")

    val inputPath = if (args.length > 0) args(0) else "dataset/cleaned_accidents_sample.csv"
    println(s"[*] Target Dataset Path: $inputPath")

    // 1. Initialize SparkSession and SparkContext
    val spark = SparkSession.builder()
      .appName("SafeRoads-TrafficAccidentAnalytics")
      .master("local[*]")
      .config("spark.driver.memory", "4g")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val startTimeTotal = System.currentTimeMillis()

    // -------------------------------------------------------------------------
    // STEP 2.1: CREATE RDD FROM DATASET & SHOW PARTITIONS
    // -------------------------------------------------------------------------
    println("\n[1] STEP 2.1: RDD CREATION & PARTITION INSPECTION")
    val rawTextRDD: RDD[String] = sc.textFile(inputPath)
    
    val initialPartitions = rawTextRDD.getNumPartitions
    println(s"    -> Successfully loaded RDD from $inputPath")
    println(s"    -> Initial Partition Count: $initialPartitions")
    println(s"    -> Spark Master: ${sc.master}")
    println(s"    -> Default Parallelism: ${sc.defaultParallelism}")

    // Extract Header line for filtering
    val headerLine = rawTextRDD.first()
    println(s"    -> Header Preview: ${headerLine.split(",").take(8).mkString(", ")} ...")

    // -------------------------------------------------------------------------
    // STEP 2.2: RDD TRANSFORMATIONS
    // -------------------------------------------------------------------------
    println("\n[2] STEP 2.2: PERFORMING RDD TRANSFORMATIONS")

    // Transformation 1: filter() - Remove header line and empty lines
    val dataWithoutHeaderRDD = rawTextRDD.filter(line => line != headerLine && line.trim.nonEmpty)

    // Transformation 2: map() - Parse CSV into strongly-typed AccidentRecord case classes
    val parsedRecordsRDD: RDD[AccidentRecord] = dataWithoutHeaderRDD.flatMap { line =>
      try {
        val tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
        if (tokens.length >= 28) {
          Some(AccidentRecord(
            id = tokens(0).replace("\"", "").trim,
            severity = tokens(1).trim.toInt,
            isSevere = tokens(2).trim.toInt,
            startTime = tokens(3).replace("\"", "").trim,
            endTime = tokens(4).replace("\"", "").trim,
            startLat = tokens(5).trim.toDouble,
            startLng = tokens(6).trim.toDouble,
            distanceMi = tokens(7).trim.toDouble,
            hour = tokens(8).trim.toInt,
            dayOfWeek = tokens(9).trim.toInt,
            month = tokens(10).trim.toInt,
            year = tokens(11).trim.toInt,
            durationMinutes = tokens(12).trim.toDouble,
            isRushHour = tokens(13).trim.toInt,
            isWeekend = tokens(14).trim.toInt,
            city = tokens(15).replace("\"", "").trim,
            county = tokens(16).replace("\"", "").trim,
            state = tokens(17).replace("\"", "").trim,
            timezone = tokens(18).replace("\"", "").trim,
            temperatureF = tokens(19).trim.toDouble,
            humidityPct = tokens(20).trim.toDouble,
            pressureIn = tokens(21).trim.toDouble,
            visibilityMi = tokens(22).trim.toDouble,
            windSpeedMph = tokens(23).trim.toDouble,
            precipitationIn = tokens(24).trim.toDouble,
            weatherCategory = tokens(25).replace("\"", "").trim,
            trafficSignal = tokens(26).trim.toInt,
            dayNight = tokens(27).replace("\"", "").trim
          ))
        } else None
      } catch {
        case _: Exception => None
      }
    }

    // -------------------------------------------------------------------------
    // STEP 2.3: DEMONSTRATE CACHE() / PERSIST() WITH BENCHMARKING
    // -------------------------------------------------------------------------
    println("\n[3] STEP 2.3: DEMONSTRATING CACHE() & PERSIST()")
    println("    -> Persisting parsedRecordsRDD into MEMORY_AND_DISK for iterative analytics...")
    
    // Uncached benchmark
    val tStartUncached = System.currentTimeMillis()
    val countUncached = parsedRecordsRDD.count()
    val tDurationUncached = System.currentTimeMillis() - tStartUncached
    println(s"    -> First Action (Uncached RDD Count): $countUncached records in ${tDurationUncached} ms")

    // Applying Cache
    parsedRecordsRDD.persist(StorageLevel.MEMORY_AND_DISK)

    // Cached benchmark
    val tStartCached = System.currentTimeMillis()
    val countCached = parsedRecordsRDD.count()
    val tDurationCached = System.currentTimeMillis() - tStartCached
    println(s"    -> Second Action (Cached RDD Count):   $countCached records in ${tDurationCached} ms")
    val speedup = if (tDurationCached > 0) f"${tDurationUncached.toDouble / tDurationCached}%.2fx" else "Instant"
    println(s"    -> Cache Speedup Factor: $speedup")

    // Transformation 3: flatMap() - Extract multiple analytical feature tags per accident
    // Emits tag tokens such as: RushHour, Severe, Night, AdverseWeather, TrafficSignal
    val riskFactorsRDD: RDD[String] = parsedRecordsRDD.flatMap { record =>
      var tags = List.empty[String]
      if (record.isRushHour == 1) tags = "Risk_Factor:Rush_Hour" :: tags
      if (record.isSevere == 1) tags = "Risk_Factor:Severe_Accident" :: tags
      if (record.dayNight == "Night") tags = "Risk_Factor:Night_Time" :: tags
      if (record.weatherCategory != "Clear" && record.weatherCategory != "Cloudy") 
        tags = s"Adverse_Weather:${record.weatherCategory}" :: tags
      if (record.trafficSignal == 1) tags = "Infrastructure:Near_Traffic_Signal" :: tags
      if (record.isWeekend == 1) tags = "Temporal:Weekend" :: tags
      tags
    }

    // Transformation 4: filter() - Filter severe accidents (Severity >= 3)
    val severeAccidentsRDD = parsedRecordsRDD.filter(_.severity >= 3)
    val severeCount = severeAccidentsRDD.count()
    println(s"\n    -> Severe Accidents Filtered (Severity >= 3): $severeCount records (${(severeCount * 100.0 / countCached).formatted("%.2f")}%)")

    // -------------------------------------------------------------------------
    // STEP 2.4: KEY-VALUE RDD OPERATIONS (TRANSFORMATIONS & ACTIONS)
    // -------------------------------------------------------------------------
    println("\n[4] STEP 2.4: KEY-VALUE RDD OPERATIONS")

    // KV Operation A: Total Accidents by State using reduceByKey()
    val stateCountRDD = parsedRecordsRDD
      .map(rec => (rec.state, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    println("\n    [A] Top 10 States by Accident Frequency (reduceByKey):")
    val top10States = stateCountRDD.take(10)
    top10States.zipWithIndex.foreach { case ((state, cnt), idx) =>
      println(f"        ${idx + 1}%2d. State: $state%-4s | Accident Count: $cnt%,7d")
    }

    // KV Operation B: Average Severity by Weather Category using combineByKey/reduceByKey + mapValues()
    val weatherStatsRDD = parsedRecordsRDD
      .map(rec => (rec.weatherCategory, (rec.severity.toDouble, 1)))
      .reduceByKey((acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .mapValues { case (totalSev, totalCount) =>
        (totalCount, totalSev / totalCount)
      }
      .sortBy(_._2._1, ascending = false)

    println("\n    [B] Weather Category Severity & Frequency Analysis (reduceByKey + mapValues):")
    weatherStatsRDD.collect().foreach { case (weather, (cnt, avgSev)) =>
      println(f"        * Weather: $weather%-12s | Total Count: $cnt%,7d | Avg Severity: $avgSev%.3f")
    }

    // KV Operation C: Accidents by Hour of Day
    val hourlyDistRDD = parsedRecordsRDD
      .map(rec => (rec.hour, 1))
      .reduceByKey(_ + _)
      .sortByKey()

    println("\n    [C] Hourly Accident Distribution (Top Peak Rush Hours):")
    val peakHours = hourlyDistRDD.sortBy(_._2, ascending = false).take(5)
    peakHours.foreach { case (hour, count) =>
      println(f"        - Hour: $hour%02d:00 | Accidents: $count%,7d")
    }

    // KV Operation D: Risk Factor Frequency Analysis using reduceByKey() on flatMapped tags
    val riskFactorCountsRDD = riskFactorsRDD
      .map(tag => (tag, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    println("\n    [D] Extracted Risk Factors Distribution (flatMap + reduceByKey):")
    riskFactorCountsRDD.take(6).foreach { case (tag, count) =>
      println(f"        + $tag%-35s : $count%,7d occurrences")
    }

    // -------------------------------------------------------------------------
    // STEP 2.5: ACTIONS DEMONSTRATION
    // -------------------------------------------------------------------------
    println("\n[5] STEP 2.5: ACTIONS DEMONSTRATION")
    
    // Action 1: count()
    val totalProcessed = parsedRecordsRDD.count()
    println(s"    -> Action count():   Total Records Processed = $totalProcessed")

    // Action 2: take(5)
    val sampleRecords = parsedRecordsRDD.take(3)
    println(s"    -> Action take(3):   Sample Processed Record:")
    sampleRecords.foreach(r => println(s"       ID=${r.id}, State=${r.state}, City=${r.city}, Severity=${r.severity}, Weather=${r.weatherCategory}, Temp=${r.temperatureF}F"))

    // Action 3: reduce() - Compute total miles of road impacted
    val totalImpactDistance = parsedRecordsRDD.map(_.distanceMi).reduce(_ + _)
    println(f"    -> Action reduce():  Total Road Impact Distance = $totalImpactDistance%,.2f miles")

    // Action 4: collect() - Gather states summary into driver memory
    val totalStatesCount = stateCountRDD.count()
    println(s"    -> Action collect(): Total Unique States Analyzed = $totalStatesCount")

    // -------------------------------------------------------------------------
    // STEP 2.6: RDD LINEAGE & DAG EXPLANATION
    // -------------------------------------------------------------------------
    println("\n[6] STEP 2.6: RDD LINEAGE & DAG (DIRECTED ACYCLIC GRAPH) EXPLANATION")
    println("    -> Printing Lineage Graph using toDebugString:")
    println("--------------------------------------------------------------------------------")
    println(stateCountRDD.toDebugString)
    println("--------------------------------------------------------------------------------")
    println("""
    DAG & Lineage Architectural Breakdown:
    1. Lineage Graph: Spark builds a logical plan representing the transformations applied 
       to the base RDD (rawTextRDD -> filteredRDD -> parsedRecordsRDD -> pairRDD -> shufflerRDD).
    2. Narrow Dependencies: map() and filter() operations do not require data shuffling across 
       partitions. Data is processed locally in the same stage (PipelinedRDD).
    3. Wide Dependencies: reduceByKey() and sortBy() require an expensive Shuffle operation 
       to redistribute data by key across cluster nodes, dividing the execution into distinct DAG Stages.
    4. Fault Tolerance: If any partition is lost due to node failure, Spark recomputes only the lost 
       partition by traversing the lineage graph back to the original source.
    5. In-Memory Persistence: By calling cache() / persist(MEMORY_AND_DISK), the intermediate parsed RDD 
       is stored in executor memory, preventing redundant re-evaluations across multiple actions.
    """)

    val totalDuration = (System.currentTimeMillis() - startTimeTotal) / 1000.0
    println(f"[✔] SCALA SPARK PIPELINE COMPLETED SUCCESSFULLY IN $totalDuration%.2f SECONDS!")
    println("================================================================================\n")

    spark.stop()
  }
}
