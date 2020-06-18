//
// BDAD Final Project
// Dataset ETL Scenarios
// Cole Smith
// Scenarios.scala
//

package bdad.etl

import bdad.Context
import bdad.etl.airdata.AirDataset
import bdad.etl.petroleumdata.PetroleumDataset
import bdad.etl.util.normalize
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Scenarios {

  /**
   * Flag determines CSV checkpointing
   * to HDFS. Turn off to re-generate
   * Scenarios
   */
  val readFromFile: Boolean = true

  /**
    * This Scenario includes all the criteria gasses from the past 5 years
    * as of 2019. The measurements are grouped into daily averages, and include
    * the latitude, longitude, state, county, and normalized feature vector
    * for the criteria gasses.
    *
    * NOTE: Only areas for which all gasses could be measured are included.
    */
  def gasses2014to2019LatLon(): (DataFrame, Array[String]) = {
    if (readFromFile) {
      val loaded = Context.spark.read.parquet("gasses-2014-2019.parquet")
      val loadedLabels = Context.context.textFile("gasses-2014-2019.labels").collect()
      val sorted = loaded.sort("year", "dayofyear")
      (normalize(sorted, loadedLabels, useMean = true, useStd = true), loadedLabels)
    } else {

      val air = new AirDataset(AirDataset.makeYearList(2014, 2019), "gasses/*")
      val criteria = air.validCriteria

      // Get the average criteria for all areas for each day from 2014-2019
      val grouped = air
        .pivotedDF(dropNull = true, dropUnit = true)
        .groupBy(year(col("dateGMT")), dayofyear(col("dateGMT")),
          col("lat"), col("lon"))
        .mean()

      val sorted = grouped.sort("year(dateGMT)", "dayofyear(dateGMT)")

      // The grouping renamed the columns, change this back
      val renamed: DataFrame = criteria
        .foldLeft(sorted)((acc, c) => acc.withColumnRenamed("avg(" + c + ")", c))
        .withColumnRenamed("year(dateGMT)", "year")
        .withColumnRenamed("dayofyear(dateGMT)", "dayofyear")

      // Remove invalid characters in columns
      val cleaned: DataFrame = renamed.columns
        .foldLeft(renamed)((acc, c) =>
          acc.withColumnRenamed(c, c.replaceAll("[()]", "").replace(" ", "_")))

      val cleanedCriteria = criteria
        .map(c => c.replaceAll("[()]", "").replace(" ", "_"))

      // Save down to file
      // renamed.write.format("avro").save("gasses-2014-2019.avro")
      cleaned.write.parquet("gasses-2014-2019.parquet")
      Context.context.parallelize(cleanedCriteria).saveAsTextFile("gasses-2014-2019.labels")

      // Create the normalized vector column and return the resulting DataFrame
      //    normalize(sorted, criteria.map("avg(" + _ + ")"), useMean = true, useStd = true)
      (normalize(cleaned, cleanedCriteria, useMean = true, useStd = true), cleanedCriteria)
    }
  }

  /**
   * This Scenario includes all the criteria gasses from the past 5 years
   * as of 2019. The measurements are grouped into daily averages, and include
   * the latitude, longitude, state, county, and normalized feature vector
   * for the criteria gasses.
   *
   * NOTE: Only areas for which all gasses could be measured are included.
   */
  def gasses2014to2019(): (DataFrame, Array[String]) = {
    if (readFromFile) {
      // val loaded = Context.spark.read.format("avro").load("gasses-2014-2019.avro")
      val loaded = Context.spark.read.parquet("gasses-2014-2019-no-latlon.parquet")
      val loadedLabels = Context.context.textFile("gasses-2014-2019-no-latlon.labels").collect()
      val sorted = loaded.sort("year", "dayofyear")
      (normalize(sorted, loadedLabels, useMean = true, useStd = true), loadedLabels)
    } else {

      val air = new AirDataset(AirDataset.makeYearList(2014, 2019), "gasses/*")
      val criteria = air.validCriteria

      // Get the average criteria for all areas for each day from 2014-2019
      val grouped = air
        .pivotedDF(dropNull = true, dropUnit = true)
        .select("dateGMT", criteria: _*)
        .groupBy(year(col("dateGMT")), dayofyear(col("dateGMT")))
        .mean()

      val sorted = grouped.sort("year(dateGMT)", "dayofyear(dateGMT)")

      // The grouping renamed the columns, change this back
      val renamed: DataFrame = criteria
        .foldLeft(sorted)((acc, c) => acc.withColumnRenamed("avg(" + c + ")", c))
        .withColumnRenamed("year(dateGMT)", "year")
        .withColumnRenamed("dayofyear(dateGMT)", "dayofyear")

      // Remove invalid characters in columns
      val cleaned: DataFrame = renamed.columns
        .foldLeft(renamed)((acc, c) =>
          acc.withColumnRenamed(c, c.replaceAll("[()]", "").replace(" ", "_")))

      val cleanedCriteria = criteria
        .map(c => c.replaceAll("[()]", "").replace(" ", "_"))

      // Save down to file
      // renamed.write.format("avro").save("gasses-2014-2019.avro")
      cleaned.write.parquet("gasses-2014-2019-no-latlon.parquet")
      Context.context.parallelize(cleanedCriteria).saveAsTextFile("gasses-2014-2019-no-latlon.labels")

      // Create the normalized vector column and return the resulting DataFrame
      //    normalize(sorted, criteria.map("avg(" + _ + ")"), useMean = true, useStd = true)
      (normalize(cleaned, cleanedCriteria, useMean = true, useStd = true), cleanedCriteria)
    }
  }

  /**
    * This Scenario includes all the toxics from the past 5 years
    * as of 2019. The measurements are grouped into daily averages, and include
    * the latitude, longitude, state, county, and normalized feature vector
    * for the criteria gasses.
    *
    * NOTE: Only areas for which all gasses could be measured are included.
    */
  def toxics2014to2019(): (DataFrame, Array[String]) = {
    if (readFromFile) {
      // val loaded = Context.spark.read.format("avro").load("gasses-2014-2019.avro")
      val loaded = Context.spark.read.parquet("toxics-2014-2019-no-latlon.parquet")
      val loadedLabels = Context.context.textFile("toxics-2014-2019-no-latlon.labels").collect()
      val sorted = loaded.sort("year", "dayofyear")
      (normalize(sorted, loadedLabels, useMean = true, useStd = true), loadedLabels)
    } else {

      val air = new AirDataset(AirDataset.makeYearList(2014, 2019), "toxics/*")
      val criteria = air.validCriteria

      // Get the average criteria for all areas for each day from 2014-2019
      val grouped = air
        .pivotedDF(dropNull = true, dropUnit = true)
        .select("dateGMT", criteria: _*)
        .groupBy(year(col("dateGMT")), dayofyear(col("dateGMT")))
        .mean()

      val sorted = grouped.sort("year(dateGMT)", "dayofyear(dateGMT)")

      // The grouping renamed the columns, change this back
      val renamed: DataFrame = criteria
        .foldLeft(sorted)((acc, c) => acc.withColumnRenamed("avg(" + c + ")", c))
        .withColumnRenamed("year(dateGMT)", "year")
        .withColumnRenamed("dayofyear(dateGMT)", "dayofyear")

      // Remove invalid characters in columns
      val cleaned: DataFrame = renamed.columns
        .foldLeft(renamed)((acc, c) =>
          acc.withColumnRenamed(c, c.replaceAll("[()]", "").replace(" ", "_")))

      val cleanedCriteria = criteria
        .map(c => c.replaceAll("[()]", "").replace(" ", "_"))

      // Save down to file
      // renamed.write.format("avro").save("gasses-2014-2019.avro")
      cleaned.write.parquet("toxics-2014-2019-no-latlon.parquet")
      Context.context.parallelize(cleanedCriteria).saveAsTextFile("toxics-2014-2019-no-latlon.labels")

      // Create the normalized vector column and return the resulting DataFrame
      //    normalize(sorted, criteria.map("avg(" + _ + ")"), useMean = true, useStd = true)
      (normalize(cleaned, cleanedCriteria, useMean = true, useStd = true), cleanedCriteria)
    }
  }

  /**
    * This Scenario includes all the criteria gasses but from only 2019. The
    * measurements are grouped into daily averages, and include the
    * latitude, longitude, state, county, and normalized feature vector
    * for the criteria gasses.
    *
    * NOTE: Only areas for which all gasses could be measured are included.
    */
  def gasses2019test(): (DataFrame, Array[String]) = {
    val air = new AirDataset(2019, "gasses/*")
    val criteria = air.validCriteria

    // Get the average criteria for all areas for each day from 2014-2019
    val grouped = air
      .pivotedDF(dropNull = true, dropUnit = true)
      .select("dateGMT", criteria: _*)
      .groupBy(year(col("dateGMT")), dayofyear(col("dateGMT")))
      .mean()

    val sorted = grouped.sort("year(dateGMT)", "dayofyear(dateGMT)")

    // The grouping renamed the columns, change this back
    val renamed: DataFrame = criteria
      .foldLeft(sorted)((acc, c) => acc.withColumnRenamed("avg(" + c + ")", c))

    // Create the normalized vector column and return the resulting DataFrame
    (normalize(renamed, criteria, useMean = true, useStd = true), criteria)
  }

  /**
   * This Scenario includes all the criteria toxics but from only 2019. The
   * measurements are grouped into daily averages, and include the
   * latitude, longitude, state, county, and normalized feature vector
   * for the criteria toxics.
   *
   * NOTE: Only areas for which all toxics could be measured are included.
   */
  def toxics2019test(): (DataFrame, Array[String]) = {
    val air = new AirDataset(2019, "toxics/*")
    val criteria = air.validCriteria

    // Get the average criteria for all areas for each day from 2014-2019
    val grouped = air
      .pivotedDF(dropNull = true, dropUnit = true)
      .select("dateGMT", criteria: _*)
      .groupBy(year(col("dateGMT")), dayofyear(col("dateGMT")))
      .mean()

    val sorted = grouped.sort("year(dateGMT)", "dayofyear(dateGMT)")

    // The grouping renamed the columns, change this back
    val renamed: DataFrame = criteria
      .foldLeft(sorted)((acc, c) => acc.withColumnRenamed("avg(" + c + ")", c))

    // Create the normalized vector column and return the resulting DataFrame
    (normalize(renamed, criteria, useMean = true, useStd = true), criteria)
  }

  /**
   * This Scenario includes the daily spot prices of petroleum only for 2019.
   * The data contains the spot price, date and extracted year, as well as day
    * of year.
    */
  def petroleum2019test(): (DataFrame, Array[String]) = {
    val petroleum = new PetroleumDataset(year = 2019)
    // preprocessed df ready to be normalized on Price and in correct format for joins on year, day of year
    val processed = petroleum.dayProcessed.filter("Year == '2019'")
    val columns = petroleum.columns

    (normalize(processed, columns, useMean = true, useStd = true), columns)
  }

  /**
    * This Scenario includes the daily spot prices of petroleum only for 2014-2019.
    * The data contains the spot price, date and extracted year, as well as day
    * of year.
    */
  def petroleum2014to2019(): (DataFrame, Array[String]) = {
    val petroleum = new PetroleumDataset()
    // preprocessed df ready to be normalized on Price and in correct format for joins on year, day of year
    val processed = petroleum.dayProcessed.filter("Year > '2013'")
    val columns = petroleum.columns

    (normalize(processed, columns, useMean = true, useStd = true), columns)
  }
}
