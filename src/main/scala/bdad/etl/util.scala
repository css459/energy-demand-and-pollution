//
// BDAD Final Project
// ETL
// Cole Smith
// util.scala
//

package bdad.etl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Utility functions and resources for
  * Extract Transform and Load operations
  */
object util {

  /**
    * Writes the provided Dataframe to disk.
    *
    * @param df       Dataframe to write to disk
    * @param filepath File path to write to (including filename.csv)
    */
  def writeToDisk(df: DataFrame, filepath: String): Unit = {
    df.write.format("csv").save(filepath)
  }

  /**
    * Subtract the mean of a column from itself. The mean can be an aggregation
    * over any of the following:
    *
    * "hourly":   Averaged over the hour of the day
    * "daily":    Averaged over the day of the month
    * "monthly":  Averaged over the months of the year
    * "yearly":   Averaged over the years in the data set
    *
    * The `meanFreq` value can be any of the above strings. The default is "daily".
    *
    * @param df          Dataframe to transform
    * @param colName     The name of the column to mean-zero
    * @param dateColName The name of the date column containing Date objects (default: "dateGMT")
    * @param meanFreq    The frequency of aggregation for the mean (default: "daily")
    */
  def removeMean(df: DataFrame, colName: String, dateColName: String = "dateGMT",
                 meanFreq: String = "daily"): DataFrame = {

    if (meanFreq.equals("daily")) {
      df
        // Form the date aggregation and mean columns
        .select(colName, dateColName)
        .groupBy(dayofmonth(col(dateColName)).alias("dayOfMonth"))
        .agg(mean(colName).alias("mean"))

        // Generate the date aggregation column for df, and join on it
        .join(df.withColumn("dayOfMonth", dayofmonth(col(dateColName))), "dayOfMonth")

        // Subtract the mean from the value
        .withColumn(colName, col(colName) - col("mean"))

        // Drop intermediate columns
        .drop("mean", "dayOfMonth")
    }
    else if (meanFreq.equals("monthly")) {
      df
        // Form the date aggregation and mean columns
        .select(colName, dateColName)
        .groupBy(month(col(dateColName)).alias("month"))
        .agg(mean(colName).alias("mean"))

        // Generate the date aggregation column for df, and join on it
        .join(df.withColumn("month", month(col(dateColName))), "month")

        // Subtract the mean from the value
        .withColumn(colName, col(colName) - col("mean"))

        // Drop intermediate columns
        .drop("mean", "month")
    }
    else if (meanFreq.equals("yearly")) {
      df
        // Form the date aggregation and mean columns
        .select(colName, dateColName)
        .groupBy(year(col(dateColName)).alias("year"))
        .agg(mean(colName).alias("mean"))

        // Generate the date aggregation column for df, and join on it
        .join(df.withColumn("year", year(col(dateColName))), "year")

        // Subtract the mean from the value
        .withColumn(colName, col(colName) - col("mean"))

        // Drop intermediate columns
        .drop("mean", "year")
    }
    else if (meanFreq.equals("hourly")) {
      df
        // Form the date aggregation and mean columns
        .select(colName, dateColName)
        .groupBy(hour(col(dateColName)).alias("hour"))
        .agg(mean(colName).alias("mean"))

        // Generate the date aggregation column for df, and join on it
        .join(df.withColumn("hour", hour(col(dateColName))), "hour")

        // Subtract the mean from the value
        .withColumn(colName, col(colName) - col("mean"))

        // Drop intermediate columns
        .drop("mean", "hour")
    }
    else {
      println("[ ERR ] Invalid meanFreq: " + meanFreq +
        ". Must be one of: hourly, daily, monthly, yearly. No Operation.")
      df
    }
  }
}
