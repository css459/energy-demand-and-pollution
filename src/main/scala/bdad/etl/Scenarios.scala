//
// BDAD Final Project
// Dataset ETL Scenarios
// Cole Smith
// Scenarios.scala
//

package bdad.etl

import bdad.etl.airdata.AirDataset
import bdad.etl.util.normalize
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Scenarios {

  /**
    * This Scenario includes all the criteria gasses from the past 5 years
    * as of 2019. The measurements are grouped into daily averages, and include
    * the latitude, longitude, state, county, and normalized feature vector
    * for the criteria gasses.
    *
    * NOTE: Only areas for which all gasses could be measured are included.
    */
  val gasses2014to2019: (DataFrame, Array[String]) = {
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

    // Create the normalized vector column and return the resulting DataFrame
    //    normalize(sorted, criteria.map("avg(" + _ + ")"), useMean = true, useStd = true)
    (normalize(renamed, criteria, useMean = true, useStd = true), criteria)
  }

  val gasses2019test: (DataFrame, Array[String]) = {
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
}
