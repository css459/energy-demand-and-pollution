package bdad.etl

import bdad.etl.airdata.AirDataset
import bdad.etl.util.{maxAbs, normalize}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Scenarios {
  val gasses2014to2019: DataFrame = {
    val air = new AirDataset(AirDataset.makeYearList(2014, 2019), "gasses/*")
    val criteria = air.validCriteria

    // Get the average criteria for all areas for each day from 2014-2019
    val grouped = air
      .pivotedDF(dropNull = true, dropUnit = true)
      .select("dateGMT", criteria: _*)
      .groupBy(year(col("dateGMT")), dayofyear(col("dateGMT")))
      .mean()

    // Create the normalized vector column and return the resulting DataFrame
    maxAbs(normalize(grouped, criteria.map("avg(" + _ + ")"), useMean = true, useStd = true))
  }
}
