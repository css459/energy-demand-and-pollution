//
// BDAD Final Project
// CostRepresentation Model
// Andrii Dobroshynskyi
// CostRepresentation.scala
//

package bdad.model.CostRepresentation

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import bdad.etl.util.{normalize, sma}

object CostRepresentation {

  /**
   * Calculations and transformations of DataFrames for computing the
   * cost representation score of pollution per area (lon, lat) pairs
   * Input is two DataFrames for Air data and Petroleum data, a list
   * of columns of Air data, and the value of an optimum lag to compute
   * a moving average for. This lag value can be computed experimentally
   * by running the correlation() functions from Main.
   *
   * @param airData        Input DataFrame for Air data
   * @param airDataColumns Labels for Air data
   * @param petroleumData  Input DataFrame for Petroleum data
   * @param lag            Optimum lag value to compute a moving average with
   */
  def pollutionCostScores(airData: DataFrame, airDataColumns: Array[String], petroleumData: DataFrame, lag: Int): DataFrame = {

    // compute a simple moving average (SMA) of length lag
    // `periods` determined experimentally and filter out
    // columns that we do not need to build a
    // normalized vector

    val smaAirDF = sma(airData, Array("lat", "lon"), airDataColumns, lag)
    val smaAirDFCols = smaAirDF.columns.filter(_.contains("_sma"))

    // normalize the SMA columns from the smaAirDF DataFrame
    // and generate a `scaled_features` vector
    // only keep the lon, lat, dateGMT and the columns need
    // to compute the normalized vector by calling the
    // `normalize function`

    val normalizedAirDFCols = Array("lat", "lon", "dateGMT") ++ smaAirDFCols
    val normalizedAirDF =  normalize(smaAirDF.select(normalizedAirDFCols.head, normalizedAirDFCols.tail: _*),
      smaAirDFCols, useMean = true, useStd = true)

    // utility function to compute the L2 norm of a vector

    val norm_udf = udf((v: Vector) => Vectors.norm(v, 2))

    // group the original DataFrame by lon, lat and aggregate
    // on the most recent date in the DataFrame for that lon, lat
    // pair
    // this is equivalent to selecting unique lon, lat pairs
    // with the latest dateGMT value across all rows of each
    // lon, lat pair

    val groupedLonLatAirDF = normalizedAirDF
      .groupBy("lat", "lon")
      .agg(max("dateGMT").as("dateGMT"))

    // narrow down the normalizedAirDF DataFrame to select
    // only the values that we need in order to construct
    // the final grouped Air DataFrame:
    // `lat`, `lon`, `dateGMT` - fields in order to JOIN on
    // `scaled_features` - field that we need in the final
    // resulting grouped and joined DataFrame

    val vectorAirDFCols = Array("lat", "lon", "dateGMT", "scaled_features")
    val vectorAirDF = normalizedAirDF.select(vectorAirDFCols.head, vectorAirDFCols.tail: _*)

    // join the DataFrame grouped by lat, lon and max(dateGMT) with
    // the original DataFrame on 3 values:
    //    `lat`, `lon`, `dateGMT`
    // in order to get a result DataFrame that contains both the
    // grouped lon, lat, but now also a corresponding `scaled_features`
    // column and a column for its L2 norm
    // for each unique lat, lon, dateGMT

    val airDF = groupedLonLatAirDF
      .join(vectorAirDF, Seq("lat", "lon", "dateGMT"), "inner")
      .withColumn("norms", norm_udf(col("scaled_features")))

    // transform the petroleum DataFrame by selecting only relevant
    // columns and renaming them for clarity in the later JOIN

    val petroleumDFCols = Array("Date", "scaled_features")
    val petroleumDFColsRenamed = Array("dateGMT", "price_features")
    val petroleumDF = petroleumData.select(petroleumDFCols.head, petroleumDFCols.tail: _*).toDF(petroleumDFColsRenamed.toSeq: _*)

    // join the petroleum price data in petroleumDF with the air data
    // in airDF grouped by lon, lat and max(dateGMT)

    val jointDF = airDF
      .join(petroleumDF, "dateGMT")
      .withColumn("price_norms", norm_udf(col("price_features")))

    // compute the score for each lat, lon pair and
    // select only the lat, lon, score columns for the
    // result DataFrame

    val scoreDFCols = Array("lat", "lon", "score")
    val scoreDF = jointDF
      .withColumn("score", col("norms") / col("price_norms"))
      .select(scoreDFCols.head, scoreDFCols.tail: _*)
    scoreDF
  }
}
