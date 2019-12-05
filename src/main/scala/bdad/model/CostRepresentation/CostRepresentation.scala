package bdad.model.CostRepresentation

import bdad.etl.airdata.AirDataset
import bdad.etl.util.{maxAbs, normalize, sma}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col

object CostRepresentation {

  def computeCostScores(airDataset: AirDataset, petroleumData: DataFrame, year: Int): DataFrame = {
    val piv = airDataset.pivotedDF(dropNull = true, dropUnit = true)
    val smaDF = sma(piv, Array("lat", "lon"), airDataset.validCriteria, 10)

    val smaCols = smaDF.columns.filter(c => c.contains("_sma"))
    val selectedCols = Array("lat", "lon", "dateGMT") ++ smaCols

    val smaOnly = smaDF.select(selectedCols.head, selectedCols.tail: _*)
    val norm =  normalize(smaOnly, smaCols, useMean = true, useStd = true)

//    val dateVec = norm.select(Array("dateGMT", "scaled_features").head, Array("dateGMT", "scaled_features").tail: _*)
//
//    val selectColumns = Array("lat", "lon", "dateGMT", "scaled_features")

//    val renamedCols = Array("lat_grouped", "lon_grouped", "dateGMT_grouped")
    val groupedByLonLat = norm.groupBy("lat", "lon").agg(max("dateGMT").as("dateGMT"))
    print(groupedByLonLat.show(false))

    val vectorColumnsForJoin = Array("lat", "lon", "dateGMT", "scaled_features")
    val withVector = norm.select(vectorColumnsForJoin.head, vectorColumnsForJoin.tail: _*)
    print(withVector.show(false))

    val joined = groupedByLonLat.join(withVector, Seq("lat", "lon", "dateGMT"), "inner")

    val norm_udf = udf((v: Vector) => Vectors.norm(v, 2))
    val normalized = joined.withColumn("norms", norm_udf(col("scaled_features")))

    val petroleumCols = Array("Date", "scaled_features")
    val renamedCols = Array("dateGMT", "price_features")
    val selectPetroleum = petroleumData.select(petroleumCols.head, petroleumCols.tail: _*).toDF(renamedCols.toSeq: _*)

    val bothDF = normalized.join(selectPetroleum, "dateGMT").withColumn("price_norms", norm_udf(col("price_features")))
    print(bothDF.show(false))

    val resultCols = Array("lat", "lon", "score")
    val result = bothDF.withColumn("score", col("norms") / col("price_norms"))
    result
  }
}
