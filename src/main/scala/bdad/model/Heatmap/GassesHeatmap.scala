package bdad.model.Heatmap

import bdad.etl.Scenarios.gasses2014to2019LatLon
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

object GassesHeatmap {

  def makeMap(year: Int, filePath: String): DataFrame = {
    val (all, labels) = gasses2014to2019LatLon()

    // Filter by year, then apply the L2 to the features
    // column and make that column "norms"
    // Select lat, lon, and

    val norm_udf = udf((v: Vector) => Vectors.norm(v, 2))
    val filtered = all.filter("year == " + 2019)
      .withColumn("norms", norm_udf(col("scaled_features"))) // .cast(VectorType)
      .select("lat", "lon", "norms")

    // Multiply norm by 100, truncate decimal,
    // and form RDD on final "norms" value
    // by place "norms" many entries of (lat, lon)

    // UDF to explode the row by "v"
    val explodeUDF = udf((lat: Double, lon: Double, v: Double) =>
      Seq.fill[(Double, Double)]((v * 10).toInt)((lat, lon)))

    val points = filtered
      .withColumn("vals",
        explode(explodeUDF(col("lat"), col("lon"), col("norms"))))
      .coalesce(1)
      .select("vals.*")
      .withColumnRenamed("_1", "lat")
      .withColumnRenamed("_2", "lon")

    points
      .coalesce(1)
      .write
      .option("header", "true")
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      .mode(SaveMode.Overwrite)
      .csv(filePath)

    points
  }
}
