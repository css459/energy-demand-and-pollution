package bdad.model.Heatmap

import bdad.etl.Scenarios.gasses2014to2019
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg.Vectors.norm
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._

object GassesHeatmap {

  def makeMap(year: Int): RDD[(Int, Int)] = {
    val (all, labels) = gasses2014to2019

    // Filter by year, then apply the L2 to the features
    // column and make that column "norms"
    // Select lat, lon, and
    val norm_udf = udf(norm _)
    val filtered = all.filter("year == " + year)
      .withColumn("norms", norm_udf(col("scaled_features").cast(VectorType)))
      .select("lat", "lon", "norms")

    // Multiply norm by 100, truncate decimal,
    // and form RDD on final "norms" value
    // by place "norms" many entries of (lat, lon)

    // UDF to explode the row by "v"
    val explodeUDF = udf((lat: Int, lon: Int, v: Int) => (0 to (v * 100.0).toInt).map(_ => (lat, lon)))
    filtered
      // Form a column of lists of "norms" many (lat,lon)
      // for each row
      .withColumn("vals",
        explodeUDF(col("lat"), col("lon"), col("norms")))

      // Take only these values as RDD
      .select("vals")
      .rdd
      .map(r => r(0).asInstanceOf[(Int, Int)])
  }
}
