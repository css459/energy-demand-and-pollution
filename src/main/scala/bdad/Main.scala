package bdad

import bdad.etl.Scenarios
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{Row, SparkSession}

object Main extends App {

  val spark = SparkSession
    .builder
    .appName("Airdata ETL Operations")
    .getOrCreate

  spark.sparkContext.setLogLevel("WARN")

  val gasses = Scenarios.gasses2014to2019
  val Row(coeff1: Matrix) = Correlation.corr(gasses, column = "scaled_features").head

  coeff1.toArray.foreach(println)
  gasses.columns.foreach(println)
}
