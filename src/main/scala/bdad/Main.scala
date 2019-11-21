package bdad

import bdad.etl.Scenarios
import bdad.etl.util.writeToDisk
import org.apache.spark.sql.SparkSession

object Main extends App {

  val spark = SparkSession
    .builder
    .appName("Airdata ETL Operations")
    .getOrCreate

  spark.sparkContext.setLogLevel("WARN")

  val gasses = Scenarios.gasses2014to2019
  writeToDisk(gasses, "gasses-2014-2019")


  //  gasses.select("scaled_features").show(20, truncate = false)
  //  breakoutVectorCols(gasses).take(20).foreach(r => println(r.toString))

  //  val Row(coeff1: Matrix) = Correlation.corr(gasses, column = "scaled_features").head
  //
  //  coeff1.toArray.foreach(println)
  //  gasses.columns.foreach(println)
}
