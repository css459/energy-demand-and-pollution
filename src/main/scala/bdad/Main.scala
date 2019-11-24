package bdad

import bdad.etl.Scenarios
import bdad.model.TLCC
import org.apache.spark.sql.SparkSession
object Main extends App {

  val spark = SparkSession
    .builder
    .appName("Airdata ETL Operations")
    .getOrCreate

  spark.sparkContext.setLogLevel("WARN")

  val gasses = Scenarios.gasses2019test

  // writeToDisk(gasses, "gasses-2014-2019")

  new TLCC(gasses, gasses, Array(1, 2, 3)).fit().take(3).foreach(println)

  //  gasses.select("scaled_features").show(20, truncate = false)
  //  breakoutVectorCols(gasses).take(20).foreach(r => println(r.toString))

  //  val Row(coeff1: Matrix) = Correlation.corr(gasses, column = "scaled_features").head
  //
  //  coeff1.toArray.foreach(println)
  //  gasses.columns.foreach(println)
}
