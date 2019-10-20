package bdad.etl.airdata

import org.apache.spark.sql.SparkSession

object AirDatasetTest extends App {

  val spark = SparkSession
    .builder
    .appName("Airdata ETL Operations")
    .config("spark.master", "local")
    .getOrCreate

  val criteria = Array("2019_EXPANDED")
  val airdata = new AirDataset(AirDataset.ALL_YEARS, criteria)

  //  val df = airdata.df.cache
  //  df.columns.foreach(println)
  //  println(df.first)

  airdata.pivotedDF(dropNull = true).count()
}