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

  // val airdata = new AirDataset(2019, "2019_EXPANDED/hourly_42101_2019.csv")

  // Show Test DF
  val df = airdata.df.cache
  df.show(20, truncate = false)
  df.printSchema()

  // Show Describe Table
  // df.describe().show(false)

  // Show distinct criteria
  // println("CRITERIA")
  // df.select("criteria").distinct.show(100,false)

  // Show distinct units
  // println("UNITS")
  // df.select("unit").distinct.show(false)

  // TODO: Why does this cause an exception?
  // airdata.pivotedDF(dropNull = true).count()
}