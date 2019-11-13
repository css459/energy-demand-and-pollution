package bdad.etl.airdata

import bdad.etl.util._
import bdad.etl.Scenarios.gasses2014to2019
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object AirDatasetTest extends App {

  val spark = SparkSession
    .builder
    .appName("Airdata ETL Operations")
    .getOrCreate

  //  // Saves the Gas Scenario to Disk
  //  // Converts the scaled features to a string
  //  val d = gasses2014to2019
  //    .withColumn("features", gasses2014to2019("scaled_features").cast("string"))
  //    .drop("scaled_features")
  //
  //  writeToDisk(d, "gasses-2014-2019.csv", coalesceTo1 = true)

  //  val criteria = Array("gasses/*")
  //  val airdata = new AirDataset(2019, criteria)

  // Show Schema
  // val df = airdata.df.cache
  // df.printSchema()

  // Show Describe Table
  // df.describe().show(false)

  // Show distinct criteria
  // println("CRITERIA")
  // df.select("criteria").distinct.show(100,false)

  // Show distinct units
  // println("UNITS")
  // df.select("unit").distinct.show(false)

  //  // Test of Utilities: Scaling Features of Pivoted DataFrame
  //  val piv = airdata.pivotedDF(dropNull = true, dropUnit = true)
  //  piv.show(false)
  //
  //  // Apply Normalization using Std and Mean
  //  val normed = normalize(piv, airdata.validCriteria, useMean = true, useStd = true)
  //
  //  // Apply Max Absolute scaling to get values in range [-1,1]
  //  maxAbs(normed, Array("scaled_features")).show(false)
}
