package bdad.etl.airdata

import org.apache.spark.sql.SparkSession

object AirDatasetTest extends App {

  val spark = SparkSession
    .builder
    .appName("Airdata ETL Operations")
    .getOrCreate

  val criteria = Array("gasses/*")
  val airdata = new AirDataset(2019, criteria)

  // Show Schema
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

  println(airdata.matrix.take(10))
}
