package bdad

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Context {

  val spark: SparkSession = {
    val s = SparkSession
      .builder
      .appName("Airdata ETL Operations")
      .master("yarn")
      .getOrCreate

    s.sparkContext.setLogLevel("WARN")
    s
  }

  val context: SparkContext = spark.sparkContext
}
