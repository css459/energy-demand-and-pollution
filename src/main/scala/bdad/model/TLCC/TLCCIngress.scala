//
// BDAD Final Project
// TLCC Model
// Cole Smith
// TLCCIngress.scala
//

package bdad.model.TLCC

import org.apache.spark.sql.DataFrame

import scala.util.Try

/**
  * DataFrame ingress for TLCC model usage. Defines the necessary
  * columns used by the `TLCC` class. The input DataFrame `df` should
  * contain a scaled features column of type `Vector`. Methods for
  * converting standard DataFrames to this format can be found in
  * `bdad.etl.util`.
  *
  * @param df              Input DataFrame for TLCC
  * @param vectorColLabels Labels for the dimensions of the Vector column
  * @param vectorCol       The name of the Vector column to process
  * @param yearCol         The name of the year column for sorting
  * @param dayCol          The name of the day of year column for sorting
  */
class TLCCIngress(val df: DataFrame, val vectorColLabels: Array[String],
                  val vectorCol: String = "scaled_features",
                  val yearCol: String = "year(dateGMT)",
                  val dayCol: String = "dayofyear(dateGMT)") {


  private def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

  // Verify input DataFrame has proper columns
  if (!hasColumn(df, vectorCol))
    throw new Exception("Ingress DataFrame did not contain vector column:" + vectorCol)

  if (!hasColumn(df, yearCol))
    throw new Exception("Ingress DataFrame did not contain year column:" + yearCol)

  if (!hasColumn(df, dayCol))
    throw new Exception("Ingress DataFrame did not contain day column:" + dayCol)

  // TODO
  // Verify column types
  var failure = false
  for ((name, dtype) <- df.dtypes) {
    println(name + " : " + dtype)
    //    if (name.equals(vectorCol) && !dtype.contains("org.apache.spark.ml.linalg.Vector")) failure = true
    //    if (name.equals(yearCol) && !dtype.equals("IntegerType")) failure = true
    //    if (name.equals(dayCol) && !dtype.equals("IntegerType")) failure = true
  }
  //
  //  if (failure)
  //    throw new Exception("Column type mismatch. Verify:\n" + df.schema.toString)
}
