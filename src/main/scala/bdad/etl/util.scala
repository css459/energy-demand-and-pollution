//
// BDAD Final Project
// ETL
// Cole Smith
// util.scala
//

package bdad.etl

import org.apache.spark.sql.DataFrame

/**
  * Utility functions and resources for
  * Extract Transform and Load operations
  */
object util {

  /**
    * Writes the provided Dataframe to disk.
    *
    * @param df       Dataframe to write to disk
    * @param filepath File path to write to (including filename.csv)
    */
  def writeToDisk(df: DataFrame, filepath: String): Unit = {
    df.write.format("csv").save(filepath)
  }
}
