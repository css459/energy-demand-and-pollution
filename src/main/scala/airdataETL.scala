import java.io.File
import java.nio.file.Paths

import org.apache.spark.sql.{DataFrame, SparkSession}

object RelevantCols {
  val COLS = Array(
    "Parameter Name",
    "Latitude",
    "Longitude",
    "Date GMT",
    "Time GMT",
    "Sample Measurement",
    "Units of Measure",
    "Date of Last Change")

  def selectRelevantCols(df: DataFrame): DataFrame = {
    df.select(RelevantCols.COLS.head, RelevantCols.COLS.tail: _*)
  }
}

object AirDataETL {

  //  val AIRDATA_HOME = "/scratch/css459/AIRDATA"
  val AIRDATA_HOME = "/home/cole/Documents/DATA/AIRDATA"

  val AIRDATA_OUTPUT = "/scratch/css459/AIRDATA_COMBINED"

  def loadCSVSet(containingDir: String): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()

    spark.read
      .format("csv")
      .option("header", "true")
      .load(Paths.get(containingDir.toString, "*.csv").toString)
  }

  def getListOfSubDirectories(directory: String): Array[String] = {
    new File(directory)
      .listFiles
      .filter(_.isDirectory)
      .map(_.getName)
  }

  def popLastDirectory(directory: String): String = {
    directory.split('/').dropRight(1).mkString("/")
  }

  def makeOutputPathFromDirectory(directory: String): String = {
    val fileName = directory.split('/').takeRight(2).mkString("_") + ".csv"
    Paths.get(AIRDATA_OUTPUT, fileName).toString
  }

  //  def etlSubdirectory(directory: String): Unit = {
  //    val dfSubclass = loadCSVSet(directory)
  //
  //  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Airdata ETL Operations")
      .config("spark.master", "local")
      .getOrCreate

    // Form the fully qualified paths to all types and subtypes
    // of EPA Air Data
    // Each directory in this list will represent one kind of observed
    // data, and it is assumed that the CSVs in these directories differ
    // only in the year of collection (not in data formatting).
    val airdataClasses = spark.sparkContext.parallelize(getListOfSubDirectories(AIRDATA_HOME))
      .map(p => Paths.get(AIRDATA_HOME, p).toString)
      .flatMap(p => getListOfSubDirectories(p)
        .map(n => Paths.get(p, n).toString))

    airdataClasses.foreach(println)

    // FOR DEBUGGING PURPOSES
    val airdataClassesReduced = spark.sparkContext.parallelize(airdataClasses.take(2))

    // Read a collection of CSVs in each Air Data class, and run
    // the ETL operations on those sets. Then, write each of them
    // back to supersets which are the concatenation of all the CSVs
    // for each Air Data class.
    //
    // This is done like so:
    //    1. Create a pair RDD containing the subsets to the combined DataFrames
    //    2. Select the relevant columns from those DataFrames
    //    3. Remove the last directory from every key, representing the superset
    //    4. Reduce the Pair RDD on the superset directory paths, combining via Row Union
    //    5. Write back combined CSV files to disk
    //
    val allDataFrames = airdataClassesReduced
      .map(groupPath => (popLastDirectory(groupPath), loadCSVSet(groupPath)))
      .mapValues(RelevantCols.selectRelevantCols)

    val combinedDataFrames = allDataFrames.reduceByKey((a, b) => a.union(b))

    combinedDataFrames.foreach({ case (path, df) => df
      .write.format("csv").save(makeOutputPathFromDirectory(path))
    })

    spark.stop
  }

}