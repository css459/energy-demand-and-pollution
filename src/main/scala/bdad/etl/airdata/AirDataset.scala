//
// BDAD Final Project
// AirDataset ETL
// Cole Smith
// AirDataset.scala
//

package bdad.etl.airdata

import java.nio.file.Paths

import bdad.etl.airdata.AirDataset.ETL
import bdad.etl.util.{maxAbs, minMax, normalize}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

// Companion Object for Static Members
object AirDataset {

  // Constants
  val AIRDATA = "hdfs:///user/css459/AIRDATA"

  // Year Listing
  val ALL_YEARS: Array[Int] = makeYearList(1980, 2019)

  //
  // Utility
  //

  /**
    * Makes a list of years given a starting and end range.
    * It is assumed that start < end. If not, then the list
    * will be reversed.
    *
    * @param start Start year (for AIRDATA, the earliest is 1980)
    * @param end   End year
    * @return Array of years for range
    */
  def makeYearList(start: Int, end: Int): Array[Int] = {
    (start to end).toArray
  }

  /**
    * Returns only a single year as an array.
    *
    * @param singularYear Singular year
    * @return Year Integer in Array
    */
  def makeYearList(singularYear: Int): Array[Int] = {
    Array(singularYear)
  }

  /**
    * Sorts the given DataFrame by its `dateGMT` column.
    *
    * @param df        The Dataframe from AirDataset to sort
    * @param ascending Whether or not the date is ascending
    * @return Sorted Dataframe
    */
  def sortByDate(df: DataFrame, ascending: Boolean = true): DataFrame = {
    if (ascending)
      df.orderBy(asc("dateGMT"))
    else
      df.orderBy(desc("dateGMT"))
  }

  //
  // ETL Object
  //

  /**
    * Object to filter Dataframes for relevant columns.
    * Converts the schema to a normalized form and cleans
    * elements for any AIRDATA Dataframe instance.
    */
  object ETL {
    val COLS = Array(
      "Parameter Name",
      "State Name",
      "County Name",
      "Latitude",
      "Longitude",
      "Date GMT",
      "Time GMT",
      "Sample Measurement",
      "Units of Measure")

    val NEW_COLS = Array(
      "criteria",
      "state",
      "county",
      "lat",
      "lon",
      "dateGMT",
      "timeGMT",
      "value",
      "unit"
    )

    /**
      * Selects only the columns from `COLS` in the given Dataframe.
      *
      * @param df Dataframe to filter
      * @return Filtered Dataframe
      */
    def selectRelevantCols(df: DataFrame): DataFrame = {
      df.select(ETL.COLS.head, ETL.COLS.tail: _*)
    }

    /**
      * Converts the original AIRDATA Dataframe schema into a normalized
      * format specified by `NEW_COLS`. Also fixes the column types to
      * proper values.
      *
      * @param df Dataframe to convert
      * @return Converted Dataframe
      */
    def convertSchema(df: DataFrame): DataFrame = {
      val newDF = df.toDF(NEW_COLS: _*)
      newDF
        // The name of the thing being measured
        .withColumn("criteria",
          regexp_replace(newDF("criteria").cast("string"), "&", "and"))

        // State and County in normalized form
        .withColumn("state", lower(newDF("state").cast("string")))
        .withColumn("county", lower(newDF("county").cast("string")))

        // The place of measurement (Precision of 6 decimals to exclude tiny mantissa values)
        .withColumn("lat", round(newDF("lat").cast("double"), 6))
        .withColumn("lon", round(newDF("lon").cast("double"), 6))

        // Date: To form a Date Type column, concatenate the two Sting columns of Day and Hour
        .withColumn("dateGMT",
          to_date(concat(newDF("dateGMT"), lit(" "), newDF("timeGMT")), "yyyy-MM-dd H:mm"))

        // The units and value of the measurement (Round to 10 to exclude tiny mantissa values)
        .withColumn("unit", newDF("unit").cast("string"))
        .withColumn("value", round(newDF("value").cast("double"), 10))
        .drop("timeGMT")
    }
  }
}


/**
  * Forms an AIRDATA Dataset using the provided years and measurement criteria.
  * The criteria array represents strings of relative paths in the AIRDATA parent
  * directory.
  *
  * For example, to take all available data, one would have:
  * Array("star/star")   // where "star" is "*"
  *
  * To take all gasses and wind data, one would have:
  * Array("gasses/star", "meteorological/Winds")
  *
  * The resulting Dataframe is stored in the `df` property.
  *
  * @param years    The list of years to use (Use Dataset.makeYearList() to form) (or singular Int year)
  * @param criteria The measurement parameters included in the Dataframe (or singular String criteria)
  */
class AirDataset(var years: Array[Int] = AirDataset.ALL_YEARS, var criteria: Array[String]) {

  //
  // Constructors
  //

  def this(years: Array[Int], criteria: String) = this(years, Array(criteria))

  def this(year: Int, criteria: String) = this(AirDataset.makeYearList(year), Array(criteria))

  def this(year: Int, criteria: Array[String]) = this(AirDataset.makeYearList(year), criteria)

  //
  // Public Fields
  //

  /**
    * The converted, cleaned, and filtered DataFrame for the given class instance.
    */
  val df: DataFrame = ETL.convertSchema(AirDataset.ETL.selectRelevantCols(formMajorDF(formFileList())))

  /**
   * A representation of the `df` contained by this object in a cleaned, scaled,
   * and featurized matrix format suitable for machine learning applications.
   *
   * The time will be rescaled to number of hours since epoch, and then
   * the range will be constrained to [0,1] such that the first observation
   * will have time=0.0 and the last observation will have time=1.0
   *
   * Each row is a **lat/lon pair at a point in time**
   *
   * The matrix will have the following layout:
   * lat lon time criteria_1 ... criteria_n
   * The ordering of criteria is the same as in `validCriteria`
   */
  lazy val matrix: RDD[Vector] = prepareMatrix(pivotedDF(dropNull = true, dropUnit = true))

  /**
    * The set of criteria covered by this instance of the AIRDATA dataset.
    */
  val validCriteria: Array[String] = df
    .select("criteria")
    .distinct
    .collect
    .map(_.toString)
    .map(_.stripPrefix("["))
    .map(_.stripSuffix("]"))

  /**
    * Returns the combined, cleaned Dataframe of this class, but pivoted on the Criteria column.
    * In this way, each row is a unique area (defined by the `latLonPrecision` at a given time,
    * `dateGMT`. Not all measurements are available in all areas, so the matrix may be sparse.
    * To remove these spare solutions, set `dropNull` to `true`, which will drop all rows with
    * ANY null value.
    *
    * The following is a table of Latitude Longitude Precision:
    * 5 ~ 1.11 m
    * 4 ~ 11.1 m
    * 3 ~ 111  m
    * 2 ~ 1.11 km
    * 1 ~ 11.1 km
    *
    * @param dropNull        Whether or not to drop all rows with any null value
    * @param latLonPrecision The number of decimal places to use for Lat and Lon
    * @param dropUnit        Whether or not to drop the unit column. This is required
    *                        if using pivoted data for normalization.
    * @return Pivoted `Dataframe` object.
    */
  def pivotedDF(dropNull: Boolean, latLonPrecision: Int = 3, dropUnit: Boolean = false): DataFrame = {

    val initial = if (dropUnit) df.drop("unit") else df
    val pivoted = initial

      // Change Precision
      .withColumn("lat", round(df("lat"), latLonPrecision))
      .withColumn("lon", round(df("lon"), latLonPrecision))

      // Group by place and time, pivot Criteria on Values
      .groupBy(
        "lat",
        "lon",
        "dateGMT")
      .pivot("criteria")

    val pivotAgg = if (dropUnit)
      pivoted.agg(avg("value"))
    else
      pivoted.agg(avg("value"), first("unit"))

    if (dropNull)
      pivotAgg.na.drop("any")
    else
      pivotAgg
  }

  //
  // Private Fields
  //

  // Forms the distinct file paths in AIRDATA that we must read from
  private def formFileList(): Array[String] = {
    // criteria.map(Paths.get(AirDataset.AIRDATA, _).toString).distinct

    val sc = SparkSession.builder.getOrCreate.sparkContext

    criteria

      // Append the AIRDATA home path, and filter down to distinct
      // paths
      .map(Paths.get(AirDataset.AIRDATA, _).toString)
      .distinct

      // Map requested years to base paths
      .flatMap(p => years.map(y => Paths.get(p, "*" + y.toString + "*.csv").toString))

      // Expand the globs to fully-qualified paths
      .flatMap(p => FileSystem
        .get(sc.hadoopConfiguration)
        .globStatus(new Path(p)))
      .map(p => p.getPath.toString)
  }

  // Returns a major Dataframe of all information in the requested AIRDATA paths
  private def formMajorDF(dirList: Array[String]): DataFrame = {
    val spark = SparkSession.builder.getOrCreate
    dirList
      .map(spark.read.option("header", "true").csv(_))
      .reduce(_ union _)
  }

  /**
   * Prepares the matrix used by the `matrix` property of AirDataset.
   * The following operations are performed:
   * lat, lon, time are rescaled to range [0,1]
   * criteria columns are normalized and scaled to range [-1,1]
   * state and county are dropped
   *
   * @param df Input DataFrame (pivoted)
   * @return RDD Matrix
   */
  private def prepareMatrix(df: DataFrame): RDD[Vector] = {
    validCriteria.foreach(println)
    val criteria = df.select(validCriteria.head, validCriteria.tail: _*)
    val criteriaMatrix: RDD[Vector] =
      maxAbs(normalize(criteria, criteria.columns, useMean = true, useStd = true))
        .select("scaled_features")
        .rdd
        .map(r => r(0).asInstanceOf[Vector])

    // DEBUG
    criteriaMatrix.take(10).foreach(println)

    val idCols = df.columns.filter(c => !validCriteria.contains(c))

    //TODO: Compute the minimum date, transform the id cols
    //      Merge the Id cols and the criteria
    val minDate = df.select("dateGMT")
    val idRows = df.select(idCols.head, idCols.tail: _*)
      .withColumn("date", df(""))
    val idMatrix: RDD[Vector] = minMax(idRows, idRows.columns)

  }
}
