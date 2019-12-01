//
// BDAD Final Project
// PetroleumDataset & PetroleumDataset ETL
// Andrii Dobroshynskyi
// PetroleumDataset.scala
//

package bdad.etl.petroleumdata
import bdad.etl.util.{maxAbs, normalize}
import org.apache.spark.sql.{ DataFrame, SparkSession}
import org.apache.spark.sql.functions.{ to_date, year, _}

// Companion Object for Static Members
object PetroleumDataset {
  val PETROLEUM = "hdfs:///user/ad3634/PETROLEUM.csv"
  val ALL_YEARS: Array[Int] = ETL.makeYearList(1980, 2019)

  object ETL {
    val COLS = Array("Date", "Price")
    val NORM_COLS = Array("Price")

    def makeYearList(start: Int, end: Int): Array[Int] = {
      (start to end).toArray
    }

    def makeYearList(singularYear: Int): Array[Int] = {
      Array(singularYear)
    }

    def createDF(fileName: String): DataFrame = {
      val spark = SparkSession.builder.getOrCreate
      val df = spark.read.format("csv").option("header", "true").load(fileName)
      val renamed = df.toDF(ETL.COLS.toSeq: _*)
      renamed
    }

    def processDF(df: DataFrame): DataFrame = {
      val processed = df.
          withColumn("Date", to_date(df.col("Date"), "yyyyMMdd"))
      val splitDate = processed.
        select(col("*"), substring(col("Date"), 0, 4).as("Year")).
        select(col("*"), substring(col("Date"), 6, 2).as("Month")).
        select(col("*"), substring(col("Date"), 9, 2).as("Day"))
      splitDate
    }
  }
}

class PetroleumDataset(var years: Array[Int] = PetroleumDataset.ALL_YEARS) {

  // extra constructor
  def this(year: Int) = this(PetroleumDataset.ETL.makeYearList(year))

  val columns: Array[String] = Array("Price")
  val df: DataFrame = PetroleumDataset.ETL.processDF(PetroleumDataset.ETL.createDF(PetroleumDataset.PETROLEUM))

  /**
   * Returns petroleum spot prices ONLY, normalized and scaled to range
   * [-1,1] akin to AirData.
   */
  lazy val prepared: DataFrame = {
    val selectedCols = PetroleumDataset.ETL.NORM_COLS
    val priceDF = df.select(selectedCols.head, selectedCols.tail: _*).withColumn("Price", df("Price").cast("double"))

    maxAbs(normalize(priceDF, Array("Price"), useMean = true, useStd = true))
  }

  /**
   * Returns ALL petroleum price data along with computed year,
   * day of year fields
   */
  lazy val dayProcessed: DataFrame = {
    val newDF = df.
      withColumn("Price", df("Price").cast("double")).
      withColumn("year(dateGMT)", year(col("Date"))).
      withColumn("dayofyear(dateGMT)", dayofyear(col("Date")))
    newDF
  }
}

