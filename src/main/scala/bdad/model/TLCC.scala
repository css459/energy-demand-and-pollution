package bdad.model

import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object TLCC {

  /**
   * Transposes the vector column of a given DataFrame such
   * that the RDD returned is a list of columns within that
   * list of Vectors. Each row in the final RDD is a dimension
   * of the input vectors.
   * The key is the dimension number. The value is the column values.
   *
   * @param df        DataFrame containing Vector column
   * @param vectorCol Name of the vector column
   * @return Columns within the list of Vectors
   */
  def breakoutVectorCols(df: DataFrame, vectorCol: String = "scaled_features"): RDD[(Int, Array[Double])] = {

    // Convert the Vector column to an RDD of Array[Double]
    val vectors: Array[Array[Double]] = df.rdd.map(r => r.getAs[Vector](vectorCol).toArray).collect

    // Get the dimensionality of the vector for transposition
    val n = vectors(0).length
    val spark = SparkContext.getOrCreate

    // For each dimension, from 0 to n, pull out the ith
    // item from each row vector to get the ith column.
    //    Use N partitions (one for each dimension)
    //    Collect each dimension to a local array
    spark.parallelize(Seq.range(0, n), n)
      .map(i => (i, vectors.map(a => a(i))))
  }

  /**
   * Shifts a signal by the specified number of units.
   * If `shift` is negative, then the signal
   * will slide backwards.
   *
   * @param signal  Time series signal to slide
   * @param shift   Number of units to shift the signal forward
   *                (positive) or backward (negative)
   * @param padding If true, appends zeros to voided elements
   * @return Shifted signal
   */
  def shiftSignal(signal: Array[Double], shift: Int, padding: Boolean = false): Array[Double] = {

    // No shift, no-op
    if (shift == 0) return signal

    val sc = SparkContext.getOrCreate

    if (shift > 0) {
      // Forward shift
      // Slice vector to range [0, length - shift]
      val sliced = signal.slice(0, signal.length - shift)

      // Pad with zeros in front

      if (padding) Array.concat(Array.fill[Double](shift)(0), sliced)
      else sliced

    } else {
      // Backward shift
      // Slice vector to range [shift, length]
      val sliced = signal.slice(shift, signal.length)

      // Pad with zeros in back
      if (padding) Array.concat(sliced, Array.fill[Double](shift)(0))
      else sliced
    }
  }

  /**
   * Pearson Correlation between two signals.
   *
   * @param s1 Signal 1
   * @param s2 Signal 2
   * @return Pearson Correlation
   */
  def cor(s1: Array[Double], s2: Array[Double]): Double = {
    val spark = SparkContext.getOrCreate
    val rdd1 = spark.parallelize(s1)
    val rdd2 = spark.parallelize(s2)

    Statistics.corr(rdd1, rdd2)
  }

  /**
   * Computes a single time-lagged cross correlation between
   * two signals by sliding the test signal forward (or backward)
   * by the shift parameter. If `shift` is negative, then the signal
   * will slide backwards.
   *
   * @param testSignal   Time series signal to slide
   * @param anchorSignal Time series signal to compare against
   * @param shift        Number of units to shift the signal forward
   *                     (positive) or backward (negative)
   * @return Pearson Correlation
   */
  def tlcc(testSignal: Array[Double], anchorSignal: Array[Double], shift: Int): Double = {
    if (shift == 0) return cor(testSignal, anchorSignal)

    // Shift the test signal without padding
    val shiftedTestSignal = TLCC.shiftSignal(testSignal, shift)

    // Slice the anchor signal
    // If forward positive shift, slice [shift, length]
    // If backward negative shift, slice [0, length - shift]
    if (shift > 0) {
      val sliced = anchorSignal.slice(shift, anchorSignal.length)
      cor(shiftedTestSignal, sliced)
    } else {
      val sliced = anchorSignal.slice(0, anchorSignal.length - shift)
      cor(shiftedTestSignal, sliced)
    }
  }
}

// TODO
class TLCC(dfX: DataFrame, dfY: DataFrame, lags: Array[Int],
           vectorColX: String = "scaled_features",
           yearColX: String = "year(dateGMT)", dayColX: String = "dayofyear(dateGMT)",
           vectorColY: String = "scaled_features",
           yearColY: String = "year(dateGMT)", dayColY: String = "dayofyear(dateGMT)") {

  // The discrete name for the vector columns of the X
  // and Y DataFrames
  private val xFeatures: String = "featuresX"
  private val yFeatures: String = "featuresY"

  /**
    * Selects the vector columns from the input DataFrames
    * and joins them on the year and dayofyear date columns.
    * The selected_features will be available in the returned
    * DataFrame as the names defined by xFeatures and yFeatures
    *
    * @return joined DataFrame
    */
  private def joinDataframes(): DataFrame = {
    val vecsX = dfX.select(yearColX, dayColX)
      .withColumn(xFeatures, dfX(vectorColX))

    val vecsY = dfY.select(yearColY, dayColY)
      .withColumn(yFeatures, dfY(vectorColY))

    vecsX.join(vecsY,
      dfX(yearColX) <=> dfY(yearColY) &&
        dfX(dayColX) <=> dfY(dayColY),
      "inner")
  }

  // RDD[((Int, Int), Double)]
  def fit(): RDD[(Int, Iterable[((Int, Array[Double]), (Int, Array[Double]))])] = {
    println("[ TLCC ] Starting TLCC Job")

    println("[ TLCC ] Joining DataFrames")
    val joined = this.joinDataframes()

    println("[ TLCC ] Breaking out vector columns")

    // Break out vectors into IDs and values
    val slideCols: RDD[(Int, Array[Double])] = TLCC.breakoutVectorCols(joined, xFeatures).persist
    val anchorCols: RDD[(Int, Array[Double])] = TLCC.breakoutVectorCols(joined, yFeatures).persist

    println("[ TLCC ] Generating combinations for jobs")

    //    val lagRDD = SparkContext.getOrCreate.parallelize(lags)
    //    val combos = slideCols.mapValues(v => lagRDD.map(l => (l, v)))

    // Combine the X vectors into a new RDD with the key as
    // the lag amount, and the values are `slideCols`
    val lagsX: RDD[(Int, (Int, Array[Double]))] = slideCols
      .flatMap { case (id, vec) => lags.map(l => (l, (id, vec))) }

    // Combine the Y vectors into a new RDD with the key as
    // the lag amount, and the values are `anchorCols`
    val lagsY: RDD[(Int, (Int, Array[Double]))] = anchorCols
      .flatMap { case (id, vec) => lags.map(l => (l, (id, vec))) }

    // Cogroup the two RDDs above
    // This will generate for each lag integer a list of two values. ie:
    //    lag : [ (id, vec), (id, vec) ]
    // Where the first entry is from X, and the second is from Y
    val groupings: RDD[(Int, (Iterable[(Int, Array[Double])], Iterable[(Int, Array[Double])]))] = lagsX.cogroup(lagsY)

    // Zip the cogroup, this will allow us to access the values
    // from a case statement and compute the TLCC using the `TLCC.tlcc`
    // function
    val g2: RDD[(Int, Iterable[((Int, Array[Double]), (Int, Array[Double]))])] = groupings.map { case (l, (iter1, iter2)) => (l, iter1.zip(iter2)) }
    g2.take(10).foreach(println)
    g2



    //    val combos = lagRDD
    //      .flatMap(lag => anchorCols.flatMap { case (idy, coly) =>
    //          slideCols.map { case (idx, colx) =>
    //            ( (idx, idy), (colx, coly) )
    //          }})


    //    .map( lag =>
    //      combos.collect.map { case ( (idy, idx), (vecY, vecX) ) =>
    //        ((idx, idy), TLCC.tlcc(vecX, vecY, lag))
    //      }
    //    )
  }

  // We will treat the dimensions in dfY as the anchor vectors, we
  // will then slide the dimensions from dfX over them in all-play-all
  // fashion.

  //  def fit(): RDD[((Int, Int), Double)] = {
  //    println("[ TLCC ] Starting TLCC Job")
  //
  //    val slideCols: RDD[(Int, RDD[Double])] = TLCC.breakoutVectorCols(dfX, vectorColX).persist
  //    val anchorCols: RDD[(Int, RDD[Double])] = TLCC.breakoutVectorCols(dfY, vectorColY).persist
  //
  //    println("[ TLCC ] Column breakout done. Starting combination generation for jobs")
  //
  //    val testCols: RDD[(Int, RDD[Double])] = slideCols.map { case (id, vec) =>
  //      (
  //        id,
  //        lags.foreach(lag => TLCC.shiftSignal(vec, padding = False))
  //      )
  //    }
  //
  //    // A collect call is necessary here since flatMap requires an interable
  //    val combos: RDD[((Int, Int), (RDD[Double], RDD[Double]))] =
  //      anchorCols.flatMap{case (idy, vecY) => slideCols.collect().map{case (idx, vecX) => ( (idy, idx), (vecY, vecX) )}}
  //
  //    println("[ TLCC ] Combinations done. Starting cross correlation jobs")
  //
  //    SparkContext.getOrCreate.parallelize(lags, lags.length).map( lag =>
  //      combos.collect.map { case ( (idy, idx), (vecY, vecX) ) =>
  //        ((idx, idy), TLCC.tlcc(vecX, vecY, lag))
  //      }
  //    )

  //    // Generate all lag columns in form
  //    //    (test col index, lag amount), [vector]
  //    val testCols: RDD[((Int, Int), RDD[Double])] =
  //    SparkContext.getOrCreate.parallelize(lags)
  //      .flatMap(lag => slideCols.map(col => ((col._1, lag), TLCC.shiftSignal(col._2, lag, padding = false))))
  //
  //    println("[ TLCC ] Column breakout done. Starting cross correlation jobs")
  //
  //    SparkContext.getOrCreate.parallelize(lags)
  //
  //      // Generate all the combinations of (lag, x col, y col) that we need
  //      .flatMap(lag =>
  //        slideCols.flatMap(xCol =>
  //          anchorCols.map(yCol =>
  //            (lag, xCol, yCol))))
  //
  //      // Execute TLCC for each (lag, x col, y col)
  //      .map((lag, xCol, yCol) => )
  //  }


}
