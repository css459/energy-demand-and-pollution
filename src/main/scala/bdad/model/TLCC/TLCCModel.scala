//
// BDAD Final Project
// TLCC Model
// Cole Smith
// TLCCModel.scala
//

package bdad.model.TLCC

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object TLCCModel {

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
  def breakoutVectorCols(df: DataFrame, vectorLabels: Array[String],
                         vectorCol: String = "scaled_features"): RDD[(String, Array[Double])] = {

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
      .map(i => (vectorLabels(i), vectors.map(a => a(i))))
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
    new PearsonsCorrelation().correlation(s1, s2)
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

    // Slice the anchor signal
    // If forward positive shift, slice [shift, length]
    // If backward negative shift, slice [0, length - shift]
    if (shift > 0) {
      // Shift the test signal without padding
      val shiftedTestSignal = TLCCModel.shiftSignal(testSignal, shift)
      val sliced = anchorSignal.slice(shift, anchorSignal.length)
      cor(shiftedTestSignal, sliced)
    } else {
      //      val sliced = anchorSignal.slice(0, anchorSignal.length - shift)
      //      cor(shiftedTestSignal, sliced)

      // A backward shift of the test signal is the same as a forward
      // shift of the anchor signal
      val shiftedTestSignal = TLCCModel.shiftSignal(anchorSignal, shift * -1)
      val sliced = testSignal.slice(shift, testSignal.length)
      cor(shiftedTestSignal, sliced)
    }
  }
}


/**
  * Computes the Pearson Cross Correlation matrix
  * for two sets of vectors, within two DataFrames.
  *
  * The DataFrames should contain a column of type `Vector`.
  * This column should represent a data point in time. The
  * column should be a scaled representation of the data, which
  * can be generated with functions from `bdad.etl.util`.
  *
  * The DataFrames will then be joined, on the `year` and `day of year`
  * The vectors are broken out column-wise, and the cartesian product
  * is used to form an all-play-all correlation matrix.
  *
  * This matrix is calculated for each lag in `lags`, which is the number
  * of units (positive for forward, negative for backward) to shift the
  * values in `dfX`, while comparing against static `dfY`.
  *
  * @param dfIX Input DataFrame Ingress object
  * @param dfIY Input DataFrame Ingress object
  * @param lags Array of lag times in units
  */
class TLCCModel(dfIX: TLCCIngress, dfIY: TLCCIngress, lags: Array[Int]) {

  //
  // Private Fields
  //

  private val dfX = dfIX.df
  private val dfY = dfIY.df

  private val vectorColX = dfIX.vectorCol
  private val yearColX = dfIX.yearCol
  private val dayColX = dfIX.dayCol
  private val vectorColY = dfIY.vectorCol
  private val yearColY = dfIY.yearCol
  private val dayColY = dfIY.dayCol

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
    val vecsX = dfX.select(yearColX, dayColX, vectorColX)
      .withColumnRenamed(vectorColX, xFeatures)

    vecsX.show(false)

    val vecsY = dfY.select(yearColY, dayColY, vectorColY)
      .withColumnRenamed(vectorColY, yFeatures)

    vecsY.show(false)

    vecsX.join(vecsY,
      dfX(yearColX) <=> dfY(yearColY) &&
        dfX(dayColX) <=> dfY(dayColY),
      "inner")
  }

  //
  // Public Functions
  //

  /**
    * Compute every combination of cross correlation
    * for every given lag using cartesian product between
    * X and Y vector dimensions from `dfX` and `dfY`.
    *
    * The result is given in the form:
    * (vectorX dim, vectorY dim, lag) -> Pearson Correlation
    *
    * @return Result RDD
    */
  def allPlayAll(): RDD[((String, String, Int), Double)] = {
    println("[ TLCC ] Starting TLCC Job")

    println("[ TLCC ] Joining DataFrames")
    val joined = this.joinDataframes()

    println("[ TLCC ] Breaking out vector columns")

    // Break out vectors into IDs and values
    val slideCols: RDD[(String, Array[Double])] =
      TLCCModel.breakoutVectorCols(joined, dfIX.vectorColLabels, xFeatures).persist
    val anchorCols: RDD[(String, Array[Double])] =
      TLCCModel.breakoutVectorCols(joined, dfIY.vectorColLabels, yFeatures).persist

    println("[ TLCC ] Generating combinations for jobs")

    // Broadcast the lags to all workers so they can be used with RDDs
    val broadcastLags: Broadcast[Array[Int]] = SparkContext.getOrCreate.broadcast(lags)

    // From the cartesian product (All-Play-All) of the slide cols and anchor cols,
    // append all needed lags to get all correlations needed
    val combos: RDD[(Int, (String, Array[Double]), (String, Array[Double]))] = slideCols.cartesian(anchorCols)
      .flatMap { case (a, b) => broadcastLags.value.map(l => (l, a, b)) }

    println("[ TLCC ] Generating cross correlations")

    // Calculate the correlations
    combos.map { case (l, (idx, vecX), (idy, vecY)) =>
      ((idx, idy, l), // Vector labels for X and Y, the lag
        TLCCModel.tlcc(vecX, vecY, l)) // The correlation
    }
  }
}
