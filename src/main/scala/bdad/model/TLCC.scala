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
  def breakoutVectorCols(df: DataFrame, vectorCol: String = "scaled_features"): RDD[(Int, RDD[Double])] = {

    // Convert the Vector column to an RDD of Array[Double]
    val vectors: RDD[Array[Double]] = df.rdd.map(r => r.getAs[Vector](vectorCol).toArray)

    // Get the dimensionality of the vector for transposition
    val n = vectors.first.length
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
  def shiftSignal(signal: RDD[Double], shift: Int, padding: Boolean = false): RDD[Double] = {

    // No shift, no-op
    if (shift == 0) return signal

    val sc = SparkContext.getOrCreate

    if (shift > 0) {
      // Forward shift
      // Slice vector to range [0, length - shift]
      val arr = signal.collect
      val sliced = arr.slice(0, arr.length - shift)

      // Pad with zeros in front

      if (padding) sc.parallelize(Array.concat(Array.fill[Double](shift)(0), sliced), 1)
      else sc.parallelize(sliced, 1)

    } else {
      // Backward shift
      // Slice vector to range [shift, length]
      val arr = signal.collect
      val sliced = arr.slice(shift, arr.length)

      // Pad with zeros in back
      if (padding) sc.parallelize(Array.concat(sliced, Array.fill[Double](shift)(0)), 1)
      else sc.parallelize(sliced, 1)
    }
  }

  /**
   * Pearson Correlation between two signals.
   *
   * @param s1 Signal 1
   * @param s2 Signal 2
   * @return Pearson Correlation
   */
  def cor(s1: RDD[Double], s2: RDD[Double]): Double = {
    Statistics.corr(s1, s2)
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
  def tlcc(testSignal: RDD[Double], anchorSignal: RDD[Double], shift: Int): Double = {
    // Shift the test signal without padding
    val shiftedTestSignal = TLCC.shiftSignal(testSignal, shift)

    if (shift == 0) return cor(shiftedTestSignal, anchorSignal)

    // Slice the anchor signal
    // If forward positive shift, slice [shift, length]
    // If backward negative shift, slice [0, length - shift]
    val sc = SparkContext.getOrCreate
    val arr = anchorSignal.collect

    if (shift > 0) {
      val sliced = arr.slice(shift, arr.length)
      cor(shiftedTestSignal, sc.parallelize(sliced, 1))
    } else {
      val sliced = arr.slice(0, arr.length - shift)
      cor(shiftedTestSignal, sc.parallelize(sliced, 1))
    }
  }
}

// TODO
class TLCC(dfX: DataFrame, vectorColX: String = "scaled_features",
           dfY: DataFrame, vectorColY: String = "scaled_features",
           lags: Array[Integer]) {


}
