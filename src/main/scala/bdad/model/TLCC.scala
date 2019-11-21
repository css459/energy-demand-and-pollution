package bdad.model

import org.apache.spark.SparkContext
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object TLCC {

  def breakoutVectorCols(df: DataFrame, vectorCol: String = "scaled_features"): RDD[Array[Double]] = {

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
      .map(i => vectors.map(a => a(i)).collect)
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
    if (shift == 0) signal

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

  //  /**
  //   * Pearson Correlation between two signals.
  //   *
  //   * @param s1 Signal 1
  //   * @param s2 Signal 2
  //   * @return Pearson Correlation
  //   */
  //  def cor(s1: Vector, s2:Vector): Double = {
  //
  //  }

  //  /**
  //   * Computes a single time-lagged cross correlation between
  //   * two signals by sliding the test signal forward (or backward)
  //   * by the shift parameter. If `shift` is negative, then the signal
  //   * will slide backwards.
  //   *
  //   * @param testSignal    Time series signal to slide
  //   * @param anchorSignal  Time series signal to compare against
  //   * @param shift         Number of units to shift the signal forward
  //   *                      (positive) or backward (negative)
  //   * @return  Pearson Correlation
  //   */
  //  def tlcc(testSignal: Vector, anchorSignal: Vector, shift: Int): Double = {
  //    // Shift the test signal without padding
  //    val shiftedTestSignal = TLCC.shiftSignal(testSignal, shift)
  //
  //    // Slice the anchor signal
  //    // If forward positive shift, slice [shift, length]
  //    // If backward negative shift, slice [0, length - shift]
  //
  //  }
}

class TLCC() {

}
