package bdad.model

import bdad.Context
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
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
    val spark = Context.context
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
  * @param dfX        Input DataFrame containing a column of Vectors
  * @param dfY        Input DataFrame containing a column of Vectors
  * @param lags       Array of lag times in units
  * @param vectorColX Name of the Vector column in `dfX`
  * @param yearColX   Name of the Vector column in `dfX`
  * @param dayColX    Name of the dayOfYear column in `dfX`
  * @param vectorColY Name of the Vector column in `dfY`
  * @param yearColY   Name of the dayOfYear column in `dfY`
  * @param dayColY    Name of the dayOfYear column in `dfY`
  */
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
  def allPlayAll(): RDD[((Int, Int, Int), Double)] = {
    println("[ TLCC ] Starting TLCC Job")

    println("[ TLCC ] Joining DataFrames")
    val joined = this.joinDataframes()

    println("[ TLCC ] Breaking out vector columns")

    // Break out vectors into IDs and values
    val slideCols: RDD[(Int, Array[Double])] = TLCC.breakoutVectorCols(joined, xFeatures).persist
    val anchorCols: RDD[(Int, Array[Double])] = TLCC.breakoutVectorCols(joined, yFeatures).persist

    println("[ TLCC ] Generating combinations for jobs")

    //    val combos = slideCols.mapValues(v => lagRDD.map(l => (l, v)))

    // Broadcast the lags to all workers so they can be used with RDDs
    val broadcastLags: Broadcast[Array[Int]] = SparkContext.getOrCreate.broadcast(lags)

    // From the cartesian product (All-Play-All) of the slide cols and anchor cols,
    // append all needed lags to get all correlations needed
    val combos: RDD[(Int, (Int, Array[Double]), (Int, Array[Double]))] = slideCols.cartesian(anchorCols)
      .flatMap { case (a, b) => broadcastLags.value.map(l => (l, a, b)) }

    println("[ TLCC ] Generating cross correlations")

    // Calculate the correlations
    combos.map { case (l, (idx, vecX), (idy, vecY)) =>
      ((idx, idy, l), // Vector labels for X and Y, the lag
        TLCC.tlcc(vecX, vecY, l)) // The correlation
    }

    //    // Combine the X vectors into a new RDD with the key as
    //    // the lag amount, and the values are `slideCols`
    //    val lagsX: RDD[(Int, (Int, Array[Double]))] = slideCols
    //      .flatMap { case (id, vec) => broadcastLags.value.map(l => (l, (id, vec))) }
    //
    //    // Combine the Y vectors into a new RDD with the key as
    //    // the lag amount, and the values are `anchorCols`
    //    val lagsY: RDD[(Int, (Int, Array[Double]))] = anchorCols
    //      .flatMap { case (id, vec) => broadcastLags.value.map(l => (l, (id, vec))) }
    //
    //    // Cogroup the two RDDs above
    //    // This will generate for each lag integer a list of two values. ie:
    //    //    lag : [ (id, vec), (id, vec) ]
    //    // Where the first entry is from X, and the second is from Y
    //
    //    val groupingsRDD: RDD[((Int, (Int, Array[Double])), (Int, (Int, Array[Double])))] = lagsX.cartesian(lagsY)

    //    val explode = groupings.ma

    //    // Zip the cogroup, this will allow us to access the values
    //    // from a case statement and compute the TLCC using the `TLCC.tlcc`
    //    // function
    //    val g2: RDD[(Int, ((Int, Array[Double]), (Int, Array[Double])))] =
    //    groupings.mapValues { case (iter1, iter2) => iter1.zip(iter2).head }
    //
    //    // DEBUG
    //    g2.take(10).foreach(println)
    //
    //    println("[ TLCC ] Generating cross correlations")
    //
    //    // Calculate the correlations
    //    val cors = g2.map { case (l, ( (idx, vecx), (idy, vecy) )) =>
    //      ((idx, idy, l),               // Vector labels for X and Y, the lag
    //        TLCC.tlcc(vecx, vecy, l))   // The correlation
    //    }
    //
    //    cors
  }

  //  (1,List(((1,[D@3e917218),(0,[D@40e0e658)), ((2,[D@1a6d31b3),(3,[D@184d3cc3)), ((3,[D@2df99c60),(1,[D@6aec6da1)), ((0,[D@197f39e6),(2,[D@6825a52e))))
  //  (2,List(((2,[D@9de2ed6),(0,[D@d6db29)), ((0,[D@600662dc),(2,[D@254fb727)), ((1,[D@5d9f85c6),(1,[D@6b2612cf)), ((3,[D@38659df5),(3,[D@60cbf351))))
  //  (3,List(((3,[D@41dbeb3b),(2,[D@1ab039a5)), ((2,[D@eae9533),(1,[D@3140becc)), ((1,[D@2f5c19ec),(3,[D@343a70b3)), ((0,[D@4e477c0a),(0,[D@7a451dd0))))


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
