//
// BDAD Final Project
// ETL
// Cole Smith
// util.scala
//

package bdad.etl

import org.apache.spark.ml.feature.{MaxAbsScaler, StandardScaler, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

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

  //  def removeMeans(df: DataFrame, cols: Array[String]): DataFrame = {
  //    cols
  //      .foldLeft(df) { (tempDf, colName) =>
  //        tempDf.withColumn(
  //          colName,
  ////          normalizeCol(df, colName, useStd = true, useMean = true)
  //          df(colName) - mean(col(colName))
  //
  //        )
  //      }
  //  }
  //
  //  def normalizeCol(df: DataFrame, col: String, useStd: Boolean, useMean: Boolean): Column = {
  //    // Make the vector assembler for input to scaler
  //    val assembler = new VectorAssembler().setInputCols(col).setOutputCol(col)
  //
  //    // Create the Standard Scaler
  //    val scaler = new StandardScaler()
  //      .setInputCol(col)
  //      .setOutputCol(col)
  //      .setWithStd(useStd)
  //      .setWithMean(useMean)
  //
  //    // Vectorize input columns for scaler
  //    val vector = assembler.transform(df.select(col))
  //
  //    // Fit, transform, return
  //    val scalerModel = scaler.fit(vector)
  //    scalerModel.transform(vector)(col)
  //  }


  /**
    * Appends to the input DataFrame a column of Vectors which correspond to the
    * scaled input features from the given "cols" of the DataFrame. The ordering
    * of the vectors is the same as the initial ordering of the input columns.
    *
    * This transformer can be chained with other transformers of this object
    * that output a "scaled_features" column. This is done by providing a
    * single-element array of "scaled_features" for "cols". This is a
    * default parameter.
    *
    * @param df      DataFrame with Columns to scale
    * @param cols    A list of Columns in the DataFrame
    * @param useStd  Scale based on Std (See StandardScaler Documentation)
    * @param useMean Scale based on Mean (See StandardScaler Documentation)
    * @return DataFrame with appended "scaled_features" column containing Vector objects
    */
  def normalize(df: DataFrame, cols: Array[String] = Array("scaled_features"),
                useStd: Boolean, useMean: Boolean): DataFrame = {
    // Identify a chained operation on "scaled_features" and
    // process it specially
    if (cols.length == 1 && cols(0).equals("scaled_features")) {
      val scaler = new StandardScaler()
        .setInputCol("scaled_features_orig")
        .setOutputCol("scaled_features")

      val dff = df
        .withColumnRenamed("scaled_features", "scaled_features_orig")

      scaler.fit(dff).transform(dff).drop("scaled_features_orig")
    } else {

      // Make the vector assembler for input to scaler
      val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")

      // Create the Standard Scaler
      val scaler = new StandardScaler()
        .setInputCol("features")
        .setOutputCol("scaled_features")
        .setWithStd(useStd)
        .setWithMean(useMean)

      // Vectorize input columns for scaler
      val vectors = assembler.transform(df)

      // Fit, transform
      val scalerModel = scaler.fit(vectors.select("features"))

      // Drop the unscaled features, return
      scalerModel.transform(vectors).drop("features")
    }
  }

  /**
    * Appends to the input DataFrame a column of Vectors which correspond to the
    * absolute-scaled input features from the given "cols" of the DataFrame. The
    * ordering of the vectors is the same as the initial ordering of the input columns.
    * Data is scaled to range [-1,1]
    *
    * This transformer can be chained with other transformers of this object
    * that output a "scaled_features" column. This is done by providing a
    * single-element array of "scaled_features" for "cols". This is a
    * default parameter.
    *
    * @param df   DataFrame with Columns to scale
    * @param cols A list of Columns in the DataFrame
    * @return DataFrame with appended "scaled_features" column containing Vector objects
    */
  def maxAbs(df: DataFrame, cols: Array[String] = Array("scaled_features")): DataFrame = {

    // Identify a chained operation on "scaled_features" and
    // process it specially
    if (cols.length == 1 && cols(0).equals("scaled_features")) {
      val scaler = new MaxAbsScaler()
        .setInputCol("scaled_features_orig")
        .setOutputCol("scaled_features")

      val dff = df
        .withColumnRenamed("scaled_features", "scaled_features_orig")

      scaler.fit(dff).transform(dff).drop("scaled_features_orig")
    } else {

      // Make the vector assembler for input to scaler
      val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")

      // Create the Standard Scaler
      val scaler = new MaxAbsScaler()
        .setInputCol("features")
        .setOutputCol("scaled_features")

      // Vectorize input columns for scaler
      val vectors = assembler.transform(df)

      // Fit, transform
      val scalerModel = scaler.fit(vectors.select("features"))

      // Drop the unscaled features, return
      scalerModel.transform(vectors).drop("features")
    }
  }

  /**
    * Subtract the mean of a column from itself. The mean can be an aggregation
    * over any of the following:
    *
    * "hourly":   Averaged over the hour of the day
    * "daily":    Averaged over the day of the month
    * "monthly":  Averaged over the months of the year
    * "yearly":   Averaged over the years in the data set
    *
    * The `meanFreq` value can be any of the above strings. The default is "daily".
    *
    * @param df          Dataframe to transform
    * @param colName     The name of the column to mean-zero
    * @param dateColName The name of the date column containing Date objects (default: "dateGMT")
    * @param meanFreq    The frequency of aggregation for the mean (default: "daily")
    */
  def timeAggregate(df: DataFrame, colName: String, dateColName: String = "dateGMT",
                    meanFreq: String = "daily"): DataFrame = {

    if (meanFreq.equals("daily")) {
      df
        // Form the date aggregation and mean columns
        .select(colName, dateColName)
        .groupBy(dayofmonth(col(dateColName)).alias("dayOfMonth"))
        .agg(mean(colName).alias("mean"))

        // Generate the date aggregation column for df, and join on it
        .join(df.withColumn("dayOfMonth", dayofmonth(col(dateColName))), "dayOfMonth")

        // Subtract the mean from the value
        .withColumn(colName, col(colName) - col("mean"))

        // Drop intermediate columns
        .drop("mean", "dayOfMonth")
    }
    else if (meanFreq.equals("monthly")) {
      df
        // Form the date aggregation and mean columns
        .select(colName, dateColName)
        .groupBy(month(col(dateColName)).alias("month"))
        .agg(mean(colName).alias("mean"))

        // Generate the date aggregation column for df, and join on it
        .join(df.withColumn("month", month(col(dateColName))), "month")

        // Subtract the mean from the value
        .withColumn(colName, col(colName) - col("mean"))

        // Drop intermediate columns
        .drop("mean", "month")
    }
    else if (meanFreq.equals("yearly")) {
      df
        // Form the date aggregation and mean columns
        .select(colName, dateColName)
        .groupBy(year(col(dateColName)).alias("year"))
        .agg(mean(colName).alias("mean"))

        // Generate the date aggregation column for df, and join on it
        .join(df.withColumn("year", year(col(dateColName))), "year")

        // Subtract the mean from the value
        .withColumn(colName, col(colName) - col("mean"))

        // Drop intermediate columns
        .drop("mean", "year")
    }
    else if (meanFreq.equals("hourly")) {
      df
        // Form the date aggregation and mean columns
        .select(colName, dateColName)
        .groupBy(hour(col(dateColName)).alias("hour"))
        .agg(mean(colName).alias("mean"))

        // Generate the date aggregation column for df, and join on it
        .join(df.withColumn("hour", hour(col(dateColName))), "hour")

        // Subtract the mean from the value
        .withColumn(colName, col(colName) - col("mean"))

        // Drop intermediate columns
        .drop("mean", "hour")
    }
    else {
      println("[ ERR ] Invalid meanFreq: " + meanFreq +
        ". Must be one of: hourly, daily, monthly, yearly. No Operation.")
      df
    }
  }
}
