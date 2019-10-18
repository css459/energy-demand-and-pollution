val df = spark.read.format("csv").option("header", "true").load("data/hourly_42101_2019.csv")

val relevantCols = Array("Latitude", "Longitude", "Date GMT", "Time GMT", "Sample Measurement", "Units of Measure", "Date of Last Change")
val dfs = df.select(relevantCols.map(col): _*)
