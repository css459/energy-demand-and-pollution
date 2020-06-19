name := "energy-demand-and-pollution"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.4.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.2.0"

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6"
