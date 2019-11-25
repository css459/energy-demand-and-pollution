package bdad

import bdad.etl.Scenarios
import bdad.model.TLCC.{TLCCIngress, TLCCModel}

object Main extends App {
  val (gasses, labels) = Scenarios.gasses2019test

  // Disk Writing Routine
  //  Context.context.parallelize(labels).saveAsTextFile("gasses2019testLabels.txt")
  //  gasses.write.json("gasses2019test.json")
  // writeToDisk(gasses, "gasses-2014-2019")

  // TODO
  // Load Gasses test data from file, you can also load from bdad.Scenarios
  //  val gasses: DataFrame = Context.spark.read.json("gasses2019test.json")
  //  val labels: Array[String] = Context.context.textFile("gasses2019testLabels.txt").collect
  //  gasses.printSchema()

  // The TLCC Ingress class informs the TLCC Model on where to find
  // relevant information in the DataFrame. Here, we are taking the defaults.
  val gassesIngress = new TLCCIngress(gasses, labels)

  // This model shows the gasses auto-correlation at a shift of plus and minus one year,
  // plus the cross-correlation between un-shifted signals. Ideally, you would change the
  // second argument to a different Ingress.
  new TLCCModel(gassesIngress, gassesIngress, Array(-30, 0, 30)).allPlayAll().take(3).foreach(println)
}
