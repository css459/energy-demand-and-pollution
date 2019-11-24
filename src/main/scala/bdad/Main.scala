package bdad

import bdad.etl.Scenarios
import bdad.model.TLCC.{TLCCIngress, TLCCModel}

object Main extends App {
  val spark = Context.spark

  val (gasses, labels) = Scenarios.gasses2019test

  gasses.show(false)

  // writeToDisk(gasses, "gasses-2014-2019")

  val gassesIngress = new TLCCIngress(gasses, labels)
  new TLCCModel(gassesIngress, gassesIngress, Array(1, 2, 3)).allPlayAll().take(3).foreach(println)

  //  gasses.select("scaled_features").show(20, truncate = false)
  //  breakoutVectorCols(gasses).take(20).foreach(r => println(r.toString))

  //  val Row(coeff1: Matrix) = Correlation.corr(gasses, column = "scaled_features").head
  //
  //  coeff1.toArray.foreach(println)
  //  gasses.columns.foreach(println)
}
