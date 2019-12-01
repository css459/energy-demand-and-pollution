package bdad

import bdad.etl.Scenarios
import bdad.model.Heatmap.GassesHeatmap
import bdad.model.TLCC.{TLCCIngress, TLCCModel}
import org.apache.spark.rdd.RDD

object Main extends App {
  // Init Context
  Context.context

  def autocorrelation(): Unit = {
    val (gasses, labels) = Scenarios.gasses2014to2019()

    // Checkpoint the gasses and labels
    Context.context.setCheckpointDir("hdfs:///user/css459/gasses2014to2019-checkpoint")
    gasses.checkpoint()


    // The TLCC Ingress class informs the TLCC Model on where to find
    // relevant information in the DataFrame. Here, we are taking the defaults.
    val gassesIngress = new TLCCIngress(gasses, labels)

    // This model shows the gasses auto-correlation at a shift of plus and minus one year,
    // plus the cross-correlation between un-shifted signals. Ideally, you would change the
    // second argument to a different Ingress.
    val cors: RDD[((String, String, Int), Double)] =
    new TLCCModel(gassesIngress, gassesIngress, Array(-90, -60, -30, 0, 30, 60, 90)).allPlayAll()
    cors.saveAsTextFile("gasses-2014-2019-autocorr3.txt")
  }

  def correlation(): Unit = {
    // Computes only on 2019 data for example purposes
    val (gasses, gas_labels) = Scenarios.gasses2019test()
    val (petroleum, petroleum_labels) = Scenarios.petroleum2019test()

    val gassesIngress = new TLCCIngress(gasses, gas_labels)
    val petroleumIngress = new TLCCIngress(petroleum, petroleum_labels)

    // Computes cross-correlation between un-shifted signals of gas and petroleum
    val cors: RDD[((String, String, Int), Double)] =
    new TLCCModel(gassesIngress, petroleumIngress, Array(0)).allPlayAll()
    cors.saveAsTextFile("gases-petroleumprice-corr-2019.txt")
  }

  def heatmap(): Unit = {
    GassesHeatmap.makeMap(2019, "heatmap-gasses-2019.csv")
  }

  // Compute the correlation between the 2 signals
  correlation()

  // Generate a heatmap for visualization of gas geo data
  heatmap()
}
