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

  def heatmap(): Unit = {
    GassesHeatmap.makeMap(2019, "heatmap-gasses-2019.csv")
  }

  heatmap()
}
