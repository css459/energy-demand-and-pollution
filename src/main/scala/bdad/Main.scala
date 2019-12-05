package bdad

import bdad.etl.util
import bdad.etl.Scenarios
import bdad.etl.airdata.AirDataset
import bdad.model.Heatmap.GassesHeatmap
import bdad.model.CostRepresentation.CostRepresentation
import bdad.model.TLCC.{TLCCIngress, TLCCModel}
import org.apache.spark.rdd.RDD

object Main extends App {
  // Init Context
  Context.context

  /**
   * Computes the Autocorrelation for the Criteria Gasses
   * from 2014 to 2019. The values are written to file
   * in HDFS, and the lags are assessed at three, two,
   * and one month in either direction.
   */
  def autocorrelation(): Unit = {
    val (gasses, labels) = Scenarios.gasses2014to2019()

    // Checkpoint the gasses and labels
    Context.context.setCheckpointDir("hdfs:///user/css459/gasses2014to2019-checkpoint")
    gasses.checkpoint()


    // The TLCC Ingress class informs the TLCC Model on where to find
    // relevant information in the DataFrame. Here, we are taking the defaults.
    val gassesIngress = new TLCCIngress(gasses, labels, yearCol = "year", dayCol = "dayofyear")

    // This model shows the gasses auto-correlation at a shift of plus and minus one year,
    // plus the cross-correlation between un-shifted signals. Ideally, you would change the
    // second argument to a different Ingress.
    val cors: RDD[((String, String, Int), Double)] =
    new TLCCModel(gassesIngress, gassesIngress, Array(-90, -60, -30, 0, 30, 60, 90)).allPlayAll()
    cors.saveAsTextFile("gasses-2014-2019-autocorrelation.txt")
  }

  /**
   * Computes the correlation between the Criteria Gasses and
   * Petroleum for 2019. This is done without lag, and correlations
   * are printed to console, and written to file.
   */
  def correlation(): Unit = {
    // Computes only on 2019 data for example purposes
    val (gasses, gas_labels) = Scenarios.gasses2019test()
    val (petroleum, petroleum_labels) = Scenarios.petroleum2019test()

    val gassesIngress = new TLCCIngress(gasses, gas_labels)
    val petroleumIngress = new TLCCIngress(petroleum, petroleum_labels)

    // Computes cross-correlation between un-shifted signals of gas and petroleum
    val cors: RDD[((String, String, Int), Double)] =
    new TLCCModel(gassesIngress, petroleumIngress, Array(0)).allPlayAll()
    cors.saveAsTextFile("gasses-petroleumprice-corr-2019.txt")
    cors.collect.foreach(println)
  }

  /**
   * Computes the correlation between the Toxic Compounds and
   * Petroleum for 2019. This is done without lag, and correlations
   * are printed to console, and written to file.
   */
  def correlationToxics(): Unit = {
    // Computes only on 2019 data for example purposes
    val (toxics, toxicsLabels) = Scenarios.toxics2019test()
    val (petroleum, petroleum_labels) = Scenarios.petroleum2019test()

    val toxicsIngress = new TLCCIngress(toxics, toxicsLabels)
    val petroleumIngress = new TLCCIngress(petroleum, petroleum_labels)

    // Computes cross-correlation between un-shifted signals of gas and petroleum
    val cors: RDD[((String, String, Int), Double)] =
      new TLCCModel(toxicsIngress, petroleumIngress, Array(0, 30)).allPlayAll()
    cors.saveAsTextFile("toxics-petroleumprice-corr-2019.txt")
    cors.collect.foreach(println)
  }

  /**
   * Triggers the generation of data from the HeatMap model
   * for the heat map visualization. For testing and illustration
   * purposes currently generates heat map data for 2019 only.
   */
  def heatmap(): Unit = {
    GassesHeatmap.makeMap(2019, "heatmap-gasses-2019.csv")
  }

  /**
   * Triggers the generation of pollution score data per area
   * (lon, lat). For testing and illustration purposes generates
   * the pollution scores for 2019 only.
   */
  def generatePollutionCostScores(): Unit = {
    val air = new AirDataset(2019, "gasses/*")
    val (petroleum, petroleum_labels) = Scenarios.petroleum2019test()

    val airData = air.pivotedDF(dropNull = true, dropUnit = true)
    val airDataColumns = air.validCriteria

    // compute the pollution cost scores with lag = 10 and save to disk
    val pollutionAreaScores = CostRepresentation.pollutionCostScores(airData, airDataColumns, petroleum, 10)
    util.writeToDisk(pollutionAreaScores, "pollution-cost-scores-2019.csv")

    // optional output to check that the DataFrame looks correct
    print(pollutionAreaScores.show(false))
  }

  // Compute the correlation between the 2 signals
  correlation()
  correlationToxics()
  autocorrelation()

  // Generate a heat map for visualization of gas geo data
  heatmap()

  // Generate the pollution scores
  generatePollutionCostScores()
}
