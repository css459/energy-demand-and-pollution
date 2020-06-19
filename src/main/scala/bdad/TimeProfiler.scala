package bdad

import bdad.etl.Scenarios
import bdad.model.TLCC.{TLCCIngress, TLCCModel}
import org.apache.spark.rdd.RDD

object TimeProfiler extends App {
  // Init Context
  Context.context

  val checkpointDir121: String = "hdfs:///user/css459/121checkpoint"
  val checkpointDir1947: String = "hdfs:///user/css459/1947checkpoint"

  val cors2: Array[Int] = Array(0, 10)
  val cors4: Array[Int] = Array(-10, 0, 10, 30)
  val cors7: Array[Int] = Array(-60, -30, -10, 0, 10, 30, 60)

  def parallel8Corr121Vec(sequential: Boolean): Unit = {
    var (gasses, labels) = Scenarios.gasses2019test()
    var (petroleum, petroleum_labels) = Scenarios.petroleum2019test()

    if (sequential) {
      gasses = gasses.coalesce(1)
      petroleum = petroleum.coalesce(1)
      printf("RUNNING SEQ: PARTITIONS: " + gasses.rdd.getNumPartitions)
    }

    Context.context.setCheckpointDir(checkpointDir121)
    gasses.checkpoint()

    val ingr = new TLCCIngress(gasses, labels)
    val pIngr = new TLCCIngress(petroleum, petroleum_labels)

    // 4 x 1 x 2 = 8
    // TIME
    val t1 = System.nanoTime
    val cors: RDD[((String, String, Int), Double)] = new TLCCModel(ingr, pIngr, cors2).allPlayAll()
    cors.count()
    val duration = (System.nanoTime - t1) / 1e9d
    println("TIME: " + duration)
  }

  def parallel64Corr121Vec(sequential: Boolean): Unit = {
    var (gasses, labels) = Scenarios.gasses2019test()

    if (sequential) {
      gasses = gasses.coalesce(1)
      printf("RUNNING SEQ: PARTITIONS: " + gasses.rdd.getNumPartitions)
    }

    Context.context.setCheckpointDir(checkpointDir121)
    gasses.checkpoint()

    val ingr = new TLCCIngress(gasses, labels)

    // 4 x 4  x 4 = 64
    // TIME
    val t1 = System.nanoTime
    val cors: RDD[((String, String, Int), Double)] = new TLCCModel(ingr, ingr, cors4).allPlayAll()
    cors.count()
    val duration = (System.nanoTime - t1) / 1e9d
    println("TIME: " + duration)
  }

  def parallel8Corr1947Vec(sequential: Boolean): Unit = {
    var (gasses, labels) = Scenarios.gasses2014to2019()
    var (petroleum, petroleum_labels) = Scenarios.petroleum2014to2019()

    if (sequential) {
      gasses = gasses.coalesce(1)
      petroleum = petroleum.coalesce(1)
      printf("RUNNING SEQ: PARTITIONS: " + gasses.rdd.getNumPartitions)
    }

    Context.context.setCheckpointDir(checkpointDir1947)
    gasses.checkpoint()

    val ingr = new TLCCIngress(gasses, labels, yearCol = "year", dayCol = "dayofyear")
    val pIngr = new TLCCIngress(petroleum, petroleum_labels)

    // 4 x 1 x 2 = 8
    // TIME
    val t1 = System.nanoTime
    val cors: RDD[((String, String, Int), Double)] = new TLCCModel(ingr, pIngr, cors2).allPlayAll()
    cors.count()
    val duration = (System.nanoTime - t1) / 1e9d
    println("TIME: " + duration)
  }

  def parallel64Corr1947Vec(sequential: Boolean): Unit = {
    var (gasses, labels) = Scenarios.gasses2014to2019()

    if (sequential) {
      gasses = gasses.coalesce(1)
      printf("RUNNING SEQ: PARTITIONS: " + gasses.rdd.getNumPartitions)
    }

    Context.context.setCheckpointDir(checkpointDir1947)
    gasses.checkpoint()

    val ingr = new TLCCIngress(gasses, labels, yearCol = "year", dayCol = "dayofyear")

    // 4 x 4  x 4 = 64
    // TIME
    val t1 = System.nanoTime
    val cors: RDD[((String, String, Int), Double)] = new TLCCModel(ingr, ingr, cors4).allPlayAll()
    cors.count()
    val duration = (System.nanoTime - t1) / 1e9d
    println("TIME: " + duration)
  }

  def parallel112Corr1947Vec(sequential: Boolean): Unit = {
    var (gasses, labels) = Scenarios.gasses2014to2019()

    if (sequential) {
      gasses = gasses.coalesce(1)
      printf("RUNNING SEQ: PARTITIONS: " + gasses.rdd.getNumPartitions)
    }

    Context.context.setCheckpointDir(checkpointDir1947)
    gasses.checkpoint()

    val ingr = new TLCCIngress(gasses, labels, yearCol = "year", dayCol = "dayofyear")

    // 4 x 4  x 4 = 64
    // TIME
    val t1 = System.nanoTime
    val cors: RDD[((String, String, Int), Double)] = new TLCCModel(ingr, ingr, cors7).allPlayAll()
    val c = cors.count()
    val duration = (System.nanoTime - t1) / 1e9d
    println("Count: " + c)
    println("TIME: " + duration)
  }

  // Warm up
  println("======== WARMUP ===========================")
  parallel8Corr121Vec(sequential = false)
  parallel8Corr1947Vec(sequential = false)

  // Parallel
  println("======== PARALLEL =========================")
  println("8 Correlations, Vector Length 121")
  parallel8Corr121Vec(sequential = false)
  println("64 Correlations, Vector Length 121")
  parallel64Corr121Vec(sequential = false)
  println("8 Correlations, Vector Length 1947")
  parallel8Corr1947Vec(sequential = false)
  println("64 Correlations, Vector Length 1947")
  parallel64Corr1947Vec(sequential = false)

  // Seq
  println("======== SEQ ==============================")
  println("8 Correlations, Vector Length 121")
  parallel8Corr121Vec(sequential = true)
  println("64 Correlations, Vector Length 121")
  parallel64Corr121Vec(sequential = true)
  println("8 Correlations, Vector Length 1947")
  parallel8Corr1947Vec(sequential = true)
  println("64 Correlations, Vector Length 1947")
  parallel64Corr1947Vec(sequential = true)
}
