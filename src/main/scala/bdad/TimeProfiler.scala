package bdad

import bdad.etl.Scenarios
import bdad.model.TLCC.{TLCCIngress, TLCCModel}
import org.apache.spark.rdd.RDD

object TimeProfiler extends App {
  // Init Context
  Context.context

  val checkpointDir: String = "hdfs:///user/css459/gasses2014to2019-checkpoint"
  val cors2: Array[Int] = Array(0, 10)
  val cors4: Array[Int] = Array(-10, 0, 10, 30)

  def parallel8Corr121Vec(sequential: Boolean): Unit = {
    var (gasses, labels) = Scenarios.gasses2019test()
    var (petroleum, petroleum_labels) = Scenarios.petroleum2019test()

    if (sequential) {
      gasses = gasses.coalesce(1)
      petroleum = petroleum.coalesce(1)
      printf("RUNNING SEQ: PARTITIONS: " + gasses.rdd.getNumPartitions)
    }

    Context.context.setCheckpointDir(checkpointDir)
    gasses.checkpoint()

    val ingr = new TLCCIngress(gasses, labels, yearCol = "year", dayCol = "dayofyear")
    val pIngr = new TLCCIngress(petroleum, petroleum_labels)

    // 4 x 1 x 2 = 8
    // TIME
    val t1 = System.nanoTime
    val cors: RDD[((String, String, Int), Double)] = new TLCCModel(ingr, pIngr, cors2).allPlayAll()
    cors.collect.foreach(println)
    val duration = (System.nanoTime - t1) / 1e9d
    println("TIME: " + duration)
  }

  def parallel64Corr121Vec(sequential: Boolean): Unit = {
    var (gasses, labels) = Scenarios.gasses2019test()

    if (sequential) {
      gasses = gasses.coalesce(1)
      printf("RUNNING SEQ: PARTITIONS: " + gasses.rdd.getNumPartitions)
    }

    Context.context.setCheckpointDir(checkpointDir)
    gasses.checkpoint()

    val ingr = new TLCCIngress(gasses, labels, yearCol = "year", dayCol = "dayofyear")

    // 4 x 4  x 4 = 64
    // TIME
    val t1 = System.nanoTime
    val cors: RDD[((String, String, Int), Double)] = new TLCCModel(ingr, ingr, cors4).allPlayAll()
    cors.collect.foreach(println)
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

    Context.context.setCheckpointDir(checkpointDir)
    gasses.checkpoint()

    val ingr = new TLCCIngress(gasses, labels, yearCol = "year", dayCol = "dayofyear")
    val pIngr = new TLCCIngress(petroleum, petroleum_labels)

    // 4 x 1 x 2 = 8
    // TIME
    val t1 = System.nanoTime
    val cors: RDD[((String, String, Int), Double)] = new TLCCModel(ingr, pIngr, cors2).allPlayAll()
    cors.collect.foreach(println)
    val duration = (System.nanoTime - t1) / 1e9d
    println("TIME: " + duration)
  }

  def parallel64Corr1947Vec(sequential: Boolean): Unit = {
    var (gasses, labels) = Scenarios.gasses2014to2019()

    if (sequential) {
      gasses = gasses.coalesce(1)
      printf("RUNNING SEQ: PARTITIONS: " + gasses.rdd.getNumPartitions)
    }

    Context.context.setCheckpointDir(checkpointDir)
    gasses.checkpoint()

    val ingr = new TLCCIngress(gasses, labels, yearCol = "year", dayCol = "dayofyear")

    // 4 x 4  x 4 = 64
    // TIME
    val t1 = System.nanoTime
    val cors: RDD[((String, String, Int), Double)] = new TLCCModel(ingr, ingr, cors4).allPlayAll()
    cors.collect.foreach(println)
    val duration = (System.nanoTime - t1) / 1e9d
    println("TIME: " + duration)
  }

  // Parallel
  parallel8Corr121Vec(sequential = false)
  parallel64Corr121Vec(sequential = false)
  parallel8Corr1947Vec(sequential = false)
  parallel64Corr1947Vec(sequential = false)

  // Seq
  parallel8Corr121Vec(sequential = true)
  parallel64Corr121Vec(sequential = true)
  parallel8Corr1947Vec(sequential = true)
  parallel64Corr1947Vec(sequential = true)
}
