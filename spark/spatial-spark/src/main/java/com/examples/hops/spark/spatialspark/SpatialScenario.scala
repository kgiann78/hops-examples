package com.examples.hops.spark.spatialspark

import com.examples.hops.spark.common.{HDFSWriter, Utils}
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import spatialspark.join.PartitionedSpatialJoin
import spatialspark.main.SpatialJoinApp.SEPARATOR
import spatialspark.operator.SpatialOperator
import spatialspark.partition.fgp.FixedGridPartitionConf
import spatialspark.util.MBR

import scala.util.Try
import scala.util.control.NonFatal

class SpatialScenario(
                       leftFile: String,
                       rightFile: String,
                       leftGeometryIndex: Int,
                       rightGeometryIndex: Int,
                       joinPredicate: SpatialOperator.Value,
                       predicate: String,
                       arglist: List[String]) {

  type OptionMap = Map[Symbol, Any]

  def run() = {
    val options = nextOption(Map(), arglist)
    val numPartitions = options.getOrElse('partition, 1).asInstanceOf[Int]
    val numCores = options.getOrElse('cores, 8).asInstanceOf[Int]
    val method = options.getOrElse('method, "fgp")
    val broadcastJoin = options.getOrElse('broadcast, false).asInstanceOf[Boolean]
    val outputPath = options.getOrElse('output, "hdfs:///Projects/demo_spark_kgiann01/Resources/geospatial_results.txt").asInstanceOf[String]

    val conf = new SparkConf().setAppName("Spatial-Spark Spatial Join App")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "spatialspark.util.KyroRegistrator")
    conf.set("spark.cores.max", String.valueOf(numCores))

    var sc = new SparkContext(conf)

    execute(sc,
      method,
      broadcastJoin,
      leftFile,
      rightFile,
      leftGeometryIndex,
      rightGeometryIndex,
      numPartitions,
      numCores,
      joinPredicate,
      outputPath)

    sc.stop()
  }

  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    list match {
      case Nil => map
      case "--help" :: tail =>
        import spatialspark.main.SpatialJoinApp.usage
        println(usage)
        sys.exit(0)
      case "--partition" :: value :: tail =>
        nextOption(map = map ++ Map('partition -> value.toInt), list = tail)
      case "--cores" :: value :: tail =>
        nextOption(map ++ Map('cores -> value.toInt), tail)
      case "--method" :: value :: tail =>
        nextOption(map = map ++ Map('method -> value), list = tail)
      case "--conf" :: value :: tail =>
        nextOption(map = map ++ Map('conf -> value), list = tail)
      case "--output" :: value :: tail =>
        nextOption(map = map ++ Map('output -> value), list = tail)
      case "--broadcast" :: value :: tail =>
        nextOption(map = map ++ Map('broadcast -> value.toBoolean), list = tail)
      case option :: tail => println("Unknown option " + option)
        sys.exit(1)
    }
  }

  def execute(sc: SparkContext,
              method: Any,
              broadcastJoin: Boolean,
              leftFile: String,
              rightFile: String,
              leftGeometryIndex: Int,
              rightGeometryIndex: Int,
              numPartitions: Int,
              numCores: Int,
              joinPredicate: SpatialOperator.Value,
              outputPath: String): Unit = {
    import spatialspark.join.BroadcastSpatialJoin
    import spatialspark.partition.bsp.BinarySplitPartitionConf
    import spatialspark.partition.stp.SortTilePartitionConf
    //load left dataset
    val leftData = sc.textFile(leftFile, numPartitions).map(x => x.split(SEPARATOR)).zipWithIndex()
    val leftGeometryById = leftData.map(x => (x._2, Try(new WKTReader().read(x._1.apply(leftGeometryIndex)))))
      .filter(_._2.isSuccess).map(x => (x._1, x._2.get))

    //load right dataset
    val rightData = sc.textFile(rightFile, numPartitions).map(x => x.split(SEPARATOR)).zipWithIndex()
    val rightGeometryById = rightData.map(x => (x._2, Try(new WKTReader().read(x._1.apply(rightGeometryIndex)))))
      .filter(_._2.isSuccess).map(x => (x._1, x._2.get))


    var matchedPairs: RDD[(Long, Long)] = sc.emptyRDD

    // Partitioned
    val radius = 0.0
    val writer: HDFSWriter = new HDFSWriter(outputPath)
    var exception: Throwable = null

    try {
      if (broadcastJoin) {
        println("Running broadcast")

        //warmup
        var beginTime = System.currentTimeMillis()

        matchedPairs = BroadcastSpatialJoin(sc, leftGeometryById, rightGeometryById, joinPredicate, radius)
        val warmupDurations: Double = System.currentTimeMillis() - beginTime

        var count = matchedPairs.toJavaRDD().count()

        println("Warmup finished after " + warmupDurations.toDouble + ", count " + count)

        val iterations = 3
        val i = 0
        /* actual runs */
        var totalDuration: Long = 0
        for (i <- 1 to iterations) {
          beginTime = System.currentTimeMillis()
          matchedPairs = BroadcastSpatialJoin(sc, leftGeometryById, rightGeometryById, joinPredicate, radius)
          count = matchedPairs.toJavaRDD().count()
          totalDuration += System.currentTimeMillis() - beginTime

          println("\nround " + i + " finished at " + (System.currentTimeMillis() - beginTime))
        }

        val avgDuration = totalDuration.toDouble / iterations

        println("Total count: " + count)
        println("Time: " + avgDuration)
        writer.write("Spatial-Spark", predicate, true, numPartitions, numCores, warmupDurations.toDouble / 1000, avgDuration.toDouble / 1000, count)
      }
      else {
        println("Running partitioned")
        try {
          val temp = leftGeometryById.map(x => x._2.getEnvelopeInternal)
            .map(x => (x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
            .reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4))
          val temp2 = rightGeometryById.map(x => x._2.getEnvelopeInternal)
            .map(x => (x.getMinX, x.getMinY, x.getMaxX, x.getMaxY))
            .reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4))

          //      val methodConf = "32:0.5:0.1"
          val methodConf = "32:32:0.1"


          val extent =
            (temp._1 min temp2._1, temp._2 min temp2._2, temp._3 max temp2._3, temp._4 max temp2._4)

          val partConf = method match {
            case "stp" =>
              println("STP partition")
              val dimX = methodConf.split(":").apply(0).toInt
              val dimY = methodConf.split(":").apply(1).toInt
              val ratio = methodConf.split(":").apply(2).toDouble
              new SortTilePartitionConf(dimX, dimY, MBR(extent._1, extent._2, extent._3, extent._4), ratio, true)
            case "bsp" =>
              println("BSP partition")
              val level = methodConf.split(":").apply(0).toLong
              val ratio = methodConf.split(":").apply(1).toDouble
              new BinarySplitPartitionConf(ratio, MBR(extent._1, extent._2, extent._3, extent._4), level, true)
            case _ =>
              println("Fix grid partition")
              val dimX = methodConf.split(":").apply(0).toInt
              val dimY = methodConf.split(":").apply(1).toInt
              new FixedGridPartitionConf(dimX, dimY, MBR(extent._1, extent._2, extent._3, extent._4))
          }


          //warmup
          var beginTime = System.currentTimeMillis()

          matchedPairs = PartitionedSpatialJoin(sc, leftGeometryById, rightGeometryById, joinPredicate, radius, partConf)

          val warmupDurations: Double = System.currentTimeMillis() - beginTime

          var count = matchedPairs.toJavaRDD().count()

          println("Warmup finished after " + warmupDurations.toDouble + ", count " + count)


          val iterations = 3
          val i = 0
          /* actual runs */
          var totalDuration: Long = 0
          for (i <- 1 to iterations) {
            beginTime = System.currentTimeMillis()
            matchedPairs = PartitionedSpatialJoin(sc, leftGeometryById, rightGeometryById, joinPredicate, radius, partConf)
            if (matchedPairs != null)
              count = matchedPairs.toJavaRDD().count()
            totalDuration += System.currentTimeMillis() - beginTime
            println("\nround " + i + " finished at " + (System.currentTimeMillis() - beginTime))

          }

          val avgDuration = totalDuration.toDouble / iterations


          println("Total count: " + count)
          println("Time: " + avgDuration)
          writer.write("Spatial-Spark", predicate, false, numPartitions, numCores, warmupDurations.toDouble / 1000, avgDuration.toDouble / 1000, count)
        } catch {
          case e: UnsupportedOperationException =>
            writer.write("Spatial-Spark", predicate + "-error", false, numPartitions, numCores, 0d, 0d, 0l)
          case NonFatal(e) => {
            exception = e
            throw e
          }
        }
      }
    } finally {
      Utils.closeAndSuppressed(exception, writer)
      writer.close()
    }
  }
}