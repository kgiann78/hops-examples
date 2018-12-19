package com.examples.hops.spark.stark

import java.util.logging.Logger

import com.examples.hops.spark.common.{HDFSWriter, Utils}
import dbis.stark.STObject
import dbis.stark.spatial.JoinPredicate
import dbis.stark.spatial.partitioner.SpatialGridPartitioner
import org.apache.spark.sql.SparkSession

import scala.util.control.NonFatal

object PolygonsPolygons {

  val log: Logger = Logger.getLogger("PolygonsPolygons")

  def main(args: Array[String]): Unit = {

    var n_partitions = 1
    var n_cores = 8

    if (args.length > 0) {
      n_partitions = Integer.parseInt(args(0))
      if (args.length > 1)
        n_cores = Integer.parseInt(args(1))
    }

    val predicate = "PolygonsContainsPolygons"
    val path = "hdfs:///Projects/demo_spark_kgiann01/Resources/geospatial_results.txt"

    val spark = SparkSession
      .builder
      .appName("PolygonsContainsPolygons")
      .getOrCreate

    val sc = spark.sparkContext

    /* read data */
    val arealm = sc.textFile("hdfs:///Projects/demo_spark_kgiann01/Resources/arealm_merge.tsv")
      .map(line => line.split('\t'))
      .map(arr =>
        (STObject(arr(0)), arr(6))
      )


    var polygonsPolygonExecutor: PolygonsPolygonExecutor = null

    if (n_partitions > 1) {
      /* fixed grid partitioner */
      val arealmGridPartitioner = new SpatialGridPartitioner(arealm, partitionsPerDimension = n_partitions, false, 2)

      polygonsPolygonExecutor = new PolygonsPolygonExecutor(arealmGridPartitioner, arealm)
    } else {
      polygonsPolygonExecutor = new PolygonsPolygonExecutor(null, arealm)
    }

    /* warmup run */
    var start = System.currentTimeMillis()
    var result = polygonsPolygonExecutor.getResult(JoinPredicate.CONTAINS)
    var count = result.toJavaRDD().count()
    val warmupDuration = System.currentTimeMillis() - start

    val iterations = 3
    val i = 0
    /* actual runs */
    var totalDuration: Long = 0
    for (i <- 1 to iterations) {
      start = System.currentTimeMillis()
      result = polygonsPolygonExecutor.getResult(JoinPredicate.CONTAINS)
      count = result.toJavaRDD().count()
      totalDuration += System.currentTimeMillis() - start
    }

    val avgDuration = totalDuration / iterations
    val writer = new HDFSWriter(path)
    var exception: Throwable = null

    try {
      writer.write("STARK", predicate, false, n_partitions, n_cores, warmupDuration.toDouble / 1000, avgDuration.toDouble / 1000, count)
    } catch {
      case NonFatal(e) => {
        exception = e
        throw e
      }
    } finally {
      Utils.closeAndSuppressed(exception, writer)
      writer.close()
    }


  }
}
