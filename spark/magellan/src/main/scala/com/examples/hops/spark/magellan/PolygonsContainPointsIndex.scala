package com.examples.hops.spark.magellan

import com.examples.hops.spark.common.HDFSWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.magellan.dsl.expressions._

object PolygonsContainPointsIndex {

  def main(args: Array[String]): Unit = {

    var n_cores = 8

    if (args.length > 0) {
        n_cores = Integer.parseInt(args(0))
    }

    val polygonsPath = "hdfs:///Projects/demo_spark_kgiann01/Resources/arealm_merge.txt"
    val pointsPath = "hdfs:///Projects/demo_spark_kgiann01/Resources/pointlm_merge.txt"
    val name = "PolygonsPointsNoIndex"
    val output = "hdfs:///Projects/demo_spark_kgiann01/Resources/geospatial_results.txt"
    val comments = "PolygonsContainsPoints"

    val session = SparkSession.builder().appName(name).config("spark.cores.max", String.valueOf(n_cores)).getOrCreate()
    import session.implicits._

    /* read data */
    val polygonsDf = session.read.format("magellan").load(polygonsPath)
    val pointsDf = session.read.format("magellan").option("magellan.index", "true")
      .option("magellan.index.precision", "15").load(pointsPath)
    magellan.Utils.injectRules(session)

    /* warmup run */
    var start = System.currentTimeMillis()
    var resultSetCount = polygonsDf.as("df1").join(pointsDf.as("df2") index 15).where($"df1.polygon" >? $"df2.point").count()
    val warmupDuration = System.currentTimeMillis() - start

    val iterations = 3
    val i = 0
    /* actual runs */
    var totalDuration: Long = 0
    for (i <- 1 to iterations) {
      start = System.currentTimeMillis()
      resultSetCount = polygonsDf.as("df1").join(pointsDf.as("df2") index 15).where($"df1.polygon" >? $"df2.point").count()
      totalDuration += System.currentTimeMillis() - start
    }

    val avgDuration = totalDuration / iterations

    val writer = new HDFSWriter(output)
    writer.write("Magellan", "PolygonContaisPoints", true, 0, n_cores, warmupDuration.toDouble / 1000, avgDuration.toDouble / 1000, resultSetCount)
    writer.close()

  }

}
