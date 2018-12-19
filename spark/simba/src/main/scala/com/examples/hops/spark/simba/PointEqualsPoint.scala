package com.examples.hops.spark.simba


import org.apache.spark.sql.simba.SimbaSession

object PointEqualsPoint {
  case class PointData(x: Double, y: Double, z: Double, other: String)

  def main(args: Array[String]): Unit = {

    var n_cores = 8

    if (args.length > 0) {
        n_cores = Integer.parseInt(args(0))
    }

    val name = "PolygonsPointsNoIndex"
    val output = "hdfs:///Projects/demo_spark_kgiann01/Resources/geospatial_results.txt"
    val comments = "PointEqualsPoint"

    val simbaSession = SimbaSession
      .builder()
      .appName("SparkSessionForSimba")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    runJoinQUery(simbaSession)
  }

  private def runJoinQUery(simba: SimbaSession): Unit = {

    import simba.implicits._

    val DS1 = (0 until 10000).map(x => PointData(x, x + 1, x + 2, x.toString)).toDS
    val DS2 = (0 until 10000).map(x => PointData(x, x, x + 1, x.toString)).toDS

    import simba.simbaImplicits._

    DS1.knnJoin(DS2, Array("x", "y"),Array("x", "y"), 3).show()

    DS1.distanceJoin(DS2, Array("x", "y"),Array("x", "y"), 3).show()

  }

}
