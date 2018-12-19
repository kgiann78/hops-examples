package com.examples.hops.spark.spatialspark

import spatialspark.operator.SpatialOperator

object PolygonContainsPolygon {

  def main(args: Array[String]): Unit = {

    val arglist = args.toList
    val leftGeometryIndex = 0
    val rightGeometryIndex = 0
        val leftFile = "hdfs:///Projects/demo_spark_kgiann01/Resources/arealm_merge.tsv"
//    val leftFile = "file:///Users/constantine/Desktop/arealm_merge.tsv"
        val rightFile = "hdfs:///Projects/demo_spark_kgiann01/Resources/arealm_merge.tsv"
//    val rightFile = "file:///Users/constantine/Desktop/arealm_merge.tsv"
    val joinPredicate = SpatialOperator.Contains

    val predicate = "PolygonsContainsPolygons"

    val scenario = new SpatialScenario(leftFile, rightFile, leftGeometryIndex, rightGeometryIndex, joinPredicate, predicate, arglist)
    scenario.run()
  }
}
