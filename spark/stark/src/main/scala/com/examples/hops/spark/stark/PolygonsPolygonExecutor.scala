package com.examples.hops.spark.stark

import dbis.stark.STObject
import dbis.stark.spatial.SpatialRDD._
import dbis.stark.spatial.partitioner.SpatialPartitioner
import dbis.stark.spatial.{JoinPredicate, SpatialJoinRDD}
import org.apache.spark.rdd.RDD

class PolygonsPolygonExecutor(
                                    spatialPartitioner: SpatialPartitioner,
                                    arealm: RDD[(STObject, String)]) {


  def getResult(joinPredicate: JoinPredicate.Value): SpatialJoinRDD[STObject, String, String] = {
    if (spatialPartitioner == null) {
      arealm.join(arealm, joinPredicate)

    } else {

      getArealm.join(getArealm, joinPredicate)
    }
  }

  def getArealm: RDD[(STObject, String)] = {
    if (spatialPartitioner == null) {
      arealm
    } else {
      arealm.partitionBy(spatialPartitioner)
    }
  }
}
