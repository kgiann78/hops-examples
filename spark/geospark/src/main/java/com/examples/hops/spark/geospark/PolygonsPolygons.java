package com.examples.hops.spark.geospark;

import com.vividsolutions.jts.geom.Polygon;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;

import java.io.IOException;
import java.util.HashSet;
import java.util.logging.Logger;

public class PolygonsPolygons {

    private static final Logger log = Logger.getLogger(PolygonsPolygons.class.getName());

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("GeoSpark_PolygonsContainsPolygons")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        String arealm = "hdfs:///Projects/demo_spark_kgiann01/Resources/arealm_merge.txt";
        FileDataSplitter splitter = FileDataSplitter.WKT; // input file format
        Integer n_partitions = 1; // number of partitions
        Integer n_cores = 8; // number of partitions

        long resultSetCount;
        long tStartRDD = System.currentTimeMillis();
        //create PointRDD from datasets NO INDEX
        PolygonRDD objectPolygonARDD = new PolygonRDD(sc, arealm, splitter, true, n_partitions, StorageLevel.MEMORY_ONLY());
        objectPolygonARDD.spatialPartitioning(GridType.EQUALGRID);
        objectPolygonARDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());
        PolygonRDD objectPolygonBRDD = new PolygonRDD(sc, arealm, splitter, true, n_partitions, StorageLevel.MEMORY_ONLY());
        objectPolygonBRDD.spatialPartitioning(objectPolygonARDD.grids);
        objectPolygonBRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY());
        long tEndRDD = System.currentTimeMillis();

        long tDelta = tEndRDD - tStartRDD;
        double elapsedSecondsRDD = tDelta / 1000.0;
        double elapsedSeconds = 0.0;

        long tStart = System.currentTimeMillis();
        JavaPairRDD<Polygon, HashSet<Polygon>> results = JoinQuery.SpatialJoinQuery(objectPolygonARDD, objectPolygonBRDD, false, true); //first execution to update the cache
        resultSetCount = results.count();
        long tEnd = System.currentTimeMillis();

        tDelta = tEnd - tStart;

        double curElapsedSeconds = tDelta / 1000.0;
        double warmupTime = curElapsedSeconds;
        int iter = 3;

        for (int i = 0; i < iter; i++) {

            tStart = System.currentTimeMillis();
            results = JoinQuery.SpatialJoinQuery(objectPolygonARDD, objectPolygonBRDD, false, true);
            resultSetCount = results.count();
            tEnd = System.currentTimeMillis();
            tDelta = tEnd - tStart;
            curElapsedSeconds = tDelta / 1000.0;
            elapsedSeconds = curElapsedSeconds + elapsedSeconds;

        }

        double avgExecTime = elapsedSeconds / iter;
        try (HDFSWriter writer = new HDFSWriter("hdfs:///Projects/demo_spark_kgiann01/Resources/GeosparkPolygonsPolygonsNoIndex"+n_partitions+".txt")) {
            writer.write(PolygonsPolygons.class.getName(), false, resultSetCount, avgExecTime, warmupTime, iter, n_partitions, n_cores);
        } catch (IOException e) {
            e.printStackTrace();
        }
        spark.stop();
    }
}
