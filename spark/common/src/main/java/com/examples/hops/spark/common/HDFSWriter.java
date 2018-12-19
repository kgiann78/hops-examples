package com.examples.hops.spark.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.util.StringJoiner;
import java.util.logging.Logger;


public class HDFSWriter implements AutoCloseable {
    private static Logger log = Logger.getLogger(HDFSWriter.class.getName());
    private FSDataOutputStream outputStream;

    public HDFSWriter(String path) throws IOException {

        //Create a path
        Path hdfswritepath = new Path(path);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(conf);

        //Init output stream
        if (fs.exists(hdfswritepath))
            outputStream = fs.append(hdfswritepath);
        else {
            outputStream = fs.create(hdfswritepath);
            outputStream.writeBytes("System, Predicate, Indexed, Partitions, Cores, Warmup, Avg Execution Time, ResultSet Count\n");
        }
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }


    public void write(String system, String predicate, Boolean indexed, Integer n_partitions, Integer n_cores, Double warmupDuration, Double avgDuration, Long count) throws IOException {
        //Classical output stream usage
        StringJoiner stringJoiner = new StringJoiner(", ", "", "\n");
        stringJoiner.add(system);
        stringJoiner.add(predicate);
        stringJoiner.add(indexed.toString());
        stringJoiner.add(n_partitions.toString());
        stringJoiner.add(n_cores.toString());
        stringJoiner.add(warmupDuration.toString());
        stringJoiner.add(avgDuration.toString());
        stringJoiner.add(count.toString());

        outputStream.writeBytes(stringJoiner.toString());
    }

}

