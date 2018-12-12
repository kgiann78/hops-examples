package com.examples.hops.spark.geospark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.util.logging.Logger;


public class HDFSWriter implements AutoCloseable {
    private static Logger  log = Logger.getLogger(HDFSWriter.class.getName());
    private FSDataOutputStream outputStream;

    public HDFSWriter(String path) throws IOException {

        //Create a path
        Path hdfswritepath = new Path(path);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(conf);

        //Init output stream
        outputStream  = fs.create(hdfswritepath);
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }



  public void write(String comments, Boolean index, Long count, Double avgDuration, Double warmupDuration, Integer n_iterations, Integer n_partitions, Integer n_cores) throws IOException {
    log.info("Begin Write file into hdfs");
    //Classical output stream usage
    outputStream.writeBytes("comments, index, partitions (per dimension), cores, iterations, warmup time, avg execution time, resultset count\n");
    outputStream.writeBytes(comments + ", " + index + ", " + n_partitions + ", " + n_cores + ", " + n_iterations + ", " + warmupDuration + ", " + avgDuration + ", " + count +"\n");
    log.info("End Write file into hdfs");
  }

}

