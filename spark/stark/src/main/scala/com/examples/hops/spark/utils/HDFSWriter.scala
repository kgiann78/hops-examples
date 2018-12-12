package com.examples.hops.spark.utils

import java.util.logging.Logger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, LocalFileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem


class HDFSWriter(val path: String) {
  val log: Logger = Logger.getLogger(classOf[HDFSWriter].getName)

  //Create a path
  val hdfswritepath = new Path(path)
  val configuration = new Configuration()
  configuration.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
  configuration.set("fs.file.impl", classOf[LocalFileSystem].getName)
  val fs: FileSystem = FileSystem.get(configuration)

  //Init output stream
  val outputStream: FSDataOutputStream = fs.create(hdfswritepath)

  def write(comments: String, index: Boolean, count: Long, avgDuration: Double, warmupDuration: Double, n_iterations: Int, n_partitions: Int, n_cores: Int): Unit ={
    log.info("Begin Write file into hdfs")
    //Classical output stream usage
    outputStream.writeBytes("comments, index, partitions (per dimension), cores, iterations, warmup time, avg execution time, resultset count\n")
    outputStream.writeBytes(comments + ", " + index + ", " + n_partitions + ", " + n_cores + ", " + n_iterations + ", " + warmupDuration + ", " + avgDuration + ", " + count +"\n")
    log.info("End Write file into hdfs")
  }

  def close(): Unit ={
    outputStream.close()
  }

}

