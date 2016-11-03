package com.stevens.transform

import java.io.File
import java.net.URI
import scala.io.Source
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.hbase.io._

object TextToSequence extends App {
  val sourceDirectory = new File(args(0))
  val outputDirectory = args(1)

  val conf = new Configuration()
  val fs = FileSystem.get(URI.create(outputDirectory), conf)
  val path = new Path(outputDirectory)
  val writer = SequenceFile.createWriter(fs, conf, path, classOf[ImmutableBytesWritable], classOf[Text])

  sourceDirectory.listFiles.filter(_.getName.endsWith(".txt")).foreach {
    file: File => {
      val text = Source.fromFile(file).mkString
      val sha = DigestUtils.sha1Hex(text)
      writer.append(new ImmutableBytesWritable(sha.getBytes), new Text(text))
    }
  }

  IOUtils.closeStream(writer)
}

