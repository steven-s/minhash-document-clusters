package com.stevens.transform

import java.io.File
import java.net.URI
import scala.io.Source
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.hbase.io._

object TextToSequence extends App {
  val sourceDirectory = new File(args(0))
  val outputDirectory = args(1)
  var writer: SequenceFile.Writer = null

  try {
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(outputDirectory), conf)
    val path = new Path(outputDirectory)
    writer = SequenceFile.createWriter(fs, conf, path, classOf[Text], classOf[Text])

    sourceDirectory.listFiles.filter(_.getName.endsWith(".txt")).foreach {
      file: File => {
        val text = Source.fromFile(file).mkString
        val key = file.getName
        writer.append(new Text(key), new Text(text))
      }
    }
  }
  finally {
    IOUtils.closeStream(writer)
  }
}

