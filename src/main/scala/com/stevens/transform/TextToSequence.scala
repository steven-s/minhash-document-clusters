package com.stevens.transform

import java.io.File
import java.net.URI
import scala.io.Source
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io._

object TextToSequence extends App {
  val sourceDirectory = new File(args(0))
  val outputDirectory = args(1)
  var writer: SequenceFile.Writer = null

  println(s"Writing text files from dir $sourceDirectory to sequence file $outputDirectory")

  try {
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(outputDirectory), conf)
    val path = new Path(outputDirectory)
    writer = SequenceFile.createWriter(fs, conf, path, classOf[Text], classOf[Text])
    val textFiles = sourceDirectory.listFiles.filter(_.getName.endsWith(".txt"))
    
    println(s"Found ${textFiles.length} files")

    textFiles.foreach {
      file: File => {
        try {
          val text = Source.fromFile(file).mkString
          val key = file.getName
          writer.append(new Text(key), new Text(text))
        } catch {
          case e: Exception => s"error reading/writing file ${e.getMessage}"
        }
      }
    }

    println("Done")
  }
  finally {
    IOUtils.closeStream(writer)
  }
}

