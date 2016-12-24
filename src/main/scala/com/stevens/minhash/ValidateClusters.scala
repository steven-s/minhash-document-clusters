package com.stevens.minhash

import java.io.File
import scala.io.Source
import scala.collection.mutable.ArrayBuffer

object ValidateClusters extends App {
  val shingleLength = args(0).toInt
  val sourceDirPath = args(1)
  val clusterFile = args(2)

  println(s"Validating clusters in $clusterFile from files in $sourceDirPath")

  val validClusters: ArrayBuffer[String] = ArrayBuffer()
  val invalidClusters: ArrayBuffer[String] = ArrayBuffer()

  Source.fromFile(clusterFile).getLines.foreach { clusterString: String =>
    val cluster: Array[String] = clusterString.split(" ")
    val comparisons = cluster.flatMap(doc1 => cluster.map(doc2 => (doc1, doc2)))
    
    val clusterGood = comparisons.map { case(doc1, doc2) =>
      val doc1File = new File(sourceDirPath, doc1) 
      val doc2File = new File(sourceDirPath, doc2)
      val doc1Text = Source.fromFile(doc1File).mkString
      val doc2Text = Source.fromFile(doc2File).mkString

      val minHash1 = new MinHashDocument(doc1Text, shingleLength=shingleLength)
      val minHash2 = new MinHashDocument(doc2Text, shingleLength=shingleLength)

      MinHashDocument.jaccardSimilarity(minHash1.generateShingles.toList, minHash2.generateShingles.toList)
    }.filter(_ > 0.8D).toList.size > 0

    if (clusterGood) {
      validClusters += s"($clusterString)"
    } else {
      invalidClusters += s"($clusterString)"
    }
  }

  println(s"Valid clusters: ${validClusters.mkString(" ")}")
  println(s"Invalid clusters: ${invalidClusters.mkString(" ")}")
}

