package com.stevens.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.hadoop.io._
import org.apache.hadoop.hbase.io._

import com.stevens.minhash._

object LSHClusters extends App {
  val signatureLength = args(0).toInt
  val numberOfBuckets = args(1).toInt
  val corpusSequence = args(2)
  val outputLocation = args(3)
  val rows = signatureLength / numberOfBuckets

  println(s"Dividing $signatureLength length MinHashes into $numberOfBuckets bands of $rows rows")
  println(s"Treshold: ~${Math.pow(1 / numberOfBuckets.toDouble, 1 / rows.toDouble)}")

  val conf = new SparkConf().setAppName("LSH Document Clusters")
  val sc = new SparkContext(conf)

  val corpusRDD = sc.sequenceFile(corpusSequence, classOf[Text], classOf[Text])
    .map { case(id, text) => (id.toString, text.toString) }

  val minHashRDD = corpusRDD.map { case(id, text) =>
    val minHash = new MinHashDocument(text, signatureLength=signatureLength)
    (id, minHash.signature)
  }

  val bandRDD = minHashRDD.flatMap { case(id, signature) =>
    signature.grouped(rows).zipWithIndex.map { case(band, bandIndex) => ((bandIndex, band.hashCode), Set(id)) }
  }.reduceByKey(_ ++ _)

  bandRDD.map { case((bandIndex, bandHash), ids) => s"($bandIndex, $bandHash): ${ids.mkString(" ")}" }.saveAsTextFile(outputLocation)
}
