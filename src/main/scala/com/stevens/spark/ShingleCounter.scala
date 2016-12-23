package com.stevens.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.hadoop.io._

import com.stevens.minhash._

object ShingleCounter extends App {
  val shingleLength = args(0).toInt
  val corpusSequence = args(1)
  val outputLocation = args(2)

  val conf = new SparkConf().setAppName("Shingle Counter")
  val sc = new SparkContext(conf)

  sc.setLogLevel("WARN")

  val shingleLengthBroadcast = sc.broadcast(shingleLength)

  val corpusRDD = sc.sequenceFile(corpusSequence, classOf[Text], classOf[Text])
    .map { case(id, text) => (id.toString, text.toString) }

  val shinglesRDD = corpusRDD.flatMap { case(id, text) =>
    val minHash = new MinHashDocument(text, shingleLength=shingleLengthBroadcast.value)
    minHash.generateShingles.map(shingle => (shingle, BigInt(1)))  
  }.reduceByKey(_ + _)

  val shingleCount = shinglesRDD.count()
  println(s"Number of distinct ${shingleLength}-shingles: $shingleCount")

  shinglesRDD.keys.saveAsTextFile(outputLocation)

  sc.stop()
}

