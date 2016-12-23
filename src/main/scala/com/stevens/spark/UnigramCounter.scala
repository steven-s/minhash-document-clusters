package com.stevens.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.hadoop.io._

import com.stevens.minhash._

object UnigramCounter extends App {
  val corpusSequence = args(0)
  val outputLocation = args(1)

  val conf = new SparkConf().setAppName("Unigram Counter")
  val sc = new SparkContext(conf)

  sc.setLogLevel("WARN")

  val corpusRDD = sc.sequenceFile(corpusSequence, classOf[Text], classOf[Text])
    .map { case(id, text) => (id.toString, text.toString) }

  val unigramRDD = corpusRDD.flatMap { case(id, text) =>
    text.split("\\s+").map(token => (token, 1)) 
  }.reduceByKey(_ + _)

  val unigramCount = unigramRDD.count()
  println(s"Number of distinct unigrams: $unigramCount")

  unigramRDD.keys.saveAsTextFile(outputLocation)

  sc.stop()
}

