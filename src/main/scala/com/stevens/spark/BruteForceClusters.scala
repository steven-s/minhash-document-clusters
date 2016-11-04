package com.stevens.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.hadoop.io._
import org.apache.hadoop.hbase.io._

import com.stevens.minhash._

object BruteForceClusters extends App {
  val corpusSequence = args(0)
  val outputLocation = args(1)

  val conf = new SparkConf().setAppName("Brute Force Document Clusters")
  val sc = new SparkContext(conf)

  val corpusRDD = sc.sequenceFile(corpusSequence, classOf[ImmutableBytesWritable], classOf[Text])
    .map { case(id, text) => (new String(id.get), text.toString) }

  val initialClustersRDD = corpusRDD.cartesian(corpusRDD).map { case(k, v) => (k, Set(v)) }.reduceByKey(_ ++ _)

  val matchingClustersRDD = initialClustersRDD.map { case((keyId, keyText), v) =>
    val similarDocs = v.map { case(compareId, compareText) =>
      val minHashKey = new MinHashDocument(keyText)
      val minHashCompare = new MinHashDocument(compareText)
      (compareId, minHashKey.shingleSimilarity(minHashCompare))
    }.filter { case(id, score) => score > 0.8D }.map { case(id, score) => id }
    (keyId, similarDocs.toSet)
  }
  
  matchingClustersRDD.map { case(key, similarDocs) => key + "\t[" + similarDocs.mkString(",") + "]" }.saveAsTextFile(outputLocation)
}

