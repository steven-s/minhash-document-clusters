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

  val corpusRDD = sc.sequenceFile(corpusSequence, classOf[Text], classOf[Text])
    .map { case(id, text) => (id.toString, text.toString) }

  val candidatePairsRDD = corpusRDD.cartesian(corpusRDD).filter { case((k1, v1), (k2, v2)) => !k1.equalsIgnoreCase(k2) }
  val reducedPairsRDD = candidatePairsRDD.map { case((k1, v1), (k2, v2)) => (k1, Set((k2, v2))) }.reduceByKey(_ ++ _)
  val comparisonPairsRDD = corpusRDD.join(reducedPairsRDD)

  val matchingClustersRDD = comparisonPairsRDD.map { case(k1, (text, possibleMatches)) =>
    val minHash = new MinHashDocument(text)
    val matchingDocs = possibleMatches.map { case(k2, otherText) =>
      val otherMinHash = new MinHashDocument(otherText)
      (k2, minHash.shingleSimilarity(otherMinHash))
    }.filter { case(k2, score) => score > 0.8D }.map { case(k2, score) => k2 }
    matchingDocs.toSet + k1
  }

  val reducedClustersRDD = matchingClustersRDD.map(cluster => (cluster.hashCode, cluster)).reduceByKey(_ ++ _)
  reducedClustersRDD.map { case(clusterId, cluster) => cluster.mkString(" ") }.saveAsTextFile(outputLocation)
}

