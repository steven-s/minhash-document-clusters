package com.stevens.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.hadoop.io._

import com.stevens.minhash._

object BruteForceClusters extends App {
  val corpusSequence = args(0)
  val outputLocation = args(1)

  val conf = new SparkConf().setAppName("Brute Force Document Clusters")
  val sc = new SparkContext(conf)

  sc.setLogLevel("WARN")

  val corpusRDD = sc.sequenceFile(corpusSequence, classOf[Text], classOf[Text])
    .map { case(id, text) => (id.toString, text.toString) }

  val minHashCorpusRDD = corpusRDD.map { case(id, text) =>
    val minHash = new MinHashDocument(text)
    (id, minHash.generateMinHashSignature.toSet)
  }

  val candidatePairsRDD = minHashCorpusRDD.cartesian(minHashCorpusRDD)
    .filter { case((k1, sig1), (k2, sig2)) => !k1.equalsIgnoreCase(k2) }.cache()

  val comparisonCount = candidatePairsRDD.count()
  println(s"Number of comparisons: $comparisonCount")

  val reducedPairsRDD = candidatePairsRDD.map { case((k1, v1), (k2, v2)) => ((k1, v1), Set((k2, v2))) }.reduceByKey(_ ++ _)

  val matchingClustersRDD = reducedPairsRDD.map { case((k1, sig1), possibleMatches) =>
    val matchingDocs = possibleMatches.map { case(k2, sig2) =>
      (k2, MinHashDocument.jaccardSimilarity(sig1, sig2))
    }.filter { case(k2, score) => score > 0.8D }.map { case(k2, score) => k2 }
    matchingDocs.toSet + k1
  }

  val reducedClustersRDD = matchingClustersRDD.map(cluster => (cluster.hashCode, cluster)).reduceByKey(_ ++ _)
  reducedClustersRDD.map { case(clusterId, cluster) => cluster.mkString(" ") }.saveAsTextFile(outputLocation)

  sc.stop()
}

