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

  // Generate minhashes for corpus
  val minHashCorpusRDD = corpusRDD.map { case(id, text) =>
    val minHash = new MinHashDocument(text)
    (id, minHash.generateMinHashSignature.toSet)
  }

  // Create pairs of all docs
  val candidatePairsRDD = minHashCorpusRDD.cartesian(minHashCorpusRDD)
    .map { case(doc1, doc2) => Set(doc1, doc2) }
    .map(pair => (pair, 1))
    .reduceByKey(_ + _)
    .map { case(pair, count) => pair }.cache()

  val comparisonCount = candidatePairsRDD.count()
  println(s"Number of comparisons: $comparisonCount")

  // Look through all the pairs for matches
  val matchingPairsRDD = candidatePairsRDD.map { pair =>
    if (pair.size == 1) {
      (pair, 1.0D)
    } else {
      (pair, MinHashDocument.jaccardSimilarity(pair.head._2, pair.tail.head._2))
    }
  }.filter { case(pair, score) => 
    score > 0.8D 
  }.map { case(pair, score) => 
    pair.map { case(key, signature) => key }
  }

  // Assemble matching pairs into clusters
  val clustersRDD = matchingPairsRDD.flatMap { pair =>
    if (pair.size == 1) {
      Set((pair.head, pair.head))
    } else {
      Set((pair.head, pair.tail.head), (pair.tail.head, pair.head))
    }
  }.aggregateByKey(collection.mutable.Set.empty[String])((s, v) => s += v, (h1, h2) => h1 ++= h2).map { case(key, cluster) =>
    val completeCluster = cluster += key
    (completeCluster, 1)
  }.reduceByKey(_ + _).map { case(cluster, count) => cluster }
 
  clustersRDD.map(cluster => cluster.mkString(" ")).saveAsTextFile(outputLocation)

  sc.stop()
}

