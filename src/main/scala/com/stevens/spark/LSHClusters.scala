package com.stevens.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.hadoop.io._

import com.stevens.minhash._

object LSHClusters extends App {
  val signatureLength: Int = args(0).toInt
  val numberOfBuckets: Int = args(1).toInt
  val corpusSequence: String = args(2)
  val outputLocation: String = args(3)
  val rows: Int = signatureLength / numberOfBuckets

  println(s"Dividing $signatureLength length MinHashes into $numberOfBuckets bands of $rows rows")
  println(s"Threshold: ~${Math.pow(1 / numberOfBuckets.toDouble, 1 / rows.toDouble)}")

  val conf = new SparkConf().setAppName("LSH Document Clusters")
  val sc = new SparkContext(conf)

  sc.setLogLevel("WARN")

  val rowsBroadcast = sc.broadcast(rows)
  val signatureLengthBroadcast = sc.broadcast(signatureLength)

  val corpusRDD = sc.sequenceFile(corpusSequence, classOf[Text], classOf[Text])
    .map { case(id, text) => (id.toString, text.toString) }

  val minHashRDD = corpusRDD.map { case(id, text) =>
    val minHash = new MinHashDocument(text, signatureLength=signatureLengthBroadcast.value)
    (id, minHash.generateMinHashSignature)
  }

  val bucketsRDD = minHashRDD.flatMap { case(id, signature) =>
    signature.grouped(rowsBroadcast.value).zipWithIndex.map { case(band, bandIndex) => 
      ((bandIndex, band.toList.hashCode), collection.mutable.Set((id, signature.toSet))) 
    }
  }.reduceByKey(_ ++= _)

  val candidatePairsRDD = bucketsRDD.flatMap { case((bandIndex, bucketId), cluster) => 
    cluster.flatMap(doc1 => cluster.map( doc2 => (doc1, doc2))).map(pair => (pair, 1))
  }.reduceByKey(_ + _).map { case(pair, count) => pair }.cache()

  val comparisonCount = candidatePairsRDD.count()
  println(s"Number of comparisons: $comparisonCount")

  // Now we can go back to Brute Force to do the comparisons
  val reducedPairsRDD = candidatePairsRDD.map { case(doc1, doc2) => (doc1, Set(doc2)) }.reduceByKey(_ ++ _)
 
  val matchingClustersRDD = reducedPairsRDD.map { case((k1, sig1), possibleMatches) =>
    val matches = possibleMatches.map { case(k2, sig2) =>
      (k2, MinHashDocument.jaccardSimilarity(sig1, sig2))
    }.filter { case(k2, score) => score > 0.8D }.map { case(k2, score) => k2 }
    matches.toSet + k1
  }

  val reducedClustersRDD = matchingClustersRDD.map(cluster => (cluster.hashCode, cluster)).reduceByKey(_ ++ _)
  reducedClustersRDD.map { case(clusterId, cluster) => cluster.mkString(" ") }.saveAsTextFile(outputLocation)

  sc.stop()
}
