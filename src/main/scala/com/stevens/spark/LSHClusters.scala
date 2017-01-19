package com.stevens.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._

import org.apache.hadoop.io._

import com.stevens.minhash._

object LSHClusters extends App {
  val shingleLength: Int = args(0).toInt
  val signatureLength: Int = args(1).toInt
  val numberOfBuckets: Int = args(2).toInt
  val corpusSequence: String = args(3)
  val outputLocation: String = args(4)
  val rows: Int = signatureLength / numberOfBuckets

  println(s"Dividing $signatureLength length MinHashes into $numberOfBuckets bands of $rows rows")
  println(s"Threshold: ~${Math.pow(1 / numberOfBuckets.toDouble, 1 / rows.toDouble)}")

  val conf = new SparkConf().setAppName("LSH Document Clusters")
  val sc = new SparkContext(conf)

  val rowsBroadcast = sc.broadcast(rows)
  val shingleLengthBroadcast = sc.broadcast(shingleLength)
  val signatureLengthBroadcast = sc.broadcast(signatureLength)

  val corpusRDD = sc.sequenceFile(corpusSequence, classOf[Text], classOf[Text])
    .map { case(id, text) => (id.toString, text.toString) }

  // Generate minhashes for corpus
  val minHashRDD = corpusRDD.map { case(id, text) =>
    val minHash = new MinHashDocument(text, shingleLength=shingleLengthBroadcast.value, signatureLength=signatureLengthBroadcast.value)
    (id, minHash.generateMinHashSignature)
  }

  // Generate our pairs according to LSH for MinHash
  val bucketsRDD = minHashRDD.flatMap { case(id, signature) =>
    signature.grouped(rowsBroadcast.value).zipWithIndex.map { case(band, bandIndex) => 
      ((bandIndex, band.toList.hashCode), (id, signature.toSet))
    }
  }.aggregateByKey(collection.mutable.Iterable.empty[(String, Set[Int])])((s, v) => s ++ Iterable(v), (i1, i2) => i1 ++ i2)

  val candidatePairsRDD = bucketsRDD.flatMap { case((bandIndex, bucketId), cluster) => 
    cluster.flatMap(doc1 => cluster.map(doc2 => Set(doc1, doc2)))
  }.distinct().cache()

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
    if (pair.size == 1) {
      ((pair.head._1, pair.head._1), score)
    } else {
      ((pair.head._1, pair.tail.head._1), score)
    }
  }

  // create graph and join complete subgraphs into clusters
  val verticiesRDD = corpusRDD.map { case (id, text) => (id.hashCode().toLong, id) }
  val edgesRDD = matchingPairsRDD.map { case ((id1, id2), score) =>
    Edge(id1.hashCode().toLong, id2.hashCode.toLong, score)
  }

  val graph = Graph(verticiesRDD, edgesRDD)

  val clustersRDD = graph.collectNeighbors(EdgeDirection.Either).map { case (vertexId, verticies) => 
    verticies.map { case(id, idString) => idString }.toSet
  }.distinct()

  clustersRDD.map(cluster => cluster.mkString(" ")).saveAsTextFile(outputLocation)

  sc.stop()
}
