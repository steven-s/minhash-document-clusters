package com.stevens.minhash

import java.nio.charset.StandardCharsets
import com.google.common.hash._

class MinHashDocument(text: String, signatureLength: Int = 100, shingleLength: Int = 5, seed: Int = 5) {
  lazy val shingles: Set[String] = createShingles().toSet
  lazy val signature: Array[Int] = createMinhashSignature()

  private def createShingles(): Iterator[String] = {
    text.split("\\s+").sliding(shingleLength).map(gram => gram.mkString(" "))
  }

  private def createMinhashSignature(): Array[Int] = {
    (seed to (seed + signatureLength - 1)).map { randomSeed => 
      val hashFunction: HashFunction = Hashing.murmur3_32(randomSeed)
      createShingles().map(shingle => hashFunction.hashString(shingle, StandardCharsets.UTF_8).asInt).min
    }.toArray
  }

  def shingleSimilarity(otherDocument: MinHashDocument): Double = {
    shingles.intersect(otherDocument.shingles).size.toDouble / shingles.union(otherDocument.shingles).size.toDouble
  }

  def minHashSimilarity(otherDocument: MinHashDocument): Double = {
    val sigSet: Set[Int] = signature.toSet
    val sigSetOther: Set[Int] = otherDocument.signature.toSet
    sigSet.intersect(sigSetOther).size.toDouble / sigSet.union(sigSetOther).size.toDouble
  }
}

object MinHashDocument {
  def shingleSimilarity(shingles1: Set[String], shingles2: Set[String]): Double = {
    shingles1.intersect(shingles2).size.toDouble / shingles1.union(shingles2).size.toDouble
  }
  
  def minHashSimilarity(hash1: Array[Int], hash2: Array[Int]): Double = {
    val hashSet1: Set[Int] = hash1.toSet
    val hashSet2: Set[Int] = hash2.toSet
    hashSet1.intersect(hashSet2).size.toDouble / hashSet1.union(hashSet2).size.toDouble
  }

  def jaccardSimilarity(set1: Set[Int], set2: Set[Int]): Double = {
    set1.intersect(set2).size.toDouble / set1.union(set2).size.toDouble
  }
}

