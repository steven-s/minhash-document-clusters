package com.stevens.minhash

import java.nio.charset.StandardCharsets
import com.google.common.hash._

class MinHashDocument(text: String, signatureLength: Int = 100, shingleLength: Int = 5, seed: Int = 5) {
  def generateShingles(): Iterator[String] = {
    text.split("\\s+").sliding(shingleLength).map(gram => gram.mkString(" "))
  }

  def generateMinHashSignature(): Array[Int] = {
    (seed to (seed + signatureLength - 1)).map { randomSeed => 
      val hashFunction: HashFunction = Hashing.murmur3_32(randomSeed)
      generateShingles().map(shingle => hashFunction.hashString(shingle, StandardCharsets.UTF_8).asInt).min
    }.toArray
  }
}

object MinHashDocument {
  def jaccardSimilarity[A](item1: Iterable[A], item2: Iterable[A]): Double = {
    val set1: Set[A] = item1.toSet
    val set2: Set[A] = item2.toSet
    set1.intersect(set2).size.toDouble / set1.union(set2).size.toDouble
  }
}

