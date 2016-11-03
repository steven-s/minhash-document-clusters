package com.stevens.minhash

import util.Random
import java.nio.charset.StandardCharsets
import com.google.common.hash._

class MinHashDocument(text: String, signatureLength: Int = 200, shingleLength: Int = 5, seed: Int = 5) {
  private val seededRandom: Random = new Random(seed)
  lazy val shingles: Set[String] = createShingles().toSet
  lazy val signature: Set[String] = createMinhashSignature()

  private def createShingles(): Iterator[String] = {
    text.split("\\s+").sliding(shingleLength).map(gram => gram.mkString(" "))
  }

  private def createMinhashSignature(): Set[String] = {
    generateRandomSeeds().map { randomSeed => 
      val hashFunction: HashFunction = Hashing.murmur3_128(randomSeed)
      createShingles().map(shingle => hashFunction.hashString(shingle, StandardCharsets.UTF_8).toString).min
    }.toSet
  }

  private def generateRandomSeeds(): List[Int] = {
    (1 to signatureLength).map(_ => seededRandom.nextInt).toList
  }

  def shingleSimilarity(otherDocument: MinHashDocument): Double = {
    shingles.intersect(otherDocument.shingles).size / shingles.union(otherDocument.shingles).size
  }
}
