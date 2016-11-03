package com.stevens.minhash



class MinHashDocument(text: String, hashLength: Int = 200, shingleLength: Int = 5, seed: Int = 5) {
  val shingles = createShingles()

  private def createShingles(): List[String] = {
    text.split("\\s+").sliding(shingleLength).map(gram => gram.mkString(" ")).toList
  }
}
