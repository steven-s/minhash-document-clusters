package com.stevens.minhash

import org.scalatest._

class MinHashDocumentSpec extends FlatSpec with Matchers {

  val testText = """
Ad sales boost Time Warner profit

Quarterly profits at US media giant TimeWarner jumped 76% to $1.13bn (£600m) for the three months to December, from $639m year-earlier.

The firm, which is now one of the biggest investors in Google, benefited from sales of high-speed internet connections and higher advert sales. TimeWarner said fourth quarter sales rose 2% to $11.1bn from $10.9bn. Its profits were buoyed by one-off gains which offset a profit dip at Warner Bros, and less users for AOL.

Time Warner said on Friday that it now owns 8% of search-engine Google. But its own internet business, AOL, had has mixed fortunes. It lost 464,000 subscribers in the fourth quarter profits were lower than in the preceding three quarters. However, the company said AOL's underlying profit before exceptional items rose 8% on the back of stronger internet advertising revenues. It hopes to increase subscribers by offering the online service free to TimeWarner internet customers and will try to sign up AOL's existing customers for high-speed broadband. TimeWarner also has to restate 2000 and 2003 results following a probe by the US Securities Exchange Commission (SEC), which is close to concluding.

Time Warner's fourth quarter profits were slightly better than analysts' expectations. But its film division saw profits slump 27% to $284m, helped by box-office flops Alexander and Catwoman, a sharp contrast to year-earlier, when the third and final film in the Lord of the Rings trilogy boosted results. For the full-year, TimeWarner posted a profit of $3.36bn, up 27% from its 2003 performance, while revenues grew 6.4% to $42.09bn. "Our financial performance was strong, meeting or exceeding all of our full-year objectives and greatly enhancing our flexibility," chairman and chief executive Richard Parsons said. For 2005, TimeWarner is projecting operating earnings growth of around 5%, and also expects higher revenue and wider profit margins.

TimeWarner is to restate its accounts as part of efforts to resolve an inquiry into AOL by US market regulators. It has already offered to pay $300m to settle charges, in a deal that is under review by the SEC. The company said it was unable to estimate the amount it needed to set aside for legal reserves, which it previously set at $500m. It intends to adjust the way it accounts for a deal with German music publisher Bertelsmann's purchase of a stake in AOL Europe, which it had reported as advertising revenue. It will now book the sale of its stake in AOL Europe as a loss on the value of that stake.
""" 

  val veryShortDocument = "Hello there I'm a short document"

  behavior of "A MinHash"

  it should "construct without error" in {
    val minHash = new MinHashDocument(testText)
  }

  it should "generate shingles" in {
    val minHash = new MinHashDocument(testText)
    assert(minHash.shingles != null)
    assert(!minHash.shingles.isEmpty)
  }

  it should "generate shingles according to its args" in {
    val minHash = new MinHashDocument(veryShortDocument, shingleLength=2)
    assert(minHash.shingles.size == 5)
  }

  it should "have exact similarity for the same document" in {
    val minHash1 = new MinHashDocument(testText)
    val minHash2 = new MinHashDocument(testText)
    assert(minHash1.shingleSimilarity(minHash2) == 1.0D)
  }

  it should "have little to no similarity with a different document" in {
    val minHash1 = new MinHashDocument(testText, shingleLength=2)
    val minHash2 = new MinHashDocument(veryShortDocument, shingleLength=2)
    assert(minHash1.shingleSimilarity(minHash2) < 0.25D)
  }

  it should "generate a minhash signature of correct length" in {
    val minHash = new MinHashDocument(veryShortDocument, shingleLength=2, signatureLength=100)
    assert(minHash.signature != null)
    assert(!minHash.signature.isEmpty)
    assert(minHash.signature.size == 100)
  }
}

