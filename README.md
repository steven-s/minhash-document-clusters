# Document Clustering utilizing MinHash signatures and LSH
This project encompasses two basic approaches to similar document clustering.

## Brute Force
The brute force clustering is executed as a spark job. It compares all pairings of documents utilizing k-shingling
for similarity.

## LSH
The LSH clustering is also executed as a spark job. It calculates MinHashes for documents and then utilizes
Locality Sensitive Hashing (LSH) to generate candidate pairs which can then be tested for similarity.

# Locality Sensitive-what now
More information on this subject can be found in Chapter 3 of the Stanford [Mining of Massive Datasets][1] text.

# Shortcomings

This project can find similarities between documents quite well, but its clustering approach with the resulting matches is not perfect and will sometimes output subsets of clusters and intersecting clusters. There are approaches that can eliminate these issues, but they are not contained within this source at this point.

# Where to find the data used when developing this project?
I found a collection of short BBC articles in plain text at this site:

http://mlg.ucd.ie/datasets/bbc.html

There are surprisingly already duplicates present in the different corpora.

[1]: http://www.mmds.org
