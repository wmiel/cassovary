package com.twitter.cassovary.algorithms.bmatrix

private class ClusteringCoefficientDepthsProcessor(matrix: HashBasedSparseMatrix, val clusteringCoefficient: Map[Int, (Double, Int)]) extends DepthsProcessor {
  var bmatrix = matrix

  def processDepths(nodeId: Int, depths: collection.Map[Int, Int]) = {
    depths.foreach(key_val => {
      val nId = key_val._1
      val depth = key_val._2

      if (depth > 0) {
        bmatrix.increment(depth, clusteringCoefficient.get(nId).get._2)
      }
    })
  }
}