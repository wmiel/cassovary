package com.twitter.cassovary.algorithms.bmatrix

private class VertexDepthsProcessor(vertexBMatrix: HashBasedSparseMatrix) extends DepthsProcessor {
  var bmatrix = vertexBMatrix

  def processDepths(nodeId: Int, depths: collection.Map[Int, Int]) = {
    depths.foreach(key_val => {
      val nId = key_val._1
      val depth = key_val._2

      if (depth > 0) {
        incrementForDepth(depth)
      }
    })
    addToBMatrix()
  }
}
