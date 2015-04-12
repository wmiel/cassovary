package com.twitter.cassovary.algorithms.bmatrix

private class VertexDepthsProcessor(vertexBMatrix: BMatrix, distanceMatrixWriter: MatrixWriter,
                                    statsWriter: StatsWriter) extends DepthsProcessor {
  var bmatrix = vertexBMatrix

  def processDepths(nodeId: Int, depths: collection.Map[Int, Int]) = {
    val buffer = distanceMatrixWriter.getNewBuffer
    depths.foreach(key_val => {
      val nId = key_val._1
      val depth = key_val._2

      if (depth > 0) {
        incrementForDepth(depth)
        distanceMatrixWriter.putInBuffer(buffer, nId, depth)
      }
    })
    distanceMatrixWriter.putBuffer(nodeId, buffer)
    distanceMatrixWriter.synchronized {
      distanceMatrixWriter.lineReady(nodeId)
    }
    addToBMatrix()
  }

  override def processEntry(key: Int, value: Int) = {
    statsWriter.synchronized {
      statsWriter.addDistance(key, value)
    }
  }
}