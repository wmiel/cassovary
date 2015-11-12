package com.twitter.cassovary.algorithms.bmatrix

private class VertexDepthsProcessor(vertexBMatrix: BMatrix, distanceMatrixWriter: MatrixWriter,
                                    statsWriter: StatsWriter) extends DepthsProcessor {
  var bmatrix = vertexBMatrix

  def processDepths(nodeId: Int, depths: collection.Map[Int, Int]) = {
    val buffer = distanceMatrixWriter.getNewBuffer

    var maxDepth:BigInt = 0
    var sumOfDepths:BigInt = 0
    var sizeOfCC:BigInt = 1

    depths.foreach(key_val => {
      val nId = key_val._1
      val depth = key_val._2

      if (depth > 0) {
        sizeOfCC += 1
        sumOfDepths += depth
        if(depth > maxDepth) {
          maxDepth = depth
        }

        incrementForDepth(depth)
        distanceMatrixWriter.putInBuffer(buffer, nId, depth)
      }
    })
    distanceMatrixWriter.putBuffer(nodeId, buffer)
    distanceMatrixWriter.synchronized {
      distanceMatrixWriter.lineReady(nodeId)
    }
    addToBMatrix()
    statsWriter.synchronized {
      statsWriter.addStats(nodeId, maxDepth, sumOfDepths, sizeOfCC)
    }
  }

  override def processEntry(key: Int, value: Int) = {
    statsWriter.synchronized {
      statsWriter.addDistance(key, value)
    }
  }
}