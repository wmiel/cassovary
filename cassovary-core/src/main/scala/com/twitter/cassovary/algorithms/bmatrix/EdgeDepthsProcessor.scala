package com.twitter.cassovary.algorithms.bmatrix

import com.twitter.cassovary.graph.DirectedGraph
import com.twitter.cassovary.graph._

private class EdgeDepthsProcessor(matrix: HashBasedSparseMatrix) extends DepthsProcessor {
  var bmatrix = matrix

  def processDepths(nodeId: Int, depths: collection.Map[Int, Int], graph: DirectedGraph[Node], undirectedFlag: Boolean) = {
    depths.keys.foreach(nodeId => {
      val node = graph.getNodeById(nodeId).get
      //var checkedNodes = scala.collection.mutable.Set[Int]()
      node.neighborIds(GraphDir.OutDir).foreach({ neighborId: Int => {
        if (((nodeId < neighborId && undirectedFlag) || (!undirectedFlag))
          && depths.contains(neighborId)) {
          val depth = depths(nodeId) + depths(neighborId)
          if (depth > 0) {
            incrementForDepth(depth)
          }
        }
      }
      })
    })
    addToBMatrix()
  }
}
