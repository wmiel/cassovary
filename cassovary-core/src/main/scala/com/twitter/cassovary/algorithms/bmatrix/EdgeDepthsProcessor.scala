package com.twitter.cassovary.algorithms.bmatrix

import com.twitter.cassovary.graph.DirectedGraph
import com.twitter.cassovary.graph._

private class EdgeDepthsProcessor(matrix: HashBasedSparseMatrix) extends DepthsProcessor {
  var bmatrix = matrix

  def processDepths(nodeId: Int, depths: collection.Map[Int, Int], graph: DirectedGraph[Node], undirectedFlag: Boolean) = {
    graph.foreach(node => {
      node.neighborIds(GraphDir.OutDir).foreach({ neighborId: Int => {
        //        if (depths.contains(neighborId) && depths.contains(node.id)) {
        val nodeId = node.id
        if (((nodeId < neighborId && undirectedFlag) || (!undirectedFlag))
          && depths.contains(neighborId) && depths.contains(nodeId)) {
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
