package com.twitter.cassovary.algorithms.bmatrix

import com.twitter.cassovary.graph.DirectedGraph
import com.twitter.cassovary.graph._

private class EdgeDepthsProcessor(edgeBMatrix: HashBasedSparseMatrix) extends DepthsProcessor {
  var bmatrix = edgeBMatrix

  def processDepths(nodeId: Int, depths: collection.Map[Int, Int], graph: DirectedGraph[Node], undirectedFlag: Boolean) = {
    graph.foreach(node => {
      node.neighborIds(GraphDir.OutDir).toSet.foreach({ neighborId: Int => {
        //        if (depths.contains(neighborId) && depths.contains(node.id)) {
        if (((undirectedFlag && node.id < neighborId) || (!undirectedFlag))
          && depths.contains(neighborId) && depths.contains(node.id)) {
          val depth = depths(node.id) + depths(neighborId)
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
