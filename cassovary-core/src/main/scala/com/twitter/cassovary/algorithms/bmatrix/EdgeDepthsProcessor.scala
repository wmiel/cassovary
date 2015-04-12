package com.twitter.cassovary.algorithms.bmatrix

import com.twitter.cassovary.graph.DirectedGraph
import com.twitter.cassovary.graph._

private class EdgeDepthsProcessor(edgeBMatrix: BMatrix, graph: DirectedGraph[Node]) extends DepthsProcessor {
  def bmatrix = edgeBMatrix

  def processDepths(nodeId: Int, depths: collection.Map[Int, Int]) = {
    graph.foreach(node => {
      node.neighborIds(GraphDir.OutDir).foreach({ neighborId => {
        val depth = depths(node.id) + depths(neighborId)
        if (depth > 0) {
          incrementForDepth(depth)
        }
      }
      })
    })
    addToBMatrix()
  }
}