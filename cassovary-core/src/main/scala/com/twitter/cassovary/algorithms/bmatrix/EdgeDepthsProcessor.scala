package com.twitter.cassovary.algorithms.bmatrix

import com.twitter.cassovary.graph.DirectedGraph
import com.twitter.cassovary.graph._

private class EdgeDepthsProcessor(edgeBMatrix: BMatrix, graph: DirectedGraph[Node]) extends DepthsProcessor {
  var bmatrix = edgeBMatrix

  def processDepths(nodeId: Int, depths: collection.Map[Int, Int]) = {
    graph.foreach(node => {
      node.neighborIds(GraphDir.OutDir).foreach({ neighborId => {
        if (node.id < neighborId && depths.contains(neighborId)) {
          val depth = depths(node.id) + depths(neighborId)
          if (depth > 0) {
            incrementForDepth(depth)
          }
        }
        //else {
        //  printf("SOURCE: %d, NODE id: %d, MISSING KEY: %d\n", nodeId, node.id, neighborId)
        //}
      }
      })
    })
    addToBMatrix()
  }
}