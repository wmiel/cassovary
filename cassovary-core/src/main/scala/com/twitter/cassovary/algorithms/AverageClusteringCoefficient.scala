package com.twitter.cassovary.algorithms

import com.twitter.cassovary.graph.{GraphDir, Node, DirectedGraph}

object AverageClusteringCoefficient {
  def apply(graph: DirectedGraph[Node]): Double = {
    new AverageClusteringCoefficient(graph).apply()
  }
}

private class AverageClusteringCoefficient(graph: DirectedGraph[Node]) {
  def apply(): Double = {
    var nodeCnt = 0
    val localCCs = graph.map { node =>
      val neighbourIds = (node.neighborIds(GraphDir.OutDir) ++ node.neighborIds(GraphDir.InDir)).distinct
      val nodePairs = for (nodeId1 <- neighbourIds; nodeId2 <- neighbourIds; if nodeId1 != nodeId2) yield (nodeId1, nodeId2)
      val actualConnections = nodePairs.map { case (nodeId1, nodeId2) =>
        var numOfConnections = 0
        graph.getNodeById(nodeId1) match {
          case Some(n) =>
            if (n.isNeighbor(GraphDir.OutDir, nodeId2) || n.isNeighbor(GraphDir.InDir, nodeId2)) {
              numOfConnections = 1
            }
          case None => {}
        }
        numOfConnections
      }
      nodeCnt += 1
      val possibleConnections = neighbourIds.size * (neighbourIds.size - 1)
      if (possibleConnections > 0) {
        actualConnections.sum.toDouble / possibleConnections.toDouble
      }
      else
        0.0
    }

    printf("%f, %d %d\n", localCCs.sum, graph.nodeCount, nodeCnt)
    localCCs.sum / graph.nodeCount
  }
}