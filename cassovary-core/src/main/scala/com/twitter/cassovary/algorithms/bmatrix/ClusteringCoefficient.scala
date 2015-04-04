package com.twitter.cassovary.algorithms.bmatrix

import com.twitter.cassovary.graph.{GraphDir, Node, DirectedGraph}

object ClusteringCoefficient {
  def apply(graph: DirectedGraph[Node], ccBMatrixBins: Int): Map[Int, (Double, Int)] = {
    new ClusteringCoefficient(graph, ccBMatrixBins).apply()
  }
}

private class ClusteringCoefficient(graph: DirectedGraph[Node], val ccBMatrixBins: Int) {
  def apply(): Map[Int, (Double, Int)] = {
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
        val cc = actualConnections.sum.toDouble / possibleConnections.toDouble
        (node.id, (cc, bucket(cc)))
      }
      else
        (node.id, (0.0, 0))
    }

    val localCCsSum = localCCs.map { case (x, (y, z)) => y }.sum
    println("Sum of local CC\tNode count\tNode count verf.\tAverage CC")
    printf("%f\t%d\t%d\t%f\n", localCCsSum, graph.nodeCount, nodeCnt, localCCsSum / graph.nodeCount)
    localCCs.toMap
  }

  private def bucket(cc: Double): Int = {
    val threshold = 1.0 / ccBMatrixBins
    val result = if (cc >= 1.0) {
      ccBMatrixBins - 1
    } else {
      (cc / threshold).floor.toInt
    }
    result
  }
}