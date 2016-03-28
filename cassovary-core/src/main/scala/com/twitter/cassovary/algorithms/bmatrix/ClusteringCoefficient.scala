package com.twitter.cassovary.algorithms.bmatrix

import com.twitter.cassovary.graph._
import com.twitter.logging.Logger
import scala.collection.Searching._

class ClusteringCoefficient(val graph: DirectedGraph[Node], val ccBMatrixBins: Int, val k: Int, val log: Logger) {
  def calculate(): Map[Int, (Double, Int)] = {
    var nodeCnt = 0
    val localCCs = graph.map {
      node =>
        val neighbourIds = (node.neighborIds(GraphDir.OutDir) ++ node.neighborIds(GraphDir.InDir)).distinct
        nodeCnt += 1
        val result = calculateForNode(node, neighbourIds, 1)
        if (nodeCnt % 100 == 0) {
          log.info("Processed " + nodeCnt + " nodes in " + k + "graph.")
        }
        result
    }

    val localCCsSum = localCCs.map {
      case (x, (y, z)) => y
    }.sum
    printf("k\tSum of local CC\tNode count\tNode count verf.\tAverage CC\n%d\t%f\t%d\t%d\t%f\n",
      k, localCCsSum, graph.nodeCount, nodeCnt, localCCsSum / graph.nodeCount)
    localCCs.toMap
  }

  def calculateForNode(node: Node, kNeighbours: Seq[Int], k: Int): (Int, (Double, Int)) = {
    val nodePairs = for (nodeId1 <- kNeighbours; nodeId2 <- kNeighbours; if nodeId1 < nodeId2) yield (nodeId1, nodeId2)
    val actualConnections = nodePairs.map { case (nodeId1, nodeId2) =>
      if (isNeighbour(k, nodeId1, nodeId2)) {
        1
      } else {
        0
      }
    }
    val possibleConnections = kNeighbours.size * (kNeighbours.size - 1)
    if (possibleConnections > 0) {
      val cc = 2 * actualConnections.sum.toDouble / possibleConnections.toDouble
      (node.id, (cc, bucket(cc)))
    }
    else {
      (node.id, (0.0, 0))
    }
  }

  def isNeighbour(k: Int, nodeId1: Int, nodeId2: Int): Boolean = {
    if (k == 1) {
      graph.getNodeById(nodeId1) match {
        case Some(n) =>
          val neighbors = n.neighborIds(GraphDir.OutDir)
          neighbors.search(nodeId2) match {
            case Found(n) => {
              return true
            }
            case InsertionPoint(x) => {
              return false
            }
          }
        case None => {}
      }
      return false
    }
    else {
      val bfs = new BreadthFirstTraverser(graph, GraphDir.OutDir, Seq(nodeId1), Walk.Limits(Option(k)))
      bfs.foreach(_ => {})
      val kNeighbours = bfs.depthAllNodes().groupBy { case (x, y) => y }(k).keySet
      return kNeighbours.contains(nodeId2)
    }
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
