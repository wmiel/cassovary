package com.twitter.cassovary.algorithms.bmatrix

import com.twitter.cassovary.graph._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class kGraphGenerator(val graph: DirectedGraph[Node]) {
  val kGraphs = new mutable.HashMap[Int, scala.collection.mutable.ListBuffer[NodeIdEdgesMaxId]]
  var processed = 0
  graph.foreach { node =>
    val bfs = new BreadthFirstTraverser(graph, GraphDir.OutDir, Seq(node.id), Walk.Limits())
    //We traverse the graph to get depths
    bfs.foreach(_ => {})

    val kNeighbours = bfs.depthAllNodes().
      filter { case (nodeId, distance) => distance >= 1 }.
      groupBy { case (nodeId, distance) => distance }.
      mapValues {
        x => {
          x.keys
        }
      }

    kNeighbours.foreach { case (k, neighbours) => {
      val ids = NodeIdEdgesMaxId(node.id, neighbours.toArray)
      if (kGraphs.contains(k)) {
        kGraphs(k).append(ids)
      } else {
        val list = new scala.collection.mutable.ListBuffer[NodeIdEdgesMaxId]
        list.append(ids)
        kGraphs(k) = list
      }
    }
    }
    processed += 1
    if (processed % 100 == 0) {
      println("Processed: " + processed)
    }

  }
  println(processed)

  def kGraph(k: Int) = {
    ArrayBasedDirectedGraph(kGraphs(k), StoredGraphDir.OnlyOut, NeighborsSortingStrategy.LeaveUnsorted)
  }

  def foreachK(f: (Int, ArrayBasedDirectedGraph) => Unit) = {
    kGraphs.keys.toList.sorted.foreach(k => {
      f(k, kGraph(k))
    })
  }
}
