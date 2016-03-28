package com.twitter.cassovary.algorithms.bmatrix

import com.twitter.cassovary.graph._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class kGraphGenerator(val graph: DirectedGraph[Node]) {
  val readyKGraphs = new mutable.HashMap[Int, DirectedGraph[Node]]
  readyKGraphs(1) = graph

  {
    val kGraphs = new mutable.HashMap[Int, scala.collection.mutable.ListBuffer[NodeIdEdgesMaxId]]
    var processed = 0
    graph.par.foreach { node =>
      val bfs = new BreadthFirstTraverser(graph, GraphDir.OutDir, Seq(node.id), Walk.Limits())
      //We traverse the graph to get depths
      bfs.foreach(_ => {})
      val kNeighbours = bfs.depthAllNodes().
        filter { case (nodeId, distance) => distance > 1 }.
        groupBy { case (nodeId, distance) => distance }.
        mapValues {
          x => {
            x.keys
          }
        }

      kNeighbours.foreach { case (k, neighbours) => {
        val ids = NodeIdEdgesMaxId(node.id, neighbours.toArray)
        kGraphs.synchronized {
          if (kGraphs.contains(k)) {
            kGraphs(k).append(ids)
          } else {
            val list = new scala.collection.mutable.ListBuffer[NodeIdEdgesMaxId]
            list.append(ids)
            kGraphs(k) = list
          }
        }
      }
      }
      processed += 1
      if (processed % 150 == 0) {
        println("Processed distances for " + processed + " nodes.")
      }
    }
    println("Finished processing distances for: " + processed + " nodes.")
    println("Converting to k-graphs.")
    kGraphs.par.foreach { case (k: Int, rawGraph: scala.collection.mutable.ListBuffer[NodeIdEdgesMaxId]) => {
      val g = ArrayBasedDirectedGraph(rawGraph, StoredGraphDir.OnlyOut, NeighborsSortingStrategy.SortWhileReading)
      readyKGraphs(k) = g
      printf("%s-graph has %s nodes and %s edges.\n", k, g.nodeCount, g.edgeCount)
    }
    }
    println("Finished converting to k-graphs.")
  }
}
