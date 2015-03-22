/*
 * Copyright 2014 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.twitter.cassovary.algorithms.bmatrix

import java.util

import com.twitter.cassovary.graph._
import com.twitter.cassovary.util.Progress
import com.twitter.logging.Logger
import it.unimi.dsi.fastutil.ints.{Int2IntMap, Int2IntOpenHashMap}
import java.{util => jutil}

object BMatrix {

  def apply(graph: DirectedGraph[Node]): Unit = {
    val bm = new BMatrix(graph)
    bm.run
  }
}

private class DepthsCounter {
  protected val underlyingMap = new Int2IntOpenHashMap

  def incrementForDepth(degree: Int) = {
    underlyingMap.addTo(degree, 1)
  }

  def addToWriter(writer: BMatrixWriter) = {
    val iterator = counters.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next
      writer.synchronized {
        writer.add(entry.getKey, entry.getValue)
      }
    }
  }

  def counters = underlyingMap
}

private class BMatrix(graph: DirectedGraph[Node]) {

  private val log = Logger.get("BMatrix")

  def setDebug() = {
    val topLog = Logger.get("")
    topLog.setLevel(Logger.DEBUG)
    topLog.getHandlers().foreach(handler => handler.setLevel(Logger.DEBUG))
  }

  def run: Unit = {
    // Let the user know if they can save memory!
    if (graph.maxNodeId.toDouble / graph.nodeCount > 1.1 && graph.maxNodeId - graph.nodeCount > 1000000)
      log.info("Warning - you may be able to reduce the memory usage of PageRank by renumbering this graph!")

    log.info("Initializing starting BMatix calculation...")
    val progress = Progress("BMatrix_calculation", 5000, Some(graph.nodeCount))
    val writer = new BMatrixWriter()

    setDebug()

    graph.par.foreach { node =>
      val bfs = new BreadthFirstTraverser(graph, GraphDir.OutDir, Seq(node.id), Walk.Limits())
      val depthCounters = new DepthsCounter

      bfs.foreach(x => {})
      bfs.depthAllNodes().values.foreach(depth => {
        if (depth > 0)
          depthCounters.incrementForDepth(depth)
      })
      depthCounters.addToWriter(writer)
      progress.inc
    }
    log.info("Printing matrix")
    writer.printMatrix()
    log.info("END")
  }
}

