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
import com.twitter.util.Stopwatch
import it.unimi.dsi.fastutil.ints.{Int2IntMap, Int2IntOpenHashMap}
import java.{util => jutil}

object BMatrixCalculation {

  def apply(graph: DirectedGraph[Node], filename: String): Unit = {
    val bm = new BMatrixCalculation(graph)
    bm.run(filename)
  }
}

private class DepthsProcessor(writer: BMatrixWriter) {
  protected val underlyingMap = new Int2IntOpenHashMap

  def incrementForDepth(degree: Int) = {
    underlyingMap.addTo(degree, 1)
  }

  def processDepths(depths: collection.Map[Int, Int]) = {
    depths.values.foreach(depth => {
      if (depth > 0)
        incrementForDepth(depth)
    })
    addToWriter
  }

  private def addToWriter() = {
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

private class BMatrixCalculation(graph: DirectedGraph[Node]) {

  private val log = Logger.get("BMatrixCalculation")

  def setDebug() = {
    val topLog = Logger.get("")
    topLog.setLevel(Logger.DEBUG)
    topLog.getHandlers().foreach(handler => handler.setLevel(Logger.DEBUG))
  }

  def run(OutFileNamePrefix: String): Unit = {
    // Let the user know if they can save memory!
    if (graph.maxNodeId.toDouble / graph.nodeCount > 1.1 && graph.maxNodeId - graph.nodeCount > 1000000)
      log.info("Warning - you may be able to reduce the memory usage of PageRank by renumbering this graph!")

    val progress = Progress("BMatrix_calculation", 5000, Some(graph.nodeCount))
    setDebug()


    log.info("Initializing BMatix calculation...\n")
    val watch = Stopwatch.start()
    val writer = new BMatrixWriter()

    graph.par.foreach { node =>
      val bfs = new BreadthFirstTraverser(graph, GraphDir.OutDir, Seq(node.id), Walk.Limits())
      val depthCounters = new DepthsProcessor(writer)
      //We traverse the graph to get depths
      bfs.foreach(x => {})

      depthCounters.processDepths(bfs.depthAllNodes())

      //Should be synchronized, but we don't care that much, it's just for debug.
      progress.inc
    }
    log.info("Finished BMatrix calculation . Time: %s\n".format(watch()))

    log.info("Initializing BMatrix writing\n")
    val writingWatch = Stopwatch.start()
    writer.printMatrix()
    log.info("Finished BMatrix writing time: %s\n".format(writingWatch()))
    log.info("Initializing BMatrix writing\n")
    val writingWatch2 = Stopwatch.start()
    writer.writeMatrix(OutFileNamePrefix)
    log.info("Finished BMatrix writing time: %s\n".format(writingWatch2()))

  }
}

