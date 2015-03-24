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

private class DepthsProcessor(bMatrixWriter: BMatrixWriter, distanceMatrixWriter: DistanceMatrixWriter) {
  protected val underlyingMap = new Int2IntOpenHashMap

  def incrementForDepth(degree: Int) = {
    underlyingMap.addTo(degree, 1)
  }

  def processDepths(nodeId: Int, depths: collection.Map[Int, Int]) = {
    val array = distanceMatrixWriter.getNewArray
    depths.foreach(key_val => {
      val nodeId = key_val._1
      val depth = key_val._2

      if (depth > 0) {
        incrementForDepth(depth)
        array(nodeId) = depth
      }
    })
    distanceMatrixWriter.putArray(nodeId, array)
    distanceMatrixWriter.synchronized {
      distanceMatrixWriter.lineReady(nodeId)
    }
    addToBMatrixWriter()
  }

  private def addToBMatrixWriter() = {
    val iterator = counters.int2IntEntrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next
      bMatrixWriter.synchronized {
        bMatrixWriter.add(entry.getKey, entry.getValue)
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

  def run(outFileNamePrefix: String): Unit = {
    // Let the user know if they can save memory!
    if (graph.maxNodeId.toDouble != graph.nodeCount)
      log.info("Warning - you may be able to reduce the memory usage of PageRank by renumbering this graph!")

    val progress = Progress("BMatrix_calculation", 5000, Some(graph.nodeCount))
    setDebug()


    log.info("Initializing BMatix calculation...\n")
    val watch = Stopwatch.start()
    val bMatrixWriter = new BMatrixWriter()
    val distanceMatrixWriter = new DistanceMatrixWriter(graph, outFileNamePrefix)

    graph.par.foreach { node =>
      val bfs = new BreadthFirstTraverser(graph, GraphDir.OutDir, Seq(node.id), Walk.Limits())
      val depthProcessor = new DepthsProcessor(bMatrixWriter, distanceMatrixWriter)
      //We traverse the graph to get depths
      bfs.foreach(x => {})

      depthProcessor.processDepths(node.id, bfs.depthAllNodes())

      //Should be synchronized, but we don't care that much, it's just for debug.
      progress.inc
    }
    distanceMatrixWriter.close()
    log.info("Finished BMatrix calculation . Time: %s\n".format(watch()))

    log.info("Initializing BMatrix writing\n")
    val writingWatch = Stopwatch.start()
    bMatrixWriter.printMatrix()
    log.info("Finished BMatrix writing time: %s\n".format(writingWatch()))
    log.info("Initializing BMatrix writing\n")
    val writingWatch2 = Stopwatch.start()
    bMatrixWriter.writeMatrix(outFileNamePrefix)
    log.info("Finished BMatrix writing time: %s\n".format(writingWatch2()))

  }
}

