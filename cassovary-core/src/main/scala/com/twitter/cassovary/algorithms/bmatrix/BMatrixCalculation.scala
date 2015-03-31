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

import java.util.concurrent.Executors

import com.twitter.cassovary.graph._
import com.twitter.cassovary.util.Progress
import com.twitter.logging.Logger
import com.twitter.util.Stopwatch
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap

import scala.concurrent.ExecutionContext

object BMatrixCalculation {
  def apply(graph: DirectedGraph[Node], distanceMatrixWriter: MatrixWriter, threads: Int): BMatrix = {
    val bm = new BMatrixCalculation(graph)
    bm.run(distanceMatrixWriter, threads)
  }
}

private class DepthsProcessor(bMatrixWriter: BMatrix, distanceMatrixWriter: MatrixWriter) {
  protected val underlyingMap = new Int2IntOpenHashMap

  def incrementForDepth(degree: Int) = {
    underlyingMap.addTo(degree, 1)
  }

  def processDepths(nodeId: Int, depths: collection.Map[Int, Int]) = {
    val buffer = distanceMatrixWriter.getNewBuffer
    depths.foreach(key_val => {
      val nId = key_val._1
      val depth = key_val._2

      if (depth > 0) {
        incrementForDepth(depth)
        distanceMatrixWriter.putInBuffer(buffer, nId, depth)
      }
    })
    distanceMatrixWriter.putBuffer(nodeId, buffer)
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
        bMatrixWriter.increment(entry.getKey, entry.getValue)
      }
    }
  }

  def counters = underlyingMap
}

//TODO: change to blocking threadpool, remove dependencies
private class Task(graph: DirectedGraph[Node], bMatrixWriter: BMatrix, distanceMatrixWriter: MatrixWriter, node: Node, log: Logger, progress: Progress) extends Runnable {
  def run() {
    //BEWARE Exceptions are silenced!
    //printf("Starting calculation for node %d\n", node.id)
    val bfs = new BreadthFirstTraverser(graph, GraphDir.OutDir, Seq(node.id), Walk.Limits())
    //We traverse the graph to get depths
    bfs.foreach(_ => {})
    val depthProcessor = new DepthsProcessor(bMatrixWriter, distanceMatrixWriter)
    depthProcessor.processDepths(node.id, bfs.depthAllNodes())
    //Should be synchronized, but we don't care that much, it's just for debug.
    printf("Finished calculation for node %d\n", node.id)
    progress.inc
  }
}

private class BMatrixCalculation(graph: DirectedGraph[Node]) {

  private val log = Logger.get("BMatrixCalculation")

  def setDebug() = {
    val topLog = Logger.get("")
    topLog.setLevel(Logger.DEBUG)
    topLog.getHandlers().foreach(handler => handler.setLevel(Logger.DEBUG))
  }

  def run(distanceMatrixWriter: MatrixWriter, threads: Int): BMatrix = {
    // Let the user know if they can save memory!
    if (graph.maxNodeId != graph.nodeCount - 1)
      log.info("Warning - you may be able to reduce the memory usage by renumbering this graph!")

    val progress = Progress("BMatrix_calculation", 500, Some(graph.nodeCount))
    setDebug()

    val watch = Stopwatch.start()
    val bMatrixWriter = new BMatrix()
    log.info("Initializing BMatix calculation...\n")

    //TODO: change to blocking threadpool, remove dependencies
    val threadPool = new ExecutionContext {
      val threadPool = Executors.newFixedThreadPool(threads)

      def execute(runnable: Runnable) {
        threadPool.submit(runnable)
      }

      def reportFailure(t: Throwable): Unit = {
        log.error(t.getMessage)
      }

      def shutdown = {
        threadPool.shutdown
      }

      def isTerminated = {
        threadPool.isTerminated
      }
    }

    graph.foreach { node =>
      threadPool.execute(new Task(graph, bMatrixWriter, distanceMatrixWriter, node, log, progress))
    }

    threadPool.shutdown
    while (!threadPool.isTerminated) {
      Thread.sleep(500)
    }

    distanceMatrixWriter.close()
    log.info("Finished BMatrix calculation . Time: %s\n".format(watch()))
    bMatrixWriter
  }
}
