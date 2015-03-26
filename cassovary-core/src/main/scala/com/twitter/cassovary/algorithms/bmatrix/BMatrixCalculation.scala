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
import java.{util => jutil}

import com.twitter.cassovary.graph._
import com.twitter.cassovary.util.Progress
import com.twitter.logging.Logger
import com.twitter.util.Stopwatch
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap

import scala.concurrent.ExecutionContext

object BMatrixCalculation {

  def apply(graph: DirectedGraph[Node], filename: String, writeDistanceMatrix: Boolean, threads: Int): Unit = {
    val bm = new BMatrixCalculation(graph)
    bm.run(filename, writeDistanceMatrix, threads)
  }
}

private class DepthsProcessor(bMatrixWriter: BMatrixWriter, distanceMatrixWriter: MatrixWriter) {
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

    //possible deadlock? Consider using concurrent hash map
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
        bMatrixWriter.add(entry.getKey, entry.getValue)
      }
    }
  }

  def counters = underlyingMap
}

class Task(graph: DirectedGraph[Node], bMatrixWriter: BMatrixWriter, distanceMatrixWriter: MatrixWriter, node: Node, log: Logger, progress: Progress, outFileNamePrefix: String) extends Runnable {
  def run() {
    //BEWARE Exceptions are silenced!
    //graph.par.foreach { node =>
    //printf("Starting calculation for node %d\n", node.id)
    val bfs = new BreadthFirstTraverser(graph, GraphDir.OutDir, Seq(node.id), Walk.Limits())
    //We traverse the graph to get depths
    bfs.foreach(_ => {})
//    try {
    val depthProcessor = new DepthsProcessor(bMatrixWriter, distanceMatrixWriter)
    depthProcessor.processDepths(node.id, bfs.depthAllNodes())
//    } catch {
//      case e: Exception => {
//        log.info(e.getClass.toString)
//        log.info(e.toString)
//
//        println(e.getClass)
//        e.printStackTrace()
//      }
//    }
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

  def run(outFileNamePrefix: String, writeDistanceMatrix: Boolean, threads: Int): Unit = {
    // Let the user know if they can save memory!
    if (graph.maxNodeId != graph.nodeCount - 1)
      log.info("Warning - you may be able to reduce the memory usage by renumbering this graph!")

    val progress = Progress("BMatrix_calculation", 500, Some(graph.nodeCount))
    setDebug()

    log.info("Initializing BMatix calculation...\n")
    if (writeDistanceMatrix) {
      log.info("Writing distance matrix.")
    }
    log.info("Using %d threads.".format(threads))

    val watch = Stopwatch.start()
    val bMatrixWriter = new BMatrixWriter()

    val distanceMatrixWriter: MatrixWriter = if (writeDistanceMatrix)
      new DistanceMatrixWriter(graph, outFileNamePrefix)
    else
      new NullDistanceMatrixWriter()

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
      threadPool.execute(new Task(graph, bMatrixWriter, distanceMatrixWriter, node, log, progress, outFileNamePrefix))
    }

    threadPool.shutdown
    while (!threadPool.isTerminated) {
      Thread.sleep(500)
    }

    distanceMatrixWriter.close()
    log.info("Finished BMatrix calculation . Time: %s\n".format(watch()))

    log.info("Initializing BMatrix writing\n")
    val writingWatch = Stopwatch.start()
    bMatrixWriter.writeMatrix(outFileNamePrefix)
    log.info("Finished BMatrix writing time: %s\n".format(writingWatch()))

  }
}

