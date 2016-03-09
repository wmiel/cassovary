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

import com.twitter.cassovary.graph._
import com.twitter.cassovary.util.Progress
import com.twitter.logging.Logger
import com.twitter.util.Stopwatch

object BMatrixCalculation {
  def apply(graph: DirectedGraph[Node], distanceMatrixWriter: MatrixWriter,
            threads: Int,
            undirectedFlag:
            Boolean,
            partition: Int,
            numberOfPartitions: Int): (FileWriter, FileWriter) = {
    val bm = new BMatrixCalculation(graph)
    bm.run(distanceMatrixWriter, threads, undirectedFlag, partition, numberOfPartitions)
  }
}

private class BfsTask(parallelExecutionContext: ParallelExecutionContext,
                      graph: DirectedGraph[Node],
                      node: Node,
                      log: Logger,
                      progress: Progress,
                      undirectedFlag: Boolean) extends Runnable {
  def run() {
    //BEWARE Exceptions are silenced!
    val executorThreadName = Thread.currentThread().getName

    val bfs = new BreadthFirstTraverser(graph, GraphDir.OutDir, Seq(node.id), Walk.Limits())
    //We traverse the graph to get depths
    bfs.foreach(_ => {})

    val depths = bfs.depthAllNodes()

    val data = parallelExecutionContext.getData(executorThreadName).get
    val vertexBMatrix = data._1
    val edgeBMatrix = data._2

    val vertexDepthProcessor = new VertexDepthsProcessor(vertexBMatrix)
    vertexDepthProcessor.processDepths(node.id, depths)

    val edgeDepthProcessor = new EdgeDepthsProcessor(edgeBMatrix)
    edgeDepthProcessor.processDepths(node.id, depths, graph, undirectedFlag)

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

  def run(distanceMatrixWriter: MatrixWriter,
          threads: Int,
          undirectedFlag: Boolean,
          partition: Int,
          numberOfPartitions: Int): (FileWriter, FileWriter) = {
    // Let the user know if they can save memory!
    if (graph.maxNodeId != graph.nodeCount - 1)
      log.info("Warning - you may be able to reduce the memory usage by renumbering this graph!")

    setDebug()

    val watch = Stopwatch.start()

    log.info("Initializing BMatix calculation...\n")

    //TODO: change to blocking threadpool, remove dependencies
    val threadPool = new ParallelExecutionContext(threads, log)

    val perPartition = math.ceil(graph.nodeCount.toDouble / numberOfPartitions).toInt
    val currentPartitionFrom = (partition - 1) * perPartition
    val currentPartitionTo = (partition) * perPartition
    val progress = Progress("BMatrix_calculation", 500, Some(perPartition))


    log.info("Partition %s/%s".format(partition, numberOfPartitions))
    log.info("Processing %s vertices from %s to %s.".format(perPartition, currentPartitionFrom, currentPartitionTo))
    var processedNodes = 0
    graph.foreach { node =>
      if (processedNodes >= currentPartitionFrom && processedNodes < currentPartitionTo) {
        threadPool.execute(new BfsTask(threadPool, graph, node, log, progress, undirectedFlag))
      }
      processedNodes += 1
    }

    threadPool.waitToFinish()
    log.info("Merging thread BMatrices.\n")
    val vertexBMatrix = new BMatrix("_vertex_bmatrix_%s".format(partition))
    val edgeBMatrix = new BMatrix("_edge_bmatrix_%s".format(partition))
    threadPool.data.foreach { case (threadName, (partialVertexMatrix, partialEdgeMatrix)) => {
      println(threadName)
      vertexBMatrix.merge(partialVertexMatrix)
      edgeBMatrix.merge(partialEdgeMatrix)
    }
    }
    log.info("Finished BMatrix calculation . Time: %s\n".format(watch()))
    log.info("Nodes processed: %s\n".format(processedNodes))
    (vertexBMatrix, edgeBMatrix)
  }
}
