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

import scala.collection.mutable.ArrayBuffer

object BMatrixCalculation {
  def apply(graph: DirectedGraph[Node], distanceMatrixWriter: MatrixWriter,
            threads: Int,
            undirectedFlag:
            Boolean,
            partition: Int,
            numberOfPartitions: Int,
            outFileNamePrefix: String,
            minMaxD: (Int, Int)) = {
    val bm = new BMatrixCalculation(graph)
    bm.run(distanceMatrixWriter, threads, undirectedFlag, partition, numberOfPartitions, outFileNamePrefix, minMaxD)
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
          numberOfPartitions: Int,
          outFileNamePrefix: String,
          minMaxD: (Int, Int)) = {
    val calculationTime = Stopwatch.start()
    // Let the user know if they can save memory!
    if (graph.maxNodeId != graph.nodeCount - 1)
      log.info("Warning - you may be able to reduce the memory usage by renumbering this graph!")

    setDebug()

    val watch = Stopwatch.start()

    log.info("Initializing BMatix calculation...\n")
    log.info("Using " + threads + " threads.")

    val perPartition = math.ceil(graph.nodeCount.toDouble / numberOfPartitions).toInt
    val currentPartitionFrom = (partition - 1) * perPartition
    val currentPartitionTo = (partition) * perPartition
    val progress = Progress("BMatrix_calculation", 1000, Some(perPartition))

    val threadPool = new ParallelExecutionContext(threads, log)

    log.info("Partition %s/%s".format(partition, numberOfPartitions))
    log.info("Processing %s vertices from %s to %s.".format(perPartition, currentPartitionFrom, currentPartitionTo))
    var processedNodes = 0

    var nodes = List[Node]()
    val multitaskLimit = 25

    val minD = minMaxD._1
    val maxD = minMaxD._2

    graph.foreach { node =>
      if (currentPartitionFrom <= processedNodes && processedNodes < currentPartitionTo) {
        if(maxD == 0 || (maxD > 0 && minD <= node.neighborCount(GraphDir.OutDir) && node.neighborCount(GraphDir.OutDir) <= maxD)) {
          if (nodes.size < multitaskLimit) {
            nodes = nodes :+ node
          } else {
            threadPool.execute(new BfsTask(threadPool, graph, nodes, log, progress, undirectedFlag))
            nodes = List(node)
          }
        }
      }
      processedNodes += 1
    }

    if (nodes.nonEmpty) {
      threadPool.execute(new BfsTask(threadPool, graph, nodes, log, progress, undirectedFlag))
    }

    threadPool.waitToFinish()
    log.info("Merging thread BMatrices.\n")

    val vertexBMatrix = new BMatrix("_vertex_bmatrix_%s".format(partition))
    val edgeBMatrix = new BMatrix("_edge_bmatrix_%s".format(partition))

    val data = threadPool.data
    for (i <- 0 until data.length) {
      val partialMatrix = data(i)
      vertexBMatrix.merge(partialMatrix(0))
      edgeBMatrix.merge(partialMatrix(1))
      printf("BFS_worker_%d results merged.\n", i)
    }

    log.info("Finished BMatrix calculation . Time: %s\n".format(watch()))
    log.info("Nodes processed: %s\n".format(processedNodes))

    println("Initializing BMatrix writing")
    val writingWatch = Stopwatch.start()

    val writers = new ArrayBuffer[BMatrix]
    writers.append(vertexBMatrix)
    writers.append(edgeBMatrix)

    writers.foreach((x) => {
      x.numberOfNodes = graph.nodeCount
      x.numberOfEdges = graph.edgeCount
      x.calculationTime = calculationTime().toString()
      x.writeToFile(outFileNamePrefix)
    })

    printf("Finished BMatrix writing time: %s\n", writingWatch())
  }
}
