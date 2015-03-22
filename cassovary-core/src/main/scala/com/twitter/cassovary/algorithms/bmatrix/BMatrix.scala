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
import com.twitter.logging.{Level, Logger}

object BMatrix {

  def apply(graph: DirectedGraph[Node]): Unit = {
    val bm = new BMatrix(graph)
    bm.run
  }

}

private class BMatrix(graph: DirectedGraph[Node]) {

  private val log = Logger.get("BMatrix")

  def setDebug() = {
    val topLog = Logger.get("")
    topLog.setLevel(Logger.DEBUG)
    topLog.getHandlers().foreach( handler => handler.setLevel(Logger.DEBUG) )
  }

  def run: Unit = {
    // Let the user know if they can save memory!
    if (graph.maxNodeId.toDouble / graph.nodeCount > 1.1 && graph.maxNodeId - graph.nodeCount > 1000000)
      log.info("Warning - you may be able to reduce the memory usage of PageRank by renumbering this graph!")

    log.info("Initializing starting BMatix calculation...")
    val progress = Progress("BMatrix_calculation", 100, Some(graph.nodeCount))
    val writer = new BMatrixWriter()

    setDebug()

    graph.foreach { node =>
      val bfs = new BreadthFirstTraverser(graph, GraphDir.OutDir, Seq(node.id), Walk.Limits())

      bfs.toList
      bfs.depthAllNodes().values.toTraversable.groupBy(k => k).map(k => {
        k._1 -> k._2.size
      }).foreach(k => {
        if (k._1 > 0)
          writer.add(k._1, k._2)
      })
      progress.inc
    }
    log.info("Printing matrix")
    writer.printMatrix()
    log.info("END")
  }
}

