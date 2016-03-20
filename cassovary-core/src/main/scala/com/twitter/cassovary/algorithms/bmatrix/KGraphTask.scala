package com.twitter.cassovary.algorithms.bmatrix

import com.twitter.cassovary.graph._
import com.twitter.cassovary.util.Progress
import com.twitter.logging.Logger

import scala.collection


private class KGraphTask(parallelExecutionContext: ParallelExecutionContext,
                         graph: DirectedGraph[Node],
                         nodes: Seq[Node],
                         log: Logger,
                         progress: Progress,
                         undirectedFlag: Boolean) extends Runnable {
  def run() {
    try {
      val executorThreadName = Thread.currentThread().getName
      val data = parallelExecutionContext.getData(executorThreadName)
      val distanceMatrix = data(0)

      nodes.foreach { node =>
        val bfs = new BreadthFirstTraverser(graph, GraphDir.OutDir, Seq(node.id), Walk.Limits())
        bfs.foreach(_ => {})

        val depths = bfs.depthAllNodes()



        progress.inc
      }
      printf("Finished calculation in %s for nodes %s\n", executorThreadName, nodes.map(x => x.id).mkString(","))
    } catch {
      case e: Exception => {
        println("KGRAPH WORKER EXCEPTION")
        println(e)
        e.printStackTrace()
        val t = Thread.currentThread()
        t.getUncaughtExceptionHandler.uncaughtException(t, e)
      }
    }
  }
}
