package com.twitter.cassovary.algorithms.bmatrix

import com.twitter.cassovary.graph._
import com.twitter.cassovary.util.Progress
import com.twitter.logging.Logger

import scala.collection


private class BfsTask(parallelExecutionContext: ParallelExecutionContext,
                      graph: DirectedGraph[Node],
                      nodes: Seq[Node],
                      log: Logger,
                      progress: Progress,
                      undirectedFlag: Boolean) extends Runnable {
  def run() {
    //BEWARE Exceptions are silenced!
    try {
      val executorThreadName = Thread.currentThread().getName
      val data = parallelExecutionContext.getData(executorThreadName)
      val vertexBMatrix = data(0)
      val edgeBMatrix = data(1)

      nodes.foreach { node =>
        val bfs = new BreadthFirstTraverser(graph, GraphDir.OutDir, Seq(node.id), Walk.Limits())
        //We traverse the graph to get depths
        bfs.foreach(_ => {})

        val depths = bfs.depthAllNodes()

        val vertexDepthProcessor = new VertexDepthsProcessor(vertexBMatrix)
        val edgeDepthProcessor = new EdgeDepthsProcessor(edgeBMatrix)

        vertexDepthProcessor.processDepths(node.id, depths)
        edgeDepthProcessor.processDepths(node.id, depths, graph, undirectedFlag)

        progress.inc
      }
      printf("Finished calculation in %s for nodes %s\n", executorThreadName, nodes.map(x => x.id).mkString(","))
    } catch {
      case e: Exception => {
        println("BFS WORKER EXCEPTION")
        println(e)
        e.printStackTrace()
        val t = Thread.currentThread()
        t.getUncaughtExceptionHandler.uncaughtException(t, e)
      }
    }
  }
}
