package com.twitter.cassovary.algorithms.bmatrix

import com.twitter.cassovary.graph._
import com.twitter.logging.Logger
import com.twitter.util.Stopwatch

import scala.collection.mutable.ArrayBuffer

object CCBMatrixCalculation {
  def apply(graph: DirectedGraph[Node],
            threads: Int,
            outFileNamePrefix: String,
            bins: Int) = {
    val bm = new CCBMatrixCalculation(graph)
    bm.run(threads, outFileNamePrefix, bins)
  }
}

private class CCBMatrixCalculation(graph: DirectedGraph[Node]) {
  private val log = Logger.get("BMatrixCalculation")

  def setDebug() = {
    val topLog = Logger.get("")
    topLog.setLevel(Logger.DEBUG)
    topLog.getHandlers().foreach(handler => handler.setLevel(Logger.DEBUG))
  }

  def run(threads: Int,
          outFileNamePrefix: String, bins: Int) = {
    val calculationTime = Stopwatch.start()
    // Let the user know if they can save memory!
    if (graph.maxNodeId != graph.nodeCount - 1)
      log.info("Warning - you may be able to reduce the memory usage by renumbering this graph!")

    setDebug()

    val watch = Stopwatch.start()

    log.info("Initializing BMatix calculation...\n")
    //log.info("Using " + threads + " threads.")

    val ccBMatrix = new BMatrix("_cc_bmatrix_")

    val kGraphs = new kGraphGenerator(graph)
    //    println("KGRAPHS")
    //    println(kGraphs.kGraphs.mkString(" - "))
    kGraphs.foreachK { (k, graph) => {
      val ccs = new ClusteringCoefficient(graph, bins).calculate()
      ccs.foreach { case (id, (value, bin)) => {
        ccBMatrix.addTo(k, bin, 1)
      }
      }

      //      println(a.mkString("\n"))
      //      println()
      //      graph.foreach { node =>
      //        //var checkedNodes = scala.collection.mutable.Set[Int]()
      //        node.neighborIds(GraphDir.OutDir).foreach({ neighborId: Int => {
      //          println(node.id + " - " + neighborId)
      //        }
      //        })
      //      }
      //      println("======================")

    }
    }

    println("Initializing BMatrix writing")
    val writingWatch = Stopwatch.start()

    val writers = new ArrayBuffer[BMatrix]
    writers.append(ccBMatrix)

    writers.foreach((x) => {
      x.numberOfNodes = graph.nodeCount
      x.numberOfEdges = graph.edgeCount
      x.calculationTime = calculationTime().toString()
      x.writeToFile(outFileNamePrefix)
    })

    printf("Finished BMatrix writing time: %s\n", writingWatch())
  }
}
