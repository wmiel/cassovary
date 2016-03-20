package com.twitter.cassovary

import com.twitter.cassovary.algorithms.bmatrix._
import com.twitter.cassovary.graph.{DirectedGraph, Node}


class CCBMatrixBenchmark(graph: DirectedGraph[Node],
                         outFileNamePrefix: String,
                         threads: Int,
                         bins: Int)
  extends OperationBenchmark {

  def operation() {
    CCBMatrixCalculation(graph, threads, outFileNamePrefix, bins)
  }
}

