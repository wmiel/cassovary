package com.twitter.cassovary

import com.twitter.cassovary.algorithms.AverageClusteringCoefficient
import com.twitter.cassovary.graph.{Node, DirectedGraph}


class AverageClusteringCoefficientBenchmark(graph: DirectedGraph[Node])
  extends OperationBenchmark {

  def operation() {
    printf("Average Clustering Coefficient: %f", AverageClusteringCoefficient(graph))
  }
}