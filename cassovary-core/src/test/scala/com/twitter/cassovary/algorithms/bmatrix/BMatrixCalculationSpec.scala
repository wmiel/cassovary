package com.twitter.cassovary.algorithms.bmatrix

import com.twitter.cassovary.graph.TestGraphs
import org.scalatest.{WordSpec, Matchers}

class BMatrixCalculationSpec extends WordSpec with Matchers {
  "Closeness centrality" should {
    "return empty hash for graph with less than two nodes" in {
      val graph = TestGraphs.g1
      val bmatrixMap = BMatrixCalculation(graph, new NullDistanceMatrixWriter, 1).matrixMap
      bmatrixMap.size() shouldEqual 0

    }
    "return proper values when normalized" in {
      val graph = TestGraphs.g2_mutual
      val bmatrixMap = BMatrixCalculation(graph, new NullDistanceMatrixWriter, 1).matrixMap
      bmatrixMap.size() shouldEqual 1
      bmatrixMap.get(1).get(1) shouldEqual 2
    }
  }
}
