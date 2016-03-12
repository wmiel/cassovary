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
package com.twitter.cassovary

import com.twitter.cassovary.algorithms.bmatrix._
import com.twitter.cassovary.graph.{DirectedGraph, Node}
import com.twitter.util.Stopwatch


class BMatrixBenchmark(graph: DirectedGraph[Node],
                       outFileNamePrefix: String,
                       writeDistanceMatrix: Boolean,
                       threads: Int,
                       undirectedFlag: Boolean,
                       partition: Int,
                       numberOfPartitions: Int,
                       ccBMatrixBins: Int
                      )
  extends OperationBenchmark {

  def operation() {
    if (writeDistanceMatrix) {
      println("Writing distance matrix.")
    }
    val distanceMatrixWriter: MatrixWriter = if (writeDistanceMatrix)
      new DistanceMatrixWriter(graph, outFileNamePrefix)
    else
      new NullDistanceMatrixWriter()
    BMatrixCalculation(graph, distanceMatrixWriter, threads, undirectedFlag, partition, numberOfPartitions, outFileNamePrefix, ccBMatrixBins)
  }
}
