package com.twitter.cassovary.algorithms.bmatrix

import java.util

import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, Int2ObjectOpenHashMap}

class BMatrix extends HashBasedSparseMatrix with FileWriter {
  private var nodeCount: Int = 0
  private var averageShortestPath: Double = 0.0
  private var numberOfNodes: Int = 0
  private var efficiency: Double = 0.0

  def entries(func: String => Unit) = {
    func("#l-shell size\tnumber of members in l-shell\tnumber of nodes")
    func("#B-Matrix START\n")

    val keys = underlyingMap.keySet().toIntArray.sorted
    keys.foreach(key => {
      val map = underlyingMap.get(key)
      val keys2 = map.keySet().toIntArray.sorted
      keys2.foreach(key2 => {
        val value = map.get(key2)
        if (key == 1)
          numberOfNodes += value
        func("%d\t%d\t%d\n".format(key, key2, value))

      })
    })

    func("#number of nodes: %d\n".format(numberOfNodes))
    func("#efficiency: %f\n".format(efficiency))
    func("#average shortest path (assuming zero for disconnected nodes): %f\n".format(averageShortestPath))
    func("#B-Matrix END\n")
  }

  def setNodeCount(nodeCount: Int) = {
    this.nodeCount = nodeCount
  }

  def addDistance(distance: Int, numberOfNodes: Int) = {
    val partialShortestPath:Double = distance * numberOfNodes
    val partialEfficiency:Double = (numberOfNodes.toDouble / distance)

    averageShortestPath += (partialShortestPath / nodeCount.toDouble) / (nodeCount - 1)
    efficiency += (partialEfficiency / nodeCount) / (nodeCount - 1)
  }


  def filename(OutFileNamePrefix: String) = {
    OutFileNamePrefix + "_bmatrix.out"
  }

  def printMatrix() = {
    writeToStdout(entries)
  }

  def writeMatrix(OutFileNamePrefix: String) = {
    writeToFile(filename(OutFileNamePrefix), entries)
  }
}
