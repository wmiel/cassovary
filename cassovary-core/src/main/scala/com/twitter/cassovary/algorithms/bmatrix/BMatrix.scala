package com.twitter.cassovary.algorithms.bmatrix

import java.util

import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, Int2ObjectOpenHashMap}

class BMatrix extends HashBasedSparseMatrix with FileWriter {
  private var nodeCount:Int = 0
  private var sumOfDistances: BigInt = 0
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
        if(key == 1)
          numberOfNodes += value
        func("%d\t%d\t%d\n".format(key, key2, value))

      })
    })

    func("#number of nodes: %d\n".format(numberOfNodes))
    func("#average shortest path (assuming zero for disconnected nodes): %f\n".format(averageShortestPath))
    func("#efficiency: %f\n".format(getEfficiency))
    func("#B-Matrix END\n")
  }

  def setNodeCount(nodeCount:Int) = {
    this.nodeCount = nodeCount
  }

  def addDistance(distance:Int, numberOfNodes:Int) = {
    sumOfDistances += distance * numberOfNodes
    efficiency += (numberOfNodes.toDouble / distance)
  }

  def averageShortestPath:Double = {
    sumOfDistances.doubleValue() / (nodeCount * (nodeCount - 1))
  }

  def getEfficiency:Double = {
    efficiency / (nodeCount * (nodeCount - 1))
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
