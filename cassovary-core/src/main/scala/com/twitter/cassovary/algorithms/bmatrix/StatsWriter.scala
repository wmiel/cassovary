package com.twitter.cassovary.algorithms.bmatrix

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

class StatsWriter(private val nodeCount:Int) extends FileWriter {
  private var averageShortestPath: Double = 0.0
  private var efficiency: Double = 0.0
  private var stats = new Int2ObjectOpenHashMap[Tuple3[BigInt, BigInt, BigInt]]

  def addDistance(distance: Int, numberOfNodes: Int) = {
    val partialShortestPath: Double = distance * numberOfNodes
    val partialEfficiency: Double = (numberOfNodes.toDouble / distance)

    averageShortestPath += (partialShortestPath / nodeCount.toDouble) / (nodeCount - 1)
    efficiency += (partialEfficiency / nodeCount) / (nodeCount - 1)
  }

  def addStats(nodeId:Int, maxDistance:BigInt, sumOfDepths:BigInt, sizeOfCC:BigInt): Unit = {
    stats.put(nodeId, (maxDistance, sumOfDepths, sizeOfCC))
  }

  override def entries(func: String => Unit) = {
    func("#efficiency: %f\n".format(efficiency))
    func("#average shortest path (assuming zero for disconnected nodes): %f\n".format(averageShortestPath))
    func("#Number of nodes: %d\n".format(nodeCount))
    func("#NodeId \t Eccentricity \t Sum Of Distances \t Size of Connected Component\n")
    val iterator = stats.int2ObjectEntrySet().fastIterator()
    while (iterator.hasNext) {
      val next = iterator.next()
      val values = next.getValue
      func("%d\t%d\t%d\t%d\n".format(next.getIntKey, values._1, values._2, values._3))
    }
    func("#END")
  }

  override def filename(OutFileNamePrefix: String) = {
    OutFileNamePrefix + "_stats.out"
  }
}
