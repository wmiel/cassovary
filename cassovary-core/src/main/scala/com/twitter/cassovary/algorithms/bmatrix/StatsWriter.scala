package com.twitter.cassovary.algorithms.bmatrix

class StatsWriter(private val nodeCount:Int) extends FileWriter {
  private var averageShortestPath: Double = 0.0
  private var efficiency: Double = 0.0

  def addDistance(distance: Int, numberOfNodes: Int) = {
    val partialShortestPath: Double = distance * numberOfNodes
    val partialEfficiency: Double = (numberOfNodes.toDouble / distance)

    averageShortestPath += (partialShortestPath / nodeCount.toDouble) / (nodeCount - 1)
    efficiency += (partialEfficiency / nodeCount) / (nodeCount - 1)
  }

  override def entries(func: String => Unit) = {
    func("#efficiency: %f\n".format(efficiency))
    func("#average shortest path (assuming zero for disconnected nodes): %f\n".format(averageShortestPath))
  }

  override def filename(OutFileNamePrefix: String) = {
    OutFileNamePrefix + "_stats.out"
  }
}
