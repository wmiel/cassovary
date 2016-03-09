package com.twitter.cassovary.algorithms.bmatrix

import java.util

import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, Int2ObjectOpenHashMap}

class BMatrix(fileNameSuffix: String) extends HashBasedSparseMatrix with FileWriter {
  private var numberOfNodes: Int = 0
  private var numberOfEdges: Int = 0

  override def entries(func: String => Unit) = {
    func("#l-shell size\tnumber of members in l-shell\tnumber of nodes\n")
    func("#B-Matrix START\n")

    val shellSizes = underlyingMap.keySet().toIntArray.sorted
    shellSizes.foreach(shellSize => {
      val numberOfMembersToNumberOfNodesMap = underlyingMap.get(shellSize)
      val numbersOfMembers = numberOfMembersToNumberOfNodesMap.keySet().toIntArray.sorted
      numbersOfMembers.foreach(numberOfMembers => {
        val NumberOfNodes = numberOfMembersToNumberOfNodesMap.get(numberOfMembers)
        if (shellSize == 1) {
          numberOfNodes += NumberOfNodes
          numberOfEdges += NumberOfNodes * numberOfMembers
        }
        func("%d\t%d\t%d\n".format(shellSize, numberOfMembers, NumberOfNodes))

      })
    })

    func("#number of nodes: %d\n".format(numberOfNodes))
    func("#number of edges: %d\n".format(numberOfEdges / 2))
    func("#B-Matrix END\n")
  }

  override def filename(OutFileNamePrefix: String) = {
    OutFileNamePrefix + fileNameSuffix + ".out"
  }
}
