package com.twitter.cassovary.algorithms.bmatrix

import java.util

import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, Int2ObjectOpenHashMap}

class BMatrix(fileNameSuffix: String) extends HashBasedSparseMatrix with FileWriter {
  private var numberOfNodes: Int = 0

  override def entries(func: String => Unit) = {
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
    func("#B-Matrix END\n")
  }

  def filename(OutFileNamePrefix: String) = {
    OutFileNamePrefix + fileNameSuffix + "_bmatrix.out"
  }
}
