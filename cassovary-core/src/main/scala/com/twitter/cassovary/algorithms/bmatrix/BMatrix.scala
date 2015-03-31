package com.twitter.cassovary.algorithms.bmatrix

import java.util

import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, Int2ObjectOpenHashMap}

class BMatrix extends HashBasedSparseMatrix with FileWriter {
  def entries(func: String => Unit) = {
    func("#l-shell size\tnumber of members in l-shell\tnumber of nodes")
    func("#B-Matrix START\n")

    val keys = underlyingMap.keySet().toIntArray.sorted
    keys.foreach(key => {
      val map = underlyingMap.get(key)
      val keys2 = map.keySet().toIntArray.sorted
      keys2.foreach(key2 => {
        func("%d\t%d\t%d\n".format(key, key2, map.get(key2)))
      })
    })

    func("#B-Matrix END")
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
