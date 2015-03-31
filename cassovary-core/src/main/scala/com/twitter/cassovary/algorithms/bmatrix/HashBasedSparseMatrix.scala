package com.twitter.cassovary.algorithms.bmatrix

import java.util

import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, Int2ObjectOpenHashMap}

class HashBasedSparseMatrix {
  protected val underlyingMap = new Int2ObjectOpenHashMap[Int2IntOpenHashMap]

  def matrixMap: util.Map[Int, util.Map[Int, Int]] = underlyingMap.asInstanceOf[util.Map[Int, util.Map[Int, Int]]]

  def increment(l: Int, k: Int) = {
    getOrCreate(l).addTo(k, 1)
  }

  private def getOrCreate(l: Int): Int2IntOpenHashMap = {
    if (underlyingMap.containsKey(l)) {
      underlyingMap.get(l)
    } else {
      val map = new Int2IntOpenHashMap()
      underlyingMap.put(l, map)
      map
    }
  }
}
