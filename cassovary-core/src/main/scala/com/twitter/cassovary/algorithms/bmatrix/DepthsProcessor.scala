package com.twitter.cassovary.algorithms.bmatrix

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap

trait DepthsProcessor {
  protected val underlyingMap = new Int2IntOpenHashMap
  protected var bmatrix: HashBasedSparseMatrix

  def addToBMatrix() = {
    val iterator = underlyingMap
      .int2IntEntrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next
      bmatrix.increment(entry.getKey, entry.getValue)
    }
  }

  def incrementForDepth(depth: Int) = {
    underlyingMap.addTo(depth, 1)
  }
}
