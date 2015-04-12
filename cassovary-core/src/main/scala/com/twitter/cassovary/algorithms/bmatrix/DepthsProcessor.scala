package com.twitter.cassovary.algorithms.bmatrix

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap

trait DepthsProcessor {
  protected val underlyingMap = new Int2IntOpenHashMap
  protected var bmatrix: BMatrix

  def addToBMatrix() = {
    val iterator = underlyingMap
      .int2IntEntrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next

      bmatrix.synchronized {
        bmatrix.increment(entry.getKey, entry.getValue)
      }

      processEntry(entry.getKey, entry.getValue)
    }
  }

  def processEntry(key:Int, value:Int) = {}

  def incrementForDepth(depth: Int) = {
    underlyingMap.addTo(depth, 1)
  }
}
