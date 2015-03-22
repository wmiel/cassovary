package com.twitter.cassovary.algorithms.bmatrix

import com.twitter.util.Stopwatch
import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, Int2ObjectOpenHashMap}

class BMatrixWriter {
  protected val underlyingMap = new Int2ObjectOpenHashMap[Int2IntOpenHashMap];

  def add(l: Int, k: Int): Unit = {
    increment(getOrCreate(l), k)
  }

  private def increment(map: Int2IntOpenHashMap, k: Int) = {
    val current = map.get(k)
    map.put(k, current + 1)
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

  def printMatrix() = {
    val watch = Stopwatch.start()
    val keys = underlyingMap.keySet().toIntArray.sorted
    keys.foreach( key => {
      val map = underlyingMap.get(key)
      val keys2 = map.keySet().toIntArray.sorted
      keys2.foreach( key2 => {
        printf("[%d,%d] = %d\n", key, key2, map.get(key2))
      })
    })
    printf("\tPrinting time: %s.\n", watch())
  }

}
