package com.twitter.cassovary.algorithms.bmatrix

import java.util

import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, Int2ObjectOpenHashMap}
import scala.collection.JavaConversions._

class HashBasedSparseMatrix {
  protected val underlyingMap = new Int2ObjectOpenHashMap[Int2IntOpenHashMap]

  def matrixMap: util.Map[Int, util.Map[Int, Int]] = underlyingMap.asInstanceOf[util.Map[Int, util.Map[Int, Int]]]

  def increment(l: Int, k: Int) = {
    addTo(l, k, 1)
  }

  def addTo(l: Int, k: Int, v: Int) = {
    getOrCreate(l).addTo(k, v)
  }

  def forEachElement(f: (Int, Int, Int) => Unit) = {
    matrixMap.foreach({ case (x: Int, map: util.Map[Int, Int]) =>
      map.foreach { case (y: Int, z: Int) => {
        f(x, y, z)
      }
      }
    })
  }

  def merge(anotherMatrix: HashBasedSparseMatrix) = {
    anotherMatrix.forEachElement((x, y, z) => {
      addTo(x, y, z)
    })
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
