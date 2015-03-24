package com.twitter.cassovary.algorithms.bmatrix

import java.io.FileOutputStream

import com.twitter.cassovary.graph._
import it.unimi.dsi.fastutil.ints.{Int2BooleanOpenHashMap, Int2ObjectOpenHashMap}
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

class DistanceMatrixWriter(graph: DirectedGraph[Node], OutFileNamePrefix: String) {
  protected val underlyingMap = new Int2ObjectOpenHashMap[Array[Int]]
  protected val missingNodeIds = new Int2BooleanOpenHashMap
  private val fout = new FileOutputStream(filename(OutFileNamePrefix))
  private val out = new FastBufferedOutputStream(fout)
  private val tab = "\t".getBytes()
  private val newline = "\n".getBytes()
  private val maxId = graph.maxNodeId
  private val nodesNumber = graph.nodeCount
  private var lastWrittenId = 0

  val nodeIds = 0 until maxId
  nodeIds.foreach(id => {
    if (!graph.existsNodeId(id)) {
      missingNodeIds.put(id, true)
    }
  })

  def getNewArray: Array[Int] = {
    new Array[Int](maxId + 1)
  }

  def putArray(nodeId: Int, array: Array[Int]) = {
    underlyingMap.put(nodeId, array)
  }

  def lineReady(nodeId: Int) = {
    while (underlyingMap.containsKey(lastWrittenId) || missingNodeIds.containsKey(lastWrittenId)) {
      val arrayToWrite = underlyingMap.remove(lastWrittenId)
      if (arrayToWrite != null) {
        writeLine(arrayToWrite)
      }
      lastWrittenId += 1
    }
  }

  private def writeLine(arrayToWrite: Array[Int]) = {
    arrayToWrite.zipWithIndex foreach { case (el, i) =>
      if (!missingNodeIds.containsKey(i)) {
        out.write(el.toString.getBytes)
        out.write(tab)
      }
    }
    out.write(newline)
  }

  def filename(OutFileNamePrefix: String) = {
    OutFileNamePrefix + "_distances.out"
  }

  def close() = {
    out.flush()
    out.close()
    fout.close()
  }
}
