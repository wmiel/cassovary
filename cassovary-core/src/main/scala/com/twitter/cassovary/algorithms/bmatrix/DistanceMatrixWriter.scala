package com.twitter.cassovary.algorithms.bmatrix

import java.io.FileOutputStream
import java.nio.channels.Channels
import java.nio.{ByteBuffer, IntBuffer}
import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.GZIPOutputStream

import com.twitter.cassovary.graph._
import it.unimi.dsi.fastutil.ints.{Int2BooleanOpenHashMap, Int2ObjectOpenHashMap}
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

trait MatrixWriter {

  def writeNodesNumber()
  def getNewBuffer: ByteBuffer
  def putInBuffer(byteBuffer: ByteBuffer, nodeId: Int, value: Int)
  def putBuffer(nodeId: Int, buffer: ByteBuffer)
  def lineReady(nodeId: Int)
  def close()
}

class NullDistanceMatrixWriter extends MatrixWriter {
  override def writeNodesNumber(): Unit = {}

  override def lineReady(nodeId: Int): Unit = {}

  override def putInBuffer(byteBuffer: ByteBuffer, nodeId: Int, value: Int): Unit = {}

  override def close(): Unit = {}

  override def putBuffer(nodeId: Int, buffer: ByteBuffer): Unit = {}

  override def getNewBuffer: ByteBuffer = null
}

class DistanceMatrixWriter(graph: DirectedGraph[Node], OutFileNamePrefix: String) extends MatrixWriter {
  protected val underlyingMap = new ConcurrentHashMap[Int, ByteBuffer]
  protected val missingNodeIds = new Int2BooleanOpenHashMap
  private val offsets = new Array[Int](graph.maxNodeId + 1)
  private val fout = new FileOutputStream(filename(OutFileNamePrefix))
  private val gzip = new GZIPOutputStream(fout)
  private val out = Channels.newChannel(gzip)
  private val maxId = graph.maxNodeId
  private val nodesNumber = graph.nodeCount
  private var lastWrittenId = 0


  val nodeIds = 0 until maxId
  nodeIds.foreach(id => {
    if (!graph.existsNodeId(id)) {
      missingNodeIds.put(id, true)
    }
  })

  writeNodesNumber
  prepareOffsets

  def writeNodesNumber = {
    val buffer = ByteBuffer.allocate(4)
    buffer.putInt(nodesNumber)
    buffer.rewind()
    out.write(buffer)
  }

  def prepareOffsets = {
    var missing = 0
    (0 until offsets.length).foreach { i =>
      if(missingNodeIds.containsKey(i)) {
        missing += 1
      }
      offsets(i) = missing
    }
  }

  def getNewBuffer: ByteBuffer = {
    val buffer = ByteBuffer.allocate(4 * nodesNumber)
    buffer
  }

  def putInBuffer(byteBuffer: ByteBuffer, nodeId: Int, value: Int) = {
    byteBuffer.putInt((nodeId - offsets(nodeId)) * 4, value)
  }

  def putBuffer(nodeId: Int, buffer: ByteBuffer) = {
    printf("Put buffer: %d\n", nodeId)
    underlyingMap.put(nodeId, buffer)
  }

  def lineReady(nodeId: Int) = {
    //var chunkWrite = 0
    //val writtenIds = new util.ArrayList[Int]
    while (underlyingMap.containsKey(lastWrittenId) || missingNodeIds.containsKey(lastWrittenId)) {
      val bufferToWrite = underlyingMap.remove(lastWrittenId)
      if (bufferToWrite != null) {
        writeLine(bufferToWrite)
      }
     //chunkWrite += 1
      //writtenIds.add(lastWrittenId)
      lastWrittenId += 1
    }
    //printf("Written in one chunk: %d, nodeId %d, written: %s\n", chunkWrite, nodeId, writtenIds.toString)
  }

  private def writeLine(buffer: ByteBuffer) = {
    buffer.rewind()
    out.write(buffer)
  }

  def filename(OutFileNamePrefix: String) = {
    OutFileNamePrefix + "_distances.out.gz"
  }

  def close() = {
    gzip.finish()
    out.close()
    gzip.close()
    fout.close()
  }
}
