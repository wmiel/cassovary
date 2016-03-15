package com.twitter.cassovary.algorithms.bmatrix

import java.util.concurrent.{Executors, ThreadFactory}

import com.twitter.logging.Logger

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class NamedThreadFactory extends ThreadFactory {
  var counter = 0
  val name = "BFS_worker_"

  def newThread(runnable: Runnable): Thread = {
    val thread_name = name + counter
    val t = new Thread(runnable, thread_name)
    counter += 1
    return t
  }
}

class ParallelExecutionContext(val threads: Int, val log: Logger) extends ExecutionContext {
  val threadPool = Executors.newFixedThreadPool(threads, new NamedThreadFactory)
  val data = Array.fill[Seq[HashBasedSparseMatrix]](threads)(Array.fill(2)(new HashBasedSparseMatrix()))

  def execute(runnable: Runnable) {
    threadPool.submit(runnable)
  }

  def shutdown = {
    threadPool.shutdown
  }

  def reportFailure(t: Throwable): Unit = {
    log.error(t.getMessage)
    throw t
  }

  def isTerminated = {
    threadPool.isTerminated
  }

  def getData(name: String): Seq[HashBasedSparseMatrix] = {
    val index = name.split("_").last.toInt
    data(index)
  }

  def waitToFinish() = {
    shutdown
    while (!isTerminated) {
      Thread.sleep(5000)
    }
  }
}