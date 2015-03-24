package com.twitter.cassovary.algorithms.bmatrix

import java.io.FileOutputStream

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

trait FileWriter {
  def writeToStdout(entries: ((String) => Unit) => Unit) = {
    entries((x: String) => print(x))
  }

  def writeToFile(filename: String, entries: ((String) => Unit) => Unit) = {
    val fout = new FileOutputStream(filename)
    val out = new FastBufferedOutputStream(fout)

    entries((x: String) => out.write(x.getBytes))

    out.flush()
    out.close()
    fout.close()
  }
}
