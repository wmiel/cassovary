package com.twitter.cassovary.algorithms.bmatrix

import java.io.FileOutputStream

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

trait FileWriter {
  def entries(func: String => Unit)

  def writeToStdout = {
    entries((x: String) => print(x))
  }

  def writeToFile(prefix: String) = {
    val fout = new FileOutputStream(filename(prefix))
    val out = new FastBufferedOutputStream(fout)

    entries((x: String) => out.write(x.getBytes))

    out.flush()
    out.close()
    fout.close()
  }

  def filename(OutFileNamePrefix: String) = {
    OutFileNamePrefix + getClass
  }
}
