/*
 * Copyright 2014 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.twitter.cassovary.util

import com.twitter.logging.Logger

object Progress {
  /**
   * Create a new Progress meter, which outputs progress information to the log
   * at a given interval. If a maximum value is specified, will also give a percentage.
   * @param name identifies this progress meter
   * @param interval reporting interval - defaulting to 1 (log every time the counter is incremented)
   * @param maxValue optional maximum value
   * @return
   */
  def apply(name: String, interval: Int = 1, maxValue: Option[Int] = None) = {
    new Progress(name, interval, maxValue)
  }
}

class Progress(val name: String, val interval: Int, val maxValue: Option[Int]) {

  private val log = Logger.get("Progress of: " + name)

  val mb = 1024 * 1024
  var count = 0
  val runtime = Runtime.getRuntime()

  /**
   * Increment Progress counter
   */
  def inc {
    count += 1
    if (count % interval == 0) {
      val memoryInfo = " Used Memory:%d MB, Free Memory:%d MB, Total Memory:%d MB, Max Memory:%d MB".format(
        (runtime.totalMemory() - runtime.freeMemory()) / mb,
        runtime.freeMemory() / mb,
        runtime.totalMemory() / mb,
        runtime.maxMemory() / mb
      )

      maxValue match {
        case Some(max) => {
          log.debug("%s (%.2f%%)%s".format(count, count.toDouble / max * 100, memoryInfo))
        }
        case None => {
          log.debug(count.toString + memoryInfo)
        }
      }
    }
  }
}
