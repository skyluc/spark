/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.receiver

import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.Random
import org.apache.spark.util.random.GapSamplingIterator

/**
 * This class provides a congestion strategy that ignores
 * any back-pressure information.
 * @see CongestionStrategy
 */
class IgnoreCongestionStrategy extends CongestionStrategy {

  override def onBlockBoundUpdate(bound: Int) {}

  override def restrictCurrentBuffer(currentBuffer: ArrayBuffer[Any],
                                     nextBuffer: ArrayBuffer[Any]): Unit = {}
}

class PushBackCongestionStrategy(blockInterval: Long) extends CongestionStrategy {

  private val latestBound = new AtomicInteger(-1)

  override def onBlockBoundUpdate(bound:Int): Unit = latestBound.set(bound)

  override def restrictCurrentBuffer(currentBuffer: ArrayBuffer[Any],
                                     nextBuffer: ArrayBuffer[Any]): Unit = {
    val bound = latestBound.get()
    val difference = currentBuffer.size - bound
    if (bound > 0 && difference > 0) {
      nextBuffer ++=: currentBuffer.takeRight(difference)
      currentBuffer.reduceToSize(bound)
    }
    // we've had our fill for the amount of time it would take us to process the difference
    // use the fact this is synchronized with data ingestion to prevent more data coming in
    val delay = math.round( blockInterval * (difference.toFloat / bound) )
    Thread.sleep(delay)
  }

}

class DropCongestionStrategy extends CongestionStrategy {

  private val latestBound = new AtomicInteger(-1)

  override def onBlockBoundUpdate(bound:Int): Unit = latestBound.set(bound)

  override def restrictCurrentBuffer(currentBuffer: ArrayBuffer[Any],
                                     nextBuffer: ArrayBuffer[Any]): Unit = {
    val bound = latestBound.get()
    val difference = currentBuffer.size - bound
    if (bound > 0 && difference > 0) {
      println(s"Dropping $difference elements. Bound: $bound")
      currentBuffer.reduceToSize(bound)
    } else {
      println(s"No drop. Bound: $bound")
    }
  }

}

class SamplingCongestionStrategy extends CongestionStrategy {

  private val rng = new Random()
  private val latestBound = new AtomicInteger(-1)

  override def onBlockBoundUpdate(bound:Int): Unit = latestBound.set(bound)

  override def restrictCurrentBuffer(currentBuffer: ArrayBuffer[Any],
                                     nextBuffer: ArrayBuffer[Any]): Unit = {
    val bound = latestBound.get()
    val f = bound.toFloat / currentBuffer.size
    if (f < 1.0) {
      val gapSampler = new GapSamplingIterator(currentBuffer.toIterator, f, rng)
      currentBuffer.clear()
      currentBuffer ++= gapSampler
    }
  }

}
