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

package org.apache.spark.streaming.scheduler

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.streaming.scheduler.rate.RateEstimator

/**
 * :: DeveloperApi ::
 * A StreamingListener that receives batch completion updates, and maintains
 * an estimate of the speed at which this stream should ingest messages,
 * given an estimate computation from a `RateEstimator`
 */
@DeveloperApi
class RateController(val streamUID: Int, rateEstimator: RateEstimator) extends StreamingListener {

  private val speedLimit : AtomicLong = new AtomicLong(-1L)

  def getLatestRate(): Long = speedLimit.get()

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted){
      val elements = batchCompleted.batchInfo.streamIdToNumRecords

    for {
      processingEnd <- batchCompleted.batchInfo.processingEndTime
      workDelay <- batchCompleted.batchInfo.processingDelay
      waitDelay <- batchCompleted.batchInfo.schedulingDelay
      elems <- elements.get(streamUID)
      newSpeed <- rateEstimator.compute(processingEnd, elems, workDelay, waitDelay)
    } speedLimit.set(newSpeed.toLong)
  }

}
