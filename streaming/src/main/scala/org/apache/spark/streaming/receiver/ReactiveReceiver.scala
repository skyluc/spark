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

import org.reactivestreams._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.annotation.DeveloperApi
import scala.collection.mutable.ArrayBuffer
import scala.util.{Try, Success, Failure}
import java.lang.IllegalStateException

@DeveloperApi
abstract class ReactiveReceiver[T](storageLevel: StorageLevel)
  extends Receiver[T](storageLevel) with Subscriber[T] { outer =>
    private var subscription: Subscription = null
    private var done: Boolean = false

    private sealed trait SpecificationViolation {
      val message: String
      def printStackTrace(t: Option[Throwable] = None): Unit = t match {
        case Some(e) => new IllegalStateException(message, e).printStackTrace(System.err)
        case None => new IllegalStateException(message).printStackTrace(System.err)
      }
    }

    private case class CancelException(s: Subscription) extends SpecificationViolation {
      val message = s"""$s violated the Reactive Streams rule 3.15
                       | by throwing an exception from cancel.""".stripMargin.replaceAll("\n", "")
    }

    private case class RequestException(s: Subscription) extends SpecificationViolation {
      val message = s"""$s violated the Reactive Streams rule 3.16
                       |by throwing an exception from request.""".stripMargin.replaceAll("\n", "")
    }

    private case class BeforeSubscribe() extends SpecificationViolation {
      val message = """Publisher violated the Reactive Streams rule 1.09 by signalling onNext,
                       | onComplete or onError prior to onSubscribe.
                       |""".stripMargin.replaceAll("\n", "")
    }

    private case class ErrorException(c: outer.type) extends SpecificationViolation {
      val message = s"""$c violates the Reactive Streams rule 2.13 by throwing an
                       | exception from onError.""".stripMargin.replaceAll("\n", "")
    }

    private def finish(): Unit = {
      done = true
      if (subscription != null) Try(subscription.cancel()) recover {
        case t: Throwable => CancelException(subscription).printStackTrace(Some(t))
      }
    }

    final override def onSubscribe(s: Subscription): Unit = {
      for { sub <- Option(s) }
      {
        if (subscription != null) {
          Try(subscription.cancel()) recover {
            case t: Throwable => CancelException(subscription).printStackTrace(Some(t))
          }
        } else {
          subscription = s
        }
      }
    }

    final override def onNext(element: T): Unit = {
      if (!done){
        if (subscription == null) {
          BeforeSubscribe().printStackTrace()
        } else {
          Try(whenNext(element)) match {
            case Success(continue) => if (!continue) finish()
            case Failure(f) => Try(onError(f)) recover {
              case t: Throwable => ErrorException(this).printStackTrace(Some(t))
            }
          }
        }
      }
    }

    final override def onError(error: Throwable): Unit = {
      if (subscription == null) {
        BeforeSubscribe().printStackTrace()
      } else {
        done = true
        whenError(error)
      }
    }

    final override def onComplete(): Unit = {
      if (subscription == null) {
        BeforeSubscribe().printStackTrace()
      } else {
        done = true
        whenComplete()
      }
    }

    protected def whenNext(element: T): Boolean = {
      store(element)
      true
    }

    protected def whenComplete(): Unit = {
      onStop()
    }

    protected def whenError(error: Throwable): Unit = {
      onStop()
    }

    val reactiveCongestionStrategy = new CongestionStrategy {

      override def onBlockBoundUpdate(bound: Int) = if (subscription != null) {
        Try(subscription.request(bound)) recover {
          case t: Throwable => RequestException(subscription).printStackTrace(Some(t))
        }
      }

      override def restrictCurrentBuffer(currentBuffer: ArrayBuffer[Any],
                                         nextBuffer: ArrayBuffer[Any]): Unit = {}
    }


}
