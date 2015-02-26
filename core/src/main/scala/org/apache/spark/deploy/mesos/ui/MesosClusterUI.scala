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

package org.apache.spark.deploy.mesos.ui

import java.io.File
import org.apache.spark.ui.{SparkUI, WebUI}
import org.apache.spark.SparkConf
import org.apache.spark.SecurityManager
import akka.pattern.AskableActorRef
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.util.AkkaUtils
import org.apache.spark.deploy.worker.ui.{ActiveWebUiUrlAccessor, LogPage}

/**
 * UI that displays driver results from the [[org.apache.spark.deploy.mesos.MesosClusterDispatcher]]
 */
private [spark] class MesosClusterUI(
    val dispatcherActorRef: AskableActorRef,
    securityManager: SecurityManager,
    port: Int,
    conf: SparkConf,
    workDir: File,
    dispatcherPublicAddress: String)
  extends WebUI(securityManager, port, conf) with ActiveWebUiUrlAccessor {

  val timeout = AkkaUtils.askTimeout(conf)

  initialize()

  def activeWebUiUrl: String = "http://" + dispatcherPublicAddress + ":" + boundPort

  override def initialize() {
    attachPage(new DriverOutputPage(this))
    attachPage(new LogPage(this, workDir))
    attachHandler(createStaticHandler(MesosClusterUI.STATIC_RESOURCE_DIR, "/static"))
  }
}

private[spark] object MesosClusterUI {
  val STATIC_RESOURCE_DIR = SparkUI.STATIC_RESOURCE_DIR
}
