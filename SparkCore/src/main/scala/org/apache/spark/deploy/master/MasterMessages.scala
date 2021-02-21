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

package org.apache.spark.deploy.master

sealed trait MasterMessages extends Serializable

/**
  * 包含仅由主机及其关联实体看到的消息。
  *
  * Contains messages seen only by the Master and its associated entities.
  */
private[master] object MasterMessages {

    // LeaderElectionAgent to Master

    case class BeginRecovery(storedApps: Seq[ApplicationInfo], storedWorkers: Seq[WorkerInfo])

    case class BoundPortsResponse(rpcEndpointPort: Int, webUIPort: Int, restPort: Option[Int])

    // Master to itself

    case object ElectedLeader

    case object RevokedLeadership

    case object CheckForWorkerTimeOut

    case object CompleteRecovery

    case object BoundPortsRequest

}
