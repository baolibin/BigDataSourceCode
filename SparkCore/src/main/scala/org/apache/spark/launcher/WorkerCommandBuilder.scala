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

package org.apache.spark.launcher

import java.io.File
import java.util.{HashMap => JHashMap, List => JList, Map => JMap}

import org.apache.spark.deploy.Command

import scala.collection.JavaConverters._

/**
  * 此类由CommandUtils使用。它在SparkLauncher中使用了一些包私有api，
  * 而且由于Java没有类似于`private的特性[org.apache.spark]`，并且我们不希望该类是公共的，
  * 需要与库的其余部分生活在同一个包中。
  *
  * This class is used by CommandUtils. It uses some package-private APIs in SparkLauncher, and since
  * Java doesn't have a feature similar to `private[org.apache.spark]`, and we don't want that class to be
  * public, needs to live in the same package as the rest of the library.
  */
private[spark] class WorkerCommandBuilder(sparkHome: String, memoryMb: Int, command: Command)
        extends AbstractCommandBuilder {

    childEnv.putAll(command.environment.asJava)
    childEnv.put(CommandBuilderUtils.ENV_SPARK_HOME, sparkHome)

    def buildCommand(): JList[String] = buildCommand(new JHashMap[String, String]())

    override def buildCommand(env: JMap[String, String]): JList[String] = {
        val cmd = buildJavaCommand(command.classPathEntries.mkString(File.pathSeparator))
        cmd.add(s"-Xmx${memoryMb}M")
        command.javaOpts.foreach(cmd.add)
        cmd
    }

}
