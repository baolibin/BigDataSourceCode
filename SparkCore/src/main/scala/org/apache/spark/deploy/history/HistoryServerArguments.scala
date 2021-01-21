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

package org.apache.spark.deploy.history

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

import scala.annotation.tailrec

/**
  * HistoryServer命令行解析。
  *
  * Command-line parser for the [[HistoryServer]].
  */
private[history] class HistoryServerArguments(conf: SparkConf, args: Array[String])
        extends Logging {
    private var propertiesFile: String = null

    parse(args.toList)

    @tailrec
    private def parse(args: List[String]): Unit = {
        if (args.length == 1) {
            setLogDirectory(args.head)
        } else {
            args match {
                case ("--dir" | "-d") :: value :: tail =>
                    setLogDirectory(value)
                    parse(tail)

                case ("--help" | "-h") :: tail =>
                    printUsageAndExit(0)

                case ("--properties-file") :: value :: tail =>
                    propertiesFile = value
                    parse(tail)

                case Nil =>

                case _ =>
                    printUsageAndExit(1)
            }
        }
    }

    private def setLogDirectory(value: String): Unit = {
        logWarning("Setting log directory through the command line is deprecated as of " +
                "Spark 1.1.0. Please set this through org.apache.spark.history.fs.logDirectory instead.")
        conf.set("org.apache.spark.history.fs.logDirectory", value)
    }

    // This mutates the SparkConf, so all accesses to it must be made after this line
    Utils.loadDefaultSparkProperties(conf, propertiesFile)

    private def printUsageAndExit(exitCode: Int) {
        // scalastyle:off println
        System.err.println(
            """
              |Usage: HistoryServer [options]
              |
              |Options:
              |  DIR                         Deprecated; set org.apache.spark.history.fs.logDirectory directly
              |  --dir DIR (-d DIR)          Deprecated; set org.apache.spark.history.fs.logDirectory directly
              |  --properties-file FILE      Path to a custom Spark properties file.
              |                              Default is conf/org.apache.spark-defaults.conf.
              |
              |Configuration options can be set by setting the corresponding JVM system property.
              |History Server options are always available; additional options depend on the provider.
              |
              |History Server options:
              |
              |  org.apache.spark.history.ui.port              Port where server will listen for connections
              |                                     (default 18080)
              |  org.apache.spark.history.acls.enable          Whether to enable view acls for all applications
              |                                     (default false)
              |  org.apache.spark.history.provider             Name of history provider class (defaults to
              |                                     file system-based provider)
              |  org.apache.spark.history.retainedApplications Max number of application UIs to keep loaded in memory
              |                                     (default 50)
              |FsHistoryProvider options:
              |
              |  org.apache.spark.history.fs.logDirectory      Directory where app logs are stored
              |                                     (default: file:/tmp/org.apache.spark-events)
              |  org.apache.spark.history.fs.updateInterval    How often to reload log data from storage
              |                                     (in seconds, default: 10)
              |""".stripMargin)
        // scalastyle:on println
        System.exit(exitCode)
    }

}

