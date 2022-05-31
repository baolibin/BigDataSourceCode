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

package org.apache.spark.util

import java.util.Collections

import org.apache.commons.lang3.SystemUtils
import org.apache.spark.internal.Logging
import org.slf4j.Logger
import sun.misc.{Signal, SignalHandler}

import scala.collection.JavaConverters._

/**
  * 包含用于处理posix信号的实用程序。
  *
  * Contains utilities for working with posix signals.
  */
private[spark] object SignalUtils extends Logging {

    /** Mapping from signal to their respective handlers. */
    private val handlers = new scala.collection.mutable.HashMap[String, ActionHandler]
    /** A flag to make sure we only register the logger once. */
    private var loggerRegistered = false

    /** Register a signal handler to log signals on UNIX-like systems. */
    def registerLogger(log: Logger): Unit = synchronized {
        if (!loggerRegistered) {
            Seq("TERM", "HUP", "INT").foreach { sig =>
                SignalUtils.register(sig) {
                    log.error("RECEIVED SIGNAL " + sig)
                    false
                }
            }
            loggerRegistered = true
        }
    }

    /**
      * 添加此进程接收到给定信号时要运行的操作。
      *
      * Adds an action to be run when a given signal is received by this process.
      *
      * Note that signals are only supported on unix-like operating systems and work on a best-effort
      * basis: if a signal is not available or cannot be intercepted, only a warning is emitted.
      *
      * All actions for a given signal are run in a separate thread.
      */
    def register(signal: String)(action: => Boolean): Unit = synchronized {
        if (SystemUtils.IS_OS_UNIX) {
            try {
                val handler = handlers.getOrElseUpdate(signal, {
                    logInfo("Registered signal handler for " + signal)
                    new ActionHandler(new Signal(signal))
                })
                handler.register(action)
            } catch {
                case ex: Exception => logWarning(s"Failed to register signal handler for " + signal, ex)
            }
        }
    }

    /**
      * 运行操作集合的给定信号的处理程序。
      *
      * A handler for the given signal that runs a collection of actions.
      */
    private class ActionHandler(signal: Signal) extends SignalHandler {

        /**
          * List of actions upon the signal; the callbacks should return true if the signal is "handled",
          * i.e. should not escalate to the next callback.
          */
        private val actions = Collections.synchronizedList(new java.util.LinkedList[() => Boolean])

        // original signal handler, before this handler was attached
        private val prevHandler: SignalHandler = Signal.handle(signal, this)

        /**
          * Called when this handler's signal is received. Note that if the same signal is received
          * before this method returns, it is escalated to the previous handler.
          */
        override def handle(sig: Signal): Unit = {
            // register old handler, will receive incoming signals while this handler is running
            Signal.handle(signal, prevHandler)

            // Run all actions, escalate to parent handler if no action catches the signal
            // (i.e. all actions return false). Note that calling `map` is to ensure that
            // all actions are run, `forall` is short-circuited and will stop evaluating
            // after reaching a first false predicate.
            val escalate = actions.asScala.map(action => action()).forall(_ == false)
            if (escalate) {
                prevHandler.handle(sig)
            }

            // re-register this handler
            Signal.handle(signal, this)
        }

        /**
          * Adds an action to be run by this handler.
          *
          * @param action An action to be run when a signal is received. Return true if the signal
          *               should be stopped with this handler, false if it should be escalated.
          */
        def register(action: => Boolean): Unit = actions.add(() => action)
    }
}
