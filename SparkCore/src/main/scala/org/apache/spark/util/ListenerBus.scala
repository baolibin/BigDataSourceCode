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

import java.util.concurrent.CopyOnWriteArrayList

import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
  * 向侦听器发布事件的事件总线。
  *
  * An event bus which posts events to its listeners.
  */
private[spark] trait ListenerBus[L <: AnyRef, E] extends Logging {

    // Marked `private[org.apache.spark]` for access in tests.
    private[spark] val listeners = new CopyOnWriteArrayList[L]

    /**
      * 添加侦听器以侦听事件。此方法是线程安全的，可以在任何线程中调用。
      *
      * Add a listener to listen events. This method is thread-safe and can be called in any thread.
      */
    final def addListener(listener: L): Unit = {
        listeners.add(listener)
    }

    /**
      * 删除侦听器，它将不会接收任何事件。此方法是线程安全的，可以在任何线程中调用。
      *
      * Remove a listener and it won't receive any events. This method is thread-safe and can be called
      * in any thread.
      */
    final def removeListener(listener: L): Unit = {
        listeners.remove(listener)
    }

    /**
      * 将事件发布到所有注册的侦听器。“postToAll”调用方应保证在同一线程中为所有事件调用“postToAll”。
      *
      * Post the event to all registered listeners. The `postToAll` caller should guarantee calling
      * `postToAll` in the same thread for all events.
      */
    def postToAll(event: E): Unit = {
        // JavaConverters can create a JIterableWrapper if we use asScala.
        // However, this method will be called frequently. To avoid the wrapper cost, here we use
        // Java Iterator directly.
        val iter = listeners.iterator
        while (iter.hasNext) {
            val listener = iter.next()
            try {
                doPostEvent(listener, event)
            } catch {
                case NonFatal(e) =>
                    logError(s"Listener ${Utils.getFormattedClassName(listener)} threw an exception", e)
            }
        }
    }

    /**
      * 将事件发布到指定的侦听器`onPostEvent `保证在同一线程中为所有侦听器调用。
      *
      * Post an event to the specified listener. `onPostEvent` is guaranteed to be called in the same
      * thread for all listeners.
      */
    protected def doPostEvent(listener: L, event: E): Unit

    private[spark] def findListenersByClass[T <: L : ClassTag](): Seq[T] = {
        val c = implicitly[ClassTag[T]].runtimeClass
        listeners.asScala.filter(_.getClass == c).map(_.asInstanceOf[T]).toSeq
    }

}
