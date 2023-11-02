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

package org.apache.spark.scheduler

import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import org.apache.spark.internal.config._
import org.apache.spark.util.Utils
import org.apache.spark.{SparkContext, SparkException}

import scala.util.DynamicVariable

/**
  * 异步地将SparkListenerEvents传递给已注册的SparkListener。
  *
  * Asynchronously passes SparkListenerEvents to registered SparkListeners.
  *
  * Until `start()` is called, all posted events are only buffered. Only after this listener bus
  * has started will events be actually propagated to all attached listeners. This listener bus
  * is stopped when `stop()` is called, and it will drop further events after stopping.
  */
private[spark] class LiveListenerBus(val sparkContext: SparkContext) extends SparkListenerBus {

    self =>

    import LiveListenerBus._

    // Cap the capacity of the event queue so we get an explicit error (rather than
    // an OOM exception) if it's perpetually being added to more quickly than it's being drained.
    private lazy val EVENT_QUEUE_CAPACITY = validateAndGetQueueSize()
    private lazy val eventQueue = new LinkedBlockingQueue[SparkListenerEvent](EVENT_QUEUE_CAPACITY)
    // Indicate if `start()` is called
    private val started = new AtomicBoolean(false)
    // Indicate if `stop()` is called
    private val stopped = new AtomicBoolean(false)
    /** A counter for dropped events. It will be reset every time we log it. */
    private val droppedEventsCounter = new AtomicLong(0L)
    private val logDroppedEvent = new AtomicBoolean(false)
    // A counter that represents the number of events produced and consumed in the queue
    private val eventLock = new Semaphore(0)
    private val listenerThread = new Thread(name) {
        setDaemon(true)

        override def run(): Unit = Utils.tryOrStopSparkContext(sparkContext) {
            LiveListenerBus.withinListenerThread.withValue(true) {
                while (true) {
                    eventLock.acquire()
                    self.synchronized {
                        processingEvent = true
                    }
                    try {
                        val event = eventQueue.poll
                        if (event == null) {
                            // Get out of the while loop and shutdown the daemon thread
                            if (!stopped.get) {
                                throw new IllegalStateException("Polling `null` from eventQueue means" +
                                    " the listener bus has been stopped. So `stopped` must be true")
                            }
                            return
                        }
                        postToAll(event)
                    } finally {
                        self.synchronized {
                            processingEvent = false
                        }
                    }
                }
            }
        }
    }
    /** When `droppedEventsCounter` was logged last time in milliseconds. */
    @volatile private var lastReportTimestamp = 0L
    // Indicate if we are processing some event
    // Guarded by `self`
    private var processingEvent = false

    /**
      * 开始向连接的侦听器发送事件。
      *
      * Start sending events to attached listeners.
      *
      * This first sends out all buffered events posted before this listener bus has started, then
      * listens for any additional events asynchronously while the listener bus is still running.
      * This should only be called once.
      *
      */
    def start(): Unit = {
        if (started.compareAndSet(false, true)) {
            listenerThread.start()
        } else {
            throw new IllegalStateException(s"$name already started!")
        }
    }

    def post(event: SparkListenerEvent): Unit = {
        if (stopped.get) {
            // Drop further events to make `listenerThread` exit ASAP
            logError(s"$name has already stopped! Dropping event $event")
            return
        }
        val eventAdded = eventQueue.offer(event)
        if (eventAdded) {
            eventLock.release()
        } else {
            onDropEvent(event)
            droppedEventsCounter.incrementAndGet()
        }

        val droppedEvents = droppedEventsCounter.get
        if (droppedEvents > 0) {
            // Don't log too frequently
            if (System.currentTimeMillis() - lastReportTimestamp >= 60 * 1000) {
                // There may be multiple threads trying to decrease droppedEventsCounter.
                // Use "compareAndSet" to make sure only one thread can win.
                // And if another thread is increasing droppedEventsCounter, "compareAndSet" will fail and
                // then that thread will update it.
                if (droppedEventsCounter.compareAndSet(droppedEvents, 0)) {
                    val prevLastReportTimestamp = lastReportTimestamp
                    lastReportTimestamp = System.currentTimeMillis()
                    logWarning(s"Dropped $droppedEvents SparkListenerEvents since " +
                        new java.util.Date(prevLastReportTimestamp))
                }
            }
        }
    }

    /**
      * 如果事件队列超过其容量，则会丢弃新事件。子类将收到已删除事件的通知。
      *
      * If the event queue exceeds its capacity, the new events will be dropped. The subclasses will be
      * notified with the dropped events.
      *
      * Note: `onDropEvent` can be called in any thread.
      */
    def onDropEvent(event: SparkListenerEvent): Unit = {
        if (logDroppedEvent.compareAndSet(false, true)) {
            // Only log the following message once to avoid duplicated annoying logs.
            logError("Dropping SparkListenerEvent because no remaining room in event queue. " +
                "This likely means one of the SparkListeners is too slow and cannot keep up with " +
                "the rate at which tasks are being started by the scheduler.")
        }
    }

    /**
      * 仅用于测试。等待队列中不再有事件，或者等待指定的时间过去。如果在队列清空之前经过了指定的时间，则引发“TimeoutException”。
      *
      * For testing only. Wait until there are no more events in the queue, or until the specified
      * time has elapsed. Throw `TimeoutException` if the specified time elapsed before the queue
      * emptied.
      * Exposed for testing.
      */
    @throws(classOf[TimeoutException])
    def waitUntilEmpty(timeoutMillis: Long): Unit = {
        val finishTime = System.currentTimeMillis + timeoutMillis
        while (!queueIsEmpty) {
            if (System.currentTimeMillis > finishTime) {
                throw new TimeoutException(
                    s"The event queue is not empty after $timeoutMillis milliseconds")
            }
            /* Sleep rather than using wait/notify, because this is used only for testing and
             * wait/notify add overhead in the general case. */
            Thread.sleep(10)
        }
    }

    /**
      * 返回事件队列是否为空。
      *
      * Return whether the event queue is empty.
      *
      * The use of synchronized here guarantees that all events that once belonged to this queue
      * have already been processed by all attached listeners, if this returns true.
      */
    private def queueIsEmpty: Boolean = synchronized {
        eventQueue.isEmpty && !processingEvent
    }

    /**
      * 仅用于测试。返回侦听器守护进程线程是否仍处于活动状态。暴露测试。
      *
      * For testing only. Return whether the listener daemon thread is still alive.
      * Exposed for testing.
      */
    def listenerThreadIsAlive: Boolean = listenerThread.isAlive

    /**
      * 停止侦听器总线。它将等待队列中的事件得到处理，但在停止后会丢弃新事件。
      *
      * Stop the listener bus. It will wait until the queued events have been processed, but drop the
      * new events after stopping.
      */
    def stop(): Unit = {
        if (!started.get()) {
            throw new IllegalStateException(s"Attempted to stop $name that has not yet started!")
        }
        if (stopped.compareAndSet(false, true)) {
            // Call eventLock.release() so that listenerThread will poll `null` from `eventQueue` and know
            // `stop` is called.
            eventLock.release()
            listenerThread.join()
        } else {
            // Keep quiet
        }
    }

    private def validateAndGetQueueSize(): Int = {
        val queueSize = sparkContext.conf.get(LISTENER_BUS_EVENT_QUEUE_SIZE)
        if (queueSize <= 0) {
            throw new SparkException("org.apache.spark.scheduler.listenerbus.eventqueue.size must be > 0!")
        }
        queueSize
    }
}

private[spark] object LiveListenerBus {
    // Allows for Context to check whether stop() call is made within listener thread
    val withinListenerThread: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)

    /**
      * Spark侦听器总线的线程名称
      *
      * The thread name of Spark listener bus
      */
    val name = "SparkListenerBus"
}

