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

import java.util.Map.Entry
import java.util.Set
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable

private[spark] case class TimeStampedValue[V](value: V, timestamp: Long)

/**
  * 这是的自定义实现scala.collection.mutable.Map，它将插入时间戳与每个键值对一起存储。
  * 如果指定，则每次访问时都可以更新每对的时间戳。
  * 然后可以使用clearOldValues方法移除其时间戳早于特定阈值时间的键值对。
  * 这是为了取代scala.collection.mutable.HashMap。
  *
  * This is a custom implementation of scala.collection.mutable.Map which stores the insertion
  * timestamp along with each key-value pair. If specified, the timestamp of each pair can be
  * updated every time it is accessed. Key-value pairs whose timestamp are older than a particular
  * threshold time can then be removed using the clearOldValues method. This is intended to
  * be a drop-in replacement of scala.collection.mutable.HashMap.
  *
  * @param updateTimeStampOnGet Whether timestamp of a pair will be updated when it is accessed
  */
private[spark] class TimeStampedHashMap[A, B](updateTimeStampOnGet: Boolean = false)
    extends mutable.Map[A, B]() with Logging {

    private val internalMap = new ConcurrentHashMap[A, TimeStampedValue[B]]()

    override def +[B1 >: B](kv: (A, B1)): mutable.Map[A, B1] = {
        val newMap = new TimeStampedHashMap[A, B1]
        val oldInternalMap = this.internalMap.asInstanceOf[ConcurrentHashMap[A, TimeStampedValue[B1]]]
        newMap.internalMap.putAll(oldInternalMap)
        kv match {
            case (a, b) => newMap.internalMap.put(a, TimeStampedValue(b, currentTime))
        }
        newMap
    }

    private def currentTime: Long = System.currentTimeMillis

    override def -(key: A): mutable.Map[A, B] = {
        val newMap = new TimeStampedHashMap[A, B]
        newMap.internalMap.putAll(this.internalMap)
        newMap.internalMap.remove(key)
        newMap
    }

    override def -=(key: A): this.type = {
        internalMap.remove(key)
        this
    }

    override def apply(key: A): B = {
        get(key).getOrElse {
            throw new NoSuchElementException()
        }
    }

    def get(key: A): Option[B] = {
        val value = internalMap.get(key)
        if (value != null && updateTimeStampOnGet) {
            internalMap.replace(key, value, TimeStampedValue(value.value, currentTime))
        }
        Option(value).map(_.value)
    }

    override def filter(p: ((A, B)) => Boolean): mutable.Map[A, B] = {
        internalMap.asScala.map { case (k, TimeStampedValue(v, t)) => (k, v) }.filter(p)
    }

    override def empty: mutable.Map[A, B] = new TimeStampedHashMap[A, B]()

    override def size: Int = internalMap.size

    override def foreach[U](f: ((A, B)) => U) {
        val it = getEntrySet.iterator
        while (it.hasNext) {
            val entry = it.next()
            val kv = (entry.getKey, entry.getValue.value)
            f(kv)
        }
    }

    def getEntrySet: Set[Entry[A, TimeStampedValue[B]]] = internalMap.entrySet

    def putIfAbsent(key: A, value: B): Option[B] = {
        val prev = internalMap.putIfAbsent(key, TimeStampedValue(value, currentTime))
        Option(prev).map(_.value)
    }

    def putAll(map: Map[A, B]) {
        map.foreach { case (k, v) => update(k, v) }
    }

    override def update(key: A, value: B) {
        this += ((key, value))
    }

    override def +=(kv: (A, B)): this.type = {
        kv match {
            case (a, b) => internalMap.put(a, TimeStampedValue(b, currentTime))
        }
        this
    }

    def toMap: Map[A, B] = iterator.toMap

    def iterator: Iterator[(A, B)] = {
        getEntrySet.iterator.asScala.map(kv => (kv.getKey, kv.getValue.value))
    }

    /**
      * 删除时间戳早于“threshTime”的旧键值对`
      *
      * Removes old key-value pairs that have timestamp earlier than `threshTime`.
      */
    def clearOldValues(threshTime: Long) {
        clearOldValues(threshTime, (_, _) => ())
    }

    def clearOldValues(threshTime: Long, f: (A, B) => Unit) {
        val it = getEntrySet.iterator
        while (it.hasNext) {
            val entry = it.next()
            if (entry.getValue.timestamp < threshTime) {
                f(entry.getKey, entry.getValue.value)
                logDebug("Removing key " + entry.getKey)
                it.remove()
            }
        }
    }

    // For testing

    def getTimestamp(key: A): Option[Long] = {
        getTimeStampedValue(key).map(_.timestamp)
    }

    def getTimeStampedValue(key: A): Option[TimeStampedValue[B]] = {
        Option(internalMap.get(key))
    }
}
