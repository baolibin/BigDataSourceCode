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

package org.apache.spark.streaming

/**
  * 这是一个简单的类，表示时间的绝对瞬间。
  * 在内部，它将时间表示为当前时间与1970年1月1日午夜UTC之间的差值（以毫秒为单位）。
  * 这与System.currentTimeMillis返回的格式相同.
  *
  * This is a simple class that represents an absolute instant of time.
  * Internally, it represents time as the difference, measured in milliseconds, between the current
  * time and midnight, January 1, 1970 UTC. This is the same format as what is returned by
  * System.currentTimeMillis.
  */
case class Time(private val millis: Long) {

    def less(that: Time): Boolean = this < that

    def <(that: Time): Boolean = (this.millis < that.millis)

    def lessEq(that: Time): Boolean = this <= that

    def <=(that: Time): Boolean = (this.millis <= that.millis)

    def greater(that: Time): Boolean = this > that

    def >(that: Time): Boolean = (this.millis > that.millis)

    def greaterEq(that: Time): Boolean = this >= that

    def >=(that: Time): Boolean = (this.millis >= that.millis)

    // Java-friendlier versions of the above.

    def plus(that: Duration): Time = this + that

    def +(that: Duration): Time = new Time(millis + that.milliseconds)

    def minus(that: Time): Duration = this - that

    def -(that: Time): Duration = new Duration(millis - that.millis)

    def minus(that: Duration): Time = this - that

    def -(that: Duration): Time = new Time(millis - that.milliseconds)

    def floor(that: Duration): Time = {
        val t = that.milliseconds
        new Time((this.millis / t) * t)
    }

    def floor(that: Duration, zeroTime: Time): Time = {
        val t = that.milliseconds
        new Time(((this.millis - zeroTime.milliseconds) / t) * t + zeroTime.milliseconds)
    }

    def milliseconds: Long = millis

    def isMultipleOf(that: Duration): Boolean =
        (this.millis % that.milliseconds == 0)

    def min(that: Time): Time = if (this < that) this else that

    def max(that: Time): Time = if (this > that) this else that

    def until(that: Time, interval: Duration): Seq[Time] = {
        (this.milliseconds) until (that.milliseconds) by (interval.milliseconds) map (new Time(_))
    }

    def to(that: Time, interval: Duration): Seq[Time] = {
        (this.milliseconds) to (that.milliseconds) by (interval.milliseconds) map (new Time(_))
    }


    override def toString: String = (millis.toString + " ms")

}

object Time {
    implicit val ordering = Ordering.by((time: Time) => time.millis)
}
