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

package org.apache.spark.ui

import java.util.{Timer, TimerTask}

import org.apache.spark._
import org.apache.spark.internal.Logging

/**
  * ConsoleProgressBar shows the progress of stages in the next line of the console. It poll the
  * status of active stages from `sc.statusTracker` periodically, the progress bar will be showed
  * up after the stage has ran at least 500ms. If multiple stages run in the same time, the status
  * of them will be combined together, showed in one line.
  */
private[spark] class ConsoleProgressBar(sc: SparkContext) extends Logging {
    // Carriage return
    private val CR = '\r'
    // Update period of progress bar, in milliseconds
    private val updatePeriodMSec =
        sc.getConf.getTimeAsMs("org.apache.spark.ui.consoleProgress.update.interval", "200")
    // Delay to show up a progress bar, in milliseconds
    private val firstDelayMSec = 500L

    // The width of terminal
    private val TerminalWidth = if (!sys.env.getOrElse("COLUMNS", "").isEmpty) {
        sys.env.get("COLUMNS").get.toInt
    } else {
        80
    }

    private var lastFinishTime = 0L
    private var lastUpdateTime = 0L
    private var lastProgressBar = ""

    // Schedule a refresh thread to run periodically
    private val timer = new Timer("refresh progress", true)
    timer.schedule(new TimerTask {
        override def run() {
            refresh()
        }
    }, firstDelayMSec, updatePeriodMSec)

    /**
      * 尝试在每个周期刷新进度条
      *
      * Try to refresh the progress bar in every cycle
      */
    private def refresh(): Unit = synchronized {
        val now = System.currentTimeMillis()
        if (now - lastFinishTime < firstDelayMSec) {
            return
        }
        val stageIds = sc.statusTracker.getActiveStageIds()
        val stages = stageIds.flatMap(sc.statusTracker.getStageInfo).filter(_.numTasks() > 1)
            .filter(now - _.submissionTime() > firstDelayMSec).sortBy(_.stageId())
        if (stages.length > 0) {
            show(now, stages.take(3)) // display at most 3 stages in same time
        }
    }

    /**
      * 在控制台中显示进度条。进度条将显示在上次输出后的下一行中，并不断覆盖自身以保持在一行中。
      * 日志记录将跟随进度条，然后进度条将显示在下一行中，而不覆盖日志。
      *
      * Show progress bar in console. The progress bar is displayed in the next line
      * after your last output, keeps overwriting itself to hold in one line. The logging will follow
      * the progress bar, then progress bar will be showed in next line without overwrite logs.
      */
    private def show(now: Long, stages: Seq[SparkStageInfo]) {
        val width = TerminalWidth / stages.size
        val bar = stages.map { s =>
            val total = s.numTasks()
            val header = s"[Stage ${s.stageId()}:"
            val tailer = s"(${s.numCompletedTasks()} + ${s.numActiveTasks()}) / $total]"
            val w = width - header.length - tailer.length
            val bar = if (w > 0) {
                val percent = w * s.numCompletedTasks() / total
                (0 until w).map { i =>
                    if (i < percent) "=" else if (i == percent) ">" else " "
                }.mkString("")
            } else {
                ""
            }
            header + bar + tailer
        }.mkString("")

        // only refresh if it's changed OR after 1 minute (or the ssh connection will be closed
        // after idle some time)
        if (bar != lastProgressBar || now - lastUpdateTime > 60 * 1000L) {
            System.err.print(CR + bar)
            lastUpdateTime = now
        }
        lastProgressBar = bar
    }

    /**
      * 如果显示，请清除进度条。
      *
      * Clear the progress bar if showed.
      */
    private def clear() {
        if (!lastProgressBar.isEmpty) {
            System.err.printf(CR + " " * TerminalWidth + CR)
            lastProgressBar = ""
        }
    }

    /**
      * 将所有阶段标记为已完成，如果显示则清除进度条，则进度将不会与作业的输出交织在一起。
      *
      * Mark all the stages as finished, clear the progress bar if showed, then the progress will not
      * interweave with output of jobs.
      */
    def finishAll(): Unit = synchronized {
        clear()
        lastFinishTime = System.currentTimeMillis()
    }

    /**
      * 拆下计时器线程。计时器线程是GC根，如果未终止，它将保留整个SparkContext。
      *
      * Tear down the timer thread.  The timer thread is a GC root, and it retains the entire
      * SparkContext if it's not terminated.
      */
    def stop(): Unit = timer.cancel()
}
