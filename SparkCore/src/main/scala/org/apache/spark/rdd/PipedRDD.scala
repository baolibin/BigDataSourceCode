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

package org.apache.spark.rdd

import java.io._
import java.util.StringTokenizer
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.util.Utils
import org.apache.spark.{Partition, SparkEnv, TaskContext}

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.ClassTag


/**
  * 一种RDD，它通过外部命令（每行打印一个）将每个父分区的内容通过管道传输，并将输出作为字符串集合返回。
  *
  * An RDD that pipes the contents of each parent partition through an external command
  * (printing them one per line) and returns the output as a collection of strings.
  */
private[spark] class PipedRDD[T: ClassTag](
                                              prev: RDD[T],
                                              command: Seq[String],
                                              envVars: Map[String, String],
                                              printPipeContext: (String => Unit) => Unit,
                                              printRDDElement: (T, String => Unit) => Unit,
                                              separateWorkingDir: Boolean,
                                              bufferSize: Int,
                                              encoding: String)
    extends RDD[String](prev) {

    override def getPartitions: Array[Partition] = firstParent[T].partitions

    override def compute(split: Partition, context: TaskContext): Iterator[String] = {
        val pb = new ProcessBuilder(command.asJava)
        // Add the environmental variables to the process.
        val currentEnvVars = pb.environment()
        envVars.foreach { case (variable, value) => currentEnvVars.put(variable, value) }

        // for compatibility with Hadoop which sets these env variables
        // so the user code can access the input filename
        if (split.isInstanceOf[HadoopPartition]) {
            val hadoopSplit = split.asInstanceOf[HadoopPartition]
            currentEnvVars.putAll(hadoopSplit.getPipeEnvVars().asJava)
        }

        // When org.apache.spark.worker.separated.working.directory option is turned on, each
        // task will be run in separate directory. This should be resolve file
        // access conflict issue
        val taskDirectory = "tasks" + File.separator + java.util.UUID.randomUUID.toString
        var workInTaskDirectory = false
        logDebug("taskDirectory = " + taskDirectory)
        if (separateWorkingDir) {
            val currentDir = new File(".")
            logDebug("currentDir = " + currentDir.getAbsolutePath())
            val taskDirFile = new File(taskDirectory)
            taskDirFile.mkdirs()

            try {
                val tasksDirFilter = new NotEqualsFileNameFilter("tasks")

                // Need to add symlinks to jars, files, and directories.  On Yarn we could have
                // directories and other files not known to the SparkContext that were added via the
                // Hadoop distributed cache.  We also don't want to symlink to the /tasks directories we
                // are creating here.
                for (file <- currentDir.list(tasksDirFilter)) {
                    val fileWithDir = new File(currentDir, file)
                    Utils.symlink(new File(fileWithDir.getAbsolutePath()),
                        new File(taskDirectory + File.separator + fileWithDir.getName()))
                }
                pb.directory(taskDirFile)
                workInTaskDirectory = true
            } catch {
                case e: Exception => logError("Unable to setup task working directory: " + e.getMessage +
                    " (" + taskDirectory + ")", e)
            }
        }

        val proc = pb.start()
        val env = SparkEnv.get
        val childThreadException = new AtomicReference[Throwable](null)

        // Start a thread to print the process's stderr to ours
        new Thread(s"stderr reader for $command") {
            override def run(): Unit = {
                val err = proc.getErrorStream
                try {
                    for (line <- Source.fromInputStream(err)(encoding).getLines) {
                        // scalastyle:off println
                        System.err.println(line)
                        // scalastyle:on println
                    }
                } catch {
                    case t: Throwable => childThreadException.set(t)
                } finally {
                    err.close()
                }
            }
        }.start()

        // Start a thread to feed the process input from our parent's iterator
        new Thread(s"stdin writer for $command") {
            override def run(): Unit = {
                TaskContext.setTaskContext(context)
                val out = new PrintWriter(new BufferedWriter(
                    new OutputStreamWriter(proc.getOutputStream, encoding), bufferSize))
                try {
                    // scalastyle:off println
                    // input the pipe context firstly
                    if (printPipeContext != null) {
                        printPipeContext(out.println)
                    }
                    for (elem <- firstParent[T].iterator(split, context)) {
                        if (printRDDElement != null) {
                            printRDDElement(elem, out.println)
                        } else {
                            out.println(elem)
                        }
                    }
                    // scalastyle:on println
                } catch {
                    case t: Throwable => childThreadException.set(t)
                } finally {
                    out.close()
                }
            }
        }.start()

        // Return an iterator that read lines from the process's stdout
        val lines = Source.fromInputStream(proc.getInputStream)(encoding).getLines
        new Iterator[String] {
            def next(): String = {
                if (!hasNext()) {
                    throw new NoSuchElementException()
                }
                lines.next()
            }

            def hasNext(): Boolean = {
                val result = if (lines.hasNext) {
                    true
                } else {
                    val exitStatus = proc.waitFor()
                    cleanup()
                    if (exitStatus != 0) {
                        throw new IllegalStateException(s"Subprocess exited with status $exitStatus. " +
                            s"Command ran: " + command.mkString(" "))
                    }
                    false
                }
                propagateChildException()
                result
            }

            private def cleanup(): Unit = {
                // cleanup task working directory if used
                if (workInTaskDirectory) {
                    scala.util.control.Exception.ignoring(classOf[IOException]) {
                        Utils.deleteRecursively(new File(taskDirectory))
                    }
                    logDebug(s"Removed task working directory $taskDirectory")
                }
            }

            private def propagateChildException(): Unit = {
                val t = childThreadException.get()
                if (t != null) {
                    val commandRan = command.mkString(" ")
                    logError(s"Caught exception while running pipe() operator. Command ran: $commandRan. " +
                        s"Exception: ${t.getMessage}")
                    proc.destroy()
                    cleanup()
                    throw t
                }
            }
        }
    }

    /**
      * 一个FilenameFilter，它接受与传入的名称不相等的任何内容。
      *
      * A FilenameFilter that accepts anything that isn't equal to the name passed in.
      *
      * @param filterName of file or directory to leave out
      */
    class NotEqualsFileNameFilter(filterName: String) extends FilenameFilter {
        def accept(dir: File, name: String): Boolean = {
            !name.equals(filterName)
        }
    }
}

private object PipedRDD {
    // Split a string into words using a standard StringTokenizer
    def tokenize(command: String): Seq[String] = {
        val buf = new ArrayBuffer[String]
        val tok = new StringTokenizer(command)
        while (tok.hasMoreElements) {
            buf += tok.nextToken()
        }
        buf
    }
}
