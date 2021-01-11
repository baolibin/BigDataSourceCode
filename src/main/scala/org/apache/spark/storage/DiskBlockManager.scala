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

package org.apache.spark.storage

import java.io.{File, IOException}
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
  * DiskBlockManager的功能是创建和维护逻辑上的BlockId和物理磁盘文件位置上的映射关系（逻辑上BlockId-物理磁盘文件）。
  *
  * Creates and maintains the logical mapping between logical blocks and physical on-disk
  * locations. One block is mapped to one file with a name given by its BlockId.
  *
  * Block files are hashed among the directories listed in spark.local.dir (or in
  * SPARK_LOCAL_DIRS, if it's set).
  */
private[spark] class DiskBlockManager(conf: SparkConf, deleteFilesOnStop: Boolean) extends Logging {
	// 每个存储目录下面子目录最大数量
	private[spark] val subDirsPerLocalDir = conf.getInt("spark.diskStore.subDirectories", 64)

	/**
	  * 本地存储数据的数组
	  * Create one local directory for each path mentioned in spark.local.dir; then, inside this
	  * directory, create multiple subdirectories that we will hash files into, in order to avoid
	  * having really large inodes at the top level.
	  */
	private[spark] val localDirs: Array[File] = createLocalDirs(conf)
	if (localDirs.isEmpty) {
		logError("Failed to create any local dir.")
		System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
	}

	// 本地存储目录的二维数组,一维是localDirs,二维是subDirsPerLocalDir
	// The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
	// of subDirs(i) is protected by the lock of subDirs(i)
	private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

	// DiskBlockManager关闭钩子
	private val shutdownHook = addShutdownHook()

	/**
	  * 查找指定文件的本地存储路径。
	  * Looks up a file by hashing it into one of our local subdirectories.
	  */
	// This method should be kept in sync with
	// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getFile().
	def getFile(filename: String): File = {
		// 计算出文件名哈希码的绝对值
		// Figure out which local directory it hashes to, and which subdirectory in that
		val hash = Utils.nonNegativeHash(filename)
		// 获取一维数组下标
		val dirId = hash % localDirs.length
		// 获取二维数组下标
		val subDirId = (hash / localDirs.length) % subDirsPerLocalDir
		// 检查文件是否存储，若不存在则根据二维子目录下标来创建，并格式化为两位十六进制表示。
		// Create the subdirectory if it doesn't already exist
		val subDir = subDirs(dirId).synchronized {
			val old = subDirs(dirId)(subDirId)
			if (old != null) {
				old
			} else {
				val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
				if (!newDir.exists() && !newDir.mkdir()) {
					throw new IOException(s"Failed to create local dir in $newDir.")
				}
				subDirs(dirId)(subDirId) = newDir
				newDir
			}
		}

		new File(subDir, filename)
	}

	def getFile(blockId: BlockId): File = getFile(blockId.name)

	/**
	  * 检查磁盘是否存在指定的Block块数据。
	  * Check if disk block manager has a block.
	  */
	def containsBlock(blockId: BlockId): Boolean = {
		getFile(blockId.name).exists()
	}

	/**
	  * 返回本地磁盘当前存储的所有文件
	  * List all the files currently stored on disk by the disk manager.
	  */
	def getAllFiles(): Seq[File] = {
		// Get all the files inside the array of array of directories
		subDirs.flatMap { dir =>
			dir.synchronized {
				// Copy the content of dir because it may be modified in other threads
				dir.clone()
			}
		}.filter(_ != null).flatMap { dir =>
			val files = dir.listFiles()
			if (files != null) files else Seq.empty
		}
	}

	/**
	  * 返回本地当前存储的所有BlockId.
	  * List all the blocks currently stored on disk by the disk manager.
	  */
	def getAllBlocks(): Seq[BlockId] = {
		getAllFiles().map(f => BlockId(f.getName))
	}

	/**
	  * 创建计算过程中的中间结果的存储文件.
	  * Produces a unique block id and File suitable for storing local intermediate results.
	  */
	def createTempLocalBlock(): (TempLocalBlockId, File) = {
		var blockId = new TempLocalBlockId(UUID.randomUUID())
		while (getFile(blockId).exists()) {
			blockId = new TempLocalBlockId(UUID.randomUUID())
		}
		(blockId, getFile(blockId))
	}

	/**
	  * 创建Shuffle Write阶段输出的存储文件.
	  * Produces a unique block id and File suitable for storing shuffled intermediate results.
	  */
	def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
		var blockId = new TempShuffleBlockId(UUID.randomUUID())
		while (getFile(blockId).exists()) {
			blockId = new TempShuffleBlockId(UUID.randomUUID())
		}
		(blockId, getFile(blockId))
	}

	/**
	  * 创建本地存储文件目录（二维数组的第一维）
	  * Create local directories for storing block data. These directories are
	  * located inside configured local directories and won't
	  * be deleted on JVM exit when using the external shuffle service.
	  */
	private def createLocalDirs(conf: SparkConf): Array[File] = {
		Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
			try {
				val localDir = Utils.createDirectory(rootDir, "blockmgr")
				logInfo(s"Created local directory at $localDir")
				Some(localDir)
			} catch {
				case e: IOException =>
					logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)
					None
			}
		}
	}

	private def addShutdownHook(): AnyRef = {
		logDebug("Adding shutdown hook") // force eager creation of logger
		ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>
			logInfo("Shutdown hook called")
			DiskBlockManager.this.doStop()
		}
	}

	/** Cleanup local dirs and stop shuffle sender. */
	private[spark] def stop() {
		// Remove the shutdown hook.  It causes memory leaks if we leave it around.
		try {
			ShutdownHookManager.removeShutdownHook(shutdownHook)
		} catch {
			case e: Exception =>
				logError(s"Exception while removing shutdown hook.", e)
		}
		doStop()
	}

	private def doStop(): Unit = {
		if (deleteFilesOnStop) {
			localDirs.foreach { localDir =>
				if (localDir.isDirectory() && localDir.exists()) {
					try {
						if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(localDir)) {
							Utils.deleteRecursively(localDir)
						}
					} catch {
						case e: Exception =>
							logError(s"Exception while deleting local spark dir: $localDir", e)
					}
				}
			}
		}
	}
}
