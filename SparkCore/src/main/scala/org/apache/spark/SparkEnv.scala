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

package org.apache.spark

import java.io.File
import java.net.Socket
import java.util.Locale
import com.google.common.collect.MapMaker
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.PythonWorkerFactory
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{BLOCK_MANAGER_PORT, DRIVER_BIND_ADDRESS, DRIVER_BLOCK_MANAGER_PORT, DRIVER_HOST_ADDRESS, IO_ENCRYPTION_ENABLED}
import org.apache.spark.memory.{MemoryManager, StaticMemoryManager, UnifiedMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.OutputCommitCoordinator.OutputCommitCoordinatorEndpoint
import org.apache.spark.scheduler.{LiveListenerBus, OutputCommitCoordinator}
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.serializer.{JavaSerializer, Serializer, SerializerManager}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage.{BlockManager, BlockManagerMaster, BlockManagerMasterEndpoint}
import org.apache.spark.util.{RpcUtils, Utils}

import scala.collection.mutable
import scala.util.Properties

/**
  * 保存运行中Spark实例（主实例或工作实例）的所有运行时环境对象，包括序列化程序、RpcEnv、块管理器、映射输出跟踪器等。
  * 当前Spark代码通过全局变量查找SparkEnv，因此所有线程都可以访问同一SparkEnv。SparkEnv可以访问它。获取（例如，在创建SparkContext之后）。
  *
  * :: DeveloperApi ::
  * Holds all the runtime environment objects for a running Spark instance (either master or worker),
  * including the serializer, RpcEnv, block manager, map output tracker, etc. Currently
  * Spark code finds the SparkEnv through a global variable, so all the threads can access the same
  * SparkEnv. It can be accessed by SparkEnv.get (e.g. after creating a SparkContext).
  *
  * NOTE: This is not intended for external use. This is exposed for Shark and may be made private
  * in a future release.
  */
@DeveloperApi
class SparkEnv(
                  val executorId: String,
                  private[spark] val rpcEnv: RpcEnv,
                  val serializer: Serializer,
                  val closureSerializer: Serializer,
                  val serializerManager: SerializerManager,
                  val mapOutputTracker: MapOutputTracker,
                  val shuffleManager: ShuffleManager,
                  val broadcastManager: BroadcastManager,
                  val blockManager: BlockManager,
                  val securityManager: SecurityManager,
                  val metricsSystem: MetricsSystem,
                  val memoryManager: MemoryManager,
                  val outputCommitCoordinator: OutputCommitCoordinator,
                  val conf: SparkConf) extends Logging {

    private val pythonWorkers = mutable.HashMap[(String, Map[String, String]), PythonWorkerFactory]()
    // A general, soft-reference map for metadata needed during HadoopRDD split computation
    // (e.g., HadoopFileRDD uses this to cache JobConfs and InputFormats).
    private[spark] val hadoopJobMetadata = new MapMaker().softValues().makeMap[String, Any]()
    private[spark] var isStopped = false
    private[spark] var driverTmpDir: Option[String] = None

    private[spark] def stop() {

        if (!isStopped) {
            isStopped = true
            pythonWorkers.values.foreach(_.stop())
            mapOutputTracker.stop()
            shuffleManager.stop()
            broadcastManager.stop()
            blockManager.stop()
            blockManager.master.stop()
            metricsSystem.stop()
            outputCommitCoordinator.stop()
            rpcEnv.shutdown()
            rpcEnv.awaitTermination()

            // If we only stop sc, but the driver process still run as a services then we need to delete
            // the tmp dir, if not, it will create too many tmp dirs.
            // We only need to delete the tmp dir create by driver
            driverTmpDir match {
                case Some(path) =>
                    try {
                        Utils.deleteRecursively(new File(path))
                    } catch {
                        case e: Exception =>
                            logWarning(s"Exception while deleting Spark temp dir: $path", e)
                    }
                case None => // We just need to delete tmp dir created by driver, so do nothing on executor
            }
        }
    }

    private[spark]
    def createPythonWorker(pythonExec: String, envVars: Map[String, String]): java.net.Socket = {
        synchronized {
            val key = (pythonExec, envVars)
            pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(pythonExec, envVars)).create()
        }
    }

    private[spark]
    def destroyPythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
        synchronized {
            val key = (pythonExec, envVars)
            pythonWorkers.get(key).foreach(_.stopWorker(worker))
        }
    }

    private[spark]
    def releasePythonWorker(pythonExec: String, envVars: Map[String, String], worker: Socket) {
        synchronized {
            val key = (pythonExec, envVars)
            pythonWorkers.get(key).foreach(_.releaseWorker(worker))
        }
    }
}

object SparkEnv extends Logging {
    private[spark] val driverSystemName = "sparkDriver"
    private[spark] val executorSystemName = "sparkExecutor"
    @volatile private var env: SparkEnv = _

    /**
      * 返回SparkEnv。
      *
      * Returns the SparkEnv.
      */
    def get: SparkEnv = {
        env
    }

    /**
      * 为driver创建SparkEnv。
      *
      * Create a SparkEnv for the driver.
      */
    private[spark] def createDriverEnv(
                                          conf: SparkConf,
                                          isLocal: Boolean,
                                          listenerBus: LiveListenerBus,
                                          numCores: Int,
                                          mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
        assert(conf.contains(DRIVER_HOST_ADDRESS),
            s"${DRIVER_HOST_ADDRESS.key} is not set on the driver!")
        assert(conf.contains("org.apache.spark.driver.port"), "org.apache.spark.driver.port is not set on the driver!")
        val bindAddress = conf.get(DRIVER_BIND_ADDRESS)
        val advertiseAddress = conf.get(DRIVER_HOST_ADDRESS)
        val port = conf.get("org.apache.spark.driver.port").toInt
        val ioEncryptionKey = if (conf.get(IO_ENCRYPTION_ENABLED)) {
            Some(CryptoStreamUtils.createKey(conf))
        } else {
            None
        }
        create(
            conf,
            SparkContext.DRIVER_IDENTIFIER,
            bindAddress,
            advertiseAddress,
            port,
            isLocal,
            numCores,
            ioEncryptionKey,
            listenerBus = listenerBus,
            mockOutputCommitCoordinator = mockOutputCommitCoordinator
        )
    }

    /**
      * Helper方法为驱动程序或执行器创建SparkEnv。
      *
      * Helper method to create a SparkEnv for a driver or an executor.
      */
    private def create(
                          conf: SparkConf,
                          executorId: String,
                          bindAddress: String,
                          advertiseAddress: String,
                          port: Int,
                          isLocal: Boolean,
                          numUsableCores: Int,
                          ioEncryptionKey: Option[Array[Byte]],
                          listenerBus: LiveListenerBus = null,
                          mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

        val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER

        // 侦听器总线仅用于驱动程序
        // Listener bus is only used on the driver
        if (isDriver) {
            assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
        }

        val securityManager = new SecurityManager(conf, ioEncryptionKey)
        ioEncryptionKey.foreach { _ =>
            if (!securityManager.isEncryptionEnabled()) {
                logWarning("I/O encryption enabled without RPC encryption: keys will be visible on the " +
                    "wire.")
            }
        }

        val systemName = if (isDriver) driverSystemName else executorSystemName
        val rpcEnv = RpcEnv.create(systemName, bindAddress, advertiseAddress, port, conf,
            securityManager, clientMode = !isDriver)

        // Figure out which port RpcEnv actually bound to in case the original port is 0 or occupied.
        // In the non-driver case, the RPC env's address may be null since it may not be listening
        // for incoming connections.
        if (isDriver) {
            conf.set("org.apache.spark.driver.port", rpcEnv.address.port.toString)
        } else if (rpcEnv.address != null) {
            conf.set("org.apache.spark.executor.port", rpcEnv.address.port.toString)
            logInfo(s"Setting org.apache.spark.executor.port to: ${rpcEnv.address.port.toString}")
        }

        // 用给定的名称创建一个类的实例，可能用我们的conf初始化它
        // Create an instance of the class with the given name, possibly initializing it with our conf
        def instantiateClass[T](className: String): T = {
            val cls = Utils.classForName(className)
            // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
            // SparkConf, then one taking no arguments
            try {
                cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
                    .newInstance(conf, new java.lang.Boolean(isDriver))
                    .asInstanceOf[T]
            } catch {
                case _: NoSuchMethodException =>
                    try {
                        cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
                    } catch {
                        case _: NoSuchMethodException =>
                            cls.getConstructor().newInstance().asInstanceOf[T]
                    }
            }
        }

        // Create an instance of the class named by the given SparkConf property, or defaultClassName
        // if the property is not set, possibly initializing it with our conf
        def instantiateClassFromConf[T](propertyName: String, defaultClassName: String): T = {
            instantiateClass[T](conf.get(propertyName, defaultClassName))
        }

        val serializer = instantiateClassFromConf[Serializer](
            "org.apache.spark.serializer", "org.apache.org.apache.spark.serializer.JavaSerializer")
        logDebug(s"Using serializer: ${serializer.getClass}")

        val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)

        val closureSerializer = new JavaSerializer(conf)

        def registerOrLookupEndpoint(
                                        name: String, endpointCreator: => RpcEndpoint):
        RpcEndpointRef = {
            if (isDriver) {
                logInfo("Registering " + name)
                rpcEnv.setupEndpoint(name, endpointCreator)
            } else {
                RpcUtils.makeDriverRef(name, conf, rpcEnv)
            }
        }

        val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)

        val mapOutputTracker = if (isDriver) {
            new MapOutputTrackerMaster(conf, broadcastManager, isLocal)
        } else {
            new MapOutputTrackerWorker(conf)
        }

        // Have to assign trackerEndpoint after initialization as MapOutputTrackerEndpoint
        // requires the MapOutputTracker itself
        mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(MapOutputTracker.ENDPOINT_NAME,
            new MapOutputTrackerMasterEndpoint(
                rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))

        // Let the user specify short names for shuffle managers
        val shortShuffleMgrNames = Map(
            "sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName,
            "tungsten-sort" -> classOf[org.apache.spark.shuffle.sort.SortShuffleManager].getName)
        val shuffleMgrName = conf.get("org.apache.spark.shuffle.manager", "sort")
        val shuffleMgrClass =
            shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase(Locale.ROOT), shuffleMgrName)
        val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)

        val useLegacyMemoryManager = conf.getBoolean("org.apache.spark.memory.useLegacyMode", false)
        val memoryManager: MemoryManager =
            if (useLegacyMemoryManager) {
                new StaticMemoryManager(conf, numUsableCores)
            } else {
                UnifiedMemoryManager(conf, numUsableCores)
            }

        val blockManagerPort = if (isDriver) {
            conf.get(DRIVER_BLOCK_MANAGER_PORT)
        } else {
            conf.get(BLOCK_MANAGER_PORT)
        }

        val blockTransferService =
            new NettyBlockTransferService(conf, securityManager, bindAddress, advertiseAddress,
                blockManagerPort, numUsableCores)

        val blockManagerMaster = new BlockManagerMaster(registerOrLookupEndpoint(
            BlockManagerMaster.DRIVER_ENDPOINT_NAME,
            new BlockManagerMasterEndpoint(rpcEnv, isLocal, conf, listenerBus)),
            conf, isDriver)

        // NB: blockManager is not valid until initialize() is called later.
        val blockManager = new BlockManager(executorId, rpcEnv, blockManagerMaster,
            serializerManager, conf, memoryManager, mapOutputTracker, shuffleManager,
            blockTransferService, securityManager, numUsableCores)

        val metricsSystem = if (isDriver) {
            // Don't start metrics system right now for Driver.
            // We need to wait for the task scheduler to give us an app ID.
            // Then we can start the metrics system.
            MetricsSystem.createMetricsSystem("driver", conf, securityManager)
        } else {
            // We need to set the executor ID before the MetricsSystem is created because sources and
            // sinks specified in the metrics configuration file will want to incorporate this executor's
            // ID into the metrics they report.
            conf.set("org.apache.spark.executor.id", executorId)
            val ms = MetricsSystem.createMetricsSystem("executor", conf, securityManager)
            ms.start()
            ms
        }

        val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
            new OutputCommitCoordinator(conf, isDriver)
        }
        val outputCommitCoordinatorRef = registerOrLookupEndpoint("OutputCommitCoordinator",
            new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))
        outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)

        val envInstance = new SparkEnv(
            executorId,
            rpcEnv,
            serializer,
            closureSerializer,
            serializerManager,
            mapOutputTracker,
            shuffleManager,
            broadcastManager,
            blockManager,
            securityManager,
            metricsSystem,
            memoryManager,
            outputCommitCoordinator,
            conf)

        // Add a reference to tmp dir created by driver, we will delete this tmp dir when stop() is
        // called, and we only need to do it for driver. Because driver may run as a service, and if we
        // don't delete this tmp dir when sc is stopped, then will create too many tmp dirs.
        if (isDriver) {
            val sparkFilesDir = Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
            envInstance.driverTmpDir = Some(sparkFilesDir)
        }

        envInstance
    }

    /**
      * 为执行者创建SparkEnv。在粗粒度模式下，执行器提供已经实例化的RpcEnv。
      * Create a SparkEnv for an executor.
      * In coarse-grained mode, the executor provides an RpcEnv that is already instantiated.
      */
    private[spark] def createExecutorEnv(
                                            conf: SparkConf,
                                            executorId: String,
                                            hostname: String,
                                            port: Int,
                                            numCores: Int,
                                            ioEncryptionKey: Option[Array[Byte]],
                                            isLocal: Boolean): SparkEnv = {
        val env = create(
            conf,
            executorId,
            hostname,
            hostname,
            port,
            isLocal,
            numCores,
            ioEncryptionKey
        )
        SparkEnv.set(env)
        env
    }

    def set(e: SparkEnv) {
        env = e
    }

    /**
      * 返回jvm信息、Spark属性、系统属性和类路径的映射表示。贴图键定义类别，贴图值将相应属性表示为KV对序列。
      * 这主要用于SparkListenerEnvironmentUpdate。
      *
      * Return a map representation of jvm information, Spark properties, system properties, and
      * class paths. Map keys define the category, and map values represent the corresponding
      * attributes as a sequence of KV pairs. This is used mainly for SparkListenerEnvironmentUpdate.
      */
    private[spark]
    def environmentDetails(
                              conf: SparkConf,
                              schedulingMode: String,
                              addedJars: Seq[String],
                              addedFiles: Seq[String]): Map[String, Seq[(String, String)]] = {

        import Properties._
        val jvmInformation = Seq(
            ("Java Version", s"$javaVersion ($javaVendor)"),
            ("Java Home", javaHome),
            ("Scala Version", versionString)
        ).sorted

        // Spark properties
        // This includes the scheduling mode whether or not it is configured (used by SparkUI)
        val schedulerMode =
        if (!conf.contains("org.apache.spark.scheduler.mode")) {
            Seq(("org.apache.spark.scheduler.mode", schedulingMode))
        } else {
            Seq[(String, String)]()
        }
        val sparkProperties = (conf.getAll ++ schedulerMode).sorted

        // System properties that are not java classpaths
        val systemProperties = Utils.getSystemProperties.toSeq
        val otherProperties = systemProperties.filter { case (k, _) =>
            k != "java.class.path" && !k.startsWith("org.apache.spark.")
        }.sorted

        // Class paths including all added jars and files
        val classPathEntries = javaClassPath
            .split(File.pathSeparator)
            .filterNot(_.isEmpty)
            .map((_, "System Classpath"))
        val addedJarsAndFiles = (addedJars ++ addedFiles).map((_, "Added By User"))
        val classPaths = (addedJarsAndFiles ++ classPathEntries).sorted

        Map[String, Seq[(String, String)]](
            "JVM Information" -> jvmInformation,
            "Spark Properties" -> sparkProperties,
            "System Properties" -> otherProperties,
            "Classpath Entries" -> classPaths)
    }
}
