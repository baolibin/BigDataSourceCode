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

import java.util.concurrent.ConcurrentHashMap

import org.apache.avro.{Schema, SchemaNormalization}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._
import scala.collection.mutable.LinkedHashSet

/**
  * Spark应用程序的配置。用于将各种Spark参数设置为键值对。
  *
  * Configuration for a Spark application. Used to set various Spark parameters as key-value pairs.
  *
  * 大多数情况下，您都会使用“new SparkConf（）`”创建一个SparkConf对象，它也会从应用程序中设置的任何“org.apache.spark.*` Java系统属性中加载值。
  * 在本例中，直接在“SparkConf”对象上设置的参数优先于系统属性。
  *
  * Most of the time, you would create a SparkConf object with `new SparkConf()`, which will load
  * values from any `org.apache.spark.*` Java system properties set in your application as well. In this case,
  * parameters you set directly on the `SparkConf` object take priority over system properties.
  *
  * 对于单元测试，您还可以调用'new SparkConf（false）`跳过加载外部设置并获得相同的配置，而不管系统属性是什么。
  *
  * For unit tests, you can also call `new SparkConf(false)` to skip loading external settings and
  * get the same configuration no matter what the system properties are.
  *
  * 此类中的所有setter方法都支持链接。例如，可以编写“new SparkConf（）.setMaster（“local”）.setAppName（“My app”）”。
  *
  * All setter methods in this class support chaining. For example, you can write
  * `new SparkConf().setMaster("local").setAppName("My app")`.
  *
  * @param loadDefaults whether to also load values from Java system properties
  *
  * spark不支持运行中更新sparkConf配置信息
  * @note Once a SparkConf object is passed to Spark, it is cloned and can no longer be modified
  * by the user. Spark does not support modifying the configuration at runtime.
  **/
class SparkConf(loadDefaults: Boolean) extends Cloneable with Logging with Serializable {

    import SparkConf._

    private final val avroNamespace = "avro.schema."
    @transient private lazy val reader: ConfigReader = {
        val _reader = new ConfigReader(new SparkConfigProvider(settings))
        _reader.bindEnv(new ConfigProvider {
            override def get(key: String): Option[String] = Option(getenv(key))
        })
        _reader
    }
    // 存放配置信息
    private val settings = new ConcurrentHashMap[String, String]()

    if (loadDefaults) {
        loadFromSystemProperties(false)
    }

    /**
      * 构造函数，默认参数
      * Create a SparkConf that loads defaults from system properties and the classpath
      */
    def this() = this(true)

    /**
      * 设置运行模式，local还是集群模式。
      * The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to
      * run locally with 4 cores, or "org.apache.spark://master:7077" to run on a Spark standalone cluster.
      */
    def setMaster(master: String): SparkConf = {
        set("org.apache.spark.master", master)
    }

    /**
      * 设置作业名字
      * Set a name for your application. Shown in the Spark web UI.
      */
    def setAppName(name: String): SparkConf = {
        set("org.apache.spark.app.name", name)
    }

    /** Set JAR files to distribute to the cluster. (Java-friendly version.) */
    def setJars(jars: Array[String]): SparkConf = {
        setJars(jars.toSeq)
    }

    /** Set JAR files to distribute to the cluster. */
    def setJars(jars: Seq[String]): SparkConf = {
        for (jar <- jars if (jar == null)) logWarning("null jar passed to SparkContext constructor")
        set("org.apache.spark.jars", jars.filter(_ != null).mkString(","))
    }

    /**
      * 设置KV值
      * Set a configuration variable.
      */
    def set(key: String, value: String): SparkConf = {
        set(key, value, false)
    }

    private[spark] def set(key: String, value: String, silent: Boolean): SparkConf = {
        if (key == null) {
            throw new NullPointerException("null key")
        }
        if (value == null) {
            throw new NullPointerException("null value for " + key)
        }
        if (!silent) {
            logDeprecationWarning(key)
        }
        settings.put(key, value)
        this
    }

    /**
      * Set multiple environment variables to be used when launching executors.
      * (Java-friendly version.)
      */
    def setExecutorEnv(variables: Array[(String, String)]): SparkConf = {
        setExecutorEnv(variables.toSeq)
    }

    /**
      * Set multiple environment variables to be used when launching executors.
      * These variables are stored as properties of the form org.apache.spark.executorEnv.VAR_NAME
      * (for example org.apache.spark.executorEnv.PATH) but this method makes them easier to set.
      */
    def setExecutorEnv(variables: Seq[(String, String)]): SparkConf = {
        for ((k, v) <- variables) {
            setExecutorEnv(k, v)
        }
        this
    }

    /**
      * Set an environment variable to be used when launching executors for this application.
      * These variables are stored as properties of the form org.apache.spark.executorEnv.VAR_NAME
      * (for example org.apache.spark.executorEnv.PATH) but this method makes them easier to set.
      */
    def setExecutorEnv(variable: String, value: String): SparkConf = {
        set("org.apache.spark.executorEnv." + variable, value)
    }

    /**
      * Set the location where Spark is installed on worker nodes.
      */
    def setSparkHome(home: String): SparkConf = {
        set("org.apache.spark.home", home)
    }

    /** Set multiple parameters together */
    def setAll(settings: Traversable[(String, String)]): SparkConf = {
        settings.foreach { case (k, v) => set(k, v) }
        this
    }

    /** Set a parameter if it isn't already configured */
    def setIfMissing(key: String, value: String): SparkConf = {
        if (settings.putIfAbsent(key, value) == null) {
            logDeprecationWarning(key)
        }
        this
    }

    /**
      * Use Kryo serialization and register the given set of classes with Kryo.
      * If called multiple times, this will append the classes from all calls together.
      */
    def registerKryoClasses(classes: Array[Class[_]]): SparkConf = {
        val allClassNames = new LinkedHashSet[String]()
        allClassNames ++= get("org.apache.spark.kryo.classesToRegister", "").split(',').map(_.trim)
                .filter(!_.isEmpty)
        allClassNames ++= classes.map(_.getName)

        set("org.apache.spark.kryo.classesToRegister", allClassNames.mkString(","))
        set("org.apache.spark.serializer", classOf[KryoSerializer].getName)
        this
    }

    /**
      * Use Kryo serialization and register the given set of Avro schemas so that the generic
      * record serializer can decrease network IO
      */
    def registerAvroSchemas(schemas: Schema*): SparkConf = {
        for (schema <- schemas) {
            set(avroNamespace + SchemaNormalization.parsingFingerprint64(schema), schema.toString)
        }
        this
    }

    /** Gets all the avro schemas in the configuration used in the generic Avro record serializer */
    def getAvroSchema: Map[Long, String] = {
        getAll.filter { case (k, v) => k.startsWith(avroNamespace) }
                .map { case (k, v) => (k.substring(avroNamespace.length).toLong, v) }
                .toMap
    }

    /** Get all parameters as a list of pairs */
    def getAll: Array[(String, String)] = {
        settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
    }

    /**
      * Get a time parameter as seconds; throws a NoSuchElementException if it's not set. If no
      * suffix is provided then seconds are assumed.
      *
      * @throws java.util.NoSuchElementException If the time parameter is not set
      */
    def getTimeAsSeconds(key: String): Long = {
        Utils.timeStringAsSeconds(get(key))
    }

    /**
      * Get a time parameter as seconds, falling back to a default if not set. If no
      * suffix is provided then seconds are assumed.
      */
    def getTimeAsSeconds(key: String, defaultValue: String): Long = {
        Utils.timeStringAsSeconds(get(key, defaultValue))
    }

    /** Get a parameter, falling back to a default if not set */
    def get(key: String, defaultValue: String): String = {
        getOption(key).getOrElse(defaultValue)
    }

    /**
      * 根据Key返回Option参数值。
      * Get a parameter as an Option
      */
    def getOption(key: String): Option[String] = {
        Option(settings.get(key)).orElse(getDeprecatedConfig(key, this))
    }

    /**
      * Get a time parameter as milliseconds; throws a NoSuchElementException if it's not set. If no
      * suffix is provided then milliseconds are assumed.
      *
      * @throws java.util.NoSuchElementException If the time parameter is not set
      */
    def getTimeAsMs(key: String): Long = {
        Utils.timeStringAsMs(get(key))
    }

    /**
      * Get a time parameter as milliseconds, falling back to a default if not set. If no
      * suffix is provided then milliseconds are assumed.
      */
    def getTimeAsMs(key: String, defaultValue: String): Long = {
        Utils.timeStringAsMs(get(key, defaultValue))
    }

    /**
      * Get a size parameter as bytes; throws a NoSuchElementException if it's not set. If no
      * suffix is provided then bytes are assumed.
      *
      * @throws java.util.NoSuchElementException If the size parameter is not set
      */
    def getSizeAsBytes(key: String): Long = {
        Utils.byteStringAsBytes(get(key))
    }

    /**
      * Get a size parameter as bytes, falling back to a default if not set. If no
      * suffix is provided then bytes are assumed.
      */
    def getSizeAsBytes(key: String, defaultValue: String): Long = {
        Utils.byteStringAsBytes(get(key, defaultValue))
    }

    /**
      * Get a size parameter as bytes, falling back to a default if not set.
      */
    def getSizeAsBytes(key: String, defaultValue: Long): Long = {
        Utils.byteStringAsBytes(get(key, defaultValue + "B"))
    }

    /**
      * Get a size parameter as Kibibytes; throws a NoSuchElementException if it's not set. If no
      * suffix is provided then Kibibytes are assumed.
      *
      * @throws java.util.NoSuchElementException If the size parameter is not set
      */
    def getSizeAsKb(key: String): Long = {
        Utils.byteStringAsKb(get(key))
    }

    /** Get a parameter; throws a NoSuchElementException if it's not set */
    def get(key: String): String = {
        getOption(key).getOrElse(throw new NoSuchElementException(key))
    }

    /**
      * Get a size parameter as Kibibytes, falling back to a default if not set. If no
      * suffix is provided then Kibibytes are assumed.
      */
    def getSizeAsKb(key: String, defaultValue: String): Long = {
        Utils.byteStringAsKb(get(key, defaultValue))
    }

    /**
      * Get a size parameter as Mebibytes; throws a NoSuchElementException if it's not set. If no
      * suffix is provided then Mebibytes are assumed.
      *
      * @throws java.util.NoSuchElementException If the size parameter is not set
      */
    def getSizeAsMb(key: String): Long = {
        Utils.byteStringAsMb(get(key))
    }

    /**
      * Get a size parameter as Mebibytes, falling back to a default if not set. If no
      * suffix is provided then Mebibytes are assumed.
      */
    def getSizeAsMb(key: String, defaultValue: String): Long = {
        Utils.byteStringAsMb(get(key, defaultValue))
    }

    /**
      * Get a size parameter as Gibibytes; throws a NoSuchElementException if it's not set. If no
      * suffix is provided then Gibibytes are assumed.
      *
      * @throws java.util.NoSuchElementException If the size parameter is not set
      */
    def getSizeAsGb(key: String): Long = {
        Utils.byteStringAsGb(get(key))
    }

    /**
      * Get a size parameter as Gibibytes, falling back to a default if not set. If no
      * suffix is provided then Gibibytes are assumed.
      */
    def getSizeAsGb(key: String, defaultValue: String): Long = {
        Utils.byteStringAsGb(get(key, defaultValue))
    }

    /** Get a parameter as an integer, falling back to a default if not set */
    def getInt(key: String, defaultValue: Int): Int = {
        getOption(key).map(_.toInt).getOrElse(defaultValue)
    }

    /** Get a parameter as a long, falling back to a default if not set */
    def getLong(key: String, defaultValue: Long): Long = {
        getOption(key).map(_.toLong).getOrElse(defaultValue)
    }

    /** Get a parameter as a double, falling back to a default if not set */
    def getDouble(key: String, defaultValue: Double): Double = {
        getOption(key).map(_.toDouble).getOrElse(defaultValue)
    }

    /** Get a parameter as a boolean, falling back to a default if not set */
    def getBoolean(key: String, defaultValue: Boolean): Boolean = {
        getOption(key).map(_.toBoolean).getOrElse(defaultValue)
    }

    /** Get all executor environment variables set on this SparkConf */
    def getExecutorEnv: Seq[(String, String)] = {
        getAllWithPrefix("org.apache.spark.executorEnv.")
    }

    /**
      * Get all parameters that start with `prefix`
      */
    def getAllWithPrefix(prefix: String): Array[(String, String)] = {
        getAll.filter { case (k, v) => k.startsWith(prefix) }
                .map { case (k, v) => (k.substring(prefix.length), v) }
    }

    /**
      * Returns the Spark application id, valid in the Driver after TaskScheduler registration and
      * from the start in the Executor.
      */
    def getAppId: String = get("org.apache.spark.app.id")

    /** Does the configuration contain a given parameter? */
    def contains(key: String): Boolean = {
        settings.containsKey(key) ||
                configsWithAlternatives.get(key).toSeq.flatten.exists { alt => contains(alt.key) }
    }

    /** Copy this object */
    override def clone: SparkConf = {
        val cloned = new SparkConf(false)
        settings.entrySet().asScala.foreach { e =>
            cloned.set(e.getKey(), e.getValue(), true)
        }
        cloned
    }

    /**
      * Return a string listing all keys and values, one per line. This is useful to print the
      * configuration out for debugging.
      */
    def toDebugString: String = {
        getAll.sorted.map { case (k, v) => k + "=" + v }.mkString("\n")
    }

    /**
      * 使用系统默认参数
      */
    private[spark] def loadFromSystemProperties(silent: Boolean): SparkConf = {
        // Load any org.apache.spark.* system properties
        for ((key, value) <- Utils.getSystemProperties if key.startsWith("org.apache.spark.")) {
            set(key, value, silent)
        }
        this
    }

    private[spark] def set[T](entry: ConfigEntry[T], value: T): SparkConf = {
        set(entry.key, entry.stringConverter(value))
        this
    }

    private[spark] def set[T](entry: OptionalConfigEntry[T], value: T): SparkConf = {
        set(entry.key, entry.rawStringConverter(value))
        this
    }

    private[spark] def setIfMissing[T](entry: ConfigEntry[T], value: T): SparkConf = {
        if (settings.putIfAbsent(entry.key, entry.stringConverter(value)) == null) {
            logDeprecationWarning(entry.key)
        }
        this
    }

    private[spark] def setIfMissing[T](entry: OptionalConfigEntry[T], value: T): SparkConf = {
        if (settings.putIfAbsent(entry.key, entry.rawStringConverter(value)) == null) {
            logDeprecationWarning(entry.key)
        }
        this
    }

    private[spark] def remove(entry: ConfigEntry[_]): SparkConf = {
        remove(entry.key)
    }

    /** Remove a parameter from the configuration */
    def remove(key: String): SparkConf = {
        settings.remove(key)
        this
    }

    /**
      * Retrieves the value of a pre-defined configuration entry.
      *
      * - This is an internal Spark API.
      * - The return type if defined by the configuration entry.
      * - This will throw an exception is the config is not optional and the value is not set.
      */
    private[spark] def get[T](entry: ConfigEntry[T]): T = {
        entry.readFrom(reader)
    }

    private[spark] def contains(entry: ConfigEntry[_]): Boolean = contains(entry.key)

    /**
      * By using this instead of System.getenv(), environment variables can be mocked
      * in unit tests.
      */
    private[spark] def getenv(name: String): String = System.getenv(name)

    /**
      * Checks for illegal or deprecated config settings. Throws an exception for the former. Not
      * idempotent - may mutate this conf object to convert deprecated settings to supported ones.
      */
    private[spark] def validateSettings() {
        if (contains("org.apache.spark.local.dir")) {
            val msg = "In Spark 1.0 and later org.apache.spark.local.dir will be overridden by the value set by " +
                    "the cluster manager (via SPARK_LOCAL_DIRS in mesos/standalone and LOCAL_DIRS in YARN)."
            logWarning(msg)
        }

        val executorOptsKey = "org.apache.spark.executor.extraJavaOptions"
        val executorClasspathKey = "org.apache.spark.executor.extraClassPath"
        val driverOptsKey = "org.apache.spark.driver.extraJavaOptions"
        val driverClassPathKey = "org.apache.spark.driver.extraClassPath"
        val driverLibraryPathKey = "org.apache.spark.driver.extraLibraryPath"
        val sparkExecutorInstances = "org.apache.spark.executor.instances"

        // Used by Yarn in 1.1 and before
        sys.props.get("org.apache.spark.driver.libraryPath").foreach { value =>
            val warning =
                s"""
                   |org.apache.spark.driver.libraryPath was detected (set to '$value').
                   |This is deprecated in Spark 1.2+.
                   |
                   |Please instead use: $driverLibraryPathKey
        """.stripMargin
            logWarning(warning)
        }

        // Validate org.apache.spark.executor.extraJavaOptions
        getOption(executorOptsKey).foreach { javaOpts =>
            if (javaOpts.contains("-Dspark")) {
                val msg = s"$executorOptsKey is not allowed to set Spark options (was '$javaOpts'). " +
                        "Set them directly on a SparkConf or in a properties file when using ./bin/org.apache.spark-submit."
                throw new Exception(msg)
            }
            if (javaOpts.contains("-Xmx")) {
                val msg = s"$executorOptsKey is not allowed to specify max heap memory settings " +
                        s"(was '$javaOpts'). Use org.apache.spark.executor.memory instead."
                throw new Exception(msg)
            }
        }

        // Validate memory fractions
        val deprecatedMemoryKeys = Seq(
            "org.apache.spark.storage.memoryFraction",
            "org.apache.spark.shuffle.memoryFraction",
            "org.apache.spark.shuffle.safetyFraction",
            "org.apache.spark.storage.unrollFraction",
            "org.apache.spark.storage.safetyFraction")
        val memoryKeys = Seq(
            "org.apache.spark.memory.fraction",
            "org.apache.spark.memory.storageFraction") ++
                deprecatedMemoryKeys
        for (key <- memoryKeys) {
            val value = getDouble(key, 0.5)
            if (value > 1 || value < 0) {
                throw new IllegalArgumentException(s"$key should be between 0 and 1 (was '$value').")
            }
        }

        // Warn against deprecated memory fractions (unless legacy memory management mode is enabled)
        val legacyMemoryManagementKey = "org.apache.spark.memory.useLegacyMode"
        val legacyMemoryManagement = getBoolean(legacyMemoryManagementKey, false)
        if (!legacyMemoryManagement) {
            val keyset = deprecatedMemoryKeys.toSet
            val detected = settings.keys().asScala.filter(keyset.contains)
            if (detected.nonEmpty) {
                logWarning("Detected deprecated memory fraction settings: " +
                        detected.mkString("[", ", ", "]") + ". As of Spark 1.6, execution and storage " +
                        "memory management are unified. All memory fractions used in the old model are " +
                        "now deprecated and no longer read. If you wish to use the old memory management, " +
                        s"you may explicitly enable `$legacyMemoryManagementKey` (not recommended).")
            }
        }

        if (contains("org.apache.spark.master") && get("org.apache.spark.master").startsWith("yarn-")) {
            val warning = s"org.apache.spark.master ${get("org.apache.spark.master")} is deprecated in Spark 2.0+, please " +
                    "instead use \"yarn\" with specified deploy mode."

            get("org.apache.spark.master") match {
                case "yarn-cluster" =>
                    logWarning(warning)
                    set("org.apache.spark.master", "yarn")
                    set("org.apache.spark.submit.deployMode", "cluster")
                case "yarn-client" =>
                    logWarning(warning)
                    set("org.apache.spark.master", "yarn")
                    set("org.apache.spark.submit.deployMode", "client")
                case _ => // Any other unexpected master will be checked when creating scheduler backend.
            }
        }

        if (contains("org.apache.spark.submit.deployMode")) {
            get("org.apache.spark.submit.deployMode") match {
                case "cluster" | "client" =>
                case e => throw new SparkException("org.apache.spark.submit.deployMode can only be \"cluster\" or " +
                        "\"client\".")
            }
        }

        val encryptionEnabled = get(NETWORK_ENCRYPTION_ENABLED) || get(SASL_ENCRYPTION_ENABLED)
        require(!encryptionEnabled || get(NETWORK_AUTH_ENABLED),
            s"${NETWORK_AUTH_ENABLED.key} must be enabled when enabling encryption.")
    }

}

private[spark] object SparkConf extends Logging {

    /**
      * Maps deprecated config keys to information about the deprecation.
      *
      * The extra information is logged as a warning when the config is present in the user's
      * configuration.
      */
    private val deprecatedConfigs: Map[String, DeprecatedConfig] = {
        val configs = Seq(
            DeprecatedConfig("org.apache.spark.cache.class", "0.8",
                "The org.apache.spark.cache.class property is no longer being used! Specify storage levels using " +
                        "the RDD.persist() method instead."),
            DeprecatedConfig("org.apache.spark.yarn.user.classpath.first", "1.3",
                "Please use org.apache.spark.{driver,executor}.userClassPathFirst instead."),
            DeprecatedConfig("org.apache.spark.kryoserializer.buffer.mb", "1.4",
                "Please use org.apache.spark.kryoserializer.buffer instead. The default value for " +
                        "org.apache.spark.kryoserializer.buffer.mb was previously specified as '0.064'. Fractional values " +
                        "are no longer accepted. To specify the equivalent now, one may use '64k'."),
            DeprecatedConfig("org.apache.spark.rpc", "2.0", "Not used any more."),
            DeprecatedConfig("org.apache.spark.scheduler.executorTaskBlacklistTime", "2.1.0",
                "Please use the new blacklisting options, org.apache.spark.blacklist.*")
        )

        Map(configs.map { cfg => (cfg.key -> cfg) }: _*)
    }

    /**
      * Maps a current config key to alternate keys that were used in previous version of Spark.
      *
      * The alternates are used in the order defined in this map. If deprecated configs are
      * present in the user's configuration, a warning is logged.
      */
    private val configsWithAlternatives = Map[String, Seq[AlternateConfig]](
        "org.apache.spark.executor.userClassPathFirst" -> Seq(
            AlternateConfig("org.apache.spark.files.userClassPathFirst", "1.3")),
        "org.apache.spark.history.fs.update.interval" -> Seq(
            AlternateConfig("org.apache.spark.history.fs.update.interval.seconds", "1.4"),
            AlternateConfig("org.apache.spark.history.fs.updateInterval", "1.3"),
            AlternateConfig("org.apache.spark.history.updateInterval", "1.3")),
        "org.apache.spark.history.fs.cleaner.interval" -> Seq(
            AlternateConfig("org.apache.spark.history.fs.cleaner.interval.seconds", "1.4")),
        "org.apache.spark.history.fs.cleaner.maxAge" -> Seq(
            AlternateConfig("org.apache.spark.history.fs.cleaner.maxAge.seconds", "1.4")),
        "org.apache.spark.yarn.am.waitTime" -> Seq(
            AlternateConfig("org.apache.spark.yarn.applicationMaster.waitTries", "1.3",
                // Translate old value to a duration, with 10s wait time per try.
                translation = s => s"${s.toLong * 10}s")),
        "org.apache.spark.reducer.maxSizeInFlight" -> Seq(
            AlternateConfig("org.apache.spark.reducer.maxMbInFlight", "1.4")),
        "org.apache.spark.kryoserializer.buffer" ->
                Seq(AlternateConfig("org.apache.spark.kryoserializer.buffer.mb", "1.4",
                    translation = s => s"${(s.toDouble * 1000).toInt}k")),
        "org.apache.spark.kryoserializer.buffer.max" -> Seq(
            AlternateConfig("org.apache.spark.kryoserializer.buffer.max.mb", "1.4")),
        "org.apache.spark.shuffle.file.buffer" -> Seq(
            AlternateConfig("org.apache.spark.shuffle.file.buffer.kb", "1.4")),
        "org.apache.spark.executor.logs.rolling.maxSize" -> Seq(
            AlternateConfig("org.apache.spark.executor.logs.rolling.size.maxBytes", "1.4")),
        "org.apache.spark.io.compression.snappy.blockSize" -> Seq(
            AlternateConfig("org.apache.spark.io.compression.snappy.block.size", "1.4")),
        "org.apache.spark.io.compression.lz4.blockSize" -> Seq(
            AlternateConfig("org.apache.spark.io.compression.lz4.block.size", "1.4")),
        "org.apache.spark.rpc.numRetries" -> Seq(
            AlternateConfig("org.apache.spark.akka.num.retries", "1.4")),
        "org.apache.spark.rpc.retry.wait" -> Seq(
            AlternateConfig("org.apache.spark.akka.retry.wait", "1.4")),
        "org.apache.spark.rpc.askTimeout" -> Seq(
            AlternateConfig("org.apache.spark.akka.askTimeout", "1.4")),
        "org.apache.spark.rpc.lookupTimeout" -> Seq(
            AlternateConfig("org.apache.spark.akka.lookupTimeout", "1.4")),
        "org.apache.spark.streaming.fileStream.minRememberDuration" -> Seq(
            AlternateConfig("org.apache.spark.streaming.minRememberDuration", "1.5")),
        "org.apache.spark.yarn.max.executor.failures" -> Seq(
            AlternateConfig("org.apache.spark.yarn.max.worker.failures", "1.5")),
        "org.apache.spark.memory.offHeap.enabled" -> Seq(
            AlternateConfig("org.apache.spark.unsafe.offHeap", "1.6")),
        "org.apache.spark.rpc.message.maxSize" -> Seq(
            AlternateConfig("org.apache.spark.akka.frameSize", "1.6")),
        "org.apache.spark.yarn.jars" -> Seq(
            AlternateConfig("org.apache.spark.yarn.jar", "2.0")),
        "org.apache.spark.yarn.access.hadoopFileSystems" -> Seq(
            AlternateConfig("org.apache.spark.yarn.access.namenodes", "2.2"))
    )

    /**
      * A view of `configsWithAlternatives` that makes it more efficient to look up deprecated
      * config keys.
      *
      * Maps the deprecated config name to a 2-tuple (new config name, alternate config info).
      */
    private val allAlternatives: Map[String, (String, AlternateConfig)] = {
        configsWithAlternatives.keys.flatMap { key =>
            configsWithAlternatives(key).map { cfg => (cfg.key -> (key -> cfg)) }
        }.toMap
    }

    /**
      * Return whether the given config should be passed to an executor on start-up.
      *
      * Certain authentication configs are required from the executor when it connects to
      * the scheduler, while the rest of the org.apache.spark configs can be inherited from the driver later.
      */
    def isExecutorStartupConf(name: String): Boolean = {
        (name.startsWith("org.apache.spark.auth") && name != SecurityManager.SPARK_AUTH_SECRET_CONF) ||
                name.startsWith("org.apache.spark.ssl") ||
                name.startsWith("org.apache.spark.rpc") ||
                name.startsWith("org.apache.spark.network") ||
                isSparkPortConf(name)
    }

    /**
      * Return true if the given config matches either `org.apache.spark.*.port` or `org.apache.spark.port.*`.
      */
    def isSparkPortConf(name: String): Boolean = {
        (name.startsWith("org.apache.spark.") && name.endsWith(".port")) || name.startsWith("org.apache.spark.port.")
    }

    /**
      * Looks for available deprecated keys for the given config option, and return the first
      * value available.
      */
    def getDeprecatedConfig(key: String, conf: SparkConf): Option[String] = {
        configsWithAlternatives.get(key).flatMap { alts =>
            alts.collectFirst { case alt if conf.contains(alt.key) =>
                val value = conf.get(alt.key)
                if (alt.translation != null) alt.translation(value) else value
            }
        }
    }

    /**
      * Logs a warning message if the given config key is deprecated.
      */
    def logDeprecationWarning(key: String): Unit = {
        deprecatedConfigs.get(key).foreach { cfg =>
            logWarning(
                s"The configuration key '$key' has been deprecated as of Spark ${cfg.version} and " +
                        s"may be removed in the future. ${cfg.deprecationMessage}")
            return
        }

        allAlternatives.get(key).foreach { case (newKey, cfg) =>
            logWarning(
                s"The configuration key '$key' has been deprecated as of Spark ${cfg.version} and " +
                        s"may be removed in the future. Please use the new key '$newKey' instead.")
            return
        }
        if (key.startsWith("org.apache.spark.akka") || key.startsWith("org.apache.spark.ssl.akka")) {
            logWarning(
                s"The configuration key $key is not supported any more " +
                        s"because Spark doesn't use Akka since 2.0")
        }
    }

    /**
      * Holds information about keys that have been deprecated and do not have a replacement.
      *
      * @param key                The deprecated key.
      * @param version            Version of Spark where key was deprecated.
      * @param deprecationMessage Message to include in the deprecation warning.
      */
    private case class DeprecatedConfig(
                                               key: String,
                                               version: String,
                                               deprecationMessage: String)

    /**
      * Information about an alternate configuration key that has been deprecated.
      *
      * @param key         The deprecated config key.
      * @param version     The Spark version in which the key was deprecated.
      * @param translation A translation function for converting old config values into new ones.
      */
    private case class AlternateConfig(
                                              key: String,
                                              version: String,
                                              translation: String => String = null)

}
