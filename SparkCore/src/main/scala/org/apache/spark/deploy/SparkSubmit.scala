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

package org.apache.spark.deploy

import java.io.{File, IOException}
import java.lang.reflect.{InvocationTargetException, Modifier, UndeclaredThrowableException}
import java.net.URL
import java.nio.file.Files
import java.security.PrivilegedExceptionAction
import java.text.ParseException

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.ivy.Ivy
import org.apache.ivy.core.LogOptions
import org.apache.ivy.core.module.descriptor._
import org.apache.ivy.core.module.id.{ArtifactId, ModuleId, ModuleRevisionId}
import org.apache.ivy.core.report.ResolveReport
import org.apache.ivy.core.resolve.ResolveOptions
import org.apache.ivy.core.retrieve.RetrieveOptions
import org.apache.ivy.core.settings.IvySettings
import org.apache.ivy.plugins.matcher.GlobPatternMatcher
import org.apache.ivy.plugins.repository.file.FileRepository
import org.apache.ivy.plugins.resolver.{ChainResolver, FileSystemResolver, IBiblioResolver}
import org.apache.spark._
import org.apache.spark.api.r.RUtils
import org.apache.spark.deploy.rest._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.util._

import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, HashMap, Map}
import scala.util.Properties

/**
  * 是否提交、终止或请求应用程序的状态。后两种操作目前仅支持单机和Mesos集群。
  *
  * Whether to submit, kill, or request the status of an application.
  * The latter two operations are currently supported only for standalone and Mesos cluster modes.
  */
private[deploy] object SparkSubmitAction extends Enumeration {
    // 提交作业的行为方式枚举
    type SparkSubmitAction = Value
    val SUBMIT, KILL, REQUEST_STATUS = Value
}

/**
  * 启动Spark应用程序的主要网关。
  *
  * Main gateway of launching a Spark application.
  *
  * 这个程序负责设置具有相关Spark依赖项的类路径，并在Spark支持的不同集群管理器和部署模式上提供一个层。
  *
  * This program handles setting up the classpath with relevant Spark dependencies and provides
  * a layer over the different cluster managers and deploy modes that Spark supports.
  */
object SparkSubmit extends CommandLineUtils {

    // 集群管理模式
    // Cluster managers
    private val YARN = 1
    private val STANDALONE = 2
    private val MESOS = 4
    private val LOCAL = 8
    private val ALL_CLUSTER_MGRS = YARN | STANDALONE | MESOS | LOCAL

    // 部署模式
    // Deploy modes
    private val CLIENT = 1
    private val CLUSTER = 2
    private val ALL_DEPLOY_MODES = CLIENT | CLUSTER

    // 一些非jar基础资源名，包括shell等。
    // Special primary resource names that represent shells rather than application jars.
    private val SPARK_SHELL = "org.apache.spark-shell"
    private val PYSPARK_SHELL = "pyspark-shell"
    private val SPARKR_SHELL = "sparkr-shell"
    private val SPARKR_PACKAGE_ARCHIVE = "sparkr.zip"
    private val R_PACKAGE_ARCHIVE = "rpkg.zip"

    // 未发现类退出异常。
    private val CLASS_NOT_FOUND_EXIT_STATUS = 101

    /**
      * 程序执行的主入口
      */
    override def main(args: Array[String]): Unit = {
        // 获取参数解析列表
        val appArgs = new SparkSubmitArguments(args)
        // 用于确认是否打印一些JVM信息，默认为false。
        if (appArgs.verbose) {
            // scalastyle:off println
            printStream.println(appArgs)
            // scalastyle:on println
        }
        // 提交作业的行为方式
        appArgs.action match {
            case SparkSubmitAction.SUBMIT => submit(appArgs) // 提交spark作业
            case SparkSubmitAction.KILL => kill(appArgs)
            case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs)
        }
    }

    // scalastyle:on println

    // scalastyle:off println
    // 运行spark-shell命令时候,输出的一些版本信息
    private[spark] def printVersionAndExit(): Unit = {
        printStream.println(
            """Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
                        """.format(SPARK_VERSION))
        printStream.println("Using Scala %s, %s, %s".format(
            Properties.versionString, Properties.javaVmName, Properties.javaVersion))
        printStream.println("Branch %s".format(SPARK_BRANCH))
        printStream.println("Compiled by user %s on %s".format(SPARK_BUILD_USER, SPARK_BUILD_DATE))
        printStream.println("Revision %s".format(SPARK_REVISION))
        printStream.println("Url %s".format(SPARK_REPO_URL))
        printStream.println("Type --help for more information.")
        exitFn(0)
    }

    /**
      * 使用REST协议杀死一个已存在的提交。
      * Kill an existing submission using the REST protocol. Standalone and Mesos cluster mode only.
      */
    private def kill(args: SparkSubmitArguments): Unit = {
        new RestSubmissionClient(args.master)
            .killSubmission(args.submissionToKill)
    }

    /**
      * 使用REST协议请求已存在提交的状态。
      * Request the status of an existing submission using the REST protocol.
      * Standalone and Mesos cluster mode only.
      */
    private def requestStatus(args: SparkSubmitArguments): Unit = {
        new RestSubmissionClient(args.master)
            .requestSubmissionStatus(args.submissionToRequestStatusFor)
    }

    /**
      * 使用提供的参数提交应用程序。
      *
      * Submit the application using the provided parameters.
      *
      * 这个过程分两步进行。首先，我们通过设置适当的类路径、系统属性和应用程序参数来准备启动环境，以便基于集群管理器和部署模式运行子主类。
      * 其次，我们使用这个启动环境来调用子主类的main方法。
      *
      * This runs in two steps. First, we prepare the launch environment by setting up
      * the appropriate classpath, system properties, and application arguments for
      * running the child main class based on the cluster manager and the deploy mode.
      * Second, we use this launch environment to invoke the main method of the child
      * main class.
      */
    @tailrec
    private def submit(args: SparkSubmitArguments): Unit = {
        // 提交前的参数准备
        val (childArgs, childClasspath, sysProps, childMainClass) = prepareSubmitEnvironment(args)

        def doRunMain(): Unit = {
            if (args.proxyUser != null) {
                // 获取单向代理对象
                val proxyUser = UserGroupInformation.createProxyUser(args.proxyUser,
                    UserGroupInformation.getCurrentUser())
                try {
                    proxyUser.doAs(new PrivilegedExceptionAction[Unit]() {
                        override def run(): Unit = {
                            // 开始调用执行
                            runMain(childArgs, childClasspath, sysProps, childMainClass, args.verbose)
                        }
                    })
                } catch {
                    case e: Exception =>
                        // Hadoop's AuthorizationException suppresses the exception's stack trace, which
                        // makes the message printed to the output by the JVM not very helpful. Instead,
                        // detect exceptions with empty stack traces here, and treat them differently.
                        if (e.getStackTrace().length == 0) {
                            // scalastyle:off println
                            printStream.println(s"ERROR: ${e.getClass().getName()}: ${e.getMessage()}")
                            // scalastyle:on println
                            exitFn(1)
                        } else {
                            throw e
                        }
                }
            } else {
                runMain(childArgs, childClasspath, sysProps, childMainClass, args.verbose)
            }
        }

        // In standalone cluster mode, there are two submission gateways:
        //   (1) The traditional RPC gateway using o.a.s.deploy.Client as a wrapper
        //   (2) The new REST-based gateway introduced in Spark 1.3
        // The latter is the default behavior as of Spark 1.3, but Spark submit will fail over
        // to use the legacy gateway if the master endpoint turns out to be not a REST server.
        // standalone模式
        if (args.isStandaloneCluster && args.useRest) {
            try {
                // scalastyle:off println
                printStream.println("Running Spark using the REST application submission protocol.")
                // scalastyle:on println
                doRunMain()
            } catch {
                // Fail over to use the legacy submission gateway
                case e: SubmitRestConnectionException =>
                    printWarning(s"Master endpoint ${args.master} was not a REST server. " +
                        "Falling back to legacy submission gateway instead.")
                    args.useRest = false
                    submit(args)
            }
            // In all other modes, just run the main class as prepared
        } else {
            // 其它cluster模式
            doRunMain()
        }
    }

    /**
      * 准备提交申请的环境。
      *
      * Prepare the environment for submitting an application.
      * This returns a 4-tuple:
      * (1) the arguments for the child process,子进程的参数
      * (2) a list of classpath entries for the child, 子程序的类路径项列表
      * (3) a map of system properties, and  系统属性的映射
      * (4) the main class for the child 子程序的主类
      * Exposed for testing.
      */
    private[deploy] def prepareSubmitEnvironment(args: SparkSubmitArguments)
    : (Seq[String], Seq[String], Map[String, String], String) = {
        // Return values
        // 返回值
        val childArgs = new ArrayBuffer[String]()
        val childClasspath = new ArrayBuffer[String]()
        val sysProps = new HashMap[String, String]()
        var childMainClass = ""

        // Set the cluster manager
        // 设置集群模式
        val clusterManager: Int = args.master match {
            case "yarn" => YARN
            case "yarn-client" | "yarn-cluster" =>
                printWarning(s"Master ${args.master} is deprecated since 2.0." +
                    " Please use master \"yarn\" with specified deploy mode instead.")
                YARN
            case m if m.startsWith("org/apache/spark") => STANDALONE
            case m if m.startsWith("mesos") => MESOS
            case m if m.startsWith("local") => LOCAL
            case _ =>
                printErrorAndExit("Master must either be yarn or start with org.apache.spark, mesos, local")
                -1
        }

        // Set the deploy mode; default is client mode
        // 设置部署模式，默认是client模式
        var deployMode: Int = args.deployMode match {
            case "client" | null => CLIENT
            case "cluster" => CLUSTER
            case _ => printErrorAndExit("Deploy mode must be either client or cluster"); -1
        }

        // Because the deprecated way of specifying "yarn-cluster" and "yarn-client" encapsulate both
        // the master and deploy mode, we have some logic to infer the master and deploy mode
        // from each other if only one is specified, or exit early if they are at odds.

        // 因为不推荐使用指定“yarn cluster”和“yarn client”方法封装的master和deploy模式,
        // 所以如果只指定了一种模式，我们就有一些逻辑来推断master和deploy模式，如果它们不一致，我们就提前退出。
        if (clusterManager == YARN) {
            (args.master, args.deployMode) match {
                case ("yarn-cluster", null) =>
                    deployMode = CLUSTER
                    args.master = "yarn"
                case ("yarn-cluster", "client") =>
                    printErrorAndExit("Client deploy mode is not compatible with master \"yarn-cluster\"")
                case ("yarn-client", "cluster") =>
                    printErrorAndExit("Cluster deploy mode is not compatible with master \"yarn-client\"")
                case (_, mode) =>
                    args.master = "yarn"
            }

            // Make sure YARN is included in our build if we're trying to use it
            // 在使用YARN之前,先确保在我们项目的构建中.
            if (!Utils.classIsLoadable("org.apache.org.apache.spark.deploy.yarn.Client") && !Utils.isTesting) {
                printErrorAndExit(
                    "Could not load YARN classes. " +
                        "This copy of Spark may not have been compiled with YARN support.")
            }
        }

        // Update args.deployMode if it is null. It will be passed down as a Spark property later.
        // 更新参数部署模式如果为空。以后它将作为Spark属性传递下去。
        (args.deployMode, deployMode) match {
            case (null, CLIENT) => args.deployMode = "client"
            case (null, CLUSTER) => args.deployMode = "cluster"
            case _ =>
        }
        val isYarnCluster = clusterManager == YARN && deployMode == CLUSTER
        val isMesosCluster = clusterManager == MESOS && deployMode == CLUSTER

        // Resolve maven dependencies if there are any and add classpath to jars. Add them to py-files
        // too for packages that include Python code
        // 如果存在maven依赖项，请解析它们，并将类路径添加到jar中。对于包含Python代码的包，也可以将它们添加到py文件中
        val exclusions: Seq[String] =
        if (!StringUtils.isBlank(args.packagesExclusions)) {
            args.packagesExclusions.split(",")
        } else {
            Nil
        }

        // Create the IvySettings, either load from file or build defaults
        // 创建IvySettings，从文件加载或生成默认值
        val ivySettings = args.sparkProperties.get("org.apache.spark.jars.ivySettings").map { ivySettingsFile =>
            SparkSubmitUtils.loadIvySettings(ivySettingsFile, Option(args.repositories),
                Option(args.ivyRepoPath))
        }.getOrElse {
            SparkSubmitUtils.buildIvySettings(Option(args.repositories), Option(args.ivyRepoPath))
        }

        val resolvedMavenCoordinates = SparkSubmitUtils.resolveMavenCoordinates(args.packages,
            ivySettings, exclusions = exclusions)
        if (!StringUtils.isBlank(resolvedMavenCoordinates)) {
            args.jars = mergeFileLists(args.jars, resolvedMavenCoordinates)
            if (args.isPython) {
                args.pyFiles = mergeFileLists(args.pyFiles, resolvedMavenCoordinates)
            }
        }

        // install any R packages that may have been passed through --jars or --packages.
        // Spark Packages may contain R source code inside the jar.
        // 安装任何可能通过-jars或-packages传递的R包。Spark包可能在jar中包含R源代码。
        if (args.isR && !StringUtils.isBlank(args.jars)) {
            RPackageUtils.checkAndBuildRPackage(args.jars, printStream, args.verbose)
        }

        // In client mode, download remote files.
        // 在客户端模式下，下载远程文件。
        if (deployMode == CLIENT) {
            val hadoopConf = new HadoopConfiguration()
            args.primaryResource = Option(args.primaryResource).map(downloadFile(_, hadoopConf)).orNull
            args.jars = Option(args.jars).map(downloadFileList(_, hadoopConf)).orNull
            args.pyFiles = Option(args.pyFiles).map(downloadFileList(_, hadoopConf)).orNull
            args.files = Option(args.files).map(downloadFileList(_, hadoopConf)).orNull
        }

        // Require all python files to be local, so we can add them to the PYTHONPATH
        // In YARN cluster mode, python files are distributed as regular files, which can be non-local.
        // In Mesos cluster mode, non-local python files are automatically downloaded by Mesos.

        // 要求所有python文件都是本地的，所以我们可以将它们添加到PYTHONPATH
        // 在YARN集群模式下，python文件作为常规文件分发，这些文件可以是非本地的。
        // 在Mesos集群模式下，Mesos会自动下载非本地python文件。
        if (args.isPython && !isYarnCluster && !isMesosCluster) {
            if (Utils.nonLocalPaths(args.primaryResource).nonEmpty) {
                printErrorAndExit(s"Only local python files are supported: ${args.primaryResource}")
            }
            val nonLocalPyFiles = Utils.nonLocalPaths(args.pyFiles).mkString(",")
            if (nonLocalPyFiles.nonEmpty) {
                printErrorAndExit(s"Only local additional python files are supported: $nonLocalPyFiles")
            }
        }

        // Require all R files to be local
        if (args.isR && !isYarnCluster && !isMesosCluster) {
            if (Utils.nonLocalPaths(args.primaryResource).nonEmpty) {
                printErrorAndExit(s"Only local R files are supported: ${args.primaryResource}")
            }
        }

        // The following modes are not supported or applicable
        (clusterManager, deployMode) match {
            case (STANDALONE, CLUSTER) if args.isPython =>
                printErrorAndExit("Cluster deploy mode is currently not supported for python " +
                    "applications on standalone clusters.")
            case (STANDALONE, CLUSTER) if args.isR =>
                printErrorAndExit("Cluster deploy mode is currently not supported for R " +
                    "applications on standalone clusters.")
            case (LOCAL, CLUSTER) =>
                printErrorAndExit("Cluster deploy mode is not compatible with master \"local\"")
            case (_, CLUSTER) if isShell(args.primaryResource) =>
                printErrorAndExit("Cluster deploy mode is not applicable to Spark shells.")
            case (_, CLUSTER) if isSqlShell(args.mainClass) =>
                printErrorAndExit("Cluster deploy mode is not applicable to Spark SQL shell.")
            case (_, CLUSTER) if isThriftServer(args.mainClass) =>
                printErrorAndExit("Cluster deploy mode is not applicable to Spark Thrift server.")
            case _ =>
        }

        // If we're running a python app, set the main class to our specific python runner
        if (args.isPython && deployMode == CLIENT) {
            if (args.primaryResource == PYSPARK_SHELL) {
                args.mainClass = "org.apache.org.apache.spark.api.python.PythonGatewayServer"
            } else {
                // If a python file is provided, add it to the child arguments and list of files to deploy.
                // Usage: PythonAppRunner <main python file> <extra python files> [app arguments]
                args.mainClass = "org.apache.org.apache.spark.deploy.PythonRunner"
                args.childArgs = ArrayBuffer(args.primaryResource, args.pyFiles) ++ args.childArgs
                if (clusterManager != YARN) {
                    // The YARN backend distributes the primary file differently, so don't merge it.
                    args.files = mergeFileLists(args.files, args.primaryResource)
                }
            }
            if (clusterManager != YARN) {
                // The YARN backend handles python files differently, so don't merge the lists.
                args.files = mergeFileLists(args.files, args.pyFiles)
            }
            if (args.pyFiles != null) {
                sysProps("org.apache.spark.submit.pyFiles") = args.pyFiles
            }
        }

        // In YARN mode for an R app, add the SparkR package archive and the R package
        // archive containing all of the built R libraries to archives so that they can
        // be distributed with the job
        if (args.isR && clusterManager == YARN) {
            val sparkRPackagePath = RUtils.localSparkRPackagePath
            if (sparkRPackagePath.isEmpty) {
                printErrorAndExit("SPARK_HOME does not exist for R application in YARN mode.")
            }
            val sparkRPackageFile = new File(sparkRPackagePath.get, SPARKR_PACKAGE_ARCHIVE)
            if (!sparkRPackageFile.exists()) {
                printErrorAndExit(s"$SPARKR_PACKAGE_ARCHIVE does not exist for R application in YARN mode.")
            }
            val sparkRPackageURI = Utils.resolveURI(sparkRPackageFile.getAbsolutePath).toString

            // Distribute the SparkR package.
            // Assigns a symbol link name "sparkr" to the shipped package.
            args.archives = mergeFileLists(args.archives, sparkRPackageURI + "#sparkr")

            // Distribute the R package archive containing all the built R packages.
            if (!RUtils.rPackages.isEmpty) {
                val rPackageFile =
                    RPackageUtils.zipRLibraries(new File(RUtils.rPackages.get), R_PACKAGE_ARCHIVE)
                if (!rPackageFile.exists()) {
                    printErrorAndExit("Failed to zip all the built R packages.")
                }

                val rPackageURI = Utils.resolveURI(rPackageFile.getAbsolutePath).toString
                // Assigns a symbol link name "rpkg" to the shipped package.
                args.archives = mergeFileLists(args.archives, rPackageURI + "#rpkg")
            }
        }

        // TODO: Support distributing R packages with standalone cluster
        if (args.isR && clusterManager == STANDALONE && !RUtils.rPackages.isEmpty) {
            printErrorAndExit("Distributing R packages with standalone cluster is not supported.")
        }

        // TODO: Support distributing R packages with mesos cluster
        if (args.isR && clusterManager == MESOS && !RUtils.rPackages.isEmpty) {
            printErrorAndExit("Distributing R packages with mesos cluster is not supported.")
        }

        // If we're running an R app, set the main class to our specific R runner
        if (args.isR && deployMode == CLIENT) {
            if (args.primaryResource == SPARKR_SHELL) {
                args.mainClass = "org.apache.org.apache.spark.api.r.RBackend"
            } else {
                // If an R file is provided, add it to the child arguments and list of files to deploy.
                // Usage: RRunner <main R file> [app arguments]
                args.mainClass = "org.apache.org.apache.spark.deploy.RRunner"
                args.childArgs = ArrayBuffer(args.primaryResource) ++ args.childArgs
                args.files = mergeFileLists(args.files, args.primaryResource)
            }
        }

        if (isYarnCluster && args.isR) {
            // In yarn-cluster mode for an R app, add primary resource to files
            // that can be distributed with the job
            args.files = mergeFileLists(args.files, args.primaryResource)
        }

        // Special flag to avoid deprecation warnings at the client
        sysProps("SPARK_SUBMIT") = "true"

        // A list of rules to map each argument to system properties or command-line options in
        // each deploy mode; we iterate through these below
        val options = List[OptionAssigner](

            // All cluster managers
            OptionAssigner(args.master, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, sysProp = "org.apache.spark.master"),
            OptionAssigner(args.deployMode, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
                sysProp = "org.apache.spark.submit.deployMode"),
            OptionAssigner(args.name, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES, sysProp = "org.apache.spark.app.name"),
            OptionAssigner(args.ivyRepoPath, ALL_CLUSTER_MGRS, CLIENT, sysProp = "org.apache.spark.jars.ivy"),
            OptionAssigner(args.driverMemory, ALL_CLUSTER_MGRS, CLIENT,
                sysProp = "org.apache.spark.driver.memory"),
            OptionAssigner(args.driverExtraClassPath, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
                sysProp = "org.apache.spark.driver.extraClassPath"),
            OptionAssigner(args.driverExtraJavaOptions, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
                sysProp = "org.apache.spark.driver.extraJavaOptions"),
            OptionAssigner(args.driverExtraLibraryPath, ALL_CLUSTER_MGRS, ALL_DEPLOY_MODES,
                sysProp = "org.apache.spark.driver.extraLibraryPath"),

            // Yarn only
            OptionAssigner(args.queue, YARN, ALL_DEPLOY_MODES, sysProp = "org.apache.spark.yarn.queue"),
            OptionAssigner(args.numExecutors, YARN, ALL_DEPLOY_MODES,
                sysProp = "org.apache.spark.executor.instances"),
            OptionAssigner(args.jars, YARN, ALL_DEPLOY_MODES, sysProp = "org.apache.spark.yarn.dist.jars"),
            OptionAssigner(args.files, YARN, ALL_DEPLOY_MODES, sysProp = "org.apache.spark.yarn.dist.files"),
            OptionAssigner(args.archives, YARN, ALL_DEPLOY_MODES, sysProp = "org.apache.spark.yarn.dist.archives"),
            OptionAssigner(args.principal, YARN, ALL_DEPLOY_MODES, sysProp = "org.apache.spark.yarn.principal"),
            OptionAssigner(args.keytab, YARN, ALL_DEPLOY_MODES, sysProp = "org.apache.spark.yarn.keytab"),

            // Other options
            OptionAssigner(args.executorCores, STANDALONE | YARN, ALL_DEPLOY_MODES,
                sysProp = "org.apache.spark.executor.cores"),
            OptionAssigner(args.executorMemory, STANDALONE | MESOS | YARN, ALL_DEPLOY_MODES,
                sysProp = "org.apache.spark.executor.memory"),
            OptionAssigner(args.totalExecutorCores, STANDALONE | MESOS, ALL_DEPLOY_MODES,
                sysProp = "org.apache.spark.cores.max"),
            OptionAssigner(args.files, LOCAL | STANDALONE | MESOS, ALL_DEPLOY_MODES,
                sysProp = "org.apache.spark.files"),
            OptionAssigner(args.jars, LOCAL, CLIENT, sysProp = "org.apache.spark.jars"),
            OptionAssigner(args.jars, STANDALONE | MESOS, ALL_DEPLOY_MODES, sysProp = "org.apache.spark.jars"),
            OptionAssigner(args.driverMemory, STANDALONE | MESOS | YARN, CLUSTER,
                sysProp = "org.apache.spark.driver.memory"),
            OptionAssigner(args.driverCores, STANDALONE | MESOS | YARN, CLUSTER,
                sysProp = "org.apache.spark.driver.cores"),
            OptionAssigner(args.supervise.toString, STANDALONE | MESOS, CLUSTER,
                sysProp = "org.apache.spark.driver.supervise"),
            OptionAssigner(args.ivyRepoPath, STANDALONE, CLUSTER, sysProp = "org.apache.spark.jars.ivy")
        )

        // In client mode, launch the application main class directly
        // In addition, add the main application jar and any added jars (if any) to the classpath
        // Also add the main application jar and any added jars to classpath in case YARN client
        // requires these jars.
        if (deployMode == CLIENT || isYarnCluster) {
            childMainClass = args.mainClass
            if (isUserJar(args.primaryResource)) {
                childClasspath += args.primaryResource
            }
            if (args.jars != null) {
                childClasspath ++= args.jars.split(",")
            }
        }

        if (deployMode == CLIENT) {
            if (args.childArgs != null) {
                childArgs ++= args.childArgs
            }
        }

        // Map all arguments to command-line options or system properties for our chosen mode
        for (opt <- options) {
            if (opt.value != null &&
                (deployMode & opt.deployMode) != 0 &&
                (clusterManager & opt.clusterManager) != 0) {
                if (opt.clOption != null) {
                    childArgs += (opt.clOption, opt.value)
                }
                if (opt.sysProp != null) {
                    sysProps.put(opt.sysProp, opt.value)
                }
            }
        }

        // Add the application jar automatically so the user doesn't have to call sc.addJar
        // For YARN cluster mode, the jar is already distributed on each node as "app.jar"
        // For python and R files, the primary resource is already distributed as a regular file
        if (!isYarnCluster && !args.isPython && !args.isR) {
            var jars = sysProps.get("org.apache.spark.jars").map(x => x.split(",").toSeq).getOrElse(Seq.empty)
            if (isUserJar(args.primaryResource)) {
                jars = jars ++ Seq(args.primaryResource)
            }
            sysProps.put("org.apache.spark.jars", jars.mkString(","))
        }

        // In standalone cluster mode, use the REST client to submit the application (Spark 1.3+).
        // All Spark parameters are expected to be passed to the client through system properties.
        if (args.isStandaloneCluster) {
            if (args.useRest) {
                childMainClass = "org.apache.org.apache.spark.deploy.rest.RestSubmissionClient"
                childArgs += (args.primaryResource, args.mainClass)
            } else {
                // In legacy standalone cluster mode, use Client as a wrapper around the user class
                childMainClass = "org.apache.org.apache.spark.deploy.Client"
                if (args.supervise) {
                    childArgs += "--supervise"
                }
                Option(args.driverMemory).foreach { m => childArgs += ("--memory", m) }
                Option(args.driverCores).foreach { c => childArgs += ("--cores", c) }
                childArgs += "launch"
                childArgs += (args.master, args.primaryResource, args.mainClass)
            }
            if (args.childArgs != null) {
                childArgs ++= args.childArgs
            }
        }

        // Let YARN know it's a pyspark app, so it distributes needed libraries.
        if (clusterManager == YARN) {
            if (args.isPython) {
                sysProps.put("org.apache.spark.yarn.isPython", "true")
            }

            if (args.pyFiles != null) {
                sysProps("org.apache.spark.submit.pyFiles") = args.pyFiles
            }
        }

        // assure a keytab is available from any place in a JVM
        if (clusterManager == YARN || clusterManager == LOCAL) {
            if (args.principal != null) {
                require(args.keytab != null, "Keytab must be specified when principal is specified")
                if (!new File(args.keytab).exists()) {
                    throw new SparkException(s"Keytab file: ${args.keytab} does not exist")
                } else {
                    // Add keytab and principal configurations in sysProps to make them available
                    // for later use; e.g. in org.apache.spark sql, the isolated class loader used to talk
                    // to HiveMetastore will use these settings. They will be set as Java system
                    // properties and then loaded by SparkConf
                    sysProps.put("org.apache.spark.yarn.keytab", args.keytab)
                    sysProps.put("org.apache.spark.yarn.principal", args.principal)

                    UserGroupInformation.loginUserFromKeytab(args.principal, args.keytab)
                }
            }
        }

        // In yarn-cluster mode, use yarn.Client as a wrapper around the user class
        if (isYarnCluster) {
            childMainClass = "org.apache.org.apache.spark.deploy.yarn.Client"
            if (args.isPython) {
                childArgs += ("--primary-py-file", args.primaryResource)
                childArgs += ("--class", "org.apache.org.apache.spark.deploy.PythonRunner")
            } else if (args.isR) {
                val mainFile = new Path(args.primaryResource).getName
                childArgs += ("--primary-r-file", mainFile)
                childArgs += ("--class", "org.apache.org.apache.spark.deploy.RRunner")
            } else {
                if (args.primaryResource != SparkLauncher.NO_RESOURCE) {
                    childArgs += ("--jar", args.primaryResource)
                }
                childArgs += ("--class", args.mainClass)
            }
            if (args.childArgs != null) {
                args.childArgs.foreach { arg => childArgs += ("--arg", arg) }
            }
        }

        if (isMesosCluster) {
            assert(args.useRest, "Mesos cluster mode is only supported through the REST submission API")
            childMainClass = "org.apache.org.apache.spark.deploy.rest.RestSubmissionClient"
            if (args.isPython) {
                // Second argument is main class
                childArgs += (args.primaryResource, "")
                if (args.pyFiles != null) {
                    sysProps("org.apache.spark.submit.pyFiles") = args.pyFiles
                }
            } else if (args.isR) {
                // Second argument is main class
                childArgs += (args.primaryResource, "")
            } else {
                childArgs += (args.primaryResource, args.mainClass)
            }
            if (args.childArgs != null) {
                childArgs ++= args.childArgs
            }
        }

        // Load any properties specified through --conf and the default properties file
        for ((k, v) <- args.sparkProperties) {
            sysProps.getOrElseUpdate(k, v)
        }

        // Ignore invalid org.apache.spark.driver.host in cluster modes.
        if (deployMode == CLUSTER) {
            sysProps -= "org.apache.spark.driver.host"
        }

        // Resolve paths in certain org.apache.spark properties
        val pathConfigs = Seq(
            "org.apache.spark.jars",
            "org.apache.spark.files",
            "org.apache.spark.yarn.dist.files",
            "org.apache.spark.yarn.dist.archives",
            "org.apache.spark.yarn.dist.jars")
        pathConfigs.foreach { config =>
            // Replace old URIs with resolved URIs, if they exist
            sysProps.get(config).foreach { oldValue =>
                sysProps(config) = Utils.resolveURIs(oldValue)
            }
        }

        // Resolve and format python file paths properly before adding them to the PYTHONPATH.
        // The resolving part is redundant in the case of --py-files, but necessary if the user
        // explicitly sets `org.apache.spark.submit.pyFiles` in his/her default properties file.
        sysProps.get("org.apache.spark.submit.pyFiles").foreach { pyFiles =>
            val resolvedPyFiles = Utils.resolveURIs(pyFiles)
            val formattedPyFiles = if (!isYarnCluster && !isMesosCluster) {
                PythonRunner.formatPaths(resolvedPyFiles).mkString(",")
            } else {
                // Ignoring formatting python path in yarn and mesos cluster mode, these two modes
                // support dealing with remote python files, they could distribute and add python files
                // locally.
                resolvedPyFiles
            }
            sysProps("org.apache.spark.submit.pyFiles") = formattedPyFiles
        }

        (childArgs, childClasspath, sysProps, childMainClass)
    }

    /**
      * 使用提供的加载环境运行子类的主方法
      * Run the main method of the child class using the provided launch environment.
      *
      * Note that this main class will not be the one provided by the user if we're
      * running cluster deploy mode or python applications.
      */
    private def runMain(
                           childArgs: Seq[String],
                           childClasspath: Seq[String],
                           sysProps: Map[String, String],
                           childMainClass: String,
                           verbose: Boolean): Unit = {
        // scalastyle:off println
        // 打印一些JVM信息
        if (verbose) {
            printStream.println(s"Main class:\n$childMainClass")
            printStream.println(s"Arguments:\n${childArgs.mkString("\n")}")
            // sysProps may contain sensitive information, so redact before printing
            printStream.println(s"System properties:\n${Utils.redact(sysProps).mkString("\n")}")
            printStream.println(s"Classpath elements:\n${childClasspath.mkString("\n")}")
            printStream.println("\n")
        }
        // scalastyle:on println

        val loader =
            if (sysProps.getOrElse("org.apache.spark.driver.userClassPathFirst", "false").toBoolean) {
                new ChildFirstURLClassLoader(new Array[URL](0),
                    Thread.currentThread.getContextClassLoader)
            } else {
                new MutableURLClassLoader(new Array[URL](0),
                    Thread.currentThread.getContextClassLoader)
            }
        Thread.currentThread.setContextClassLoader(loader)

        for (jar <- childClasspath) {
            addJarToClasspath(jar, loader)
        }

        for ((key, value) <- sysProps) {
            System.setProperty(key, value)
        }

        var mainClass: Class[_] = null

        try {
            mainClass = Utils.classForName(childMainClass)
        } catch {
            case e: ClassNotFoundException =>
                e.printStackTrace(printStream)
                if (childMainClass.contains("thriftserver")) {
                    // scalastyle:off println
                    printStream.println(s"Failed to load main class $childMainClass.")
                    printStream.println("You need to build Spark with -Phive and -Phive-thriftserver.")
                    // scalastyle:on println
                }
                System.exit(CLASS_NOT_FOUND_EXIT_STATUS)
            case e: NoClassDefFoundError =>
                e.printStackTrace(printStream)
                if (e.getMessage.contains("org/apache/hadoop/hive")) {
                    // scalastyle:off println
                    printStream.println(s"Failed to load hive class.")
                    printStream.println("You need to build Spark with -Phive and -Phive-thriftserver.")
                    // scalastyle:on println
                }
                System.exit(CLASS_NOT_FOUND_EXIT_STATUS)
        }

        // SPARK-4170
        if (classOf[scala.App].isAssignableFrom(mainClass)) {
            printWarning("Subclasses of scala.App may not work correctly. Use a main() method instead.")
        }

        val mainMethod = mainClass.getMethod("main", new Array[String](0).getClass)
        if (!Modifier.isStatic(mainMethod.getModifiers)) {
            throw new IllegalStateException("The main method in the given main class must be static")
        }

        @tailrec
        def findCause(t: Throwable): Throwable = t match {
            case e: UndeclaredThrowableException =>
                if (e.getCause() != null) findCause(e.getCause()) else e
            case e: InvocationTargetException =>
                if (e.getCause() != null) findCause(e.getCause()) else e
            case e: Throwable =>
                e
        }

        try {
            // 反射调用，真正开始执行用户提交的app
            mainMethod.invoke(null, childArgs.toArray)
        } catch {
            case t: Throwable =>
                findCause(t) match {
                    case SparkUserAppException(exitCode) =>
                        System.exit(exitCode)

                    case t: Throwable =>
                        throw t
                }
        }
    }

    /**
      * 添加jar包到环境变量里。
      */
    private def addJarToClasspath(localJar: String, loader: MutableURLClassLoader) {
        val uri = Utils.resolveURI(localJar)
        uri.getScheme match {
            case "file" | "local" =>
                val file = new File(uri.getPath)
                if (file.exists()) {
                    loader.addURL(file.toURI.toURL)
                } else {
                    printWarning(s"Local jar $file does not exist, skipping.")
                }
            case _ =>
                printWarning(s"Skip remote jar $uri.")
        }
    }

    /**
      * 返回给定的主资源是否表示用户jar。
      * Return whether the given primary resource represents a user jar.
      */
    private[deploy] def isUserJar(res: String): Boolean = {
        !isShell(res) && !isPython(res) && !isInternal(res) && !isR(res)
    }

    /**
      * 返回给定的资源是否表示一个shell。
      * Return whether the given primary resource represents a shell.
      */
    private[deploy] def isShell(res: String): Boolean = {
        (res == SPARK_SHELL || res == PYSPARK_SHELL || res == SPARKR_SHELL)
    }

    /**
      * 返回给定的资源是否表示一个sql shell。
      * Return whether the given main class represents a sql shell.
      */
    private[deploy] def isSqlShell(mainClass: String): Boolean = {
        mainClass == "org.apache.org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver"
    }

    /**
      * 返回给定的资源是否表示一个thrift server。
      * Return whether the given main class represents a thrift server.
      */
    private def isThriftServer(mainClass: String): Boolean = {
        mainClass == "org.apache.org.apache.spark.sql.hive.thriftserver.HiveThriftServer2"
    }

    /**
      * 返回给定的主资源是否需要运行python。
      * Return whether the given primary resource requires running python.
      */
    private[deploy] def isPython(res: String): Boolean = {
        res != null && res.endsWith(".py") || res == PYSPARK_SHELL
    }

    /**
      * 返回给定的主资源是否需要运行R。
      * Return whether the given primary resource requires running R.
      */
    private[deploy] def isR(res: String): Boolean = {
        res != null && res.endsWith(".R") || res == SPARKR_SHELL
    }

    private[deploy] def isInternal(res: String): Boolean = {
        res == SparkLauncher.NO_RESOURCE
    }

    /**
      * 将一系列以逗号分隔的文件列表，其中一些列表可能为空，表示没有文件，合并到一个以逗号分隔的字符串中。
      *
      * Merge a sequence of comma-separated file lists, some of which may be null to indicate
      * no files, into a single comma-separated string.
      */
    private def mergeFileLists(lists: String*): String = {
        val merged = lists.filterNot(StringUtils.isBlank)
            .flatMap(_.split(","))
            .mkString(",")
        if (merged == "") null else merged
    }

    /**
      * 将远程文件列表下载到临时本地文件。
      *
      * Download a list of remote files to temp local files. If the file is local, the original file
      * will be returned.
      *
      * @param fileList A comma separated file list.
      * @return A comma separated local files list.
      */
    private[deploy] def downloadFileList(
                                            fileList: String,
                                            hadoopConf: HadoopConfiguration): String = {
        require(fileList != null, "fileList cannot be null.")
        fileList.split(",").map(downloadFile(_, hadoopConf)).mkString(",")
    }

    /**
      * 将文件从远程下载到本地临时目录。
      *
      * Download a file from the remote to a local temporary directory. If the input path points to
      * a local path, returns it with no operation.
      */
    private[deploy] def downloadFile(path: String, hadoopConf: HadoopConfiguration): String = {
        require(path != null, "path cannot be null.")
        val uri = Utils.resolveURI(path)
        uri.getScheme match {
            case "file" | "local" =>
                path

            case _ =>
                val fs = FileSystem.get(uri, hadoopConf)
                val tmpFile = new File(Files.createTempDirectory("tmp").toFile, uri.getPath)
                // scalastyle:off println
                printStream.println(s"Downloading ${uri.toString} to ${tmpFile.getAbsolutePath}.")
                // scalastyle:on println
                fs.copyToLocalFile(new Path(uri), new Path(tmpFile.getAbsolutePath))
                Utils.resolveURI(tmpFile.getAbsolutePath).toString
        }
    }
}

/**
  * 提供在SparkSubmit中使用的实用函数。
  *
  * Provides utility functions to be used inside SparkSubmit.
  */
private[spark] object SparkSubmitUtils {

    // Exposed for testing
    var printStream = SparkSubmit.printStream

    /**
      * 使用带有默认解析器的选项构建Ivy设置。
      *
      * Build Ivy Settings using options with default resolvers
      *
      * @param remoteRepos Comma-delimited string of remote repositories other than maven central
      * @param ivyPath     The path to the local ivy repository
      * @return An IvySettings object
      */
    def buildIvySettings(remoteRepos: Option[String], ivyPath: Option[String]): IvySettings = {
        val ivySettings: IvySettings = new IvySettings
        processIvyPathArg(ivySettings, ivyPath)

        // create a pattern matcher
        ivySettings.addMatcher(new GlobPatternMatcher)
        // create the dependency resolvers
        val repoResolver = createRepoResolvers(ivySettings.getDefaultIvyUserDir)
        ivySettings.addResolver(repoResolver)
        ivySettings.setDefaultResolver(repoResolver.getName)
        processRemoteRepoArg(ivySettings, remoteRepos)
        ivySettings
    }

    /**
      * 从逗号分隔的字符串中提取maven坐标。
      *
      * Extracts maven coordinates from a comma-delimited string
      *
      * @param defaultIvyUserDir The default user path for Ivy
      * @return A ChainResolver used by Ivy to search for and resolve dependencies.
      */
    def createRepoResolvers(defaultIvyUserDir: File): ChainResolver = {
        // We need a chain resolver if we want to check multiple repositories
        val cr = new ChainResolver
        cr.setName("org.apache.spark-list")

        val localM2 = new IBiblioResolver
        localM2.setM2compatible(true)
        localM2.setRoot(m2Path.toURI.toString)
        localM2.setUsepoms(true)
        localM2.setName("local-m2-cache")
        cr.add(localM2)

        val localIvy = new FileSystemResolver
        val localIvyRoot = new File(defaultIvyUserDir, "local")
        localIvy.setLocal(true)
        localIvy.setRepository(new FileRepository(localIvyRoot))
        val ivyPattern = Seq(localIvyRoot.getAbsolutePath, "[organisation]", "[module]", "[revision]",
            "ivys", "ivy.xml").mkString(File.separator)
        localIvy.addIvyPattern(ivyPattern)
        val artifactPattern = Seq(localIvyRoot.getAbsolutePath, "[organisation]", "[module]",
            "[revision]", "[type]s", "[artifact](-[classifier]).[ext]").mkString(File.separator)
        localIvy.addArtifactPattern(artifactPattern)
        localIvy.setName("local-ivy-cache")
        cr.add(localIvy)

        // the biblio resolver resolves POM declared dependencies
        val br: IBiblioResolver = new IBiblioResolver
        br.setM2compatible(true)
        br.setUsepoms(true)
        br.setName("central")
        cr.add(br)

        val sp: IBiblioResolver = new IBiblioResolver
        sp.setM2compatible(true)
        sp.setUsepoms(true)
        sp.setRoot("http://dl.bintray.com/spark-packages/maven")
        sp.setName("org.apache.spark-packages")
        cr.add(sp)
        cr
    }

    /**
      * 本地Maven缓存路径。
      * Path of the local Maven cache.
      */
    private[spark] def m2Path: File = {
        if (Utils.isTesting) {
            // test builds delete the maven cache, and this can cause flakiness
            new File("dummy", ".m2" + File.separator + "repository")
        } else {
            new File(System.getProperty("user.home"), ".m2" + File.separator + "repository")
        }
    }

    /**
      * 如果提供选项，则为缓存位置设置ivy设置。
      *
      * Set ivy settings for location of cache, if option is supplied
      */
    private def processIvyPathArg(ivySettings: IvySettings, ivyPath: Option[String]): Unit = {
        ivyPath.filterNot(_.trim.isEmpty).foreach { alternateIvyDir =>
            ivySettings.setDefaultIvyUserDir(new File(alternateIvyDir))
            ivySettings.setDefaultCache(new File(alternateIvyDir, "cache"))
        }
    }

    /**
      * 添加任何可选的其他远程存储库。
      *
      * Add any optional additional remote repositories
      */
    private def processRemoteRepoArg(ivySettings: IvySettings, remoteRepos: Option[String]): Unit = {
        remoteRepos.filterNot(_.trim.isEmpty).map(_.split(",")).foreach { repositoryList =>
            val cr = new ChainResolver
            cr.setName("user-list")

            // add current default resolver, if any
            Option(ivySettings.getDefaultResolver).foreach(cr.add)

            // add additional repositories, last resolution in chain takes precedence
            repositoryList.zipWithIndex.foreach { case (repo, i) =>
                val brr: IBiblioResolver = new IBiblioResolver
                brr.setM2compatible(true)
                brr.setUsepoms(true)
                brr.setRoot(repo)
                brr.setName(s"repo-${i + 1}")
                cr.add(brr)
                // scalastyle:off println
                printStream.println(s"$repo added as a remote repository with the name: ${brr.getName}")
                // scalastyle:on println
            }

            ivySettings.addResolver(cr)
            ivySettings.setDefaultResolver(cr.getName)
        }
    }

    /**
      * 使用提供的解析器从给定的文件名加载ivy设置。
      *
      * Load Ivy settings from a given filename, using supplied resolvers
      *
      * @param settingsFile Path to Ivy settings file
      * @param remoteRepos  Comma-delimited string of remote repositories other than maven central
      * @param ivyPath      The path to the local ivy repository
      * @return An IvySettings object
      */
    def loadIvySettings(
                           settingsFile: String,
                           remoteRepos: Option[String],
                           ivyPath: Option[String]): IvySettings = {
        val file = new File(settingsFile)
        require(file.exists(), s"Ivy settings file $file does not exist")
        require(file.isFile(), s"Ivy settings file $file is not a normal file")
        val ivySettings: IvySettings = new IvySettings
        try {
            ivySettings.load(file)
        } catch {
            case e@(_: IOException | _: ParseException) =>
                throw new SparkException(s"Failed when loading Ivy settings from $settingsFile", e)
        }
        processIvyPathArg(ivySettings, ivyPath)
        processRemoteRepoArg(ivySettings, remoteRepos)
        ivySettings
    }

    /**
      * 解析通过maven坐标提供的任何依赖项。
      *
      * Resolves any dependencies that were supplied through maven coordinates
      *
      * @param coordinates Comma-delimited string of maven coordinates
      * @param ivySettings An IvySettings containing resolvers to use
      * @param exclusions  Exclusions to apply when resolving transitive dependencies
      * @return The comma-delimited path to the jars of the given maven artifacts including their
      *         transitive dependencies
      */
    def resolveMavenCoordinates(
                                   coordinates: String,
                                   ivySettings: IvySettings,
                                   exclusions: Seq[String] = Nil,
                                   isTest: Boolean = false): String = {
        if (coordinates == null || coordinates.trim.isEmpty) {
            ""
        } else {
            val sysOut = System.out
            try {
                // To prevent ivy from logging to system out
                System.setOut(printStream)
                val artifacts = extractMavenCoordinates(coordinates)
                // Directories for caching downloads through ivy and storing the jars when maven coordinates
                // are supplied to org.apache.spark-submit
                val packagesDirectory: File = new File(ivySettings.getDefaultIvyUserDir, "jars")
                // scalastyle:off println
                printStream.println(
                    s"Ivy Default Cache set to: ${ivySettings.getDefaultCache.getAbsolutePath}")
                printStream.println(s"The jars for the packages stored in: $packagesDirectory")
                // scalastyle:on println

                val ivy = Ivy.newInstance(ivySettings)
                // Set resolve options to download transitive dependencies as well
                val resolveOptions = new ResolveOptions
                resolveOptions.setTransitive(true)
                val retrieveOptions = new RetrieveOptions
                // Turn downloading and logging off for testing
                if (isTest) {
                    resolveOptions.setDownload(false)
                    resolveOptions.setLog(LogOptions.LOG_QUIET)
                    retrieveOptions.setLog(LogOptions.LOG_QUIET)
                } else {
                    resolveOptions.setDownload(true)
                }

                // Default configuration name for ivy
                val ivyConfName = "default"

                // A Module descriptor must be specified. Entries are dummy strings
                val md = getModuleDescriptor
                // clear ivy resolution from previous launches. The resolution file is usually at
                // ~/.ivy2/org.apache.org.apache.spark-org.apache.spark-submit-parent-default.xml. In between runs, this file
                // leads to confusion with Ivy when the files can no longer be found at the repository
                // declared in that file/
                val mdId = md.getModuleRevisionId
                val previousResolution = new File(ivySettings.getDefaultCache,
                    s"${mdId.getOrganisation}-${mdId.getName}-$ivyConfName.xml")
                if (previousResolution.exists) previousResolution.delete

                md.setDefaultConf(ivyConfName)

                // Add exclusion rules for Spark and Scala Library
                addExclusionRules(ivySettings, ivyConfName, md)
                // add all supplied maven artifacts as dependencies
                addDependenciesToIvy(md, artifacts, ivyConfName)
                exclusions.foreach { e =>
                    md.addExcludeRule(createExclusion(e + ":*", ivySettings, ivyConfName))
                }
                // resolve dependencies
                val rr: ResolveReport = ivy.resolve(md, resolveOptions)
                if (rr.hasError) {
                    throw new RuntimeException(rr.getAllProblemMessages.toString)
                }
                // retrieve all resolved dependencies
                ivy.retrieve(rr.getModuleDescriptor.getModuleRevisionId,
                    packagesDirectory.getAbsolutePath + File.separator +
                        "[organization]_[artifact]-[revision].[ext]",
                    retrieveOptions.setConfs(Array(ivyConfName)))
                resolveDependencyPaths(rr.getArtifacts.toArray, packagesDirectory)
            } finally {
                System.setOut(sysOut)
            }
        }
    }

    /**
      * 输出以逗号分隔的路径列表，供下载的jar添加到类路径。
      *
      * Output a comma-delimited list of paths for the downloaded jars to be added to the classpath
      * (will append to jars in SparkSubmit).
      *
      * @param artifacts      Sequence of dependencies that were resolved and retrieved
      * @param cacheDirectory directory where jars are cached
      * @return a comma-delimited list of paths for the dependencies
      */
    def resolveDependencyPaths(
                                  artifacts: Array[AnyRef],
                                  cacheDirectory: File): String = {
        artifacts.map { artifactInfo =>
            val artifact = artifactInfo.asInstanceOf[Artifact].getModuleRevisionId
            cacheDirectory.getAbsolutePath + File.separator +
                s"${artifact.getOrganisation}_${artifact.getName}-${artifact.getRevision}.jar"
        }.mkString(",")
    }

    /**
      * 将给定的maven坐标添加到Ivy的模块描述符中。
      *
      * Adds the given maven coordinates to Ivy's module descriptor.
      */
    def addDependenciesToIvy(
                                md: DefaultModuleDescriptor,
                                artifacts: Seq[MavenCoordinate],
                                ivyConfName: String): Unit = {
        artifacts.foreach { mvn =>
            val ri = ModuleRevisionId.newInstance(mvn.groupId, mvn.artifactId, mvn.version)
            val dd = new DefaultDependencyDescriptor(ri, false, false)
            dd.addDependencyConfiguration(ivyConfName, ivyConfName + "(runtime)")
            // scalastyle:off println
            printStream.println(s"${dd.getDependencyId} added as a dependency")
            // scalastyle:on println
            md.addDependency(dd)
        }
    }

    /**
      * 为已包含在spark程序集中的依赖项添加排除规则。
      *
      * Add exclusion rules for dependencies already included in the org.apache.spark-assembly
      */
    def addExclusionRules(
                             ivySettings: IvySettings,
                             ivyConfName: String,
                             md: DefaultModuleDescriptor): Unit = {
        // Add scala exclusion rule
        md.addExcludeRule(createExclusion("*:scala-library:*", ivySettings, ivyConfName))

        // We need to specify each component explicitly, otherwise we miss org.apache.spark-streaming-kafka-0-8 and
        // other org.apache.spark-streaming utility components. Underscore is there to differentiate between
        // org.apache.spark-streaming_2.1x and org.apache.spark-streaming-kafka-0-8-assembly_2.1x
        val components = Seq("catalyst_", "core_", "graphx_", "hive_", "mllib_", "repl_",
            "sql_", "streaming_", "yarn_", "network-common_", "network-shuffle_", "network-yarn_")

        components.foreach { comp =>
            md.addExcludeRule(createExclusion(s"org.apache.org.apache.spark:org.apache.spark-$comp*:*", ivySettings,
                ivyConfName))
        }
    }

    private[deploy] def createExclusion(
                                           coords: String,
                                           ivySettings: IvySettings,
                                           ivyConfName: String): ExcludeRule = {
        val c = extractMavenCoordinates(coords)(0)
        val id = new ArtifactId(new ModuleId(c.groupId, c.artifactId), "*", "*", "*")
        val rule = new DefaultExcludeRule(id, ivySettings.getMatcher("glob"), null)
        rule.addConfiguration(ivyConfName)
        rule
    }

    /**
      * 从逗号分隔的字符串中提取maven坐标。
      *
      * Extracts maven coordinates from a comma-delimited string. Coordinates should be provided
      * in the format `groupId:artifactId:version` or `groupId/artifactId:version`.
      *
      * @param coordinates Comma-delimited string of maven coordinates
      * @return Sequence of Maven coordinates
      */
    def extractMavenCoordinates(coordinates: String): Seq[MavenCoordinate] = {
        coordinates.split(",").map { p =>
            val splits = p.replace("/", ":").split(":")
            require(splits.length == 3, s"Provided Maven Coordinates must be in the form " +
                s"'groupId:artifactId:version'. The coordinate provided is: $p")
            require(splits(0) != null && splits(0).trim.nonEmpty, s"The groupId cannot be null or " +
                s"be whitespace. The groupId provided is: ${splits(0)}")
            require(splits(1) != null && splits(1).trim.nonEmpty, s"The artifactId cannot be null or " +
                s"be whitespace. The artifactId provided is: ${splits(1)}")
            require(splits(2) != null && splits(2).trim.nonEmpty, s"The version cannot be null or " +
                s"be whitespace. The version provided is: ${splits(2)}")
            new MavenCoordinate(splits(0), splits(1), splits(2))
        }
    }

    /**
      * 在测试中也可以使用一个很好的函数。值是伪字符串。
      *
      * A nice function to use in tests as well. Values are dummy strings.
      */
    def getModuleDescriptor: DefaultModuleDescriptor = DefaultModuleDescriptor.newDefaultInstance(
        ModuleRevisionId.newInstance("org.apache.org.apache.spark", "org.apache.spark-submit-parent", "1.0"))

    /**
      * 表示一个Maven的坐标
      * Represents a Maven Coordinate
      *
      * @param groupId    the groupId of the coordinate
      * @param artifactId the artifactId of the coordinate
      * @param version    the version of the coordinate
      */
    private[deploy] case class MavenCoordinate(groupId: String, artifactId: String, version: String) {
        override def toString: String = s"$groupId:$artifactId:$version"
    }

}

/**
  * 提供一个间接层，用于将参数作为系统属性传递，标记到用户的驱动程序或下游启动程序工具。
  *
  * Provides an indirection layer for passing arguments as system properties or flags to
  * the user's driver program or to downstream launcher tools.
  */
private case class OptionAssigner(
                                     value: String,
                                     clusterManager: Int,
                                     deployMode: Int,
                                     clOption: String = null,
                                     sysProp: String = null)
