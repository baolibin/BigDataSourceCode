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

package org.apache.spark.rpc

import java.io.File
import java.nio.channels.ReadableByteChannel

import org.apache.spark.rpc.netty.NettyRpcEnvFactory
import org.apache.spark.util.RpcUtils
import org.apache.spark.{SecurityManager, SparkConf}

import scala.concurrent.Future


/**
  * RpcEnv实现必须具有具有空构造函数的[[RpcEnvFactory]]实现，以便可以通过反射创建它。
  *
  * A RpcEnv implementation must have a [[RpcEnvFactory]] implementation with an empty constructor
  * so that it can be created via Reflection.
  */
private[spark] object RpcEnv {

    def create(
                  name: String,
                  host: String,
                  port: Int,
                  conf: SparkConf,
                  securityManager: SecurityManager,
                  clientMode: Boolean = false): RpcEnv = {
        create(name, host, host, port, conf, securityManager, clientMode)
    }

    def create(
                  name: String,
                  bindAddress: String,
                  advertiseAddress: String,
                  port: Int,
                  conf: SparkConf,
                  securityManager: SecurityManager,
                  clientMode: Boolean): RpcEnv = {
        val config = RpcEnvConfig(conf, name, bindAddress, advertiseAddress, port, securityManager,
            clientMode)
        new NettyRpcEnvFactory().create(config)
    }
}


/**
  * RPC环境。[[RpcEndpoint]]s需要向[[RpcEnv]]注册一个名称才能接收消息。
  *
  * An RPC environment. [[RpcEndpoint]]s need to register itself with a name to [[RpcEnv]] to
  * receives messages. Then [[RpcEnv]] will process messages sent from [[RpcEndpointRef]] or remote
  * nodes, and deliver them to corresponding [[RpcEndpoint]]s. For uncaught exceptions caught by
  * [[RpcEnv]], [[RpcEnv]] will use [[RpcCallContext.sendFailure]] to send exceptions back to the
  * sender, or logging them if no such sender or `NotSerializableException`.
  *
  * [[RpcEnv]] also provides some methods to retrieve [[RpcEndpointRef]]s given name or uri.
  */
private[spark] abstract class RpcEnv(conf: SparkConf) {

    private[spark] val defaultLookupTimeout = RpcUtils.lookupRpcTimeout(conf)

    /**
      * 返回[[RpcEnv]]正在侦听的地址。
      *
      * Return the address that [[RpcEnv]] is listening to.
      */
    def address: RpcAddress

    /**
      * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
      * guarantee thread-safety.
      */
    def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

    /**
      * Retrieve the [[RpcEndpointRef]] represented by `uri` asynchronously.
      */
    def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

    /**
      * Retrieve the [[RpcEndpointRef]] represented by `address` and `endpointName`.
      * This is a blocking action.
      */
    def setupEndpointRef(address: RpcAddress, endpointName: String): RpcEndpointRef = {
        setupEndpointRefByURI(RpcEndpointAddress(address, endpointName).toString)
    }

    /**
      * Retrieve the [[RpcEndpointRef]] represented by `uri`. This is a blocking action.
      */
    def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
        defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri))
    }

    /**
      * 停止“endpoint”指定的[[RpcEndpoint]]。
      *
      * Stop [[RpcEndpoint]] specified by `endpoint`.
      */
    def stop(endpoint: RpcEndpointRef): Unit

    /**
      * 异步关闭此[[RpcEnv]]。如果需要确保[[RpcEnv]]成功退出，请在[[shutdown（）]]之后直接调用[[awaitTermination（）]]。
      *
      * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
      * call [[awaitTermination()]] straight after [[shutdown()]].
      */
    def shutdown(): Unit

    /**
      * 等待[[RpcEnv]]退出。
      *
      * Wait until [[RpcEnv]] exits.
      *
      * TODO do we need a timeout parameter?
      */
    def awaitTermination(): Unit

    /**
      * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
      * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
      */
    def deserialize[T](deserializationAction: () => T): T

    /**
      * 返回用于提供文件的文件服务器的实例。如果RpcEnv未在服务器模式下运行，则此值可能为“null”。
      *
      * Return the instance of the file server used to serve files. This may be `null` if the
      * RpcEnv is not operating in server mode.
      */
    def fileServer: RpcEnvFileServer

    /**
      * 打开一个通道以从给定的URI下载文件。如果RpcEnvFileServer返回的URI使用“org.apache.spark”方案，则Utils类将调用此方法来检索文件。
      *
      * Open a channel to download a file from the given URI. If the URIs returned by the
      * RpcEnvFileServer use the "org.apache.spark" scheme, this method will be called by the Utils class to
      * retrieve the files.
      *
      * @param uri URI with location of the file.
      */
    def openChannel(uri: String): ReadableByteChannel

    /**
      * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
      * [[RpcEndpoint.self]]. Return `null` if the corresponding [[RpcEndpointRef]] does not exist.
      */
    private[rpc] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef
}

/**
  * RpcEnv使用的服务器，用于将文件服务器到应用程序拥有的其他进程。
  *
  * A server used by the RpcEnv to server files to other processes owned by the application.
  *
  * The file server can return URIs handled by common libraries (such as "http" or "hdfs"), or
  * it can return "org.apache.spark" URIs which will be handled by `RpcEnv#fetchFile`.
  */
private[spark] trait RpcEnvFileServer {

    /**
      * 添加要由此RpcEnv提供服务的文件。当文件存储在驱动程序的本地文件系统上时，它用于将文件从驱动程序提供给执行程序。
      *
      * Adds a file to be served by this RpcEnv. This is used to serve files from the driver
      * to executors when they're stored on the driver's local file system.
      *
      * @param file Local file to serve.
      * @return A URI for the location of the file.
      */
    def addFile(file: File): String

    /**
      * 添加一个由此RpcEnv提供的罐子。类似于“addFile”，但适用于使用“SparkContext.addJar”添加的jar。
      *
      * Adds a jar to be served by this RpcEnv. Similar to `addFile` but for jars added using
      * `SparkContext.addJar`.
      *
      * @param file Local file to serve.
      * @return A URI for the location of the file.
      */
    def addJar(file: File): String

    /**
      * 添加要通过此文件服务器提供服务的本地目录。
      *
      * Adds a local directory to be served via this file server.
      *
      * @param baseUri Leading URI path (files can be retrieved by appending their relative
      *                path to this base URI). This cannot be "files" nor "jars".
      * @param path    Path to the local directory.
      * @return URI for the root of the directory in the file server.
      */
    def addDirectory(baseUri: String, path: File): String

    /**
      * 验证并规范化目录的基本URI
      *
      * Validates and normalizes the base URI for directories.
      */
    protected def validateDirectoryUri(baseUri: String): String = {
        val fixedBaseUri = "/" + baseUri.stripPrefix("/").stripSuffix("/")
        require(fixedBaseUri != "/files" && fixedBaseUri != "/jars",
            "Directory URI cannot be /files nor /jars.")
        fixedBaseUri
    }

}

private[spark] case class RpcEnvConfig(
                                          conf: SparkConf,
                                          name: String,
                                          bindAddress: String,
                                          advertiseAddress: String,
                                          port: Int,
                                          securityManager: SecurityManager,
                                          clientMode: Boolean)
