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

import java.net.{URI, URL}
import javax.servlet.DispatcherType
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.xml.Node

import org.eclipse.jetty.client.api.Response
import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.client.http.HttpClientTransportOverHTTP
import org.eclipse.jetty.proxy.ProxyServlet
import org.eclipse.jetty.server._
import org.eclipse.jetty.server.handler._
import org.eclipse.jetty.server.handler.gzip.GzipHandler
import org.eclipse.jetty.servlet._
import org.eclipse.jetty.util.component.LifeCycle
import org.eclipse.jetty.util.thread.{QueuedThreadPool, ScheduledExecutorScheduler}
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.{pretty, render}

import org.apache.spark.{SecurityManager, SparkConf, SSLOptions}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
  * 使用Jetty的HTTP server类启动web服务器的实用程序
  *
  * Utilities for launching a web server using Jetty's HTTP Server class
  */
private[spark] object JettyUtils extends Logging {

    val SPARK_CONNECTOR_NAME = "Spark"
    val REDIRECT_CONNECTOR_NAME = "HttpsRedirect"

    // Base type for a function that returns something based on an HTTP request. Allows for
    // implicit conversion from many types of functions to jetty Handlers.
    type Responder[T] = HttpServletRequest => T

    class ServletParams[T <% AnyRef](val responder: Responder[T],
                                     val contentType: String,
                                     val extractFn: T => String = (in: Any) => in.toString) {}

    // Conversions from various types of Responder's to appropriate servlet parameters
    implicit def jsonResponderToServlet(responder: Responder[JValue]): ServletParams[JValue] =
        new ServletParams(responder, "text/json", (in: JValue) => pretty(render(in)))

    implicit def htmlResponderToServlet(responder: Responder[Seq[Node]]): ServletParams[Seq[Node]] =
        new ServletParams(responder, "text/html", (in: Seq[Node]) => "<!DOCTYPE html>" + in.toString)

    implicit def textResponderToServlet(responder: Responder[String]): ServletParams[String] =
        new ServletParams(responder, "text/plain")

    def createServlet[T <% AnyRef](
                                      servletParams: ServletParams[T],
                                      securityMgr: SecurityManager,
                                      conf: SparkConf): HttpServlet = {

        // SPARK-10589 avoid frame-related click-jacking vulnerability, using X-Frame-Options
        // (see http://tools.ietf.org/html/rfc7034). By default allow framing only from the
        // same origin, but allow framing for a specific named URI.
        // Example: org.apache.spark.ui.allowFramingFrom = https://example.com/
        val allowFramingFrom = conf.getOption("org.apache.spark.ui.allowFramingFrom")
        val xFrameOptionsValue =
            allowFramingFrom.map(uri => s"ALLOW-FROM $uri").getOrElse("SAMEORIGIN")

        new HttpServlet {
            override def doGet(request: HttpServletRequest, response: HttpServletResponse) {
                try {
                    if (securityMgr.checkUIViewPermissions(request.getRemoteUser)) {
                        response.setContentType("%s;charset=utf-8".format(servletParams.contentType))
                        response.setStatus(HttpServletResponse.SC_OK)
                        val result = servletParams.responder(request)
                        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate")
                        response.setHeader("X-Frame-Options", xFrameOptionsValue)
                        response.getWriter.print(servletParams.extractFn(result))
                    } else {
                        response.setStatus(HttpServletResponse.SC_FORBIDDEN)
                        response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate")
                        response.sendError(HttpServletResponse.SC_FORBIDDEN,
                            "User is not authorized to access this page.")
                    }
                } catch {
                    case e: IllegalArgumentException =>
                        response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage)
                    case e: Exception =>
                        logWarning(s"GET ${request.getRequestURI} failed: $e", e)
                        throw e
                }
            }

            // SPARK-5983 ensure TRACE is not supported
            protected override def doTrace(req: HttpServletRequest, res: HttpServletResponse): Unit = {
                res.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
            }
        }
    }

    /** Create a context handler that responds to a request with the given path prefix */
    def createServletHandler[T <% AnyRef](
                                             path: String,
                                             servletParams: ServletParams[T],
                                             securityMgr: SecurityManager,
                                             conf: SparkConf,
                                             basePath: String = ""): ServletContextHandler = {
        createServletHandler(path, createServlet(servletParams, securityMgr, conf), basePath)
    }

    /** Create a context handler that responds to a request with the given path prefix */
    def createServletHandler(
                                path: String,
                                servlet: HttpServlet,
                                basePath: String): ServletContextHandler = {
        val prefixedPath = if (basePath == "" && path == "/") {
            path
        } else {
            (basePath + path).stripSuffix("/")
        }
        val contextHandler = new ServletContextHandler
        val holder = new ServletHolder(servlet)
        contextHandler.setContextPath(prefixedPath)
        contextHandler.addServlet(holder, "/")
        contextHandler
    }

    /**
      * 创建始终将用户重定向到给定路径的处理程序
      *
      * Create a handler that always redirects the user to the given path
      */
    def createRedirectHandler(
                                 srcPath: String,
                                 destPath: String,
                                 beforeRedirect: HttpServletRequest => Unit = x => (),
                                 basePath: String = "",
                                 httpMethods: Set[String] = Set("GET")): ServletContextHandler = {
        val prefixedDestPath = basePath + destPath
        val servlet = new HttpServlet {
            override def doGet(request: HttpServletRequest, response: HttpServletResponse): Unit = {
                if (httpMethods.contains("GET")) {
                    doRequest(request, response)
                } else {
                    response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
                }
            }

            override def doPost(request: HttpServletRequest, response: HttpServletResponse): Unit = {
                if (httpMethods.contains("POST")) {
                    doRequest(request, response)
                } else {
                    response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
                }
            }

            private def doRequest(request: HttpServletRequest, response: HttpServletResponse): Unit = {
                beforeRedirect(request)
                // Make sure we don't end up with "//" in the middle
                val newUrl = new URL(new URL(request.getRequestURL.toString), prefixedDestPath).toString
                response.sendRedirect(newUrl)
            }

            // SPARK-5983 ensure TRACE is not supported
            protected override def doTrace(req: HttpServletRequest, res: HttpServletResponse): Unit = {
                res.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
            }
        }
        createServletHandler(srcPath, servlet, basePath)
    }

    /** Create a handler for serving files from a static directory */
    def createStaticHandler(resourceBase: String, path: String): ServletContextHandler = {
        val contextHandler = new ServletContextHandler
        contextHandler.setInitParameter("org.eclipse.jetty.servlet.Default.gzip", "false")
        val staticHandler = new DefaultServlet
        val holder = new ServletHolder(staticHandler)
        Option(Utils.getSparkClassLoader.getResource(resourceBase)) match {
            case Some(res) =>
                holder.setInitParameter("resourceBase", res.toString)
            case None =>
                throw new Exception("Could not find resource path for Web UI: " + resourceBase)
        }
        contextHandler.setContextPath(path)
        contextHandler.addServlet(holder, "/")
        contextHandler
    }

    /** Create a handler for proxying request to Workers and Application Drivers */
    def createProxyHandler(
                              prefix: String,
                              target: String): ServletContextHandler = {
        val servlet = new ProxyServlet {
            override def rewriteTarget(request: HttpServletRequest): String = {
                val rewrittenURI = createProxyURI(
                    prefix, target, request.getRequestURI(), request.getQueryString())
                if (rewrittenURI == null) {
                    return null
                }
                if (!validateDestination(rewrittenURI.getHost(), rewrittenURI.getPort())) {
                    return null
                }
                rewrittenURI.toString()
            }

            override def newHttpClient(): HttpClient = {
                // SPARK-21176: Use the Jetty logic to calculate the number of selector threads (#CPUs/2),
                // but limit it to 8 max.
                // Otherwise, it might happen that we exhaust the threadpool since in reverse proxy mode
                // a proxy is instantiated for each executor. If the head node has many processors, this
                // can quickly add up to an unreasonably high number of threads.
                val numSelectors = math.max(1, math.min(8, Runtime.getRuntime().availableProcessors() / 2))
                new HttpClient(new HttpClientTransportOverHTTP(numSelectors), null)
            }

            override def filterServerResponseHeader(
                                                       clientRequest: HttpServletRequest,
                                                       serverResponse: Response,
                                                       headerName: String,
                                                       headerValue: String): String = {
                if (headerName.equalsIgnoreCase("location")) {
                    val newHeader = createProxyLocationHeader(
                        prefix, headerValue, clientRequest, serverResponse.getRequest().getURI())
                    if (newHeader != null) {
                        return newHeader
                    }
                }
                super.filterServerResponseHeader(
                    clientRequest, serverResponse, headerName, headerValue)
            }
        }

        val contextHandler = new ServletContextHandler
        val holder = new ServletHolder(servlet)
        contextHandler.setContextPath(prefix)
        contextHandler.addServlet(holder, "/")
        contextHandler
    }

    /** Add filters, if any, to the given list of ServletContextHandlers */
    def addFilters(handlers: Seq[ServletContextHandler], conf: SparkConf) {
        val filters: Array[String] = conf.get("org.apache.spark.ui.filters", "").split(',').map(_.trim())
        filters.foreach {
            case filter: String =>
                if (!filter.isEmpty) {
                    logInfo("Adding filter: " + filter)
                    val holder: FilterHolder = new FilterHolder()
                    holder.setClassName(filter)
                    // Get any parameters for each filter
                    conf.get("org.apache.spark." + filter + ".params", "").split(',').map(_.trim()).toSet.foreach {
                        param: String =>
                            if (!param.isEmpty) {
                                val parts = param.split("=")
                                if (parts.length == 2) holder.setInitParameter(parts(0), parts(1))
                            }
                    }

                    val prefix = s"org.apache.spark.$filter.param."
                    conf.getAll
                        .filter { case (k, v) => k.length() > prefix.length() && k.startsWith(prefix) }
                        .foreach { case (k, v) => holder.setInitParameter(k.substring(prefix.length()), v) }

                    val enumDispatcher = java.util.EnumSet.of(DispatcherType.ASYNC, DispatcherType.ERROR,
                        DispatcherType.FORWARD, DispatcherType.INCLUDE, DispatcherType.REQUEST)
                    handlers.foreach { case (handler) => handler.addFilter(holder, "/*", enumDispatcher) }
                }
        }
    }

    /**
      * Attempt to start a Jetty server bound to the supplied hostName:port using the given
      * context handlers.
      *
      * If the desired port number is contended, continues incrementing ports until a free port is
      * found. Return the jetty Server object, the chosen port, and a mutable collection of handlers.
      */
    def startJettyServer(
                            hostName: String,
                            port: Int,
                            sslOptions: SSLOptions,
                            handlers: Seq[ServletContextHandler],
                            conf: SparkConf,
                            serverName: String = ""): ServerInfo = {

        addFilters(handlers, conf)

        // Start the server first, with no connectors.
        val pool = new QueuedThreadPool
        if (serverName.nonEmpty) {
            pool.setName(serverName)
        }
        pool.setDaemon(true)

        val server = new Server(pool)

        val errorHandler = new ErrorHandler()
        errorHandler.setShowStacks(true)
        errorHandler.setServer(server)
        server.addBean(errorHandler)

        val collection = new ContextHandlerCollection
        server.setHandler(collection)

        // Executor used to create daemon threads for the Jetty connectors.
        val serverExecutor = new ScheduledExecutorScheduler(s"$serverName-JettyScheduler", true)

        try {
            server.start()

            // As each acceptor and each selector will use one thread, the number of threads should at
            // least be the number of acceptors and selectors plus 1. (See SPARK-13776)
            var minThreads = 1

            def newConnector(
                                connectionFactories: Array[ConnectionFactory],
                                port: Int): (ServerConnector, Int) = {
                val connector = new ServerConnector(
                    server,
                    null,
                    serverExecutor,
                    null,
                    -1,
                    -1,
                    connectionFactories: _*)
                connector.setPort(port)
                connector.start()

                // Currently we only use "SelectChannelConnector"
                // Limit the max acceptor number to 8 so that we don't waste a lot of threads
                connector.setAcceptQueueSize(math.min(connector.getAcceptors, 8))
                connector.setHost(hostName)
                // The number of selectors always equals to the number of acceptors
                minThreads += connector.getAcceptors * 2

                (connector, connector.getLocalPort())
            }

            // If SSL is configured, create the secure connector first.
            val securePort = sslOptions.createJettySslContextFactory().map { factory =>
                val securePort = sslOptions.port.getOrElse(if (port > 0) Utils.userPort(port, 400) else 0)
                val secureServerName = if (serverName.nonEmpty) s"$serverName (HTTPS)" else serverName
                val connectionFactories = AbstractConnectionFactory.getFactories(factory,
                    new HttpConnectionFactory())

                def sslConnect(currentPort: Int): (ServerConnector, Int) = {
                    newConnector(connectionFactories, currentPort)
                }

                val (connector, boundPort) = Utils.startServiceOnPort[ServerConnector](securePort,
                    sslConnect, conf, secureServerName)
                connector.setName(SPARK_CONNECTOR_NAME)
                server.addConnector(connector)
                boundPort
            }

            // Bind the HTTP port.
            def httpConnect(currentPort: Int): (ServerConnector, Int) = {
                newConnector(Array(new HttpConnectionFactory()), currentPort)
            }

            val (httpConnector, httpPort) = Utils.startServiceOnPort[ServerConnector](port, httpConnect,
                conf, serverName)

            // If SSL is configured, then configure redirection in the HTTP connector.
            securePort match {
                case Some(p) =>
                    httpConnector.setName(REDIRECT_CONNECTOR_NAME)
                    val redirector = createRedirectHttpsHandler(p, "https")
                    collection.addHandler(redirector)
                    redirector.start()

                case None =>
                    httpConnector.setName(SPARK_CONNECTOR_NAME)
            }

            server.addConnector(httpConnector)

            // Add all the known handlers now that connectors are configured.
            handlers.foreach { h =>
                h.setVirtualHosts(toVirtualHosts(SPARK_CONNECTOR_NAME))
                val gzipHandler = new GzipHandler()
                gzipHandler.setHandler(h)
                collection.addHandler(gzipHandler)
                gzipHandler.start()
            }

            pool.setMaxThreads(math.max(pool.getMaxThreads, minThreads))
            ServerInfo(server, httpPort, securePort, collection)
        } catch {
            case e: Exception =>
                server.stop()
                if (serverExecutor.isStarted()) {
                    serverExecutor.stop()
                }
                if (pool.isStarted()) {
                    pool.stop()
                }
                throw e
        }
    }

    private def createRedirectHttpsHandler(securePort: Int, scheme: String): ContextHandler = {
        val redirectHandler: ContextHandler = new ContextHandler
        redirectHandler.setContextPath("/")
        redirectHandler.setVirtualHosts(toVirtualHosts(REDIRECT_CONNECTOR_NAME))
        redirectHandler.setHandler(new AbstractHandler {
            override def handle(
                                   target: String,
                                   baseRequest: Request,
                                   request: HttpServletRequest,
                                   response: HttpServletResponse): Unit = {
                if (baseRequest.isSecure) {
                    return
                }
                val httpsURI = createRedirectURI(scheme, baseRequest.getServerName, securePort,
                    baseRequest.getRequestURI, baseRequest.getQueryString)
                response.setContentLength(0)
                response.sendRedirect(response.encodeRedirectURL(httpsURI))
                baseRequest.setHandled(true)
            }
        })
        redirectHandler
    }

    def createProxyURI(prefix: String, target: String, path: String, query: String): URI = {
        if (!path.startsWith(prefix)) {
            return null
        }

        val uri = new StringBuilder(target)
        val rest = path.substring(prefix.length())

        if (!rest.isEmpty()) {
            if (!rest.startsWith("/")) {
                uri.append("/")
            }
            uri.append(rest)
        }

        val rewrittenURI = URI.create(uri.toString())
        if (query != null) {
            return new URI(
                rewrittenURI.getScheme(),
                rewrittenURI.getAuthority(),
                rewrittenURI.getPath(),
                query,
                rewrittenURI.getFragment()
            ).normalize()
        }
        rewrittenURI.normalize()
    }

    def createProxyLocationHeader(
                                     prefix: String,
                                     headerValue: String,
                                     clientRequest: HttpServletRequest,
                                     targetUri: URI): String = {
        val toReplace = targetUri.getScheme() + "://" + targetUri.getAuthority()
        if (headerValue.startsWith(toReplace)) {
            clientRequest.getScheme() + "://" + clientRequest.getHeader("host") +
                prefix + headerValue.substring(toReplace.length())
        } else {
            null
        }
    }

    // Create a new URI from the arguments, handling IPv6 host encoding and default ports.
    private def createRedirectURI(
                                     scheme: String, server: String, port: Int, path: String, query: String) = {
        val redirectServer = if (server.contains(":") && !server.startsWith("[")) {
            s"[${server}]"
        } else {
            server
        }
        val authority = s"$redirectServer:$port"
        new URI(scheme, authority, path, query, null).toString
    }

    def toVirtualHosts(connectors: String*): Array[String] = connectors.map("@" + _).toArray

}

private[spark] case class ServerInfo(
                                        server: Server,
                                        boundPort: Int,
                                        securePort: Option[Int],
                                        private val rootHandler: ContextHandlerCollection) {

    def addHandler(handler: ContextHandler): Unit = {
        handler.setVirtualHosts(JettyUtils.toVirtualHosts(JettyUtils.SPARK_CONNECTOR_NAME))
        rootHandler.addHandler(handler)
        if (!handler.isStarted()) {
            handler.start()
        }
    }

    def removeHandler(handler: ContextHandler): Unit = {
        rootHandler.removeHandler(handler)
        if (handler.isStarted) {
            handler.stop()
        }
    }

    def stop(): Unit = {
        server.stop()
        // Stop the ThreadPool if it supports stop() method (through LifeCycle).
        // It is needed because stopping the Server won't stop the ThreadPool it uses.
        val threadPool = server.getThreadPool
        if (threadPool != null && threadPool.isInstanceOf[LifeCycle]) {
            threadPool.asInstanceOf[LifeCycle].stop
        }
    }
}
