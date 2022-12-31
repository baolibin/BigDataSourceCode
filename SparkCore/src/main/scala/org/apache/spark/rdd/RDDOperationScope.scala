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

import java.util.concurrent.atomic.AtomicInteger

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude, JsonPropertyOrder}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.base.Objects
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

/**
  * 表示实例化RDD的操作的通用命名代码块。
  *
  * A general, named code block representing an operation that instantiates RDDs.
  *
  * All RDDs instantiated in the corresponding code block will store a pointer to this object.
  * Examples include, but will not be limited to, existing RDD operations, such as textFile,
  * reduceByKey, and treeAggregate.
  *
  * An operation scope may be nested in other scopes. For instance, a SQL query may enclose
  * scopes associated with the public RDD APIs it uses under the hood.
  *
  * There is no particular relationship between an operation scope and a stage or a job.
  * A scope may live inside one stage (e.g. map) or span across multiple jobs (e.g. take).
  */
@JsonInclude(Include.NON_NULL)
@JsonPropertyOrder(Array("id", "name", "parent"))
private[spark] class RDDOperationScope(
                                          val name: String,
                                          val parent: Option[RDDOperationScope] = None,
                                          val id: String = RDDOperationScope.nextScopeId().toString) {

    /**
      * 返回此作用域所属的作用域列表，包括此作用域本身。结果从最外面的作用域（最早的祖先）排序到此作用域。
      *
      * Return a list of scopes that this scope is a part of, including this scope itself.
      * The result is ordered from the outermost scope (eldest ancestor) to this scope.
      */
    @JsonIgnore
    def getAllScopes: Seq[RDDOperationScope] = {
        parent.map(_.getAllScopes).getOrElse(Seq.empty) ++ Seq(this)
    }

    override def equals(other: Any): Boolean = {
        other match {
            case s: RDDOperationScope =>
                id == s.id && name == s.name && parent == s.parent
            case _ => false
        }
    }

    override def hashCode(): Int = Objects.hashCode(id, name, parent)

    override def toString: String = toJson

    def toJson: String = {
        RDDOperationScope.jsonMapper.writeValueAsString(this)
    }
}

/**
  * 一组实用程序方法，用于构建RDD作用域的分层表示。RDD范围跟踪创建给定RDD的一系列操作。
  *
  * A collection of utility methods to construct a hierarchical representation of RDD scopes.
  * An RDD scope tracks the series of operations that created a given RDD.
  */
private[spark] object RDDOperationScope extends Logging {
    private val jsonMapper = new ObjectMapper().registerModule(DefaultScalaModule)
    private val scopeCounter = new AtomicInteger(0)

    /**
      * 返回全局唯一的操作范围ID
      * Return a globally unique operation scope ID.
      */
    def nextScopeId(): Int = scopeCounter.getAndIncrement

    /**
      * 执行给定的主体，以便在此主体中创建的所有RDD都具有相同的作用域。作用域的名称将是堆栈跟踪中与此方法不同的第一个方法名称。
      *
      * Execute the given body such that all RDDs created in this body will have the same scope.
      * The name of the scope will be the first method name in the stack trace that is not the
      * same as this method's.
      *
      * Note: Return statements are NOT allowed in body.
      */
    private[spark] def withScope[T](
                                       sc: SparkContext,
                                       allowNesting: Boolean = false)(body: => T): T = {
        val ourMethodName = "withScope"
        val callerMethodName = Thread.currentThread.getStackTrace()
            .dropWhile(_.getMethodName != ourMethodName)
            .find(_.getMethodName != ourMethodName)
            .map(_.getMethodName)
            .getOrElse {
                // Log a warning just in case, but this should almost certainly never happen
                logWarning("No valid method name for this RDD operation scope!")
                "N/A"
            }
        withScope[T](sc, callerMethodName, allowNesting, ignoreParent = false)(body)
    }

    /**
      * 执行给定的主体，以便在此主体中创建的所有RDD都具有相同的作用域。
      *
      * Execute the given body such that all RDDs created in this body will have the same scope.
      *
      * If nesting is allowed, any subsequent calls to this method in the given body will instantiate
      * child scopes that are nested within our scope. Otherwise, these calls will take no effect.
      *
      * Additionally, the caller of this method may optionally ignore the configurations and scopes
      * set by the higher level caller. In this case, this method will ignore the parent caller's
      * intention to disallow nesting, and the new scope instantiated will not have a parent. This
      * is useful for scoping physical operations in Spark SQL, for instance.
      *
      * Note: Return statements are NOT allowed in body.
      */
    private[spark] def withScope[T](
                                       sc: SparkContext,
                                       name: String,
                                       allowNesting: Boolean,
                                       ignoreParent: Boolean)(body: => T): T = {
        // Save the old scope to restore it later
        val scopeKey = SparkContext.RDD_SCOPE_KEY
        val noOverrideKey = SparkContext.RDD_SCOPE_NO_OVERRIDE_KEY
        val oldScopeJson = sc.getLocalProperty(scopeKey)
        val oldScope = Option(oldScopeJson).map(RDDOperationScope.fromJson)
        val oldNoOverride = sc.getLocalProperty(noOverrideKey)
        try {
            if (ignoreParent) {
                // Ignore all parent settings and scopes and start afresh with our own root scope
                sc.setLocalProperty(scopeKey, new RDDOperationScope(name).toJson)
            } else if (sc.getLocalProperty(noOverrideKey) == null) {
                // Otherwise, set the scope only if the higher level caller allows us to do so
                sc.setLocalProperty(scopeKey, new RDDOperationScope(name, oldScope).toJson)
            }
            // Optionally disallow the child body to override our scope
            if (!allowNesting) {
                sc.setLocalProperty(noOverrideKey, "true")
            }
            body
        } finally {
            // Remember to restore any state that was modified before exiting
            sc.setLocalProperty(scopeKey, oldScopeJson)
            sc.setLocalProperty(noOverrideKey, oldNoOverride)
        }
    }

    def fromJson(s: String): RDDOperationScope = {
        jsonMapper.readValue(s, classOf[RDDOperationScope])
    }
}
