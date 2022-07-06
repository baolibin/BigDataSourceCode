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

package org.apache.spark.sql

import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.internal.config.{ConfigEntry, OptionalConfigEntry}
import org.apache.spark.sql.internal.SQLConf


/**
  * Spark的运行时配置接口。要访问此文件，请使用`SparkSession.conf文件`。
  *
  * Runtime configuration interface for Spark. To access this, use `SparkSession.conf`.
  *
  * Options set here are automatically propagated to the Hadoop configuration during I/O.
  *
  * @since 2.0.0
  */
@InterfaceStability.Stable
class RuntimeConfig private[sql](sqlConf: SQLConf = new SQLConf) {

    /**
      * 设置给定的Spark运行时配置属性。
      *
      * Sets the given Spark runtime configuration property.
      *
      * @since 2.0.0
      */
    def set(key: String, value: Boolean): Unit = {
        requireNonStaticConf(key)
        set(key, value.toString)
    }

    /**
      * 设置给定的Spark运行时配置属性。
      *
      * Sets the given Spark runtime configuration property.
      *
      * @since 2.0.0
      */
    def set(key: String, value: String): Unit = {
        requireNonStaticConf(key)
        sqlConf.setConfString(key, value)
    }

    private def requireNonStaticConf(key: String): Unit = {
        if (SQLConf.staticConfKeys.contains(key)) {
            throw new AnalysisException(s"Cannot modify the value of a static config: $key")
        }
    }

    /**
      * 设置给定的Spark运行时配置属性。
      *
      * Sets the given Spark runtime configuration property.
      *
      * @since 2.0.0
      */
    def set(key: String, value: Long): Unit = {
        requireNonStaticConf(key)
        set(key, value.toString)
    }

    /**
      * 返回给定键的Spark runtime configuration属性的值。
      *
      * Returns the value of Spark runtime configuration property for the given key.
      *
      * @since 2.0.0
      */
    def get(key: String, default: String): String = {
        sqlConf.getConfString(key, default)
    }

    /**
      * 返回在此配置中设置的所有属性。
      *
      * Returns all properties set in this conf.
      *
      * @since 2.0.0
      */
    def getAll: Map[String, String] = {
        sqlConf.getAllConfs
    }

    /**
      * 返回给定键的Spark runtime configuration属性的值。
      *
      * Returns the value of Spark runtime configuration property for the given key.
      *
      * @since 2.0.0
      */
    def getOption(key: String): Option[String] = {
        try Option(get(key)) catch {
            case _: NoSuchElementException => None
        }
    }

    /**
      * 返回给定键的Spark runtime configuration属性的值。
      *
      * Returns the value of Spark runtime configuration property for the given key.
      *
      * @throws java.util.NoSuchElementException if the key is not set and does not have a default
      *                                          value
      * @since 2.0.0
      */
    @throws[NoSuchElementException]("if the key is not set")
    def get(key: String): String = {
        sqlConf.getConfString(key)
    }

    /**
      * 重置给定密钥的配置属性。
      *
      * Resets the configuration property for the given key.
      *
      * @since 2.0.0
      */
    def unset(key: String): Unit = {
        requireNonStaticConf(key)
        sqlConf.unsetConf(key)
    }

    /**
      * 返回给定键的Spark runtime configuration属性的值。
      *
      * Returns the value of Spark runtime configuration property for the given key.
      */
    @throws[NoSuchElementException]("if the key is not set")
    protected[sql] def get[T](entry: ConfigEntry[T]): T = {
        sqlConf.getConf(entry)
    }

    protected[sql] def get[T](entry: OptionalConfigEntry[T]): Option[T] = {
        sqlConf.getConf(entry)
    }

    /**
      * 返回给定键的Spark runtime configuration属性的值。
      *
      * Returns the value of Spark runtime configuration property for the given key.
      */
    protected[sql] def get[T](entry: ConfigEntry[T], default: T): T = {
        sqlConf.getConf(entry, default)
    }

    /**
      * 返回是否设置了特定键。
      *
      * Returns whether a particular key is set.
      */
    protected[sql] def contains(key: String): Boolean = {
        sqlConf.contains(key)
    }
}
