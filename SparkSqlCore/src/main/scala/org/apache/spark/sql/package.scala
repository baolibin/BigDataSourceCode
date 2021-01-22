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

import org.apache.spark.annotation.{DeveloperApi, InterfaceStability}
import org.apache.spark.sql.execution.SparkStrategy

/**
  * 允许执行关系查询，包括使用Spark在SQL中表示的查询。
  *
  * Allows the execution of relational queries, including those expressed in SQL using Spark.
  *
  * @groupname dataType Data types
  * @groupdesc Spark SQL data types.
  * @groupprio dataType -3
  * @groupname field Field
  * @groupprio field -2
  * @groupname row Row
  * @groupprio row -1
  */
package object sql {

    /**
      * 将逻辑计划转换为零个或多个SparkPlan。
      * 这个API是为了试验queryplanner而公开的，并不是为了在spark版本中保持稳定而设计的。
      * 编写库的开发人员应该考虑使用中提供的稳定api[[org.apache.spark.sql.sources]]
      *
      * Converts a logical plan into zero or more SparkPlans.  This API is exposed for experimenting
      * with the query planner and is not designed to be stable across spark releases.  Developers
      * writing libraries should instead consider using the stable APIs provided in
      * [[org.apache.spark.sql.sources]]
      */
    @DeveloperApi
    @InterfaceStability.Unstable
    type Strategy = SparkStrategy

    type DataFrame = Dataset[Row]
}
