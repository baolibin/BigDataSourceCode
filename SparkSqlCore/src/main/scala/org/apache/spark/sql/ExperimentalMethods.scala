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

import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * 最勇敢的实验方法的持有者。我们不能保证方法的二进制兼容性和源代码兼容性的稳定性。
  *
  * :: Experimental ::
  * Holder for experimental methods for the bravest. We make NO guarantee about the stability
  * regarding binary compatibility and source compatibility of methods here.
  *
  * {{{
  *   spark.experimental.extraStrategies += ...
  * }}}
  *
  * @since 1.3.0
  */
@Experimental
@InterfaceStability.Unstable
class ExperimentalMethods private[sql]() {

    /**
      * Allows extra strategies to be injected into the query planner at runtime.  Note this API
      * should be considered experimental and is not intended to be stable across releases.
      *
      * @since 1.3.0
      */
    @volatile var extraStrategies: Seq[Strategy] = Nil

    @volatile var extraOptimizations: Seq[Rule[LogicalPlan]] = Nil

    override def clone(): ExperimentalMethods = {
        val result = new ExperimentalMethods
        result.extraStrategies = extraStrategies
        result.extraOptimizations = extraOptimizations
        result
    }
}
