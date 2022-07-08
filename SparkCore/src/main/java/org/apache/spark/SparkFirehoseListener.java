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

package org.apache.spark;

import org.apache.spark.scheduler.*;

/**
 * 该类允许用户接收所有SparkListener事件。用户应该重写OneEvent方法。
 * 这是一个具体的Java类，以确保在向SparkListener添加新方法时不会忘记对其进行更新：
 * 忘记添加方法将导致编译错误（如果这是一个具体的Scala类，则新事件处理程序的默认实现将继承自SparkListener特性）。
 * <p>
 * Class that allows users to receive all SparkListener events.
 * Users should override the onEvent method.
 * <p>
 * This is a concrete Java class in order to ensure that we don't forget to update it when adding
 * new methods to SparkListener: forgetting to add a method will result in a compilation error (if
 * this was a concrete Scala class, default implementations of new event handlers would be inherited
 * from the SparkListener trait).
 */
public class SparkFirehoseListener implements SparkListenerInterface {

    public void onEvent(SparkListenerEvent event) {
    }

    @Override
    public final void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        onEvent(stageCompleted);
    }

    @Override
    public final void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        onEvent(stageSubmitted);
    }

    @Override
    public final void onTaskStart(SparkListenerTaskStart taskStart) {
        onEvent(taskStart);
    }

    @Override
    public final void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {
        onEvent(taskGettingResult);
    }

    @Override
    public final void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        onEvent(taskEnd);
    }

    @Override
    public final void onJobStart(SparkListenerJobStart jobStart) {
        onEvent(jobStart);
    }

    @Override
    public final void onJobEnd(SparkListenerJobEnd jobEnd) {
        onEvent(jobEnd);
    }

    @Override
    public final void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {
        onEvent(environmentUpdate);
    }

    @Override
    public final void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) {
        onEvent(blockManagerAdded);
    }

    @Override
    public final void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) {
        onEvent(blockManagerRemoved);
    }

    @Override
    public final void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) {
        onEvent(unpersistRDD);
    }

    @Override
    public final void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        onEvent(applicationStart);
    }

    @Override
    public final void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        onEvent(applicationEnd);
    }

    @Override
    public final void onExecutorMetricsUpdate(
            SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
        onEvent(executorMetricsUpdate);
    }

    @Override
    public final void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
        onEvent(executorAdded);
    }

    @Override
    public final void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
        onEvent(executorRemoved);
    }

    @Override
    public final void onExecutorBlacklisted(SparkListenerExecutorBlacklisted executorBlacklisted) {
        onEvent(executorBlacklisted);
    }

    @Override
    public final void onExecutorUnblacklisted(
            SparkListenerExecutorUnblacklisted executorUnblacklisted) {
        onEvent(executorUnblacklisted);
    }

    @Override
    public final void onNodeBlacklisted(SparkListenerNodeBlacklisted nodeBlacklisted) {
        onEvent(nodeBlacklisted);
    }

    @Override
    public final void onNodeUnblacklisted(SparkListenerNodeUnblacklisted nodeUnblacklisted) {
        onEvent(nodeUnblacklisted);
    }

    @Override
    public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) {
        onEvent(blockUpdated);
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        onEvent(event);
    }
}
