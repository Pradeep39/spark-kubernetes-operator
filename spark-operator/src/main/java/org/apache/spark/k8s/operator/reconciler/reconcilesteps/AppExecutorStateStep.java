/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.k8s.operator.reconciler.reconcilesteps;

import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.proceed;

import java.util.Collection;
import java.util.SortedMap;

import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ExecutorState;
import org.apache.spark.k8s.operator.utils.DeletedExecutorPods;
import org.apache.spark.k8s.operator.utils.ExecutorStateUtils;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

/**
 * Records the state of each executor of the application in its {@code status.executorState}, the
 * way the Kubeflow Spark Operator does. The executor pods come from the informer of the
 * application's pods, so a change of an executor pod reconciles the application and is recorded
 * here. The final states of the executor pods which were deleted since come from {@link
 * DeletedExecutorPods}, and are forgotten there once they are recorded. The status is written only
 * when an executor state changed.
 */
@Slf4j
@RequiredArgsConstructor
public final class AppExecutorStateStep extends AppReconcileStep {
  private final DeletedExecutorPods deletedExecutorPods;

  /**
   * Reconciles the executor states of the application from its executor pods.
   *
   * @param context The SparkAppContext for the application.
   * @param statusRecorder The SparkAppStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating the next step.
   */
  @Override
  public ReconcileProgress reconcile(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder) {
    SparkApplication app = context.getResource();
    ResourceID appId = ResourceID.fromResource(app);
    Collection<DeletedExecutorPods.DeletedExecutor> deletedExecutors =
        deletedExecutorPods.get(appId);
    // A suspended application releases its executors, and its executor states start over when it
    // is suspended, so the pods which are being deleted are not recorded again.
    if (ApplicationStateSummary.Suspended
        == app.getStatus().getCurrentState().getCurrentStateSummary()) {
      deletedExecutorPods.remove(appId, deletedExecutors);
      return proceed();
    }
    SortedMap<String, ExecutorState> executorState =
        ExecutorStateUtils.observe(
            app,
            context.getExecutorsForApplication(),
            deletedExecutors,
            SparkOperatorConf.MAX_TRACKED_EXECUTOR_PER_APP.getValue());
    if (executorState.equals(app.getStatus().getExecutorState())) {
      deletedExecutorPods.remove(appId, deletedExecutors);
      return proceed();
    }
    log.debug("Executor states changed to {}", executorState);
    ReconcileProgress progress =
        attemptStatusUpdate(
            context, statusRecorder, app.getStatus().withExecutorState(executorState), proceed());
    // Kept for the next reconciliation if the status could not be written
    if (!progress.isCompleted()) {
      deletedExecutorPods.remove(appId, deletedExecutors);
    }
    return progress;
  }
}
