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

import java.util.Optional;

import io.fabric8.kubernetes.api.model.Pod;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.utils.DynamicAllocationTargets;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

/**
 * Records the Spark Dynamic Allocation target of a running application in its {@code
 * status.targetExecutors}, read from its driver by {@link DynamicAllocationTargets}. The target
 * changes before the driver creates or deletes the executor pods, so schedulers such as Kueue can
 * follow the demand of the application ahead of its pods. The status is written only when the
 * target changed.
 */
@Slf4j
@RequiredArgsConstructor
public final class AppDynamicAllocationTargetStep extends AppReconcileStep {
  private final DynamicAllocationTargets targets;

  /**
   * Reconciles the Dynamic Allocation target of the application from its driver.
   *
   * @param context The SparkAppContext for the application.
   * @param statusRecorder The SparkAppStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating the next step.
   */
  @Override
  public ReconcileProgress reconcile(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder) {
    SparkApplication app = context.getResource();
    if (!DynamicAllocationTargets.isTracked(app)
        || !app.getStatus().getCurrentState().getCurrentStateSummary().isDriverReady()) {
      return proceed();
    }
    Optional<Pod> driverPod = context.getDriverPod();
    if (driverPod.isEmpty()) {
      return proceed();
    }
    Optional<Integer> target = targets.pollIfDue(app, driverPod.get());
    if (target.isEmpty() || target.get().equals(app.getStatus().getTargetExecutors())) {
      return proceed();
    }
    log.debug("Dynamic Allocation target changed to {}", target.get());
    return attemptStatusUpdate(
        context, statusRecorder, app.getStatus().withTargetExecutors(target.get()), proceed());
  }
}
