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

import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndDefaultRequeue;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.proceed;

import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

/**
 * Honors the {@code .spec.suspend} flag so that an external scheduler can gate admission of an
 * application, or reclaim the resources of one that is already running.
 *
 * <p>This step runs after {@link AppCleanUpStep} and before any state-specific step, so that a
 * suspended application never reaches {@link AppInitStep}.
 */
@Slf4j
public final class AppSuspendStep extends AppReconcileStep {

  /**
   * Withholds or releases the driver based on {@code .spec.suspend}.
   *
   * <p>When suspension is requested, the application takes one of two paths:
   *
   * <ul>
   *   <li>while initializing, no driver is requested and the application moves to {@link
   *       ApplicationStateSummary#Suspended}, where it waits for the flag to be cleared
   *   <li>once the driver has been requested or is running, the application moves to {@link
   *       ApplicationStateSummary#StoppedByScheduler}, a stopping state that lets {@link
   *       AppCleanUpStep} release the driver and put the application back into {@code Suspended}
   * </ul>
   *
   * @param context The SparkAppContext for the application.
   * @param statusRecorder The SparkAppStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating the next step.
   */
  @Override
  public ReconcileProgress reconcile(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder) {
    SparkApplication app = context.getResource();
    ApplicationStateSummary currentStateSummary =
        app.getStatus().getCurrentState().getCurrentStateSummary();
    if (!isSuspendRequested(app)) {
      return proceed();
    }
    if (currentStateSummary.isStopping() || currentStateSummary.isTerminated()) {
      // the attempt is already being torn down, suspension has nothing left to release
      return proceed();
    }
    if (ApplicationStateSummary.Suspended == currentStateSummary) {
      // already parked, wait for an external scheduler to clear the flag
      log.debug("Application remains suspended, no driver would be requested.");
      return completeAndDefaultRequeue();
    }
    final ApplicationState nextState;
    if (currentStateSummary.isInitializing()) {
      log.info("Application is suspended before start up, withholding driver request.");
      nextState =
          new ApplicationState(ApplicationStateSummary.Suspended, Constants.SUSPENDED_MESSAGE);
    } else {
      log.info("Application is suspended after start up, releasing driver on scheduler request.");
      nextState =
          new ApplicationState(
              ApplicationStateSummary.StoppedByScheduler, Constants.STOPPED_BY_SCHEDULER_MESSAGE);
    }
    return appendStateAndImmediateRequeue(context, statusRecorder, nextState);
  }

  /**
   * Checks whether suspension has been requested for the given application.
   *
   * @param app The SparkApplication to check.
   * @return True if {@code .spec.suspend} is set to true, false when it is false or unset.
   */
  private boolean isSuspendRequested(final SparkApplication app) {
    return Optional.ofNullable(app.getSpec()).map(ApplicationSpec::getSuspend).orElse(false);
  }
}
