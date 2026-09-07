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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

class AppSuspendStepTest {
  private SparkAppContext mockContext;
  private SparkAppStatusRecorder mockRecorder;
  private SparkApplication app;
  private ApplicationSpec appSpec;
  private AppSuspendStep appSuspendStep;

  @BeforeEach
  void setUp() {
    mockContext = mock(SparkAppContext.class);
    mockRecorder = mock(SparkAppStatusRecorder.class);
    app = new SparkApplication();
    appSpec = new ApplicationSpec();
    appSuspendStep = new AppSuspendStep();

    app.setSpec(appSpec);
    app.setStatus(new ApplicationStatus());

    when(mockContext.getResource()).thenReturn(app);
    when(mockRecorder.appendNewStateAndPersist(
            any(SparkAppContext.class), any(ApplicationState.class)))
        .thenAnswer(
            invocation -> {
              ApplicationState newState = invocation.getArgument(1);
              app.setStatus(app.getStatus().appendNewState(newState));
              return true;
            });
  }

  private void moveToState(ApplicationStateSummary summary) {
    app.setStatus(app.getStatus().appendNewState(new ApplicationState(summary, "test")));
  }

  private ApplicationStateSummary currentStateSummary() {
    return app.getStatus().getCurrentState().getCurrentStateSummary();
  }

  @Test
  void unsetSuspendProceedsWithoutStatusUpdate() {
    ReconcileProgress progress = appSuspendStep.reconcile(mockContext, mockRecorder);

    assertFalse(progress.isCompleted());
    assertEquals(ApplicationStateSummary.Submitted, currentStateSummary());
    verify(mockRecorder, never())
        .appendNewStateAndPersist(any(SparkAppContext.class), any(ApplicationState.class));
  }

  @Test
  void suspendFalseProceedsWithoutStatusUpdate() {
    appSpec.setSuspend(false);

    ReconcileProgress progress = appSuspendStep.reconcile(mockContext, mockRecorder);

    assertFalse(progress.isCompleted());
    assertEquals(ApplicationStateSummary.Submitted, currentStateSummary());
  }

  @Test
  void suspendBeforeStartUpWithholdsDriverRequest() {
    appSpec.setSuspend(true);

    ReconcileProgress progress = appSuspendStep.reconcile(mockContext, mockRecorder);

    assertTrue(progress.isCompleted());
    assertEquals(ApplicationStateSummary.Suspended, currentStateSummary());
  }

  @Test
  void suspendOnScheduledToRestartWithholdsDriverRequest() {
    moveToState(ApplicationStateSummary.ScheduledToRestart);
    appSpec.setSuspend(true);

    ReconcileProgress progress = appSuspendStep.reconcile(mockContext, mockRecorder);

    assertTrue(progress.isCompleted());
    assertEquals(ApplicationStateSummary.Suspended, currentStateSummary());
  }

  @Test
  void alreadySuspendedApplicationIsNotUpdatedAgain() {
    moveToState(ApplicationStateSummary.Suspended);
    appSpec.setSuspend(true);

    ReconcileProgress progress = appSuspendStep.reconcile(mockContext, mockRecorder);

    assertTrue(progress.isCompleted());
    assertTrue(progress.isRequeue());
    assertEquals(ApplicationStateSummary.Suspended, currentStateSummary());
    verify(mockRecorder, never())
        .appendNewStateAndPersist(any(SparkAppContext.class), any(ApplicationState.class));
  }

  @Test
  void clearingSuspendOnSuspendedApplicationProceedsToInit() {
    moveToState(ApplicationStateSummary.Suspended);
    appSpec.setSuspend(false);

    ReconcileProgress progress = appSuspendStep.reconcile(mockContext, mockRecorder);

    // proceeding lets AppInitStep run, which accepts Suspended as an initializing state
    assertFalse(progress.isCompleted());
    assertTrue(ApplicationStateSummary.Suspended.isInitializing());
  }

  @Test
  void suspendOnRequestedDriverAsksForStop() {
    moveToState(ApplicationStateSummary.DriverRequested);
    appSpec.setSuspend(true);

    ReconcileProgress progress = appSuspendStep.reconcile(mockContext, mockRecorder);

    assertTrue(progress.isCompleted());
    assertEquals(ApplicationStateSummary.StoppedByScheduler, currentStateSummary());
  }

  @Test
  void suspendOnRunningApplicationAsksForStop() {
    moveToState(ApplicationStateSummary.RunningHealthy);
    appSpec.setSuspend(true);

    ReconcileProgress progress = appSuspendStep.reconcile(mockContext, mockRecorder);

    assertTrue(progress.isCompleted());
    assertEquals(ApplicationStateSummary.StoppedByScheduler, currentStateSummary());
  }

  @Test
  void suspendOnStoppingApplicationIsIgnored() {
    moveToState(ApplicationStateSummary.Failed);
    appSpec.setSuspend(true);

    ReconcileProgress progress = appSuspendStep.reconcile(mockContext, mockRecorder);

    assertFalse(progress.isCompleted());
    assertEquals(ApplicationStateSummary.Failed, currentStateSummary());
    verify(mockRecorder, never())
        .appendNewStateAndPersist(any(SparkAppContext.class), any(ApplicationState.class));
  }

  @Test
  void suspendOnTerminatedApplicationIsIgnored() {
    moveToState(ApplicationStateSummary.ResourceReleased);
    appSpec.setSuspend(true);

    ReconcileProgress progress = appSuspendStep.reconcile(mockContext, mockRecorder);

    assertFalse(progress.isCompleted());
    assertEquals(ApplicationStateSummary.ResourceReleased, currentStateSummary());
  }
}
