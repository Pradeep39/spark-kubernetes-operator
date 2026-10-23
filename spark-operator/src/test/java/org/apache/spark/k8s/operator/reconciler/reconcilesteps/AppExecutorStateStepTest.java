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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.status.ExecutorState;
import org.apache.spark.k8s.operator.utils.DeletedExecutorPods;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;
import org.apache.spark.k8s.operator.utils.TestUtils;

class AppExecutorStateStepTest {
  private SparkAppContext mockContext;
  private SparkAppStatusRecorder mockRecorder;
  private SparkApplication app;
  private DeletedExecutorPods deletedExecutorPods;
  private AppExecutorStateStep step;

  @BeforeEach
  void setUp() {
    mockContext = mock(SparkAppContext.class);
    mockRecorder = mock(SparkAppStatusRecorder.class);
    app = TestUtils.createMockApp("default");
    app.setStatus(
        new ApplicationStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.RunningHealthy, "")));
    deletedExecutorPods = new DeletedExecutorPods();
    step = new AppExecutorStateStep(deletedExecutorPods);
    when(mockContext.getResource()).thenReturn(app);
    when(mockRecorder.persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class)))
        .thenAnswer(
            invocation -> {
              app.setStatus(invocation.getArgument(1));
              return true;
            });
  }

  private static Pod executorPod(String name, String phase) {
    return new PodBuilder()
        .withNewMetadata()
        .withName(name)
        .withNamespace("default")
        .addToLabels(Constants.LABEL_SPARK_APPLICATION_NAME, "sentinel")
        .addToLabels(Constants.LABEL_SPARK_ROLE_NAME, Constants.LABEL_SPARK_ROLE_EXECUTOR_VALUE)
        .addToLabels(Constants.LABEL_SPARK_APP_ID, "sentinel-0")
        .endMetadata()
        .withNewStatus()
        .withPhase(phase)
        .endStatus()
        .build();
  }

  @Test
  void recordsAChangedExecutorState() {
    when(mockContext.getExecutorsForApplication())
        .thenReturn(Set.of(executorPod("app-exec-1", "Running")));

    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));

    assertEquals(Map.of("app-exec-1", ExecutorState.RUNNING), app.getStatus().getExecutorState());
    assertEquals(
        ApplicationStateSummary.RunningHealthy,
        app.getStatus().getCurrentState().getCurrentStateSummary());
  }

  @Test
  void skipsTheStatusUpdateWhenNothingChanged() {
    app.setStatus(app.getStatus().withExecutorState(Map.of("app-exec-1", ExecutorState.RUNNING)));
    when(mockContext.getExecutorsForApplication())
        .thenReturn(Set.of(executorPod("app-exec-1", "Running")));

    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));

    verify(mockRecorder, never())
        .persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class));
  }

  @Test
  void skipsASuspendedApplication() {
    app.setStatus(
        app.getStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.Suspended, "")));
    when(mockContext.getExecutorsForApplication())
        .thenReturn(Set.of(executorPod("app-exec-1", "Running")));

    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));

    verify(mockRecorder, never())
        .persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class));
    assertEquals(Map.of(), app.getStatus().getExecutorState());
  }

  @Test
  void requeuesWhenTheStatusUpdateFails() {
    when(mockRecorder.persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class)))
        .thenReturn(false);
    when(mockContext.getExecutorsForApplication())
        .thenReturn(Set.of(executorPod("app-exec-1", "Pending")));

    assertEquals(
        ReconcileProgress.completeAndImmediateRequeue(),
        step.reconcile(mockContext, mockRecorder));
  }

  @Test
  void recordsAndForgetsTheFinalStateOfADeletedExecutor() {
    app.setStatus(app.getStatus().withExecutorState(Map.of("app-exec-1", ExecutorState.RUNNING)));
    deletedExecutorPods.record(executorPod("app-exec-1", "Succeeded"));
    when(mockContext.getExecutorsForApplication()).thenReturn(Set.of());

    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));

    assertEquals(
        Map.of("app-exec-1", ExecutorState.COMPLETED), app.getStatus().getExecutorState());
    assertTrue(deletedExecutorPods.get(ResourceID.fromResource(app)).isEmpty());
  }

  @Test
  void keepsADeletedExecutorWhenTheStatusUpdateFails() {
    when(mockRecorder.persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class)))
        .thenReturn(false);
    deletedExecutorPods.record(executorPod("app-exec-1", "Failed"));
    when(mockContext.getExecutorsForApplication()).thenReturn(Set.of());

    assertEquals(
        ReconcileProgress.completeAndImmediateRequeue(),
        step.reconcile(mockContext, mockRecorder));

    assertEquals(1, deletedExecutorPods.get(ResourceID.fromResource(app)).size());
  }

  @Test
  void forgetsTheDeletedExecutorsOfASuspendedApplication() {
    app.setStatus(
        app.getStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.Suspended, "")));
    deletedExecutorPods.record(executorPod("app-exec-1", "Succeeded"));

    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));

    assertTrue(deletedExecutorPods.get(ResourceID.fromResource(app)).isEmpty());
    assertEquals(Map.of(), app.getStatus().getExecutorState());
  }
}
