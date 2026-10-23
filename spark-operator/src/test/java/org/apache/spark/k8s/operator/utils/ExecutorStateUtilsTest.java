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

package org.apache.spark.k8s.operator.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.spec.RestartConfig;
import org.apache.spark.k8s.operator.spec.RestartPolicy;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.status.ExecutorState;

class ExecutorStateUtilsTest {
  private SparkApplication app;

  @BeforeEach
  void setUp() {
    app = TestUtils.createMockApp("default");
    app.setStatus(
        new ApplicationStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.RunningHealthy, "")));
  }

  private static Pod executorPod(String name, String appId, String executorId, String phase) {
    PodBuilder builder =
        new PodBuilder()
            .withNewMetadata()
            .withName(name)
            .addToLabels(Constants.LABEL_SPARK_ROLE_NAME, Constants.LABEL_SPARK_ROLE_EXECUTOR_VALUE)
            .addToLabels(Constants.LABEL_SPARK_APP_ID, appId)
            .endMetadata();
    if (executorId != null) {
      builder
          .editMetadata()
          .addToLabels(Constants.LABEL_SPARK_EXECUTOR_ID, executorId)
          .endMetadata();
    }
    if (phase != null) {
      builder.withNewStatus().withPhase(phase).endStatus();
    }
    return builder.build();
  }

  private Pod currentExecutorPod(String name, String executorId, String phase) {
    return executorPod(name, ExecutorStateUtils.currentAttemptAppId(app), executorId, phase);
  }

  private void setState(ApplicationStateSummary state, Map<String, ExecutorState> executors) {
    app.setStatus(
        app.getStatus()
            .appendNewState(new ApplicationState(state, ""))
            .withExecutorState(executors));
  }

  @Test
  void fromPodPhaseMapsEachPhase() {
    assertEquals(ExecutorState.PENDING,
        ExecutorStateUtils.fromPodPhase(executorPod("e", "a", "1", "Pending")));
    assertEquals(ExecutorState.RUNNING,
        ExecutorStateUtils.fromPodPhase(executorPod("e", "a", "1", "Running")));
    assertEquals(ExecutorState.COMPLETED,
        ExecutorStateUtils.fromPodPhase(executorPod("e", "a", "1", "Succeeded")));
    assertEquals(ExecutorState.FAILED,
        ExecutorStateUtils.fromPodPhase(executorPod("e", "a", "1", "Failed")));
    assertEquals(ExecutorState.UNKNOWN,
        ExecutorStateUtils.fromPodPhase(executorPod("e", "a", "1", "Unknown")));
    assertEquals(ExecutorState.UNKNOWN,
        ExecutorStateUtils.fromPodPhase(executorPod("e", "a", "1", null)));
  }

  @Test
  void currentAttemptAppIdFollowsTheAttempt() {
    assertEquals("sentinel-0", ExecutorStateUtils.currentAttemptAppId(app));
    RestartConfig restartConfig = new RestartConfig();
    restartConfig.setRestartPolicy(RestartPolicy.Always);
    restartConfig.setMaxRestartAttempts(1L);
    app.setStatus(
        app.getStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.Failed, ""))
            .terminateOrRestart(restartConfig, "", false));
    assertEquals("sentinel-1", ExecutorStateUtils.currentAttemptAppId(app));
    app.getSpec().getSparkConf().put("spark.app.id", "configured");
    assertEquals("configured", ExecutorStateUtils.currentAttemptAppId(app));
  }

  @Test
  void observeRecordsTheExecutorsOfTheCurrentAttempt() {
    SortedMap<String, ExecutorState> executorState =
        ExecutorStateUtils.observe(
            app,
            List.of(
                currentExecutorPod("app-exec-1", "1", "Running"),
                currentExecutorPod("app-exec-2", "2", "Pending"),
                executorPod("old-exec-1", "sentinel-previous", "1", "Running")),
            1000);
    assertEquals(
        Map.of("app-exec-1", ExecutorState.RUNNING, "app-exec-2", ExecutorState.PENDING),
        executorState);
  }

  @Test
  void observeMatchesAConfiguredAppId() {
    app.getSpec().getSparkConf().put("spark.app.id", "configured");
    SortedMap<String, ExecutorState> executorState =
        ExecutorStateUtils.observe(
            app,
            List.of(
                executorPod("app-exec-1", "configured", "1", "Running"),
                executorPod("app-exec-2", "sentinel-0", "2", "Running")),
            1000);
    assertEquals(Map.of("app-exec-1", ExecutorState.RUNNING), executorState);
  }

  @Test
  void observeSkipsExecutorIdsAboveTheLimit() {
    List<Pod> pods =
        List.of(
            currentExecutorPod("app-exec-2", "2", "Running"),
            currentExecutorPod("app-exec-3", "3", "Running"),
            currentExecutorPod("app-exec-x", "x", "Running"),
            currentExecutorPod("app-exec-none", null, "Running"));
    assertEquals(
        List.of("app-exec-2", "app-exec-none", "app-exec-x"),
        List.copyOf(ExecutorStateUtils.observe(app, pods, 2).keySet()));
    assertEquals(4, ExecutorStateUtils.observe(app, pods, 0).size());
    assertEquals(4, ExecutorStateUtils.observe(app, pods, -1).size());
  }

  @Test
  void observeKeepsAMissingExecutorAsUnknownWhileTheDriverRuns() {
    setState(
        ApplicationStateSummary.RunningHealthy,
        Map.of(
            "app-exec-1", ExecutorState.RUNNING,
            "app-exec-2", ExecutorState.PENDING,
            "app-exec-3", ExecutorState.FAILED,
            "app-exec-4", ExecutorState.COMPLETED));
    assertEquals(
        Map.of(
            "app-exec-1", ExecutorState.UNKNOWN,
            "app-exec-2", ExecutorState.UNKNOWN,
            "app-exec-3", ExecutorState.FAILED,
            "app-exec-4", ExecutorState.COMPLETED),
        ExecutorStateUtils.observe(app, List.of(), 1000));
  }

  @Test
  void observeCompletesAMissingExecutorAfterTheApplicationSucceeded() {
    setState(ApplicationStateSummary.Succeeded, Map.of("app-exec-1", ExecutorState.RUNNING));
    assertEquals(
        Map.of("app-exec-1", ExecutorState.COMPLETED),
        ExecutorStateUtils.observe(app, List.of(), 1000));

    RestartConfig noRetryConfig = new RestartConfig();
    noRetryConfig.setRestartPolicy(RestartPolicy.Never);
    app.setStatus(app.getStatus().terminateOrRestart(noRetryConfig, "", false));
    assertEquals(
        ApplicationStateSummary.ResourceReleased,
        app.getStatus().getCurrentState().getCurrentStateSummary());
    assertEquals(
        Map.of("app-exec-1", ExecutorState.COMPLETED),
        ExecutorStateUtils.observe(app, List.of(), 1000));
  }

  @Test
  void observeFailsAMissingExecutorOtherwise() {
    setState(ApplicationStateSummary.Failed, Map.of("app-exec-1", ExecutorState.UNKNOWN));
    assertEquals(
        Map.of("app-exec-1", ExecutorState.FAILED),
        ExecutorStateUtils.observe(app, List.of(), 1000));
  }

  @Test
  void observeUpdatesARecordedExecutorWhosePodIsPresent() {
    setState(ApplicationStateSummary.RunningHealthy, Map.of("app-exec-1", ExecutorState.FAILED));
    assertEquals(
        Map.of("app-exec-1", ExecutorState.RUNNING),
        ExecutorStateUtils.observe(
            app, List.of(currentExecutorPod("app-exec-1", "1", "Running")), 1000));
  }

  @Test
  void observeDoesNotChangeTheStatus() {
    setState(ApplicationStateSummary.RunningHealthy, Map.of("app-exec-1", ExecutorState.RUNNING));
    ExecutorStateUtils.observe(
        app, List.of(currentExecutorPod("app-exec-2", "2", "Running")), 1000);
    assertEquals(Map.of("app-exec-1", ExecutorState.RUNNING), app.getStatus().getExecutorState());
  }

  @Test
  void hasSucceededLooksAtTheLatestStoppingState() {
    assertFalse(ExecutorStateUtils.hasSucceeded(app.getStatus()));
    setState(ApplicationStateSummary.Succeeded, Map.of());
    assertTrue(ExecutorStateUtils.hasSucceeded(app.getStatus()));
  }
}
