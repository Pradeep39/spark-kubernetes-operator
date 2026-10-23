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

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import io.fabric8.kubernetes.api.model.Pod;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.SparkAppSubmissionWorker;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.status.ExecutorState;

/**
 * Infers the state of each executor of a Spark application from its pods, the way the Kubeflow
 * Spark Operator records its {@code status.executorState}: each executor pod of the current attempt
 * is recorded with the state of its phase, an executor which was recorded is kept after its pod is
 * gone, and an executor whose id is above a configured limit is not recorded.
 */
public final class ExecutorStateUtils {

  /** The Spark configuration which sets the application id, see {@link #currentAttemptAppId}. */
  private static final String SPARK_APP_ID_CONF = "spark.app.id";

  /** The states in which the driver of the current attempt is running. */
  private static final Set<ApplicationStateSummary> DRIVER_RUNNING_STATES =
      EnumSet.of(
          ApplicationStateSummary.DriverStarted,
          ApplicationStateSummary.DriverReady,
          ApplicationStateSummary.InitializedBelowThresholdExecutors,
          ApplicationStateSummary.RunningHealthy,
          ApplicationStateSummary.RunningWithPartialCapacity,
          ApplicationStateSummary.RunningWithBelowThresholdExecutors);

  private ExecutorStateUtils() {}

  /**
   * Returns the state of an executor from the phase of its pod.
   *
   * @param pod The executor pod.
   * @return PENDING, RUNNING, COMPLETED or FAILED for the phases Pending, Running, Succeeded and
   *     Failed, and UNKNOWN for any other phase.
   */
  public static ExecutorState fromPodPhase(final Pod pod) {
    String phase = pod.getStatus() == null ? null : pod.getStatus().getPhase();
    if (phase == null) {
      return ExecutorState.UNKNOWN;
    }
    return switch (phase) {
      case "Pending" -> ExecutorState.PENDING;
      case "Running" -> ExecutorState.RUNNING;
      case "Succeeded" -> ExecutorState.COMPLETED;
      case "Failed" -> ExecutorState.FAILED;
      default -> ExecutorState.UNKNOWN;
    };
  }

  /**
   * Returns the executor states of the application after observing its executor pods.
   *
   * <ul>
   *   <li>An executor pod of the current attempt, see {@link #currentAttemptAppId}, is recorded
   *       with the state of its phase, unless its executor id is above maxTrackedExecutorPerApp.
   *   <li>A recorded executor which has not terminated and whose pod is gone is kept as UNKNOWN
   *       while the driver is running, as COMPLETED after the application succeeded, and as
   *       FAILED otherwise.
   *   <li>Any other recorded executor is kept as it is.
   * </ul>
   *
   * @param app The Spark application, with the executor states recorded so far.
   * @param executorPods The executor pods of the application, of any attempt.
   * @param maxTrackedExecutorPerApp The highest executor id to record, or a non-positive number to
   *     record every executor.
   * @return The executor states by executor pod name.
   */
  public static SortedMap<String, ExecutorState> observe(
      final SparkApplication app,
      final Collection<Pod> executorPods,
      final int maxTrackedExecutorPerApp) {
    ApplicationStatus status = app.getStatus();
    SortedMap<String, ExecutorState> executorState = new TreeMap<>(status.getExecutorState());
    String appId = currentAttemptAppId(app);
    Set<String> observed = new HashSet<>();
    for (Pod pod : executorPods) {
      if (!appId.equals(pod.getMetadata().getLabels().get(Constants.LABEL_SPARK_APP_ID))
          || (maxTrackedExecutorPerApp > 0 && executorId(pod) > maxTrackedExecutorPerApp)) {
        continue;
      }
      String name = pod.getMetadata().getName();
      executorState.put(name, fromPodPhase(pod));
      observed.add(name);
    }
    ApplicationStateSummary currentState = status.getCurrentState().getCurrentStateSummary();
    for (Map.Entry<String, ExecutorState> entry : status.getExecutorState().entrySet()) {
      if (entry.getValue().isTerminated() || observed.contains(entry.getKey())) {
        continue;
      }
      if (DRIVER_RUNNING_STATES.contains(currentState)) {
        executorState.put(entry.getKey(), ExecutorState.UNKNOWN);
      } else if (hasSucceeded(status)) {
        executorState.put(entry.getKey(), ExecutorState.COMPLETED);
      } else {
        executorState.put(entry.getKey(), ExecutorState.FAILED);
      }
    }
    return executorState;
  }

  /**
   * Returns the Spark application id of the current attempt, which Spark sets as the {@link
   * Constants#LABEL_SPARK_APP_ID} label of its executor pods. The operator generates a different id
   * for each attempt, unless the application sets {@code spark.app.id} itself.
   *
   * @param app The Spark application.
   * @return The Spark application id of the current attempt.
   */
  static String currentAttemptAppId(final SparkApplication app) {
    String configured = app.getSpec().getSparkConf().get(SPARK_APP_ID_CONF);
    if (configured == null) {
      return SparkAppSubmissionWorker.generateSparkAppId(app);
    }
    return configured;
  }

  /**
   * Returns the id of an executor from the {@link Constants#LABEL_SPARK_EXECUTOR_ID} label of its
   * pod, or 0 if the label is missing or not a number, so that such a pod is recorded.
   *
   * @param pod The executor pod.
   * @return The executor id, or 0.
   */
  static long executorId(final Pod pod) {
    String id = pod.getMetadata().getLabels().get(Constants.LABEL_SPARK_EXECUTOR_ID);
    if (id == null) {
      return 0L;
    }
    try {
      return Long.parseLong(id);
    } catch (NumberFormatException e) {
      return 0L;
    }
  }

  /**
   * Checks whether the current attempt of the application succeeded: its latest stopping state is
   * Succeeded. This also holds once the resources of a succeeded attempt are released.
   *
   * @param status The status of the application.
   * @return True if the current attempt succeeded.
   */
  static boolean hasSucceeded(final ApplicationStatus status) {
    for (ApplicationState state : status.getStateTransitionHistory().reversed().values()) {
      if (state.getCurrentStateSummary().isStopping()) {
        return ApplicationStateSummary.Succeeded == state.getCurrentStateSummary();
      }
    }
    return false;
  }
}
