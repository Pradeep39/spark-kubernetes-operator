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

import static org.apache.spark.k8s.operator.utils.TestUtils.setConfigKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.DynamicAllocationTargets;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;
import org.apache.spark.k8s.operator.utils.TestUtils;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
class AppDynamicAllocationTargetStepTest {
  private SparkAppContext mockContext;
  private SparkAppStatusRecorder mockRecorder;
  private SparkApplication app;
  private final AtomicInteger reportedTarget = new AtomicInteger(5);
  private final AtomicInteger fetches = new AtomicInteger();
  private AppDynamicAllocationTargetStep step;

  @BeforeEach
  void setUp() {
    setConfigKey(SparkOperatorConf.DYNAMIC_ALLOCATION_TARGET_POLL_INTERVAL_SECONDS, 10L);
    mockContext = mock(SparkAppContext.class);
    mockRecorder = mock(SparkAppStatusRecorder.class);
    app = TestUtils.createMockApp("default");
    app.getSpec().getSparkConf().put("spark.dynamicAllocation.enabled", "true");
    app.setStatus(
        new ApplicationStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.RunningHealthy, "")));
    // A clock which never advances would space the reads out, so each read gets a new instant
    AtomicInteger tick = new AtomicInteger();
    Clock clock =
        new Clock() {
          @Override
          public Instant instant() {
            return Instant.EPOCH.plusSeconds(60L * tick.getAndIncrement());
          }

          @Override
          public ZoneOffset getZone() {
            return ZoneOffset.UTC;
          }

          @Override
          public Clock withZone(ZoneId zone) {
            return this;
          }
        };
    step =
        new AppDynamicAllocationTargetStep(
            new DynamicAllocationTargets(
                (uri, timeout) -> {
                  fetches.incrementAndGet();
                  String json =
                      "{\"gauges\":{\"spark-a1.driver.ExecutorAllocationManager.executors."
                          + "numberTargetExecutors\":{\"value\":"
                          + reportedTarget.get()
                          + "}}}";
                  return new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
                },
                clock));
    when(mockContext.getResource()).thenReturn(app);
    when(mockContext.getDriverPod()).thenReturn(Optional.of(driverPod()));
    when(mockRecorder.persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class)))
        .thenAnswer(
            invocation -> {
              app.setStatus(invocation.getArgument(1));
              return true;
            });
  }

  @AfterEach
  void tearDown() {
    setConfigKey(SparkOperatorConf.DYNAMIC_ALLOCATION_TARGET_POLL_INTERVAL_SECONDS, 0L);
  }

  private static Pod driverPod() {
    return new PodBuilder()
        .withNewMetadata()
        .withName("sentinel-0-driver")
        .withNamespace("default")
        .endMetadata()
        .withNewStatus()
        .withPodIP("10.1.2.3")
        .endStatus()
        .build();
  }

  @Test
  void recordsAChangedTarget() {
    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));
    assertEquals(5, app.getStatus().getTargetExecutors());
    assertEquals(
        ApplicationStateSummary.RunningHealthy,
        app.getStatus().getCurrentState().getCurrentStateSummary());

    reportedTarget.set(8);
    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));
    assertEquals(8, app.getStatus().getTargetExecutors());
  }

  @Test
  void skipsTheStatusUpdateWhenTheTargetDidNotChange() {
    app.setStatus(app.getStatus().withTargetExecutors(5));

    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));

    assertEquals(1, fetches.get());
    verify(mockRecorder, never())
        .persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class));
  }

  @Test
  void skipsAnApplicationWhoseDriverIsNotReady() {
    app.setStatus(
        new ApplicationStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverStarted, "")));

    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));

    assertEquals(0, fetches.get());
    assertNull(app.getStatus().getTargetExecutors());
  }

  @Test
  void skipsAnApplicationWithoutDynamicAllocation() {
    app.getSpec().getSparkConf().put("spark.dynamicAllocation.enabled", "false");

    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));

    assertEquals(0, fetches.get());
  }

  @Test
  void skipsAnApplicationWithoutDriverPod() {
    when(mockContext.getDriverPod()).thenReturn(Optional.empty());

    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));

    assertEquals(0, fetches.get());
  }

  @Test
  void requeuesWhenTheStatusCannotBeWritten() {
    when(mockRecorder.persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class)))
        .thenReturn(false);

    ReconcileProgress progress = step.reconcile(mockContext, mockRecorder);

    assertEquals(ReconcileProgress.completeAndImmediateRequeue(), progress);
    assertNull(app.getStatus().getTargetExecutors());
  }
}
