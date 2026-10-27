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

import static org.apache.spark.k8s.operator.utils.TestUtils.setConfigKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;

@SuppressWarnings("PMD.AvoidUsingHardCodedIP")
class DynamicAllocationTargetsTest {
  /** The metrics servlet JSON of a Spark 4 driver, trimmed to a few gauges. */
  private static final String METRICS_JSON =
      """
      {"version":"4.0.0","gauges":{
        "spark-a1.driver.BlockManager.memory.maxMem_MB":{"value":434},
        "spark-a1.driver.ExecutorAllocationManager.executors.numberAllExecutors":{"value":2},
        "spark-a1.driver.ExecutorAllocationManager.executors.numberTargetExecutors":{"value":5}},
       "counters":{},"histograms":{},"meters":{},"timers":{}}
      """;

  private SparkApplication app;
  private Pod driverPod;

  @BeforeEach
  void setUp() {
    setConfigKey(SparkOperatorConf.DYNAMIC_ALLOCATION_TARGET_POLL_INTERVAL_SECONDS, 10L);
    app = TestUtils.createMockApp("default");
    app.getSpec().getSparkConf().put("spark.dynamicAllocation.enabled", "true");
    driverPod = driverPod("10.1.2.3");
  }

  @AfterEach
  void tearDown() {
    setConfigKey(SparkOperatorConf.DYNAMIC_ALLOCATION_TARGET_POLL_INTERVAL_SECONDS, 0L);
  }

  private static Pod driverPod(String ip) {
    return new PodBuilder()
        .withNewMetadata()
        .withName("sentinel-0-driver")
        .withNamespace("default")
        .endMetadata()
        .withNewStatus()
        .withPodIP(ip)
        .endStatus()
        .build();
  }

  private static InputStream body(String json) {
    return new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void parsesTheTargetGauge() throws IOException {
    assertEquals(Optional.of(5), DynamicAllocationTargets.parseTarget(body(METRICS_JSON)));
  }

  @Test
  void parsesTheTargetGaugeOfACustomMetricsNamespace() throws IOException {
    String json =
        "{\"gauges\":{\"my-app.driver.ExecutorAllocationManager.executors."
            + "numberTargetExecutors\":{\"value\":0}}}";
    assertEquals(Optional.of(0), DynamicAllocationTargets.parseTarget(body(json)));
  }

  @Test
  void reportsNoTargetWithoutDynamicAllocationMetrics() throws IOException {
    String json = "{\"gauges\":{\"spark-a1.driver.BlockManager.memory.maxMem_MB\":{\"value\":1}}}";
    assertEquals(Optional.empty(), DynamicAllocationTargets.parseTarget(body(json)));
    assertEquals(Optional.empty(), DynamicAllocationTargets.parseTarget(body("{}")));
  }

  @Test
  void ignoresTheGaugeOfAnotherInstance() throws IOException {
    String json =
        "{\"gauges\":{\"spark-a1.executor.ExecutorAllocationManager.executors."
            + "numberTargetExecutors\":{\"value\":3}}}";
    assertEquals(Optional.empty(), DynamicAllocationTargets.parseTarget(body(json)));
  }

  @Test
  void rejectsInvalidTargets() throws IOException {
    String prefix =
        "{\"gauges\":{\"spark-a1.driver.ExecutorAllocationManager.executors."
            + "numberTargetExecutors\":{\"value\":";
    for (String value : List.of("-1", "2.5", "\"5\"", "null", "99999999999")) {
      assertEquals(
          Optional.empty(), DynamicAllocationTargets.parseTarget(body(prefix + value + "}}}")));
    }
  }

  @Test
  void rejectsInvalidAndOversizedBodies() throws IOException {
    assertEquals(Optional.empty(), DynamicAllocationTargets.parseTarget(body("not json")));
    byte[] oversized = new byte[DynamicAllocationTargets.MAX_RESPONSE_BYTES + 1];
    assertEquals(
        Optional.empty(),
        DynamicAllocationTargets.parseTarget(new ByteArrayInputStream(oversized)));
  }

  @Test
  void buildsTheMetricsUriOfTheDriverPod() {
    assertEquals(
        Optional.of(URI.create("http://10.1.2.3:4040/metrics/json")),
        DynamicAllocationTargets.metricsUri(app, driverPod));
    assertEquals(
        Optional.of(URI.create("http://[fd00::12]:4040/metrics/json")),
        DynamicAllocationTargets.metricsUri(app, driverPod("fd00::12")));
  }

  @Test
  void buildsTheMetricsUriFromTheUiAndMetricsConfiguration() {
    app.getSpec().getSparkConf().put("spark.ui.port", "8090");
    app.getSpec().getSparkConf().put("spark.metrics.conf.*.sink.servlet.path", "/m/all");
    assertEquals(
        Optional.of(URI.create("http://10.1.2.3:8090/m/all")),
        DynamicAllocationTargets.metricsUri(app, driverPod));
    app.getSpec().getSparkConf().put("spark.metrics.conf.driver.sink.servlet.path", "/m/driver");
    assertEquals(
        Optional.of(URI.create("http://10.1.2.3:8090/m/driver")),
        DynamicAllocationTargets.metricsUri(app, driverPod));
  }

  @Test
  void hasNoMetricsUriWithoutAPodIpOrAKnownPort() {
    assertEquals(Optional.empty(), DynamicAllocationTargets.metricsUri(app, driverPod(null)));
    assertEquals(Optional.empty(), DynamicAllocationTargets.metricsUri(app, new Pod()));
    app.getSpec().getSparkConf().put("spark.ui.port", "0");
    assertEquals(Optional.empty(), DynamicAllocationTargets.metricsUri(app, driverPod));
  }

  @Test
  void tracksOnlyDynamicAllocationApplicationsWithAnHttpUi() {
    assertTrue(DynamicAllocationTargets.isTracked(app));

    app.getSpec().getSparkConf().put("spark.ssl.enabled", "true");
    assertFalse(DynamicAllocationTargets.isTracked(app));
    app.getSpec().getSparkConf().put("spark.ssl.ui.enabled", "false");
    assertTrue(DynamicAllocationTargets.isTracked(app));

    app.getSpec().getSparkConf().put("spark.ui.enabled", "false");
    assertFalse(DynamicAllocationTargets.isTracked(app));
    app.getSpec().getSparkConf().put("spark.ui.enabled", "true");

    app.getSpec().getSparkConf().put("spark.dynamicAllocation.enabled", "false");
    assertFalse(DynamicAllocationTargets.isTracked(app));
  }

  @Test
  void tracksNothingWhenPollingIsDisabled() {
    setConfigKey(SparkOperatorConf.DYNAMIC_ALLOCATION_TARGET_POLL_INTERVAL_SECONDS, 0L);
    assertFalse(DynamicAllocationTargets.isTracked(app));
  }

  @Test
  void readsADriverAtMostOncePerInterval() {
    MutableClock clock = new MutableClock(Instant.parse("2026-10-27T00:00:00Z"));
    List<URI> fetched = new ArrayList<>();
    DynamicAllocationTargets targets =
        new DynamicAllocationTargets(
            (uri, timeout) -> {
              fetched.add(uri);
              return body(METRICS_JSON);
            },
            clock);

    assertEquals(Optional.of(5), targets.pollIfDue(app, driverPod));
    clock.advance(Duration.ofSeconds(9));
    assertEquals(Optional.empty(), targets.pollIfDue(app, driverPod));
    clock.advance(Duration.ofSeconds(1));
    assertEquals(Optional.of(5), targets.pollIfDue(app, driverPod));
    assertEquals(2, fetched.size());

    // A forgotten application is read again right away
    targets.forget(ResourceID.fromResource(app));
    assertEquals(Optional.of(5), targets.pollIfDue(app, driverPod));
    assertEquals(3, fetched.size());
  }

  @Test
  void reportsNoTargetWhenTheDriverCannotBeRead() {
    MutableClock clock = new MutableClock(Instant.parse("2026-10-27T00:00:00Z"));
    DynamicAllocationTargets targets =
        new DynamicAllocationTargets(
            (uri, timeout) -> {
              throw new IOException("Connection refused");
            },
            clock);
    assertEquals(Optional.empty(), targets.pollIfDue(app, driverPod));
  }

  @Test
  void passesTheConfiguredTimeout() {
    setConfigKey(SparkOperatorConf.DYNAMIC_ALLOCATION_TARGET_POLL_TIMEOUT_MILLIS, 750L);
    try {
      List<Duration> timeouts = new ArrayList<>();
      DynamicAllocationTargets targets =
          new DynamicAllocationTargets(
              (uri, timeout) -> {
                timeouts.add(timeout);
                return body(METRICS_JSON);
              },
              new MutableClock(Instant.EPOCH));
      targets.pollIfDue(app, driverPod);
      assertEquals(List.of(Duration.ofMillis(750)), timeouts);
    } finally {
      setConfigKey(SparkOperatorConf.DYNAMIC_ALLOCATION_TARGET_POLL_TIMEOUT_MILLIS, 2000L);
    }
  }

  @Test
  void skipsADriverPodWithoutIp() {
    List<URI> fetched = new ArrayList<>();
    DynamicAllocationTargets targets =
        new DynamicAllocationTargets(
            (uri, timeout) -> {
              fetched.add(uri);
              return body(METRICS_JSON);
            },
            new MutableClock(Instant.EPOCH));
    assertEquals(Optional.empty(), targets.pollIfDue(app, driverPod(null)));
    assertTrue(fetched.isEmpty());
  }

  private static final class MutableClock extends Clock {
    private Instant now;

    MutableClock(Instant now) {
      super();
      this.now = now;
    }

    void advance(Duration duration) {
      now = now.plus(duration);
    }

    @Override
    public Instant instant() {
      return now;
    }

    @Override
    public ZoneOffset getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }
  }
}
