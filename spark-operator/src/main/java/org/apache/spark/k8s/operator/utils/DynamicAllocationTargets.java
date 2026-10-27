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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Pod;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;

/**
 * Reads the Spark Dynamic Allocation target of running applications from their drivers, for their
 * {@code status.targetExecutors}. The driver publishes the target as the {@code
 * numberTargetExecutors} gauge of its {@code ExecutorAllocationManager} metrics source: the
 * executors it keeps plus those it requests, before their pods exist. Its UI serves the gauges as
 * JSON through the metrics servlet, at {@code /metrics/json} by default, so the operator reads them
 * from the driver pod and no change to Spark or to the driver's permissions is needed.
 *
 * <p>A driver is read at most once per {@link
 * SparkOperatorConf#DYNAMIC_ALLOCATION_TARGET_POLL_INTERVAL_SECONDS}, however often its application
 * is reconciled. A driver which does not answer, or answers without the gauge, leaves the recorded
 * target unchanged.
 */
@Slf4j
public class DynamicAllocationTargets {
  /** The name of the gauge, after the metrics namespace and the {@code driver} instance. */
  static final String TARGET_GAUGE = "ExecutorAllocationManager.executors.numberTargetExecutors";

  /** The largest metrics response which is read, to bound the memory a driver can make it use. */
  static final int MAX_RESPONSE_BYTES = 4 * 1024 * 1024;

  private static final String DEFAULT_SERVLET_PATH = "/metrics/json";
  private static final int DEFAULT_UI_PORT = 4040;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final MetricsFetcher fetcher;
  private final Clock clock;
  private final ConcurrentMap<ResourceID, Instant> lastPolled = new ConcurrentHashMap<>();

  /** Fetches the body of the metrics servlet of a driver. */
  @FunctionalInterface
  public interface MetricsFetcher {
    /**
     * Fetches the body at the given URI.
     *
     * @param uri The URI of the metrics servlet of a driver.
     * @param timeout The timeout of the request.
     * @return The body, which the caller closes.
     * @throws IOException if the driver cannot be read, or answers with an error.
     * @throws InterruptedException if the request is interrupted.
     */
    InputStream fetch(URI uri, Duration timeout) throws IOException, InterruptedException;
  }

  /** Creates a reader which fetches the metrics of the drivers over HTTP. */
  public DynamicAllocationTargets() {
    this(httpFetcher(), Clock.systemUTC());
  }

  /**
   * Creates a reader with the given fetcher and clock.
   *
   * @param fetcher Fetches the metrics of a driver.
   * @param clock The clock which spaces out the reads of a driver.
   */
  public DynamicAllocationTargets(final MetricsFetcher fetcher, final Clock clock) {
    this.fetcher = fetcher;
    this.clock = clock;
  }

  /**
   * Returns the interval between two reads of the target of an application.
   *
   * @return The interval, zero or negative if the targets are not read.
   */
  public static Duration pollInterval() {
    return Duration.ofSeconds(
        SparkOperatorConf.DYNAMIC_ALLOCATION_TARGET_POLL_INTERVAL_SECONDS.getValue());
  }

  /**
   * Checks whether the target of the application is read: reading targets is enabled, and the
   * application uses Dynamic Allocation and serves its driver UI over HTTP.
   *
   * @param app The Spark application.
   * @return True if the target of the application is read.
   */
  public static boolean isTracked(final SparkApplication app) {
    if (pollInterval().isNegative() || pollInterval().isZero()) {
      return false;
    }
    Map<String, String> conf = app.getSpec().getSparkConf();
    boolean dynamicAllocation =
        Boolean.parseBoolean(conf.getOrDefault("spark.dynamicAllocation.enabled", "false").trim());
    boolean ui = Boolean.parseBoolean(conf.getOrDefault("spark.ui.enabled", "true").trim());
    // An HTTPS UI is not read: the operator does not hold the trust material of the application
    boolean sslUi =
        Boolean.parseBoolean(
            conf.getOrDefault(
                    "spark.ssl.ui.enabled", conf.getOrDefault("spark.ssl.enabled", "false"))
                .trim());
    return dynamicAllocation && ui && !sslUi;
  }

  /**
   * Reads the target of the application from its driver, unless it was read less than {@link
   * #pollInterval()} ago.
   *
   * @param app The Spark application.
   * @param driverPod The driver pod of its current attempt.
   * @return The target, or empty if the driver was not read or did not report it.
   */
  public Optional<Integer> pollIfDue(final SparkApplication app, final Pod driverPod) {
    ResourceID id = ResourceID.fromResource(app);
    Instant now = clock.instant();
    Instant last = lastPolled.get(id);
    if (last != null && now.isBefore(last.plus(pollInterval()))) {
      return Optional.empty();
    }
    lastPolled.put(id, now);
    Optional<URI> uri = metricsUri(app, driverPod);
    if (uri.isEmpty()) {
      return Optional.empty();
    }
    Duration timeout =
        Duration.ofMillis(
            SparkOperatorConf.DYNAMIC_ALLOCATION_TARGET_POLL_TIMEOUT_MILLIS.getValue());
    try (InputStream body = fetcher.fetch(uri.get(), timeout)) {
      Optional<Integer> target = parseTarget(body);
      if (target.isEmpty()) {
        log.debug("Driver metrics at {} report no Dynamic Allocation target", uri.get());
      }
      return target;
    } catch (IOException e) {
      log.debug("Cannot read the Dynamic Allocation target from {}: {}", uri.get(), e.toString());
      return Optional.empty();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Optional.empty();
    }
  }

  /**
   * Forgets an application, e.g. when it is deleted.
   *
   * @param app The id of the SparkApplication.
   */
  public void forget(final ResourceID app) {
    lastPolled.remove(app);
  }

  /**
   * Returns the URI of the metrics servlet of the driver: the driver pod's IP, the UI port of the
   * application, {@code spark.ui.port}, and the servlet path of its metrics configuration.
   *
   * @param app The Spark application.
   * @param driverPod The driver pod of its current attempt.
   * @return The URI, or empty if the pod has no IP yet or the configuration is not usable.
   */
  static Optional<URI> metricsUri(final SparkApplication app, final Pod driverPod) {
    String ip = driverPod.getStatus() == null ? null : driverPod.getStatus().getPodIP();
    if (ip == null || ip.isBlank()) {
      return Optional.empty();
    }
    Map<String, String> conf = app.getSpec().getSparkConf();
    int port;
    try {
      port = Integer.parseInt(conf.getOrDefault("spark.ui.port", "").trim());
    } catch (NumberFormatException e) {
      port = DEFAULT_UI_PORT;
    }
    // Port 0 is a random port, which the operator cannot know
    if (port <= 0 || port > 65_535) {
      return Optional.empty();
    }
    String path =
        conf.getOrDefault(
            "spark.metrics.conf.driver.sink.servlet.path",
            conf.getOrDefault("spark.metrics.conf.*.sink.servlet.path", DEFAULT_SERVLET_PATH));
    if (!path.startsWith("/")) {
      return Optional.empty();
    }
    String host = ip.indexOf(':') >= 0 ? "[" + ip + "]" : ip;
    try {
      return Optional.of(new URI("http", null, host, port, path, null, null));
    } catch (URISyntaxException e) {
      return Optional.empty();
    }
  }

  /**
   * Returns the target from the JSON of the metrics servlet: the value of the first gauge named
   * {@code <namespace>.driver.ExecutorAllocationManager.executors.numberTargetExecutors}.
   *
   * @param body The JSON of the metrics servlet.
   * @return The target, or empty if the gauge is missing, is not a non-negative integer, or the
   *     body is not valid JSON or is larger than {@link #MAX_RESPONSE_BYTES}.
   * @throws IOException if the body cannot be read.
   */
  static Optional<Integer> parseTarget(final InputStream body) throws IOException {
    byte[] bytes = body.readNBytes(MAX_RESPONSE_BYTES + 1);
    if (bytes.length > MAX_RESPONSE_BYTES) {
      return Optional.empty();
    }
    JsonNode gauges;
    try {
      gauges = MAPPER.readTree(bytes).path("gauges");
    } catch (IOException e) {
      return Optional.empty();
    }
    Iterator<Map.Entry<String, JsonNode>> fields = gauges.properties().iterator();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> gauge = fields.next();
      if (gauge.getKey().endsWith(".driver." + TARGET_GAUGE)) {
        JsonNode value = gauge.getValue().path("value");
        if (value.canConvertToInt() && value.isIntegralNumber() && value.intValue() >= 0) {
          return Optional.of(value.intValue());
        }
        return Optional.empty();
      }
    }
    return Optional.empty();
  }

  private static MetricsFetcher httpFetcher() {
    HttpClient client =
        HttpClient.newBuilder()
            .connectTimeout(
                Duration.ofMillis(
                    SparkOperatorConf.DYNAMIC_ALLOCATION_TARGET_POLL_TIMEOUT_MILLIS.getValue()))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
    return (uri, timeout) -> {
      HttpRequest request = HttpRequest.newBuilder(uri).timeout(timeout).GET().build();
      HttpResponse<InputStream> response =
          client.send(request, HttpResponse.BodyHandlers.ofInputStream());
      if (response.statusCode() != 200) {
        response.body().close();
        throw new IOException("HTTP " + response.statusCode());
      }
      return response.body();
    };
  }
}
