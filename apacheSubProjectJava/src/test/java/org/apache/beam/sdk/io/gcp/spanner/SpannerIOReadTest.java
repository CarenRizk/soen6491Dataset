/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.spanner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Options.ReadAndQueryOption;
import com.google.cloud.spanner.Options.ReadQueryUpdateTransactionOption;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Unit tests for {@link SpannerIO}. */
@RunWith(JUnit4.class)
public class SpannerIOReadTest implements Serializable {

  private static final TimestampBound TIMESTAMP_BOUND =
      TimestampBound.ofReadTimestamp(Timestamp.ofTimeMicroseconds(12345));
  public static final String PROJECT_ID = "1234";
  public static final String INSTANCE_CONFIG_ID = "5678";
  public static final String INSTANCE_ID = "123";
  public static final String DATABASE_ID = "aaa";
  public static final String TABLE_ID = "users";
  public static final String QUERY_NAME = "My-query";
  public static final String QUERY_STATEMENT = "SELECT * FROM users";

  @Rule
  public final transient TestPipeline pipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private FakeServiceFactory serviceFactory;
  private BatchReadOnlyTransaction mockBatchTx;
  private Partition fakePartition;
  private SpannerConfig spannerConfig;

  private static final Type FAKE_TYPE =
      Type.struct(
          Type.StructField.of("id", Type.int64()), Type.StructField.of("name", Type.string()));

  private static final List<Struct> FAKE_ROWS =
      Arrays.asList(
          Struct.newBuilder().set("id").to(Value.int64(1)).set("name").to("Alice").build(),
          Struct.newBuilder().set("id").to(Value.int64(2)).set("name").to("Bob").build(),
          Struct.newBuilder().set("id").to(Value.int64(3)).set("name").to("Carl").build(),
          Struct.newBuilder().set("id").to(Value.int64(4)).set("name").to("Dan").build(),
          Struct.newBuilder().set("id").to(Value.int64(5)).set("name").to("Evan").build(),
          Struct.newBuilder().set("id").to(Value.int64(6)).set("name").to("Floyd").build());

  @Before
  public void setUp() throws Exception {
    serviceFactory = new FakeServiceFactory();
    mockBatchTx = Mockito.mock(BatchReadOnlyTransaction.class);
 
    // Setup the ProcessWideContainer for testing metrics are set.
    MetricsContainerImpl container = new MetricsContainerImpl(null);
    MetricsEnvironment.setProcessWideContainer(container);
    MetricsEnvironment.setCurrentContainer(container);
  }

  private PipelineResult runNaiveQueryTest(SpannerIO.Read readTransform) {
    readTransform = readTransform.withBatching(false);
    PCollection<Struct> results = pipeline.apply("read q", readTransform);
    when(mockBatchTx.executeQuery(
            eq(Statement.of(QUERY_STATEMENT)), any(ReadQueryUpdateTransactionOption.class)))
        .thenReturn(ResultSets.forRows(FAKE_TYPE, FAKE_ROWS));

    PAssert.that(results).containsInAnyOrder(FAKE_ROWS);
    PipelineResult result = pipeline.run();
    verifyQueryRequestMetricWasSet(readTransform.getSpannerConfig(), QUERY_NAME, "ok", 1);
    return result;
  }

  private void runBatchReadTest(SpannerIO.Read readTransform) {

    PCollection<Struct> results = pipeline.apply("read q", readTransform);
    when(mockBatchTx.partitionRead(
            any(PartitionOptions.class),
            eq(TABLE_ID),
            eq(KeySet.all()),
            eq(Arrays.asList("id", "name")),
            any(ReadQueryUpdateTransactionOption.class),
            any(ReadAndQueryOption.class)))
        .thenReturn(Arrays.asList(fakePartition, fakePartition, fakePartition));
    when(mockBatchTx.execute(any(Partition.class)))
        .thenReturn(
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(0, 2)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(2, 4)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(4, 6)));

    PAssert.that(results).containsInAnyOrder(FAKE_ROWS);
    pipeline.run();
    verifyTableRequestMetricWasSet(readTransform.getSpannerConfig(), TABLE_ID, "ok", 4);
  }

  @Test
  public void runReadFailsToRetrieveSchema() {
    PCollection<Struct> spannerRows =
        pipeline.apply(
            SpannerIO.read()
                .withInstanceId(INSTANCE_ID)
                .withDatabaseId(DATABASE_ID)
                .withTable(TABLE_ID)
                .withColumns("id", "name"));

    Exception exception = assertThrows(IllegalStateException.class, spannerRows::getSchema);
    checkMessage("Cannot call getSchema when there is no schema", exception.getMessage());
  }

  private void checkMessage(String substring, @Nullable String message) {
    if (message != null) {
      assertThat(message, containsString(substring));
    } else {
      fail();
    }
  }

  private long getRequestMetricCount(HashMap<String, String> baseLabels) {
    MonitoringInfoMetricName name =
        MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);
    MetricsContainerImpl container =
        (MetricsContainerImpl) MetricsEnvironment.getCurrentContainer();
    return container.getCounter(name).getCumulative();
  }

  private long getTableRequestMetric(SpannerConfig config, String table, String status) {
    HashMap<String, String> baseLabels = getBaseMetricsLabels(config);
    baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "Read");
    baseLabels.put(MonitoringInfoConstants.Labels.TABLE_ID, table);
    baseLabels.put(
        MonitoringInfoConstants.Labels.RESOURCE,
        GcpResourceIdentifiers.spannerTable(
            baseLabels.get(MonitoringInfoConstants.Labels.SPANNER_PROJECT_ID),
            config.getInstanceId().get(),
            config.getDatabaseId().get(),
            table));
    baseLabels.put(MonitoringInfoConstants.Labels.STATUS, status);
    return getRequestMetricCount(baseLabels);
  }

  private long getQueryRequestMetric(SpannerConfig config, String queryName, String status) {
    HashMap<String, String> baseLabels = getBaseMetricsLabels(config);
    baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "Read");
    baseLabels.put(MonitoringInfoConstants.Labels.SPANNER_QUERY_NAME, queryName);
    baseLabels.put(
        MonitoringInfoConstants.Labels.RESOURCE,
        GcpResourceIdentifiers.spannerQuery(
            baseLabels.get(MonitoringInfoConstants.Labels.SPANNER_PROJECT_ID),
            config.getInstanceId().get(),
            config.getDatabaseId().get(),
            queryName));
    baseLabels.put(MonitoringInfoConstants.Labels.STATUS, status);
    return getRequestMetricCount(baseLabels);
  }

  private void verifyTableRequestMetricWasSet(
      SpannerConfig config, String table, String status, long count) {
    assertEquals(count, getTableRequestMetric(config, table, status));
  }

  private void verifyQueryRequestMetricWasSet(
      SpannerConfig config, String queryName, String status, long count) {
    assertEquals(count, getQueryRequestMetric(config, queryName, status));
  }

  private HashMap<String, String> getBaseMetricsLabels(SpannerConfig config) {
    HashMap<String, String> baseLabels = new HashMap<>();
    baseLabels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
    baseLabels.put(MonitoringInfoConstants.Labels.SERVICE, "Spanner");
    baseLabels.put(
        MonitoringInfoConstants.Labels.SPANNER_PROJECT_ID,
        config.getProjectId() == null || config.getProjectId().get() == null
            ? SpannerOptions.getDefaultProjectId()
            : config.getProjectId().get());
    baseLabels.put(
        MonitoringInfoConstants.Labels.SPANNER_INSTANCE_ID, config.getInstanceId().get());
    baseLabels.put(
        MonitoringInfoConstants.Labels.SPANNER_DATABASE_ID, config.getDatabaseId().get());
    return baseLabels;
  }
}