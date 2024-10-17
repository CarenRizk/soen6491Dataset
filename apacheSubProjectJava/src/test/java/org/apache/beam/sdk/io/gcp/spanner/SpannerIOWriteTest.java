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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.CommitResponse;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Options.ReadQueryUpdateTransactionOption;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.BatchableMutationFilterFn;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.FailureMode;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.GatherSortCreateBatchesFn;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.Write;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.WriteToSpannerFn;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.FinishBundleContext;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for {@link SpannerIO}.
 *
 * <p>Note that because batching and sorting work on Bundles, and the TestPipeline does not bundle
 * small numbers of elements, the batching and sorting DoFns need to be unit tested outside of the
 * pipeline.
 */
@RunWith(JUnit4.class)
public class SpannerIOWriteTest implements Serializable {

  private static final long CELLS_PER_KEY = 7;
  private static final String PROJECT_NAME = "test-project";
  private static final String INSTANCE_CONFIG_NAME = "regional-us-central1";
  private static final String INSTANCE_NAME = "test-instance";
  private static final String DATABASE_NAME = "test-database";
  private static final String TABLE_NAME = "test-table";
  private static final SpannerConfig SPANNER_CONFIG =
      SpannerConfig.create()
          .withDatabaseId(DATABASE_NAME)
          .withInstanceId(INSTANCE_NAME)
          .withProjectId(PROJECT_NAME);

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  @Captor public transient ArgumentCaptor<Iterable<Mutation>> mutationBatchesCaptor;
  @Captor public transient ArgumentCaptor<ReadQueryUpdateTransactionOption> optionsCaptor;
  @Captor public transient ArgumentCaptor<Iterable<MutationGroup>> mutationGroupListCaptor;
  @Captor public transient ArgumentCaptor<MutationGroup> mutationGroupCaptor;

  private FakeServiceFactory serviceFactory;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    serviceFactory = new FakeServiceFactory();

    // Setup the ProcessWideContainer for testing metrics are set.
    MetricsContainerImpl container = new MetricsContainerImpl(null);
    MetricsEnvironment.setProcessWideContainer(container);
    MetricsEnvironment.setCurrentContainer(container);
  }

  private SpannerSchema getSchema() {
    return SpannerSchema.builder()
        .addColumn("tEsT-TaBlE", "key", "INT64", CELLS_PER_KEY)
        .addKeyPart("tEsT-TaBlE", "key", false)
        .build();
  }

  static Struct columnMetadata(
      String tableName, String columnName, String type, long cellsMutated) {
    return Struct.newBuilder()
        .set("table_name")
        .to(tableName)
        .set("column_name")
        .to(columnName)
        .set("spanner_type")
        .to(type)
        .set("cells_mutated")
        .to(cellsMutated)
        .build();
  }

  static Struct pkMetadata(String tableName, String columnName, String ordering) {
    return Struct.newBuilder()
        .set("table_name")
        .to(tableName)
        .set("column_name")
        .to(columnName)
        .set("column_ordering")
        .to(ordering)
        .build();
  }

  static void prepareColumnMetadata(ReadOnlyTransaction tx, List<Struct> rows) {
    Type type =
        Type.struct(
            Type.StructField.of("table_name", Type.string()),
            Type.StructField.of("column_name", Type.string()),
            Type.StructField.of("spanner_type", Type.string()),
            Type.StructField.of("cells_mutated", Type.int64()));
    when(tx.executeQuery(
            argThat(
                new ArgumentMatcher<Statement>() {

                  @Override
                  public boolean matches(Statement argument) {
                    if (!(argument instanceof Statement)) {
                      return false;
                    }
                    Statement st = (Statement) argument;
                    return st.getSql().contains("information_schema.columns");
                  }
                })))
        .thenReturn(ResultSets.forRows(type, rows));
  }

  static void preparePgColumnMetadata(ReadOnlyTransaction tx, List<Struct> rows) {
    Type type =
        Type.struct(
            Type.StructField.of("table_name", Type.string()),
            Type.StructField.of("column_name", Type.string()),
            Type.StructField.of("spanner_type", Type.string()),
            Type.StructField.of("cells_mutated", Type.int64()));
    when(tx.executeQuery(
            argThat(
                new ArgumentMatcher<Statement>() {

                  @Override
                  public boolean matches(Statement argument) {
                    if (!(argument instanceof Statement)) {
                      return false;
                    }
                    Statement st = (Statement) argument;
                    return st.getSql().contains("information_schema.columns")
                        && st.getSql().contains("'public'");
                  }
                })))
        .thenReturn(ResultSets.forRows(type, rows));
  }

  static void preparePkMetadata(ReadOnlyTransaction tx, List<Struct> rows) {
    Type type =
        Type.struct(
            Type.StructField.of("table_name", Type.string()),
            Type.StructField.of("column_name", Type.string()),
            Type.StructField.of("column_ordering", Type.string()));
    when(tx.executeQuery(
            argThat(
                new ArgumentMatcher<Statement>() {

                  @Override
                  public boolean matches(Statement argument) {
                    if (!(argument instanceof Statement)) {
                      return false;
                    }
                    Statement st = (Statement) argument;
                    return st.getSql().contains("information_schema.index_columns");
                  }
                })))
        .thenReturn(ResultSets.forRows(type, rows));
  }

  @Test
  public void emptyTransform() throws Exception {
    SpannerIO.Write write = SpannerIO.write();
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires instance id to be set with");
    write.expand(null);
  }

  @Test
  public void emptyInstanceId() throws Exception {
    SpannerIO.Write write = SpannerIO.write().withDatabaseId("123");
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires instance id to be set with");
    write.expand(null);
  }

  @Test
  public void emptyDatabaseId() throws Exception {
    SpannerIO.Write write = SpannerIO.write().withInstanceId("123");
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires database id to be set with");
    write.expand(null);
  }

  @Test
  public void testBatchableMutationFilterFn_cells() {
    Mutation all = Mutation.delete(TABLE_NAME, KeySet.all());
    Mutation prefix = Mutation.delete(TABLE_NAME, KeySet.prefixRange(Key.of(1L)));
    Mutation range =
        Mutation.delete(
            TABLE_NAME, KeySet.range(KeyRange.openOpen(Key.of(1L), Key.newBuilder().build())));
    MutationGroup[] mutationGroups =
        new MutationGroup[] {
          buildMutationGroup(buildUpsertMutation(1L)),
          buildMutationGroup(buildUpsertMutation(2L), buildUpsertMutation(3L)),
          buildMutationGroup(
              buildUpsertMutation(2L),
              buildUpsertMutation(3L),
              buildUpsertMutation(4L),
              buildUpsertMutation(5L)), // not batchable - too big.
          buildMutationGroup(buildDeleteMutation(1L)),
          buildMutationGroup(buildDeleteMutation(5L, 6L)), // not point delete.
          buildMutationGroup(all),
          buildMutationGroup(prefix),
          buildMutationGroup(range)
        };

    BatchableMutationFilterFn testFn =
        new BatchableMutationFilterFn(null, null, 10000000, 3 * CELLS_PER_KEY, 1000);

    BatchableMutationFilterFn.ProcessContext mockProcessContext =
        Mockito.mock(ProcessContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());

    // Capture the outputs.
    doNothing().when(mockProcessContext).output(mutationGroupCaptor.capture());
    doNothing().when(mockProcessContext).output(any(), mutationGroupListCaptor.capture());

    // Process all elements.
    for (MutationGroup m : mutationGroups) {
      when(mockProcessContext.element()).thenReturn(m);
      testFn.processElement(mockProcessContext);
    }

    // Verify captured batchable elements.
    assertThat(
        mutationGroupCaptor.getAllValues(),
        containsInAnyOrder(
            buildMutationGroup(buildUpsertMutation(1L)),
            buildMutationGroup(buildUpsertMutation(2L), buildUpsertMutation(3L)),
            buildMutationGroup(buildDeleteMutation(1L))));

    // Verify captured unbatchable mutations
    Iterable<MutationGroup> unbatchableMutations =
        Iterables.concat(mutationGroupListCaptor.getAllValues());
    assertThat(
        unbatchableMutations,
        containsInAnyOrder(
            buildMutationGroup(
                buildUpsertMutation(2L),
                buildUpsertMutation(3L),
                buildUpsertMutation(4L),
                buildUpsertMutation(5L)), // not batchable - too big.
            buildMutationGroup(buildDeleteMutation(5L, 6L)), // not point delete.
            buildMutationGroup(all),
            buildMutationGroup(prefix),
            buildMutationGroup(range)));
  }

  @Test
  public void testBatchableMutationFilterFn_size() {
    Mutation all = Mutation.delete(TABLE_NAME, KeySet.all());
    Mutation prefix = Mutation.delete(TABLE_NAME, KeySet.prefixRange(Key.of(1L)));
    Mutation range =
        Mutation.delete(
            TABLE_NAME, KeySet.range(KeyRange.openOpen(Key.of(1L), Key.newBuilder().build())));
    MutationGroup[] mutationGroups =
        new MutationGroup[] {
          buildMutationGroup(buildUpsertMutation(1L)),
          buildMutationGroup(buildUpsertMutation(2L), buildUpsertMutation(3L)),
          buildMutationGroup(
              buildUpsertMutation(1L),
              buildUpsertMutation(3L),
              buildUpsertMutation(4L),
              buildUpsertMutation(5L)), // not batchable - too big.
          buildMutationGroup(buildDeleteMutation(1L)),
          buildMutationGroup(buildDeleteMutation(5L, 6L)), // not point delete.
          buildMutationGroup(all),
          buildMutationGroup(prefix),
          buildMutationGroup(range)
        };

    long mutationSize = MutationSizeEstimator.sizeOf(buildUpsertMutation(1L));
    BatchableMutationFilterFn testFn =
        new BatchableMutationFilterFn(null, null, mutationSize * 3, 1000, 1000);

    DoFn<MutationGroup, MutationGroup>.ProcessContext mockProcessContext =
        Mockito.mock(ProcessContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());

    // Capture the outputs.
    doNothing().when(mockProcessContext).output(mutationGroupCaptor.capture());
    doNothing().when(mockProcessContext).output(any(), mutationGroupListCaptor.capture());

    // Process all elements.
    for (MutationGroup m : mutationGroups) {
      when(mockProcessContext.element()).thenReturn(m);
      testFn.processElement(mockProcessContext);
    }

    // Verify captured batchable elements.
    assertThat(
        mutationGroupCaptor.getAllValues(),
        containsInAnyOrder(
            buildMutationGroup(buildUpsertMutation(1L)),
            buildMutationGroup(buildUpsertMutation(2L), buildUpsertMutation(3L)),
            buildMutationGroup(buildDeleteMutation(1L))));

    // Verify captured unbatchable mutations
    Iterable<MutationGroup> unbatchableMutations =
        Iterables.concat(mutationGroupListCaptor.getAllValues());
    assertThat(
        unbatchableMutations,
        containsInAnyOrder(
            buildMutationGroup(
                buildUpsertMutation(1L),
                buildUpsertMutation(3L),
                buildUpsertMutation(4L),
                buildUpsertMutation(5L)), // not batchable - too big.
            buildMutationGroup(buildDeleteMutation(5L, 6L)), // not point delete.
            buildMutationGroup(all),
            buildMutationGroup(prefix),
            buildMutationGroup(range)));
  }

  @Test
  public void testBatchableMutationFilterFn_rows() {
    Mutation all = Mutation.delete(TABLE_NAME, KeySet.all());
    Mutation prefix = Mutation.delete(TABLE_NAME, KeySet.prefixRange(Key.of(1L)));
    Mutation range =
        Mutation.delete(
            TABLE_NAME, KeySet.range(KeyRange.openOpen(Key.of(1L), Key.newBuilder().build())));
    MutationGroup[] mutationGroups =
        new MutationGroup[] {
          buildMutationGroup(buildUpsertMutation(1L)),
          buildMutationGroup(buildUpsertMutation(2L), buildUpsertMutation(3L)),
          buildMutationGroup(
              buildUpsertMutation(1L),
              buildUpsertMutation(3L),
              buildUpsertMutation(4L),
              buildUpsertMutation(5L)), // not batchable - too many rows.
          buildMutationGroup(buildDeleteMutation(1L)),
          buildMutationGroup(buildDeleteMutation(5L, 6L)), // not point delete.
          buildMutationGroup(all),
          buildMutationGroup(prefix),
          buildMutationGroup(range)
        };

    BatchableMutationFilterFn testFn = new BatchableMutationFilterFn(null, null, 1000, 1000, 3);

    BatchableMutationFilterFn.ProcessContext mockProcessContext =
        Mockito.mock(ProcessContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());

    // Capture the outputs.
    doNothing().when(mockProcessContext).output(mutationGroupCaptor.capture());
    doNothing().when(mockProcessContext).output(any(), mutationGroupListCaptor.capture());

    // Process all elements.
    for (MutationGroup m : mutationGroups) {
      when(mockProcessContext.element()).thenReturn(m);
      testFn.processElement(mockProcessContext);
    }

    // Verify captured batchable elements.
    assertThat(
        mutationGroupCaptor.getAllValues(),
        containsInAnyOrder(
            buildMutationGroup(buildUpsertMutation(1L)),
            buildMutationGroup(buildUpsertMutation(2L), buildUpsertMutation(3L)),
            buildMutationGroup(buildDeleteMutation(1L))));

    // Verify captured unbatchable mutations
    Iterable<MutationGroup> unbatchableMutations =
        Iterables.concat(mutationGroupListCaptor.getAllValues());
    assertThat(
        unbatchableMutations,
        containsInAnyOrder(
            buildMutationGroup(
                buildUpsertMutation(1L),
                buildUpsertMutation(3L),
                buildUpsertMutation(4L),
                buildUpsertMutation(5L)), // not batchable - too many rows.
            buildMutationGroup(buildDeleteMutation(5L, 6L)), // not point delete.
            buildMutationGroup(all),
            buildMutationGroup(prefix),
            buildMutationGroup(range)));
  }

  @Test
  public void testBatchableMutationFilterFn_batchingDisabled() {
    MutationGroup[] mutationGroups =
        new MutationGroup[] {
          buildMutationGroup(buildUpsertMutation(1L)),
          buildMutationGroup(buildUpsertMutation(2L)),
          buildMutationGroup(buildDeleteMutation(1L)),
          buildMutationGroup(buildDeleteMutation(5L, 6L))
        };

    BatchableMutationFilterFn testFn = new BatchableMutationFilterFn(null, null, 0, 0, 0);

    BatchableMutationFilterFn.ProcessContext mockProcessContext =
        Mockito.mock(ProcessContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());

    // Capture the outputs.
    doNothing().when(mockProcessContext).output(mutationGroupCaptor.capture());
    doNothing().when(mockProcessContext).output(any(), mutationGroupListCaptor.capture());

    // Process all elements.
    for (MutationGroup m : mutationGroups) {
      when(mockProcessContext.element()).thenReturn(m);
      testFn.processElement(mockProcessContext);
    }

    // Verify captured batchable elements.
    assertTrue(mutationGroupCaptor.getAllValues().isEmpty());

    // Verify captured unbatchable mutations
    Iterable<MutationGroup> unbatchableMutations =
        Iterables.concat(mutationGroupListCaptor.getAllValues());
    assertThat(unbatchableMutations, containsInAnyOrder(mutationGroups));
  }

  @Test
  public void testGatherSortAndBatchFn() throws Exception {

    GatherSortCreateBatchesFn testFn =
        new GatherSortCreateBatchesFn(
            10000000, // batch bytes
            100, // batch up to 35 mutated cells.
            5, // batch rows
            100, // groupingFactor
            null);

    GatherSortCreateBatchesFn.ProcessContext mockProcessContext =
        Mockito.mock(ProcessContext.class);
    GatherSortCreateBatchesFn.FinishBundleContext mockFinishBundleContext =
        Mockito.mock(FinishBundleContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());

    // Capture the outputs.
    doNothing()
        .when(mockFinishBundleContext)
        .output(mutationGroupListCaptor.capture(), any(), any());

    MutationGroup[] mutationGroups =
        new MutationGroup[] {
          // Unsorted group of 12 mutations.
          // each mutation is considered 7 cells,
          // should be sorted and output as 2 lists of 5, then 1 list of 2
          // with mutations sorted in order.
          buildMutationGroup(buildUpsertMutation(4L)),
          buildMutationGroup(buildUpsertMutation(1L)),
          buildMutationGroup(buildUpsertMutation(7L)),
          buildMutationGroup(buildUpsertMutation(12L)),
          buildMutationGroup(buildUpsertMutation(10L)),
          buildMutationGroup(buildUpsertMutation(11L)),
          buildMutationGroup(buildUpsertMutation(2L)),
          buildMutationGroup(buildDeleteMutation(8L)),
          buildMutationGroup(buildUpsertMutation(3L)),
          buildMutationGroup(buildUpsertMutation(6L)),
          buildMutationGroup(buildUpsertMutation(9L)),
          buildMutationGroup(buildUpsertMutation(5L))
        };

    // Process all elements as one bundle.
    for (MutationGroup m : mutationGroups) {
      when(mockProcessContext.element()).thenReturn(m);
      // outputReceiver should not be called until end of bundle.
      testFn.processElement(mockProcessContext, null);
    }
    testFn.finishBundle(mockFinishBundleContext);

    verify(mockProcessContext, never()).output(any());
    verify(mockFinishBundleContext, times(3)).output(any(), any(), any());

    // Verify output are 3 batches of sorted values
    assertThat(
        mutationGroupListCaptor.getAllValues(),
        contains(
            Arrays.asList(
                buildMutationGroup(buildUpsertMutation(1L)),
                buildMutationGroup(buildUpsertMutation(2L)),
                buildMutationGroup(buildUpsertMutation(3L)),
                buildMutationGroup(buildUpsertMutation(4L)),
                buildMutationGroup(buildUpsertMutation(5L))),
            Arrays.asList(
                buildMutationGroup(buildUpsertMutation(6L)),
                buildMutationGroup(buildUpsertMutation(7L)),
                buildMutationGroup(buildDeleteMutation(8L)),
                buildMutationGroup(buildUpsertMutation(9L)),
                buildMutationGroup(buildUpsertMutation(10L))),
            Arrays.asList(
                buildMutationGroup(buildUpsertMutation(11L)),
                buildMutationGroup(buildUpsertMutation(12L)))));
  }

  @Test
  public void testGatherBundleAndSortFn_flushOversizedBundle() throws Exception {

    // Setup class to bundle every 6 rows and create batches of 2.
    GatherSortCreateBatchesFn testFn =
        new GatherSortCreateBatchesFn(
            10000000, // batch bytes
            100, // batch up to 14 mutated cells.
            2, // batch rows
            3, // groupingFactor
            null);

    GatherSortCreateBatchesFn.ProcessContext mockProcessContext =
        Mockito.mock(ProcessContext.class);
    GatherSortCreateBatchesFn.FinishBundleContext mockFinishBundleContext =
        Mockito.mock(FinishBundleContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());
    OutputReceiver<Iterable<MutationGroup>> mockOutputReceiver = mock(OutputReceiver.class);

    // Capture the outputs.
    doNothing().when(mockOutputReceiver).output(mutationGroupListCaptor.capture());
    // Capture the outputs.
    doNothing()
        .when(mockFinishBundleContext)
        .output(mutationGroupListCaptor.capture(), any(), any());

    MutationGroup[] mutationGroups =
        new MutationGroup[] {
          // Unsorted group of 12 mutations.
          // each mutation is considered 7 cells,
          // should be sorted and output as 2 lists of 5, then 1 list of 2
          // with mutations sorted in order.
          buildMutationGroup(buildUpsertMutation(4L)),
          buildMutationGroup(buildUpsertMutation(1L)),
          buildMutationGroup(buildUpsertMutation(7L)),
          buildMutationGroup(buildUpsertMutation(9L)),
          buildMutationGroup(buildUpsertMutation(10L)),
          buildMutationGroup(buildUpsertMutation(11L)),
          // end group
          buildMutationGroup(buildUpsertMutation(2L)),
          buildMutationGroup(buildDeleteMutation(8L)), // end batch
          buildMutationGroup(buildUpsertMutation(3L)),
          buildMutationGroup(buildUpsertMutation(6L)), // end batch
          buildMutationGroup(buildUpsertMutation(5L))
          // end bundle, so end group and end batch.
        };

    // Process all elements as one bundle.
    for (MutationGroup m : mutationGroups) {
      when(mockProcessContext.element()).thenReturn(m);
      testFn.processElement(mockProcessContext, mockOutputReceiver);
    }
    testFn.finishBundle(mockFinishBundleContext);

    // processElement ouput receiver should have been called 3 times when the 1st group was full.
    verify(mockOutputReceiver, times(3)).output(any());
    // finsihBundleContext output should be called 3 times when the bundle was finished.
    verify(mockFinishBundleContext, times(3)).output(any(), any(), any());

    List<Iterable<MutationGroup>> mgListGroups = mutationGroupListCaptor.getAllValues();

    assertEquals(6, mgListGroups.size());
    // verify contents of 6 sorted groups.
    // first group should be 1,3,4,7,9,11
    assertThat(
        mgListGroups.get(0),
        contains(
            buildMutationGroup(buildUpsertMutation(1L)),
            buildMutationGroup(buildUpsertMutation(4L))));
    assertThat(
        mgListGroups.get(1),
        contains(
            buildMutationGroup(buildUpsertMutation(7L)),
            buildMutationGroup(buildUpsertMutation(9L))));
    assertThat(
        mgListGroups.get(2),
        contains(
            buildMutationGroup(buildUpsertMutation(10L)),
            buildMutationGroup(buildUpsertMutation(11L))));

    // second group at finishBundle should be 2,3,5,6,8
    assertThat(
        mgListGroups.get(3),
        contains(
            buildMutationGroup(buildUpsertMutation(2L)),
            buildMutationGroup(buildUpsertMutation(3L))));
    assertThat(
        mgListGroups.get(4),
        contains(
            buildMutationGroup(buildUpsertMutation(5L)),
            buildMutationGroup(buildUpsertMutation(6L))));
    assertThat(mgListGroups.get(5), contains(buildMutationGroup(buildDeleteMutation(8L))));
  }

  @Test
  public void testBatchFn_cells() throws Exception {

    // Setup class to batch every 3 mutations (3xCELLS_PER_KEY cell mutations)
    GatherSortCreateBatchesFn testFn =
        new GatherSortCreateBatchesFn(
            10000000, // batch bytes
            3 * CELLS_PER_KEY, // batch up to 21 mutated cells - 3 mutations.
            100, // batch rows
            100, // groupingFactor
            null);

    testAndVerifyBatches(testFn);
  }

  @Test
  public void testBatchFn_size() throws Exception {

    long mutationSize = MutationSizeEstimator.sizeOf(buildUpsertMutation(1L));

    // Setup class to bundle every 3 mutations by size)
    GatherSortCreateBatchesFn testFn =
        new GatherSortCreateBatchesFn(
            mutationSize * 3, // batch bytes = 3 mutations.
            100, // batch cells
            100, // batch rows
            100, // groupingFactor
            null);

    testAndVerifyBatches(testFn);
  }

  @Test
  public void testBatchFn_rows() throws Exception {

    // Setup class to bundle every 3 rows
    GatherSortCreateBatchesFn testFn =
        new GatherSortCreateBatchesFn(
            10000, // batch bytes = 3 mutations.
            100, // batch cells
            3, // batch rows
            100, // groupingFactor
            null);

    testAndVerifyBatches(testFn);
  }

  private void testAndVerifyBatches(GatherSortCreateBatchesFn testFn) throws Exception {
    GatherSortCreateBatchesFn.ProcessContext mockProcessContext =
        Mockito.mock(ProcessContext.class);
    GatherSortCreateBatchesFn.FinishBundleContext mockFinishBundleContext =
        Mockito.mock(FinishBundleContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());

    // Capture the output at finish bundle..
    doNothing()
        .when(mockFinishBundleContext)
        .output(mutationGroupListCaptor.capture(), any(), any());

    List<MutationGroup> mutationGroups =
        Arrays.asList(
            buildMutationGroup(buildUpsertMutation(1L)),
            buildMutationGroup(buildUpsertMutation(4L)),
            buildMutationGroup(
                buildUpsertMutation(5L),
                buildUpsertMutation(6L),
                buildUpsertMutation(7L),
                buildUpsertMutation(8L),
                buildUpsertMutation(9L)),
            buildMutationGroup(buildUpsertMutation(3L)),
            buildMutationGroup(buildUpsertMutation(10L)),
            buildMutationGroup(buildUpsertMutation(11L)),
            buildMutationGroup(buildUpsertMutation(2L)));

    // Process elements.
    for (MutationGroup m : mutationGroups) {
      when(mockProcessContext.element()).thenReturn(m);
      testFn.processElement(mockProcessContext, null);
    }
    testFn.finishBundle(mockFinishBundleContext);

    verify(mockFinishBundleContext, times(4)).output(any(), any(), any());

    List<Iterable<MutationGroup>> batches = mutationGroupListCaptor.getAllValues();
    assertEquals(4, batches.size());

    // verify contents of 4 batches.
    assertThat(
        batches.get(0),
        contains(
            buildMutationGroup(buildUpsertMutation(1L)),
            buildMutationGroup(buildUpsertMutation(2L)),
            buildMutationGroup(buildUpsertMutation(3L))));
    assertThat(
        batches.get(1),
        contains(
            buildMutationGroup(
                buildUpsertMutation(4L)))); // small batch : next mutation group is too big.
    assertThat(
        batches.get(2),
        contains(
            buildMutationGroup(
                buildUpsertMutation(5L),
                buildUpsertMutation(6L),
                buildUpsertMutation(7L),
                buildUpsertMutation(8L),
                buildUpsertMutation(9L))));
    assertThat(
        batches.get(3),
        contains(
            buildMutationGroup(buildUpsertMutation(10L)),
            buildMutationGroup(buildUpsertMutation(11L))));
  }

  static MutationGroup buildMutationGroup(Mutation m, Mutation... other) {
    return MutationGroup.create(m, other);
  }

  static Mutation buildUpsertMutation(Long key) {
    return Mutation.newInsertOrUpdateBuilder(TABLE_NAME).set("key").to(key).build();
  }

  private static Iterable<Mutation> buildMutationBatch(Mutation... m) {
    return Arrays.asList(m);
  }

  private static Mutation buildDeleteMutation(Long... keys) {

    KeySet.Builder builder = KeySet.newBuilder();
    for (Long key : keys) {
      builder.addKey(Key.of(key));
    }
    return Mutation.delete(TABLE_NAME, builder.build());
  }

  private static Iterable<Mutation> mutationsInNoOrder(Iterable<Mutation> expected) {
    final ImmutableSet<Mutation> mutations = ImmutableSet.copyOf(expected);
    return argThat(
        new ArgumentMatcher<Iterable<Mutation>>() {

          @Override
          public boolean matches(Iterable<Mutation> argument) {
            if (!(argument instanceof Iterable)) {
              return false;
            }
            ImmutableSet<Mutation> actual = ImmutableSet.copyOf((Iterable) argument);
            return actual.equals(mutations);
          }

          @Override
          public String toString() {
            return "Iterable must match " + mutations;
          }
        });
  }

  private void verifyTableWriteRequestMetricWasSet(
      SpannerConfig config, String table, String status, long count) {

    HashMap<String, String> baseLabels = getBaseMetricsLabels(config);
    baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "Write");
    baseLabels.put(MonitoringInfoConstants.Labels.TABLE_ID, table);
    baseLabels.put(
        MonitoringInfoConstants.Labels.RESOURCE,
        GcpResourceIdentifiers.spannerTable(
            baseLabels.get(MonitoringInfoConstants.Labels.SPANNER_PROJECT_ID),
            config.getInstanceId().get(),
            config.getDatabaseId().get(),
            table));
    baseLabels.put(MonitoringInfoConstants.Labels.STATUS, status);

    MonitoringInfoMetricName name =
        MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);
    MetricsContainerImpl container =
        (MetricsContainerImpl) MetricsEnvironment.getCurrentContainer();
    assertEquals(count, (long) container.getCounter(name).getCumulative());
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