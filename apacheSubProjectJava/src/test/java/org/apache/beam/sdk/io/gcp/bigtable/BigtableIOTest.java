package org.apache.beam.sdk.io.gcp.bigtable;

import static org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSourcesEqualReferenceSource;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionExhaustive;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionFails;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Verify.verifyNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.api.gax.rpc.ApiException;
import com.google.auto.value.AutoValue;
import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.BigtableSource;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipeline.PipelineRunMissingException;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class BigtableIOTest {
  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ExpectedLogs logged = ExpectedLogs.none(BigtableIO.class);

  
  public interface ReadOptions extends GcpOptions {
    @Description("The project that contains the table to export.")
    ValueProvider<String> getBigtableProject();

    @SuppressWarnings("unused")
    void setBigtableProject(ValueProvider<String> projectId);

    @Description("The Bigtable instance id that contains the table to export.")
    ValueProvider<String> getBigtableInstanceId();

    @SuppressWarnings("unused")
    void setBigtableInstanceId(ValueProvider<String> instanceId);

    @Description("The Bigtable table id to export.")
    ValueProvider<String> getBigtableTableId();

    @SuppressWarnings("unused")
    void setBigtableTableId(ValueProvider<String> tableId);
  }

  static final ValueProvider<String> NOT_ACCESSIBLE_VALUE =
      new ValueProvider<String>() {
        @Override
        public String get() {
          throw new IllegalStateException("Value is not accessible");
        }

        @Override
        public boolean isAccessible() {
          return false;
        }
      };

  private static BigtableConfig config;
  private static FakeBigtableService service;
  private static final BigtableOptions BIGTABLE_OPTIONS =
      BigtableOptions.builder()
          .setProjectId("options_project")
          .setInstanceId("options_instance")
          .build();

  private static BigtableIO.Read defaultRead =
      BigtableIO.read().withInstanceId("instance").withProjectId("project");
  private static BigtableIO.Write defaultWrite =
      BigtableIO.write().withInstanceId("instance").withProjectId("project");
  private Coder<KV<ByteString, Iterable<Mutation>>> bigtableCoder;
  private static final TypeDescriptor<KV<ByteString, Iterable<Mutation>>> BIGTABLE_WRITE_TYPE =
      new TypeDescriptor<KV<ByteString, Iterable<Mutation>>>() {};

  private static final SerializableFunction<BigtableOptions.Builder, BigtableOptions.Builder>
      PORT_CONFIGURATOR = input -> input.setPort(1234);

  private static final ValueProvider<List<ByteKeyRange>> ALL_KEY_RANGE =
      StaticValueProvider.of(Collections.singletonList(ByteKeyRange.ALL_KEYS));

  private FakeServiceFactory factory;

  private static BigtableServiceFactory.ConfigId configId;

  @Before
  public void setup() throws Exception {
    service = new FakeBigtableService();

    factory = new FakeServiceFactory(service);

    configId = factory.newId();

    defaultRead = defaultRead.withServiceFactory(factory);

    defaultWrite = defaultWrite.withServiceFactory(factory);

    bigtableCoder = p.getCoderRegistry().getCoder(BIGTABLE_WRITE_TYPE);

    config = BigtableConfig.builder().setValidate(true).build();
  }

  private static ByteKey makeByteKey(ByteString key) {
    return ByteKey.copyFrom(key.asReadOnlyByteBuffer());
  }

  @Test
  public void testReadBuildsCorrectly() {
    BigtableIO.Read read =
        BigtableIO.read()
            .withBigtableOptions(BIGTABLE_OPTIONS)
            .withTableId("table")
            .withInstanceId("instance")
            .withProjectId("project")
            .withAppProfileId("app-profile")
            .withBigtableOptionsConfigurator(PORT_CONFIGURATOR);
    assertEquals("options_project", read.getBigtableOptions().getProjectId());
    assertEquals("options_instance", read.getBigtableOptions().getInstanceId());
    assertEquals("instance", read.getBigtableConfig().getInstanceId().get());
    assertEquals("project", read.getBigtableConfig().getProjectId().get());
    assertEquals("app-profile", read.getBigtableConfig().getAppProfileId().get());
    assertEquals("table", read.getTableId());
    assertEquals(PORT_CONFIGURATOR, read.getBigtableConfig().getBigtableOptionsConfigurator());
  }

  @Test
  public void testReadValidationFailsMissingTable() {
    BigtableIO.Read read = BigtableIO.read().withBigtableOptions(BIGTABLE_OPTIONS);

    thrown.expect(IllegalArgumentException.class);
    read.expand(null);
  }

  @Test
  public void testReadValidationFailsMissingInstanceId() {
    BigtableIO.Read read =
        BigtableIO.read()
            .withTableId("table")
            .withProjectId("project")
            .withBigtableOptions(BigtableOptions.builder().build());

    thrown.expect(IllegalArgumentException.class);

    read.expand(null);
  }

  @Test
  public void testReadValidationFailsMissingProjectId() {
    BigtableIO.Read read =
        BigtableIO.read()
            .withTableId("table")
            .withInstanceId("instance")
            .withBigtableOptions(BigtableOptions.builder().build());

    thrown.expect(IllegalArgumentException.class);

    read.expand(null);
  }

  @Test
  public void testReadValidationFailsMissingInstanceIdAndProjectId() {
    BigtableIO.Read read =
        BigtableIO.read()
            .withTableId("table")
            .withBigtableOptions(BigtableOptions.builder().build());

    thrown.expect(IllegalArgumentException.class);

    read.expand(null);
  }

  @Test
  public void testReadWithRuntimeParametersValidationFailed() {
    ReadOptions options = PipelineOptionsFactory.fromArgs().withValidation().as(ReadOptions.class);

    BigtableIO.Read read =
        BigtableIO.read()
            .withProjectId(options.getBigtableProject())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("tableId was not supplied");

    p.apply(read);
  }

  @Test
  public void testReadWithRuntimeParametersValidationDisabled() {
    ReadOptions options = PipelineOptionsFactory.fromArgs().withValidation().as(ReadOptions.class);

    BigtableIO.Read read =
        BigtableIO.read()
            .withoutValidation()
            .withProjectId(options.getBigtableProject())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId());

    
    thrown.expect(PipelineRunMissingException.class);

    p.apply(read);
  }

  @Test
  public void testWriteBuildsCorrectly() {
    BigtableIO.Write write =
        BigtableIO.write()
            .withBigtableOptions(BIGTABLE_OPTIONS)
            .withTableId("table")
            .withInstanceId("instance")
            .withProjectId("project")
            .withAppProfileId("app-profile");
    assertEquals("table", write.getBigtableWriteOptions().getTableId().get());
    assertEquals("options_project", write.getBigtableOptions().getProjectId());
    assertEquals("options_instance", write.getBigtableOptions().getInstanceId());
    assertEquals("instance", write.getBigtableConfig().getInstanceId().get());
    assertEquals("project", write.getBigtableConfig().getProjectId().get());
    assertEquals("app-profile", write.getBigtableConfig().getAppProfileId().get());
  }

  @Test
  public void testWriteValidationFailsMissingInstanceId() {
    BigtableIO.WriteWithResults write =
        BigtableIO.write()
            .withTableId("table")
            .withProjectId("project")
            .withBigtableOptions(BigtableOptions.builder().build())
            .withWriteResults();

    thrown.expect(IllegalArgumentException.class);

    write.expand(null);
  }

  @Test
  public void testWriteValidationFailsMissingProjectId() {
    BigtableIO.WriteWithResults write =
        BigtableIO.write()
            .withTableId("table")
            .withInstanceId("instance")
            .withBigtableOptions(BigtableOptions.builder().build())
            .withWriteResults();

    thrown.expect(IllegalArgumentException.class);

    write.expand(null);
  }

  @Test
  public void testWriteValidationFailsMissingInstanceIdAndProjectId() {
    BigtableIO.WriteWithResults write =
        BigtableIO.write()
            .withTableId("table")
            .withBigtableOptions(BigtableOptions.builder().build())
            .withWriteResults();

    thrown.expect(IllegalArgumentException.class);

    write.expand(null);
  }

  @Test
  public void testWriteValidationFailsMissingOptionsAndInstanceAndProject() {
    BigtableIO.WriteWithResults write = BigtableIO.write().withTableId("table").withWriteResults();

    thrown.expect(IllegalArgumentException.class);

    write.expand(null);
  }

  
  private static KV<ByteString, Iterable<Mutation>> makeWrite(String key, String value) {
    ByteString rowKey = ByteString.copyFromUtf8(key);
    Iterable<Mutation> mutations =
        ImmutableList.of(
            Mutation.newBuilder()
                .setSetCell(SetCell.newBuilder().setValue(ByteString.copyFromUtf8(value)))
                .build());
    return KV.of(rowKey, mutations);
  }

  
  private static KV<ByteString, Iterable<Mutation>> makeBadWrite(String key) {
    Iterable<Mutation> mutations = ImmutableList.of(Mutation.newBuilder().build());
    return KV.of(ByteString.copyFromUtf8(key), mutations);
  }

  
  @Test
  public void testReadingFailsTableDoesNotExist() throws Exception {
    final String table = "TEST-TABLE";

    BigtableIO.Read read =
        BigtableIO.read()
            .withBigtableOptions(BIGTABLE_OPTIONS)
            .withTableId(table)
            .withServiceFactory(factory);

    
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Table %s does not exist", table));

    p.apply(read);
    p.run();
  }
  
  
  private static class KeyMatchesRegex implements Predicate<ByteString> {
    private final String regex;

    public KeyMatchesRegex(String regex) {
      this.regex = regex;
    }

    @Override
    public boolean apply(@Nullable ByteString input) {
      verifyNotNull(input, "input");
      return input.toStringUtf8().matches(regex);
    }
  }

  private static List<Row> filterToRange(List<Row> rows, final ByteKeyRange range) {
    return filterToRanges(rows, ImmutableList.of(range));
  }

  private static List<Row> filterToRanges(List<Row> rows, final List<ByteKeyRange> ranges) {
    return Lists.newArrayList(
        rows.stream()
            .filter(
                input -> {
                  verifyNotNull(input, "input");
                  for (ByteKeyRange range : ranges) {
                    if (range.containsKey(makeByteKey(input.getKey()))) {
                      return true;
                    }
                  }
                  return false;
                })
            .collect(Collectors.toList()));
  }

  private void runReadTest(BigtableIO.Read read, List<Row> expected) {
    PCollection<Row> rows =
        p.apply(read.getTableId() + "_" + read.getBigtableReadOptions().getKeyRanges(), read);
    PAssert.that(rows).containsInAnyOrder(expected);
    p.run();
  }

  
  @Test
  public void testReadingWithKeyRange() throws Exception {
    final String table = "TEST-KEY-RANGE-TABLE";
    final int numRows = 1001;
    List<Row> testRows = makeTableData(table, numRows);
    ByteKey startKey = ByteKey.copyFrom("key000000100".getBytes(StandardCharsets.UTF_8));
    ByteKey endKey = ByteKey.copyFrom("key000000300".getBytes(StandardCharsets.UTF_8));

    service.setupSampleRowKeys(table, numRows / 10, "key000000100".length());
    
    final ByteKeyRange prefixRange = ByteKeyRange.ALL_KEYS.withEndKey(startKey);
    List<Row> prefixRows = filterToRange(testRows, prefixRange);
    runReadTest(defaultRead.withTableId(table).withKeyRange(prefixRange), prefixRows);

    
    final ByteKeyRange suffixRange = ByteKeyRange.ALL_KEYS.withStartKey(startKey);
    List<Row> suffixRows = filterToRange(testRows, suffixRange);
    runReadTest(defaultRead.withTableId(table).withKeyRange(suffixRange), suffixRows);

    
    final ByteKeyRange middleRange = ByteKeyRange.of(startKey, endKey);
    List<Row> middleRows = filterToRange(testRows, middleRange);
    runReadTest(defaultRead.withTableId(table).withKeyRange(middleRange), middleRows);

    

    
    assertThat(prefixRows, allOf(hasSize(lessThan(numRows)), hasSize(greaterThan(0))));
    assertThat(suffixRows, allOf(hasSize(lessThan(numRows)), hasSize(greaterThan(0))));
    assertThat(middleRows, allOf(hasSize(lessThan(numRows)), hasSize(greaterThan(0))));

    
    List<Row> union = Lists.newArrayList(prefixRows);
    union.addAll(suffixRows);
    assertThat(
        "prefix + suffix = total", union, containsInAnyOrder(testRows.toArray(new Row[] {})));

    
    assertThat(suffixRows, hasItems(middleRows.toArray(new Row[] {})));
  }

  
  @Test
  public void testReadingWithRuntimeParameterizedKeyRange() throws Exception {
    final String table = "TEST-KEY-RANGE-TABLE";
    final int numRows = 1001;
    List<Row> testRows = makeTableData(table, numRows);
    ByteKey startKey = ByteKey.copyFrom("key000000100".getBytes(StandardCharsets.UTF_8));
    ByteKey endKey = ByteKey.copyFrom("key000000300".getBytes(StandardCharsets.UTF_8));

    service.setupSampleRowKeys(table, numRows / 10, "key000000100".length());

    final ByteKeyRange middleRange = ByteKeyRange.of(startKey, endKey);
    List<Row> middleRows = filterToRange(testRows, middleRange);
    runReadTest(
        defaultRead
            .withTableId(table)
            .withKeyRanges(StaticValueProvider.of(Collections.singletonList(middleRange))),
        middleRows);

    assertThat(middleRows, allOf(hasSize(lessThan(numRows)), hasSize(greaterThan(0))));
  }

  
  @Test
  public void testReadingWithKeyRanges() throws Exception {
    final String table = "TEST-KEY-RANGE-TABLE";
    final int numRows = 11;
    List<Row> testRows = makeTableData(table, numRows);
    ByteKey startKey1 = ByteKey.copyFrom("key000000001".getBytes(StandardCharsets.UTF_8));
    ByteKey endKey1 = ByteKey.copyFrom("key000000003".getBytes(StandardCharsets.UTF_8));
    ByteKey startKey2 = ByteKey.copyFrom("key000000004".getBytes(StandardCharsets.UTF_8));
    ByteKey endKey2 = ByteKey.copyFrom("key000000007".getBytes(StandardCharsets.UTF_8));
    ByteKey startKey3 = ByteKey.copyFrom("key000000008".getBytes(StandardCharsets.UTF_8));
    ByteKey endKey3 = ByteKey.copyFrom("key000000009".getBytes(StandardCharsets.UTF_8));

    service.setupSampleRowKeys(table, numRows / 10, "key000000001".length());

    final ByteKeyRange range1 = ByteKeyRange.of(startKey1, endKey1);
    final ByteKeyRange range2 = ByteKeyRange.of(startKey2, endKey2);
    final ByteKeyRange range3 = ByteKeyRange.of(startKey3, endKey3);
    List<ByteKeyRange> ranges = ImmutableList.of(range1, range2, range3);
    List<Row> rangeRows = filterToRanges(testRows, ranges);
    runReadTest(defaultRead.withTableId(table).withKeyRanges(ranges), rangeRows);

    
    assertThat(rangeRows, allOf(hasSize(lessThan(numRows)), hasSize(greaterThan(0))));
  }

  
  @Test
  public void testReadingWithFilter() {
    final String table = "TEST-FILTER-TABLE";
    final int numRows = 1001;
    List<Row> testRows = makeTableData(table, numRows);
    String regex = ".*17.*";
    final KeyMatchesRegex keyPredicate = new KeyMatchesRegex(regex);
    Iterable<Row> filteredRows =
        testRows.stream()
            .filter(
                input -> {
                  verifyNotNull(input, "input");
                  return keyPredicate.apply(input.getKey());
                })
            .collect(Collectors.toList());

    RowFilter filter =
        RowFilter.newBuilder().setRowKeyRegexFilter(ByteString.copyFromUtf8(regex)).build();
    service.setupSampleRowKeys(table, 5, 10L);

    runReadTest(
        defaultRead.withTableId(table).withRowFilter(filter), Lists.newArrayList(filteredRows));
  }

  
  @Test
  public void testReadingWithRuntimeParameterizedFilter() throws Exception {
    final String table = "TEST-FILTER-TABLE";
    final int numRows = 1001;
    List<Row> testRows = makeTableData(table, numRows);
    String regex = ".*17.*";
    final KeyMatchesRegex keyPredicate = new KeyMatchesRegex(regex);
    Iterable<Row> filteredRows =
        testRows.stream()
            .filter(
                input -> {
                  verifyNotNull(input, "input");
                  return keyPredicate.apply(input.getKey());
                })
            .collect(Collectors.toList());

    RowFilter filter =
        RowFilter.newBuilder().setRowKeyRegexFilter(ByteString.copyFromUtf8(regex)).build();
    service.setupSampleRowKeys(table, 5, 10L);

    runReadTest(
        defaultRead.withTableId(table).withRowFilter(StaticValueProvider.of(filter)),
        Lists.newArrayList(filteredRows));
  }
  
  @Test
  public void testReadingSplitAtFractionExhaustive() throws Exception {
    final String table = "TEST-FEW-ROWS-SPLIT-EXHAUSTIVE-TABLE";
    final int numRows = 10;
    final int numSamples = 1;
    final long bytesPerRow = 1L;
    BigtableSource source = extracted(table, numRows, numSamples, bytesPerRow);
    assertSplitAtFractionExhaustive(source, null);
  }

private BigtableSource extracted(final String table, final int numRows, final int numSamples, final long bytesPerRow) {
	makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    BigtableSource source =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(
                    StaticValueProvider.of(Collections.singletonList(service.getTableRange(table))))
                .build(),
            null);
	return source;
}

  
  @Test
  public void testReadingSplitAtFraction() throws Exception {
    final String table = "TEST-SPLIT-AT-FRACTION";
    final int numRows = 10;
    final int numSamples = 1;
    final long bytesPerRow = 1L;
    BigtableSource source = extracted(table, numRows, numSamples, bytesPerRow);
    
    assertSplitAtFractionFails(source, 0, 0.1, null );
    assertSplitAtFractionFails(source, 0, 1.0, null );
    
    assertSplitAtFractionSucceedsAndConsistent(source, 1, 0.333, null );
    assertSplitAtFractionSucceedsAndConsistent(source, 1, 0.666, null );
    
    assertSplitAtFractionFails(source, 3, 0.2, null );
    assertSplitAtFractionSucceedsAndConsistent(source, 3, 0.571, null );
    assertSplitAtFractionSucceedsAndConsistent(source, 3, 0.9, null );
    
    assertSplitAtFractionFails(source, 6, 0.5, null );
    assertSplitAtFractionSucceedsAndConsistent(source, 6, 0.7, null );
  }

  
  @Test
  public void testReadingWithSplits() throws Exception {
    final String table = "TEST-MANY-ROWS-SPLITS-TABLE";
    final int numRows = 1500;
    final int numSamples = 10;
    setupAndSplitBigtableSource(table, numRows, numSamples);
  }

private void setupAndSplitBigtableSource(final String table, final int numRows, final int numSamples)
		throws Exception, @UnknownKeyFor @NonNull @Initialized Exception {
	final long bytesPerRow = 100L;

    
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    
    BigtableSource source =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(ALL_KEY_RANGE)
                .build(),
            null );
    List<BigtableSource> splits =
        source.split(numRows * bytesPerRow / numSamples, null );

    
    assertThat(splits, hasSize(numSamples));
    assertSourcesEqualReferenceSource(source, splits, null );
}

  
  @Test
  public void testSplittingWithDesiredBundleSizeZero() throws Exception {
    final String table = "TEST-SPLIT-DESIRED-BUNDLE-SIZE-ZERO-TABLE";
    final int numRows = 10;
    final int numSamples = 10;
    final long bytesPerRow = 1L;

    
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    
    BigtableSource source =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(ALL_KEY_RANGE)
                .build(),
            null );
    List<BigtableSource> splits = source.split(0, null );

    
    assertThat(splits, hasSize(numSamples));
    assertSourcesEqualReferenceSource(source, splits, null );
  }

  private void assertAllSourcesHaveSingleAdjacentRanges(List<BigtableSource> sources) {
    if (sources.size() > 0) {
      assertThat(sources.get(0).getRanges(), hasSize(1));
      for (int i = 1; i < sources.size(); i++) {
        assertThat(sources.get(i).getRanges(), hasSize(1));
        ByteKey lastEndKey = sources.get(i - 1).getRanges().get(0).getEndKey();
        ByteKey currentStartKey = sources.get(i).getRanges().get(0).getStartKey();
        assertEquals(lastEndKey, currentStartKey);
      }
    }
  }

  private void assertAllSourcesHaveSingleRanges(List<BigtableSource> sources) {
    for (BigtableSource source : sources) {
      assertThat(source.getRanges(), hasSize(1));
    }
  }

  private ByteKey createByteKey(int key) {
    return ByteKey.copyFrom(String.format("key%09d", key).getBytes(StandardCharsets.UTF_8));
  }

  
  @Test
  public void testReduceSplitsWithSomeNonAdjacentRanges() throws Exception {
    final String table = "TEST-MANY-ROWS-SPLITS-TABLE";
    final int numRows = 10;
    final int numSamples = 10;
    final long bytesPerRow = 100L;
    final int maxSplit = 3;

    
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    
    List<ByteKeyRange> keyRanges =
        Arrays.asList(
            ByteKeyRange.of(ByteKey.EMPTY, createByteKey(1)),
            ByteKeyRange.of(createByteKey(1), createByteKey(2)),
            ByteKeyRange.of(createByteKey(3), createByteKey(4)),
            ByteKeyRange.of(createByteKey(4), createByteKey(5)),
            ByteKeyRange.of(createByteKey(6), createByteKey(7)),
            ByteKeyRange.of(createByteKey(8), createByteKey(9)));

    
    List<ByteKeyRange> expectedKeyRangesAfterReducedSplits =
        Arrays.asList(
            ByteKeyRange.of(ByteKey.EMPTY, createByteKey(2)),
            ByteKeyRange.of(createByteKey(3), createByteKey(5)),
            ByteKeyRange.of(createByteKey(6), createByteKey(7)),
            ByteKeyRange.of(createByteKey(8), createByteKey(9)));

    createAndSplitBigtableSource(table, maxSplit, expectedKeyRangesAfterReducedSplits);
  }

  
  @Test
  public void testReduceSplitsWithAllNonAdjacentRange() throws Exception {
    final String table = "TEST-MANY-ROWS-SPLITS-TABLE";
    final int numRows = 10;
    final int numSamples = 10;
    final long bytesPerRow = 100L;
    final int maxSplit = 3;

    
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    
    List<ByteKeyRange> keyRanges =
        Arrays.asList(
            ByteKeyRange.of(ByteKey.EMPTY, createByteKey(1)),
            ByteKeyRange.of(createByteKey(2), createByteKey(3)),
            ByteKeyRange.of(createByteKey(4), createByteKey(5)),
            ByteKeyRange.of(createByteKey(6), createByteKey(7)),
            ByteKeyRange.of(createByteKey(8), createByteKey(9)));

    
    createAndSplitBigtableSource(table, maxSplit, keyRanges);
  }

private void createAndSplitBigtableSource(final String table, final int maxSplit, List<ByteKeyRange> keyRanges)
		throws IOException {
	BigtableSource source =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(StaticValueProvider.of(keyRanges))
                .build(),
            null );

    List<BigtableSource> splits = new ArrayList<>();
    for (ByteKeyRange range : keyRanges) {
      splits.add(source.withSingleRange(range));
    }

    List<BigtableSource> reducedSplits = source.reduceSplits(splits, null, maxSplit);

    List<ByteKeyRange> actualRangesAfterSplit = new ArrayList<>();

    for (BigtableSource splitSource : reducedSplits) {
      actualRangesAfterSplit.addAll(splitSource.getRanges());
    }

    assertAllSourcesHaveSingleRanges(reducedSplits);

    
    assertThat(
        actualRangesAfterSplit,
        IsIterableContainingInAnyOrder.containsInAnyOrder(keyRanges.toArray()));
}

  
  @Test
  public void tesReduceSplitsWithAdjacentRanges() throws Exception {
    final String table = "TEST-MANY-ROWS-SPLITS-TABLE";
    final int numRows = 10;
    final int numSamples = 10;
    final long bytesPerRow = 100L;
    final int maxSplit = 3;

    
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    
    BigtableSource source =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(ALL_KEY_RANGE)
                .build(),
            null );
    List<BigtableSource> splits = new ArrayList<>();
    List<ByteKeyRange> keyRanges =
        Arrays.asList(
            ByteKeyRange.of(ByteKey.EMPTY, createByteKey(1)),
            ByteKeyRange.of(createByteKey(1), createByteKey(2)),
            ByteKeyRange.of(createByteKey(2), createByteKey(3)),
            ByteKeyRange.of(createByteKey(3), createByteKey(4)),
            ByteKeyRange.of(createByteKey(4), createByteKey(5)),
            ByteKeyRange.of(createByteKey(5), createByteKey(6)),
            ByteKeyRange.of(createByteKey(6), createByteKey(7)),
            ByteKeyRange.of(createByteKey(7), createByteKey(8)),
            ByteKeyRange.of(createByteKey(8), createByteKey(9)),
            ByteKeyRange.of(createByteKey(9), ByteKey.EMPTY));
    for (ByteKeyRange range : keyRanges) {
      splits.add(source.withSingleRange(range));
    }

    
    
    List<ByteKeyRange> expectedKeyRangesAfterReducedSplits =
        Arrays.asList(
            ByteKeyRange.of(ByteKey.EMPTY, createByteKey(4)),
            ByteKeyRange.of(createByteKey(4), createByteKey(8)),
            ByteKeyRange.of(createByteKey(8), ByteKey.EMPTY));

    List<BigtableSource> reducedSplits = source.reduceSplits(splits, null, maxSplit);

    List<ByteKeyRange> actualRangesAfterSplit = new ArrayList<>();

    for (BigtableSource splitSource : reducedSplits) {
      actualRangesAfterSplit.addAll(splitSource.getRanges());
    }

    assertThat(
        actualRangesAfterSplit,
        IsIterableContainingInAnyOrder.containsInAnyOrder(
            expectedKeyRangesAfterReducedSplits.toArray()));
    assertAllSourcesHaveSingleAdjacentRanges(reducedSplits);
    assertSourcesEqualReferenceSource(source, reducedSplits, null );
  }

  
  @Test
  public void testReadingWithSplitsWithSeveralKeyRanges() throws Exception {
    final String table = "TEST-MANY-ROWS-SPLITS-TABLE-MULTIPLE-RANGES";
    final int numRows = 1500;
    final int numSamples = 10;
    
    
    final int numSplits = 12;
    final long bytesPerRow = 100L;

    
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    ByteKey splitKey1 = ByteKey.copyFrom("key000000500".getBytes(StandardCharsets.UTF_8));
    ByteKey splitKey2 = ByteKey.copyFrom("key000001000".getBytes(StandardCharsets.UTF_8));

    BigtableSource source = createBigtableSourceWithKeyRanges(table, splitKey1, splitKey2);
    BigtableSource referenceSource =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(
                    StaticValueProvider.of(Collections.singletonList(service.getTableRange(table))))
                .build(),
            null );
    List<BigtableSource> splits = 
        source.split(numRows * bytesPerRow / numSamples, null );

    
    assertThat(splits, hasSize(numSplits));
    assertSourcesEqualReferenceSource(referenceSource, splits, null );
  }

private BigtableSource createBigtableSourceWithKeyRanges(final String table, ByteKey splitKey1, ByteKey splitKey2) {
	ByteKeyRange tableRange = service.getTableRange(table);
    List<ByteKeyRange> keyRanges =
        Arrays.asList(
            tableRange.withEndKey(splitKey1),
            tableRange.withStartKey(splitKey1).withEndKey(splitKey2),
            tableRange.withStartKey(splitKey2));
    
    BigtableSource source =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(StaticValueProvider.of(keyRanges))
                .build(),
            null );
	return source;
}

  
  @Test
  public void testReadingWithSubSplits() throws Exception {
    final String table = "TEST-MANY-ROWS-SPLITS-TABLE";
    final int numRows = 1000;
    final int numSamples = 10;
    final int numSplits = 20;
    final long bytesPerRow = 100L;

    
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    
    BigtableSource source =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(ALL_KEY_RANGE)
                .build(),
            null );
    List<BigtableSource> splits = source.split(numRows * bytesPerRow / numSplits, null);

    
    assertThat(splits, hasSize(numSplits));
    assertSourcesEqualReferenceSource(source, splits, null );
  }

  
  @Test
  public void testReadingWithSubSplitsWithSeveralKeyRanges() throws Exception {
    final String table = "TEST-MANY-ROWS-SPLITS-TABLE-MULTIPLE-RANGES";
    final int expectedNumSplits = 24;
    setupAndSplitBigtableSource(table, expectedNumSplits, expectedNumSplits);
  }

  
  @Test
  public void testReadingWithFilterAndSubSplits() throws Exception {
    final String table = "TEST-FILTER-SUB-SPLITS";
    final int numRows = 1700;
    final int numSamples = 10;
    final int numSplits = 20;
    final long bytesPerRow = 100L;

    
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    
    RowFilter filter =
        RowFilter.newBuilder().setRowKeyRegexFilter(ByteString.copyFromUtf8(".*17.*")).build();
    BigtableSource source =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setRowFilter(StaticValueProvider.of(filter))
                .setKeyRanges(ALL_KEY_RANGE)
                .build(),
            null );
    List<BigtableSource> splits = source.split(numRows * bytesPerRow / numSplits, null);

    
    assertThat(splits, hasSize(numSplits));
    assertSourcesEqualReferenceSource(source, splits, null );
  }

  @Test
  public void testReadWithoutValidate() {
    final String table = "fooTable";
    BigtableIO.Read read =
        BigtableIO.read()
            .withBigtableOptions(BIGTABLE_OPTIONS)
            .withTableId(table)
            .withoutValidation();

    
    read.validate(TestPipeline.testingPipelineOptions());
  }

  @Test
  public void testWriteWithoutValidate() {
    final String table = "fooTable";
    BigtableIO.Write write =
        BigtableIO.write()
            .withBigtableOptions(BIGTABLE_OPTIONS)
            .withTableId(table)
            .withoutValidation();

    
    write.validate(TestPipeline.testingPipelineOptions());
  }

  
  @Test
  public void testWritingEmitsResultsWhenDoneInGlobalWindow() {
    final String table = "table";
    final String key = "key";
    final String value = "value";

    service.createTable(table);

    PCollection<BigtableWriteResult> results =
        p.apply("single row", Create.of(makeWrite(key, value)).withCoder(bigtableCoder))
            .apply("write", defaultWrite.withTableId(table).withWriteResults());
    PAssert.that(results)
        .inWindow(GlobalWindow.INSTANCE)
        .containsInAnyOrder(BigtableWriteResult.create(1));

    p.run();
  }

  
  @Test
  public void testWritingAndWaitingOnResults() {
    final String table = "table";
    final String key = "key";
    final String value = "value";

    service.createTable(table);

    Instant elementTimestamp = Instant.parse("2019-06-10T00:00:00");
    Duration windowDuration = Duration.standardMinutes(1);

    TestStream<KV<ByteString, Iterable<Mutation>>> writeInputs =
        TestStream.create(bigtableCoder)
            .advanceWatermarkTo(elementTimestamp)
            .addElements(makeWrite(key, value))
            .advanceWatermarkToInfinity();

    TestStream<String> testInputs =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(elementTimestamp)
            .addElements("done")
            .advanceWatermarkToInfinity();

    PCollection<BigtableWriteResult> writes =
        p.apply("rows", writeInputs)
            .apply(
                "window rows",
                Window.<KV<ByteString, Iterable<Mutation>>>into(FixedWindows.of(windowDuration))
                    .withAllowedLateness(Duration.ZERO))
            .apply("write", defaultWrite.withTableId(table).withWriteResults());

    PCollection<String> inputs =
        p.apply("inputs", testInputs)
            .apply("window inputs", Window.into(FixedWindows.of(windowDuration)))
            .apply("wait", Wait.on(writes));

    BoundedWindow expectedWindow = new IntervalWindow(elementTimestamp, windowDuration);

    PAssert.that(inputs).inWindow(expectedWindow).containsInAnyOrder("done");

    p.run();
  }

  
  private static class WriteGeneratorDoFn extends DoFn<Long, KV<ByteString, Iterable<Mutation>>> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
      for (int i = 0; i < ctx.element(); i++) {
        ctx.output(makeWrite("key", "value"));
      }
    }
  }

  
  @Test
  public void testWritingEmitsResultsWhenDoneInFixedWindow() throws Exception {
    final String table = "table";

    service.createTable(table);

    Instant elementTimestamp = Instant.parse("2019-06-10T00:00:00");
    Duration windowDuration = Duration.standardMinutes(1);

    TestStream<Long> input =
        TestStream.create(VarLongCoder.of())
            .advanceWatermarkTo(elementTimestamp)
            .addElements(1L)
            .advanceWatermarkTo(elementTimestamp.plus(windowDuration))
            .addElements(2L)
            .advanceWatermarkToInfinity();

    BoundedWindow expectedFirstWindow = new IntervalWindow(elementTimestamp, windowDuration);
    BoundedWindow expectedSecondWindow =
        new IntervalWindow(elementTimestamp.plus(windowDuration), windowDuration);

    PCollection<BigtableWriteResult> results =
        p.apply("rows", input)
            .apply("window", Window.into(FixedWindows.of(windowDuration)))
            .apply("expand", ParDo.of(new WriteGeneratorDoFn()))
            .apply("write", defaultWrite.withTableId(table).withWriteResults());
    PAssert.that(results)
        .inWindow(expectedFirstWindow)
        .containsInAnyOrder(BigtableWriteResult.create(1));
    PAssert.that(results)
        .inWindow(expectedSecondWindow)
        .containsInAnyOrder(BigtableWriteResult.create(2));

    p.run();
  }

  
  @Test
  public void testWritingFailsTableDoesNotExist() throws Exception {
    final String table = "TEST-TABLE";

    PCollection<KV<ByteString, Iterable<Mutation>>> emptyInput = createEmptyInputPCollection();

    
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Table %s does not exist", table));

    emptyInput.apply("write", defaultWrite.withTableId(table));
    p.run();
  }

private PCollection<KV<ByteString, Iterable<Mutation>>> createEmptyInputPCollection() {
	PCollection<KV<ByteString, Iterable<Mutation>>> emptyInput =
        p.apply(
            Create.empty(
                KvCoder.of(ByteStringCoder.of(), IterableCoder.of(ProtoCoder.of(Mutation.class)))));
	return emptyInput;
}

  
  @Test
  public void testTableCheckIgnoredWhenCanNotAccessConfig() throws Exception {
    PCollection<KV<ByteString, Iterable<Mutation>>> emptyInput =
        p.apply(
            Create.empty(
                KvCoder.of(ByteStringCoder.of(), IterableCoder.of(ProtoCoder.of(Mutation.class)))));

    emptyInput.apply("write", defaultWrite.withTableId(NOT_ACCESSIBLE_VALUE));
    p.run();
  }

  @Test
  public void testGetSplitPointsConsumed() throws Exception {
    final String table = "TEST-TABLE";
    final int numRows = 100;
    int splitPointsConsumed = 0;

    makeTableData(table, numRows);

    BigtableSource source =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(ALL_KEY_RANGE)
                .build(),
            null);

    BoundedReader<Row> reader = source.createReader(TestPipeline.testingPipelineOptions());

    reader.start();
    
    assertEquals(
        "splitPointsConsumed starting", splitPointsConsumed, reader.getSplitPointsConsumed());

    
    while (reader.advance()) {
      assertEquals(
          "splitPointsConsumed advancing", ++splitPointsConsumed, reader.getSplitPointsConsumed());
    }

    
    assertEquals("splitPointsConsumed done", numRows, reader.getSplitPointsConsumed());

    reader.close();
  }

  @Test
  public void testReadWithBigTableOptionsSetsRetryOptions() {
    final int initialBackoffMillis = -1;

    BigtableOptions.Builder optionsBuilder = BIGTABLE_OPTIONS.toBuilder();

    RetryOptions.Builder retryOptionsBuilder = RetryOptions.builder();
    retryOptionsBuilder.setInitialBackoffMillis(initialBackoffMillis);

    optionsBuilder.setRetryOptions(retryOptionsBuilder.build());

    BigtableIO.Read read = BigtableIO.read().withBigtableOptions(optionsBuilder.build());

    BigtableOptions options = read.getBigtableOptions();
    assertEquals(initialBackoffMillis, options.getRetryOptions().getInitialBackoffMillis());

    assertThat(options.getRetryOptions(), Matchers.equalTo(retryOptionsBuilder.build()));
  }

  @Test
  public void testWriteWithBigTableOptionsSetsBulkOptionsAndRetryOptions() {
    final int maxInflightRpcs = 1;
    final int initialBackoffMillis = -1;

    BigtableOptions.Builder optionsBuilder = BIGTABLE_OPTIONS.toBuilder();

    BulkOptions.Builder bulkOptionsBuilder = BulkOptions.builder();
    bulkOptionsBuilder.setMaxInflightRpcs(maxInflightRpcs);

    RetryOptions.Builder retryOptionsBuilder = RetryOptions.builder();
    retryOptionsBuilder.setInitialBackoffMillis(initialBackoffMillis);

    optionsBuilder
        .setBulkOptions(bulkOptionsBuilder.build())
        .setRetryOptions(retryOptionsBuilder.build());

    BigtableIO.Write write = BigtableIO.write().withBigtableOptions(optionsBuilder.build());

    BigtableOptions options = write.getBigtableOptions();
    assertTrue(options.getBulkOptions().useBulkApi());
    assertEquals(maxInflightRpcs, options.getBulkOptions().getMaxInflightRpcs());
    assertEquals(initialBackoffMillis, options.getRetryOptions().getInitialBackoffMillis());

    assertThat(
        options.getBulkOptions(), Matchers.equalTo(bulkOptionsBuilder.setUseBulkApi(true).build()));
    assertThat(options.getRetryOptions(), Matchers.equalTo(retryOptionsBuilder.build()));
  }

  
  private static final String COLUMN_FAMILY_NAME = "family";
  private static final ByteString COLUMN_NAME = ByteString.copyFromUtf8("column");
  private static final Column TEST_COLUMN = Column.newBuilder().setQualifier(COLUMN_NAME).build();
  private static final Family TEST_FAMILY = Family.newBuilder().setName(COLUMN_FAMILY_NAME).build();

  
  private static Row makeRow(ByteString key, ByteString value) {
    
    Column.Builder newColumn = TEST_COLUMN.toBuilder().addCells(Cell.newBuilder().setValue(value));
    return Row.newBuilder()
        .setKey(key)
        .addFamilies(TEST_FAMILY.toBuilder().addColumns(newColumn))
        .build();
  }

  
  private static List<Row> makeTableData(
      FakeBigtableService fakeService, String tableId, int numRows) {
    fakeService.createTable(tableId);
    Map<ByteString, ByteString> testData = fakeService.getTable(tableId);

    List<Row> testRows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; ++i) {
      ByteString key = ByteString.copyFromUtf8(String.format("key%09d", i));
      ByteString value = ByteString.copyFromUtf8(String.format("value%09d", i));
      testData.put(key, value);
      testRows.add(makeRow(key, value));
    }

    return testRows;
  }

  
  private static List<Row> makeTableData(String tableId, int numRows) {
    return makeTableData(service, tableId, numRows);
  }

  
  private static class FakeBigtableService implements BigtableService {
    private final Map<String, SortedMap<ByteString, ByteString>> tables = new HashMap<>();
    private final Map<String, List<KeyOffset>> sampleRowKeys = new HashMap<>();

    public @Nullable SortedMap<ByteString, ByteString> getTable(String tableId) {
      return tables.get(tableId);
    }

    public ByteKeyRange getTableRange(String tableId) {
      verifyTableExists(tableId);
      SortedMap<ByteString, ByteString> data = tables.get(tableId);
      return ByteKeyRange.of(makeByteKey(data.firstKey()), makeByteKey(data.lastKey()));
    }

    public void createTable(String tableId) {
      tables.put(tableId, new TreeMap<>(new ByteStringComparator()));
    }

    public boolean tableExists(String tableId) {
      return tables.containsKey(tableId);
    }

    public void verifyTableExists(String tableId) {
      checkArgument(tableExists(tableId), "Table %s does not exist", tableId);
    }

    @Override
    public FakeBigtableReader createReader(BigtableSource source) {
      return new FakeBigtableReader(source);
    }

    @Override
    public FakeBigtableWriter openForWriting(BigtableWriteOptions writeOptions) {
      return new FakeBigtableWriter(writeOptions.getTableId().get());
    }

    @Override
    public List<KeyOffset> getSampleRowKeys(BigtableSource source) {
      List<KeyOffset> samples = sampleRowKeys.get(source.getTableId().get());
      checkNotNull(samples, "No samples found for table %s", source.getTableId().get());
      return samples;
    }

    @Override
    public void close() {}

    
    void setupSampleRowKeys(String tableId, int numSamples, long bytesPerRow) {
      verifyTableExists(tableId);
      checkArgument(numSamples > 0, "Number of samples must be positive: %s", numSamples);
      checkArgument(bytesPerRow > 0, "Bytes/Row must be positive: %s", bytesPerRow);

      ImmutableList.Builder<KeyOffset> ret = ImmutableList.builder();
      SortedMap<ByteString, ByteString> rows = getTable(tableId);
      int currentSample = 1;
      int rowsSoFar = 0;
      for (Map.Entry<ByteString, ByteString> entry : rows.entrySet()) {
        if (((double) rowsSoFar) / rows.size() >= ((double) currentSample) / numSamples) {
          
          ret.add(KeyOffset.create(entry.getKey(), rowsSoFar * bytesPerRow));
          
          currentSample++;
        }
        ++rowsSoFar;
      }

      
      ret.add(KeyOffset.create(ByteString.EMPTY, rows.size() * bytesPerRow));

      sampleRowKeys.put(tableId, ret.build());
    }
  }

  
  private static class FailureBigtableService extends FakeBigtableService {
    public FailureBigtableService(FailureOptions options) {
      failureOptions = options;
    }

    @Override
    public FakeBigtableReader createReader(BigtableSource source) {
      return new FailureBigtableReader(source, this, failureOptions);
    }

    @Override
    public FailureBigtableWriter openForWriting(BigtableWriteOptions writeOptions) {
      return new FailureBigtableWriter(writeOptions.getTableId().get(), this, failureOptions);
    }

    @Override
    public List<KeyOffset> getSampleRowKeys(BigtableSource source) {
      if (failureOptions.getFailAtSplit()) {
        throw new RuntimeException("Fake Exception in getSampleRowKeys()");
      }
      return super.getSampleRowKeys(source);
    }

    @Override
    public void close() {}

    private final FailureOptions failureOptions;
  }

  
  private static class FakeBigtableReader implements BigtableService.Reader {
    private final BigtableSource source;
    private final FakeBigtableService service;
    private Iterator<Map.Entry<ByteString, ByteString>> rows;
    private Row currentRow;
    private final Predicate<ByteString> filter;

    public FakeBigtableReader(BigtableSource source, FakeBigtableService service) {
      this.source = source;
      this.service = service;
      if (source.getRowFilter() == null) {
        filter = Predicates.alwaysTrue();
      } else {
        ByteString keyRegex = source.getRowFilter().getRowKeyRegexFilter();
        checkArgument(!keyRegex.isEmpty(), "Only RowKeyRegexFilter is supported");
        filter = new KeyMatchesRegex(keyRegex.toStringUtf8());
      }
      service.verifyTableExists(source.getTableId().get());
    }

    public FakeBigtableReader(BigtableSource source) {
      this(source, BigtableIOTest.service);
    }

    @Override
    public boolean start() throws IOException {
      rows = service.tables.get(source.getTableId().get()).entrySet().iterator();
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      
      Map.Entry<ByteString, ByteString> entry = null;
      while (rows.hasNext()) {
        entry = rows.next();
        if (!filter.apply(entry.getKey())
            || !rangesContainsKey(source.getRanges(), makeByteKey(entry.getKey()))) {
          
          entry = null;
          continue;
        }
        
        break;
      }

      
      if (entry == null) {
        currentRow = null;
        return false;
      }

      
      currentRow = makeRow(entry.getKey(), entry.getValue());
      return true;
    }

    private boolean rangesContainsKey(List<ByteKeyRange> ranges, ByteKey key) {
      for (ByteKeyRange range : ranges) {
        if (range.containsKey(key)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public Row getCurrentRow() {
      if (currentRow == null) {
        throw new NoSuchElementException();
      }
      return currentRow;
    }

    @Override
    public void close() {}
  }

  
  private static class FailureBigtableReader extends FakeBigtableReader {
    public FailureBigtableReader(
        BigtableSource source, FakeBigtableService service, FailureOptions options) {
      super(source, service);
      failureOptions = options;
      numAdvance = 0;
    }

    @Override
    public boolean start() throws IOException {
      if (failureOptions.getFailAtStart()) {
        throw new IOException("Fake IOException at start()");
      }
      return super.start();
    }

    @Override
    public boolean advance() throws IOException {
      if (failureOptions.getFailAtAdvance() && numAdvance > 0) {
        
        throw new IOException("Fake IOException at advance()");
      }
      ++numAdvance;
      return super.advance();
    }

    private long numAdvance;
    private final FailureOptions failureOptions;
  }

  
  private static class FakeBigtableWriter implements BigtableService.Writer {
    private final String tableId;
    private final FakeBigtableService service;

    public FakeBigtableWriter(String tableId, FakeBigtableService service) {
      this.tableId = tableId;
      this.service = service;
    }

    public FakeBigtableWriter(String tableId) {
      this(tableId, BigtableIOTest.service);
    }

    @Override
    public CompletableFuture<MutateRowResponse> writeRecord(
        KV<ByteString, Iterable<Mutation>> record) throws IOException {
      service.verifyTableExists(tableId);
      Map<ByteString, ByteString> table = service.getTable(tableId);
      ByteString key = record.getKey();
      for (Mutation m : record.getValue()) {
        SetCell cell = m.getSetCell();
        if (cell.getValue().isEmpty()) {
          CompletableFuture<MutateRowResponse> result = new CompletableFuture<>();
          result.completeExceptionally(new IOException("cell value missing"));
          return result;
        }
        table.put(key, cell.getValue());
      }
      return CompletableFuture.completedFuture(MutateRowResponse.getDefaultInstance());
    }

    @Override
    public void writeSingleRecord(KV<ByteString, Iterable<Mutation>> record) {}

    @Override
    public void close() {}
  }

  
  private static class FailureBigtableWriter extends FakeBigtableWriter {
    public FailureBigtableWriter(
        String tableId, FailureBigtableService service, FailureOptions options) {
      super(tableId, service);
      this.failureOptions = options;
    }

    @Override
    public CompletableFuture<MutateRowResponse> writeRecord(
        KV<ByteString, Iterable<Mutation>> record) throws IOException {
      if (failureOptions.getFailAtWriteRecord()) {
        throw new IOException("Fake IOException in writeRecord()");
      }
      return super.writeRecord(record);
    }

    @Override
    public void writeSingleRecord(KV<ByteString, Iterable<Mutation>> record) throws ApiException {
      if (failureOptions.getFailAtWriteRecord()) {
        throw new RuntimeException("Fake RuntimeException in writeRecord()");
      }
    }

    private final FailureOptions failureOptions;
  }

  
  private static final class ByteStringComparator implements Comparator<ByteString>, Serializable {
    @Override
    public int compare(ByteString o1, ByteString o2) {
      return makeByteKey(o1).compareTo(makeByteKey(o2));
    }
  }

  
  @AutoValue
  abstract static class FailureOptions implements Serializable {
    abstract Boolean getFailAtStart();

    abstract Boolean getFailAtAdvance();

    abstract Boolean getFailAtSplit();

    abstract Boolean getFailAtWriteRecord();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFailAtStart(Boolean failAtStart);

      abstract Builder setFailAtAdvance(Boolean failAtAdvance);

      abstract Builder setFailAtSplit(Boolean failAtSplit);

      abstract Builder setFailAtWriteRecord(Boolean failAtWriteRecord);

      abstract FailureOptions build();
    }
  }

  static class FakeServiceFactory extends BigtableServiceFactory {
    private FakeBigtableService service;

    FakeServiceFactory(FakeBigtableService service) {
      this.service = service;
    }

    @Override
    BigtableServiceEntry getServiceForReading(
        ConfigId configId,
        BigtableConfig config,
        BigtableReadOptions opts,
        PipelineOptions pipelineOptions) {
      return BigtableServiceEntry.create(configId, service);
    }

    @Override
    BigtableServiceEntry getServiceForWriting(
        ConfigId configId,
        BigtableConfig config,
        BigtableWriteOptions opts,
        PipelineOptions pipelineOptions) {
      return BigtableServiceEntry.create(configId, service);
    }

    @Override
    boolean checkTableExists(BigtableConfig config, PipelineOptions pipelineOptions, String tableId)
        throws IOException {
      return service.tableExists(tableId);
    }

    @Override
    synchronized ConfigId newId() {
      return ConfigId.create();
    }
  }

  

  @Test
  public void testReadChangeStreamBuildsCorrectly() {
    Instant startTime = Instant.now();
    BigtableIO.ReadChangeStream readChangeStream =
        BigtableIO.readChangeStream()
            .withProjectId("project")
            .withInstanceId("instance")
            .withTableId("table")
            .withAppProfileId("app-profile")
            .withChangeStreamName("change-stream-name")
            .withMetadataTableProjectId("metadata-project")
            .withMetadataTableInstanceId("metadata-instance")
            .withMetadataTableTableId("metadata-table")
            .withMetadataTableAppProfileId("metadata-app-profile")
            .withStartTime(startTime)
            .withBacklogReplicationAdjustment(Duration.standardMinutes(1))
            .withCreateOrUpdateMetadataTable(false)
            .withExistingPipelineOptions(BigtableIO.ExistingPipelineOptions.FAIL_IF_EXISTS);
    assertEquals("project", readChangeStream.getBigtableConfig().getProjectId().get());
    assertEquals("instance", readChangeStream.getBigtableConfig().getInstanceId().get());
    assertEquals("app-profile", readChangeStream.getBigtableConfig().getAppProfileId().get());
    assertEquals("table", readChangeStream.getTableId());
    assertEquals(
        "metadata-project", readChangeStream.getMetadataTableBigtableConfig().getProjectId().get());
    assertEquals(
        "metadata-instance",
        readChangeStream.getMetadataTableBigtableConfig().getInstanceId().get());
    assertEquals(
        "metadata-app-profile",
        readChangeStream.getMetadataTableBigtableConfig().getAppProfileId().get());
    assertEquals("metadata-table", readChangeStream.getMetadataTableId());
    assertEquals("change-stream-name", readChangeStream.getChangeStreamName());
    assertEquals(startTime, readChangeStream.getStartTime());
    assertEquals(Duration.standardMinutes(1), readChangeStream.getBacklogReplicationAdjustment());
    assertEquals(false, readChangeStream.getCreateOrUpdateMetadataTable());
    assertEquals(
        BigtableIO.ExistingPipelineOptions.FAIL_IF_EXISTS,
        readChangeStream.getExistingPipelineOptions());
  }

  @Test
  public void testReadChangeStreamFailsValidation() {
	BigtableIO.ReadChangeStream readChangeStream = setupReadChangeStream();

    readChangeStream.validate(TestPipeline.testingPipelineOptions());
  }

  @Test
  public void testReadChangeStreamPassWithoutValidation() {
    BigtableIO.ReadChangeStream readChangeStream =
        BigtableIO.readChangeStream()
            .withProjectId("project")
            .withInstanceId("instance")
            .withTableId("table")
            .withoutValidation();
    
    readChangeStream.validate(TestPipeline.testingPipelineOptions());
  }

private BigtableIO.ReadChangeStream setupReadChangeStream() {
	BigtableIO.ReadChangeStream readChangeStream =
        BigtableIO.readChangeStream()
            .withProjectId("project")
            .withInstanceId("instance")
            .withTableId("table");
    
    thrown.expect(RuntimeException.class);
	return readChangeStream;
}
}
