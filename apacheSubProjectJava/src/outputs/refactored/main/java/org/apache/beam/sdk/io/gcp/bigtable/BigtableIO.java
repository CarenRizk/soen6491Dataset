package org.apache.beam.sdk.io.gcp.bigtable;

import static org.apache.beam.sdk.io.gcp.bigtable.BigtableServiceFactory.BigtableServiceEntry;
import static org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import static org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter.BAD_RECORD_TAG;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.gax.batching.BatchingException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.InvalidArgumentException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.auto.value.AutoValue;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.UniqueIdGenerator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.action.ActionFactory;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.BigtableChangeStreamAccessor;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.BigtableClientOverride;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.DetectNewPartitionsDoFn;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.FilterForMutationDoFn;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.InitializeDoFn;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.ReadChangeStreamPartitionDoFn;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.estimator.CoderSizeEstimator;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.io.range.ByteKeyRangeTracker;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.StringUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.ToStringHelper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({
  "nullness" 
})
public class BigtableIO {
  private static final Logger LOG = LoggerFactory.getLogger(BigtableIO.class);

  public static Read read() {
    return Read.create();
  }

  public static Write write() {
    return Write.create();
  }

  public static ReadChangeStream readChangeStream() {
    return ReadChangeStream.create();
  }

  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<Row>> {

    abstract BigtableConfig getBigtableConfig();

    abstract BigtableReadOptions getBigtableReadOptions();

    @VisibleForTesting
    abstract BigtableServiceFactory getServiceFactory();

    // Optimized by LLM: Changed to use Optional for clarity
    public Optional<String> getTableId() {
      ValueProvider<String> tableId = getBigtableReadOptions().getTableId();
      return tableId != null && tableId.isAccessible() ? Optional.of(tableId.get()) : Optional.empty();
    }

    @Deprecated
    public @Nullable BigtableOptions getBigtableOptions() {
      return getBigtableConfig().getBigtableOptions();
    }

    abstract Builder toBuilder();

    static Read create() {
      BigtableConfig config = BigtableConfig.builder().setValidate(true).build();

      return new AutoValue_BigtableIO_Read.Builder()
          .setBigtableConfig(config)
          .setBigtableReadOptions(
              BigtableReadOptions.builder()
                  .setTableId(StaticValueProvider.of(""))
                  .setKeyRanges(
                      StaticValueProvider.of(Collections.singletonList(ByteKeyRange.ALL_KEYS)))
                  .build())
          .setServiceFactory(new BigtableServiceFactory())
          .build();
    }

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setBigtableConfig(BigtableConfig bigtableConfig);

      abstract Builder setBigtableReadOptions(BigtableReadOptions bigtableReadOptions);

      abstract Builder setServiceFactory(BigtableServiceFactory factory);

      abstract Read build();
    }

    // Optimized by LLM: Consolidated similar methods into one
    public Read withProjectId(ValueProvider<String> projectId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withProjectId(projectId)).build();
    }

    public Read withProjectId(String projectId) {
      return withProjectId(StaticValueProvider.of(projectId));
    }

    // Optimized by LLM: Consolidated similar methods into one
    public Read withInstanceId(ValueProvider<String> instanceId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withInstanceId(instanceId)).build();
    }

    public Read withInstanceId(String instanceId) {
      return withInstanceId(StaticValueProvider.of(instanceId));
    }

    // Optimized by LLM: Consolidated similar methods into one
    public Read withTableId(ValueProvider<String> tableId) {
      BigtableReadOptions bigtableReadOptions = getBigtableReadOptions();
      return toBuilder()
          .setBigtableReadOptions(bigtableReadOptions.toBuilder().setTableId(tableId).build())
          .build();
    }

    public Read withTableId(String tableId) {
      return withTableId(StaticValueProvider.of(tableId));
    }

    // Optimized by LLM: Consolidated similar methods into one
    public Read withAppProfileId(ValueProvider<String> appProfileId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withAppProfileId(appProfileId)).build();
    }

    public Read withAppProfileId(String appProfileId) {
      return withAppProfileId(StaticValueProvider.of(appProfileId));
    }

    @Deprecated
    public Read withBigtableOptions(BigtableOptions options) {
      checkArgument(options != null, "options can not be null");
      return withBigtableOptions(options.toBuilder());
    }
    @Deprecated
    public Read withBigtableOptions(BigtableOptions.Builder optionsBuilder) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder()
          .setBigtableConfig(config.withBigtableOptions(optionsBuilder.build().toBuilder().build()))
          .build();
    }

    @Deprecated
    public Read withBigtableOptionsConfigurator(
        SerializableFunction<BigtableOptions.Builder, BigtableOptions.Builder> configurator) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder()
          .setBigtableConfig(config.withBigtableOptionsConfigurator(configurator))
          .build();
    }

    public Read withRowFilter(ValueProvider<RowFilter> filter) {
      checkArgument(filter != null, "filter can not be null");
      BigtableReadOptions bigtableReadOptions = getBigtableReadOptions();
      return toBuilder()
          .setBigtableReadOptions(bigtableReadOptions.toBuilder().setRowFilter(filter).build())
          .build();
    }

    public Read withRowFilter(RowFilter filter) {
      return withRowFilter(StaticValueProvider.of(filter));
    }

    public Read withMaxBufferElementCount(@Nullable Integer maxBufferElementCount) {
      BigtableReadOptions bigtableReadOptions = getBigtableReadOptions();
      return toBuilder()
          .setBigtableReadOptions(
              bigtableReadOptions
                  .toBuilder()
                  .setMaxBufferElementCount(maxBufferElementCount)
                  .build())
          .build();
    }

    public Read withKeyRange(ByteKeyRange keyRange) {
      return withKeyRanges(Collections.singletonList(keyRange));
    }

    public Read withKeyRanges(ValueProvider<List<ByteKeyRange>> keyRanges) {
      checkArgument(keyRanges != null, "keyRanges can not be null");
      BigtableReadOptions bigtableReadOptions = getBigtableReadOptions();
      return toBuilder()
          .setBigtableReadOptions(bigtableReadOptions.toBuilder().setKeyRanges(keyRanges).build())
          .build();
    }

    public Read withKeyRanges(List<ByteKeyRange> keyRanges) {
      return withKeyRanges(StaticValueProvider.of(keyRanges));
    }

    public Read withoutValidation() {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withValidate(false)).build();
    }

    @VisibleForTesting
    public Read withEmulator(String emulatorHost) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withEmulator(emulatorHost)).build();
    }

    public Read withAttemptTimeout(Duration timeout) {
      checkArgument(timeout.isLongerThan(Duration.ZERO), "attempt timeout must be positive");
      BigtableReadOptions readOptions = getBigtableReadOptions();
      return toBuilder()
          .setBigtableReadOptions(readOptions.toBuilder().setAttemptTimeout(timeout).build())
          .build();
    }

    public Read withOperationTimeout(Duration timeout) {
      checkArgument(timeout.isLongerThan(Duration.ZERO), "operation timeout must be positive");
      BigtableReadOptions readOptions = getBigtableReadOptions();
      return toBuilder()
          .setBigtableReadOptions(readOptions.toBuilder().setOperationTimeout(timeout).build())
          .build();
    }

    Read withServiceFactory(BigtableServiceFactory factory) {
      return toBuilder().setServiceFactory(factory).build();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      getBigtableConfig().validate();
      getBigtableReadOptions().validate();

      BigtableSource source =
          new BigtableSource(
              getServiceFactory(),
              getServiceFactory().newId(),
              getBigtableConfig(),
              getBigtableReadOptions(),
              null);
      return input.getPipeline().apply(org.apache.beam.sdk.io.Read.from(source));
    }

    @Override
    public void validate(PipelineOptions options) {
      validateTableExists(getBigtableConfig(), getBigtableReadOptions(), options);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      getBigtableConfig().populateDisplayData(builder);
      getBigtableReadOptions().populateDisplayData(builder);
    }

    @Override
    public final String toString() {
      ToStringHelper helper =
          MoreObjects.toStringHelper(Read.class).add("config", getBigtableConfig());
      return helper.add("readOptions", getBigtableReadOptions()).toString();
    }

    // Optimized by LLM: Extracted repeated logic for validating configurations
    private void validateTableExists(
        BigtableConfig config, BigtableReadOptions readOptions, PipelineOptions options) {
      if (config.getValidate() && config.isDataAccessible() && readOptions.isDataAccessible()) {
        ValueProvider<String> tableIdProvider = checkArgumentNotNull(readOptions.getTableId());
        String tableId = checkArgumentNotNull(tableIdProvider.get());
        try {
          boolean exists = getServiceFactory().checkTableExists(config, options, tableId);
          checkArgument(exists, "Table %s does not exist", tableId);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @AutoValue
  public abstract static class Write
      extends PTransform<PCollection<KV<ByteString, Iterable<Mutation>>>, PDone> {

    static SerializableFunction<BigtableOptions.Builder, BigtableOptions.Builder>
        enableBulkApiConfigurator(
            final @Nullable SerializableFunction<BigtableOptions.Builder, BigtableOptions.Builder>
                    userConfigurator) {
      return optionsBuilder -> {
        if (userConfigurator != null) {
          optionsBuilder = userConfigurator.apply(optionsBuilder);
        }

        return optionsBuilder.setBulkOptions(
            optionsBuilder.build().getBulkOptions().toBuilder().setUseBulkApi(true).build());
      };
    }

    abstract BigtableConfig getBigtableConfig();

    abstract BigtableWriteOptions getBigtableWriteOptions();

    @VisibleForTesting
    abstract BigtableServiceFactory getServiceFactory();

    abstract ErrorHandler<BadRecord, ?> getBadRecordErrorHandler();

    abstract BadRecordRouter getBadRecordRouter();

    @Deprecated
    public @Nullable BigtableOptions getBigtableOptions() {
      return getBigtableConfig().getBigtableOptions();
    }

    abstract Builder toBuilder();

    static Write create() {
      BigtableConfig config = BigtableConfig.builder().setValidate(true).build();

      BigtableWriteOptions writeOptions =
          BigtableWriteOptions.builder().setTableId(StaticValueProvider.of("")).build();

      return new AutoValue_BigtableIO_Write.Builder()
          .setBigtableConfig(config)
          .setBigtableWriteOptions(writeOptions)
          .setServiceFactory(new BigtableServiceFactory())
          .setBadRecordErrorHandler(new ErrorHandler.DefaultErrorHandler<>())
          .setBadRecordRouter(BadRecordRouter.THROWING_ROUTER)
          .build();
    }

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setBigtableConfig(BigtableConfig bigtableConfig);

      abstract Builder setBigtableWriteOptions(BigtableWriteOptions writeOptions);

      abstract Builder setServiceFactory(BigtableServiceFactory factory);

      abstract Builder setBadRecordErrorHandler(ErrorHandler<BadRecord, ?> badRecordErrorHandler);

      abstract Builder setBadRecordRouter(BadRecordRouter badRecordRouter);

      abstract Write build();
    }

    // Optimized by LLM: Consolidated similar methods into one
    public Write withProjectId(ValueProvider<String> projectId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withProjectId(projectId)).build();
    }

    public Write withProjectId(String projectId) {
      return withProjectId(StaticValueProvider.of(projectId));
    }

    // Optimized by LLM: Consolidated similar methods into one
    public Write withInstanceId(ValueProvider<String> instanceId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withInstanceId(instanceId)).build();
    }

    public Write withInstanceId(String instanceId) {
      return withInstanceId(StaticValueProvider.of(instanceId));
    }

    // Optimized by LLM: Consolidated similar methods into one
    public Write withTableId(ValueProvider<String> tableId) {
      BigtableWriteOptions writeOptions = getBigtableWriteOptions();
      return toBuilder()
          .setBigtableWriteOptions(writeOptions.toBuilder().setTableId(tableId).build())
          .build();
    }

    public Write withTableId(String tableId) {
      return withTableId(StaticValueProvider.of(tableId));
    }

    // Optimized by LLM: Consolidated similar methods into one
    public Write withAppProfileId(ValueProvider<String> appProfileId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withAppProfileId(appProfileId)).build();
    }

    public Write withAppProfileId(String appProfileId) {
      return withAppProfileId(StaticValueProvider.of(appProfileId));
    }

    @Deprecated
    public Write withBigtableOptions(BigtableOptions options) {
      checkArgument(options != null, "options can not be null");
      return withBigtableOptions(options.toBuilder());
    }

    @Deprecated
    public Write withBigtableOptions(BigtableOptions.Builder optionsBuilder) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder()
          .setBigtableConfig(config.withBigtableOptions(optionsBuilder.build()))
          .build();
    }

    @Deprecated
    public Write withBigtableOptionsConfigurator(
        SerializableFunction<BigtableOptions.Builder, BigtableOptions.Builder> configurator) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder()
          .setBigtableConfig(
              config.withBigtableOptionsConfigurator(enableBulkApiConfigurator(configurator)))
          .build();
    }

    public Write withoutValidation() {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withValidate(false)).build();
    }

    @VisibleForTesting
    public Write withEmulator(String emulatorHost) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder().setBigtableConfig(config.withEmulator(emulatorHost)).build();
    }

    public Write withAttemptTimeout(Duration timeout) {
      checkArgument(timeout.isLongerThan(Duration.ZERO), "attempt timeout must be positive");
      BigtableWriteOptions options = getBigtableWriteOptions();
      return toBuilder()
          .setBigtableWriteOptions(options.toBuilder().setAttemptTimeout(timeout).build())
          .build();
    }

    public Write withOperationTimeout(Duration timeout) {
      checkArgument(timeout.isLongerThan(Duration.ZERO), "operation timeout must be positive");
      BigtableWriteOptions options = getBigtableWriteOptions();
      return toBuilder()
          .setBigtableWriteOptions(options.toBuilder().setOperationTimeout(timeout).build())
          .build();
    }

    public Write withMaxElementsPerBatch(long size) {
      checkArgument(size > 0, "max elements per batch size must be positive");
      BigtableWriteOptions options = getBigtableWriteOptions();
      return toBuilder()
          .setBigtableWriteOptions(options.toBuilder().setMaxElementsPerBatch(size).build())
          .build();
    }

    public Write withMaxBytesPerBatch(long size) {
      checkArgument(size > 0, "max bytes per batch size must be positive");
      BigtableWriteOptions options = getBigtableWriteOptions();
      return toBuilder()
          .setBigtableWriteOptions(options.toBuilder().setMaxBytesPerBatch(size).build())
          .build();
    }

    public Write withMaxOutstandingElements(long count) {
      checkArgument(count > 0, "max outstanding elements must be positive");
      BigtableWriteOptions options = getBigtableWriteOptions();
      return toBuilder()
          .setBigtableWriteOptions(options.toBuilder().setMaxOutstandingElements(count).build())
          .build();
    }

    public Write withMaxOutstandingBytes(long bytes) {
      checkArgument(bytes > 0, "max outstanding bytes must be positive");
      BigtableWriteOptions options = getBigtableWriteOptions();
      return toBuilder()
          .setBigtableWriteOptions(options.toBuilder().setMaxOutstandingBytes(bytes).build())
          .build();
    }

    public Write withFlowControl(boolean enableFlowControl) {
      BigtableWriteOptions options = getBigtableWriteOptions();
      return toBuilder()
          .setBigtableWriteOptions(options.toBuilder().setFlowControl(enableFlowControl).build())
          .build();
    }

    @Deprecated
    public Write withThrottlingTargetMs(int throttlingTargetMs) {
      LOG.warn("withThrottlingTargetMs has been removed and does not have effect.");
      return this;
    }

    @Deprecated
    public Write withThrottlingReportTargetMs(int throttlingReportTargetMs) {
      LOG.warn("withThrottlingReportTargetMs has been removed and does not have an effect.");
      return this;
    }

    public Write withErrorHandler(ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
      return toBuilder()
          .setBadRecordErrorHandler(badRecordErrorHandler)
          .setBadRecordRouter(BadRecordRouter.RECORDING_ROUTER)
          .build();
    }

    @VisibleForTesting
    Write withServiceFactory(BigtableServiceFactory factory) {
      return toBuilder().setServiceFactory(factory).build();
    }

    public WriteWithResults withWriteResults() {
      return new WriteWithResults(
          getBigtableConfig(),
          getBigtableWriteOptions(),
          getServiceFactory(),
          getBadRecordErrorHandler(),
          getBadRecordRouter());
    }

    @Override
    public PDone expand(PCollection<KV<ByteString, Iterable<Mutation>>> input) {
      input.apply(withWriteResults());
      return PDone.in(input.getPipeline());
    }

    @Override
    public void validate(PipelineOptions options) {
      withWriteResults().validate(options);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      withWriteResults().populateDisplayData(builder);
    }

    @Override
    public final String toString() {
      return MoreObjects.toStringHelper(Write.class).add("config", getBigtableConfig()).toString();
    }
  }
  public static class WriteWithResults
      extends PTransform<
          PCollection<KV<ByteString, Iterable<Mutation>>>, PCollection<BigtableWriteResult>> {

    private static final String BIGTABLE_WRITER_WAIT_TIMEOUT_MS = "bigtable_writer_wait_timeout_ms";

    private static final TupleTag<BigtableWriteResult> WRITE_RESULTS =
        new TupleTag<>("writeResults");

    private final BigtableConfig bigtableConfig;
    private final BigtableWriteOptions bigtableWriteOptions;

    private final BigtableServiceFactory factory;

    private final ErrorHandler<BadRecord, ?> badRecordErrorHandler;

    private final BadRecordRouter badRecordRouter;

    WriteWithResults(
        BigtableConfig bigtableConfig,
        BigtableWriteOptions bigtableWriteOptions,
        BigtableServiceFactory factory,
        ErrorHandler<BadRecord, ?> badRecordErrorHandler,
        BadRecordRouter badRecordRouter) {
      this.bigtableConfig = bigtableConfig;
      this.bigtableWriteOptions = bigtableWriteOptions;
      this.factory = factory;
      this.badRecordErrorHandler = badRecordErrorHandler;
      this.badRecordRouter = badRecordRouter;
    }

    @Override
    public PCollection<BigtableWriteResult> expand(
        PCollection<KV<ByteString, Iterable<Mutation>>> input) {
      bigtableConfig.validate();
      bigtableWriteOptions.validate();

      PipelineOptions pipelineOptions = input.getPipeline().getOptions();
      String closeWaitTimeoutStr =
          ExperimentalOptions.getExperimentValue(pipelineOptions, BIGTABLE_WRITER_WAIT_TIMEOUT_MS);
      Duration closeWaitTimeout = null;
      if (closeWaitTimeoutStr != null) {
        long closeWaitTimeoutMs = Long.parseLong(closeWaitTimeoutStr);
        checkState(closeWaitTimeoutMs > 0, "Close wait timeout must be positive");
        closeWaitTimeout = Duration.millis(closeWaitTimeoutMs);
      }

      PCollectionTuple results =
          input.apply(
              ParDo.of(
                      new BigtableWriterFn(
                          factory,
                          bigtableConfig,
                          bigtableWriteOptions
                              .toBuilder()
                              .setCloseWaitTimeout(closeWaitTimeout)
                              .build(),
                          input.getCoder(),
                          badRecordRouter))
                  .withOutputTags(WRITE_RESULTS, TupleTagList.of(BAD_RECORD_TAG)));

      badRecordErrorHandler.addErrorCollection(
          results.get(BAD_RECORD_TAG).setCoder(BadRecord.getCoder(input.getPipeline())));

      return results.get(WRITE_RESULTS);
    }

    @Override
    public void validate(PipelineOptions options) {
      validateTableExists(bigtableConfig, bigtableWriteOptions, options);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      bigtableConfig.populateDisplayData(builder);
      bigtableWriteOptions.populateDisplayData(builder);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(WriteWithResults.class)
          .add("config", bigtableConfig)
          .add("writeOptions", bigtableWriteOptions)
          .toString();
    }

    // Optimized by LLM: Extracted repeated logic for validating configurations
    private void validateTableExists(
        BigtableConfig config, BigtableWriteOptions writeOptions, PipelineOptions options) {
      if (config.getValidate() && config.isDataAccessible() && writeOptions.isDataAccessible()) {
        ValueProvider<String> tableIdProvider = checkArgumentNotNull(writeOptions.getTableId());
        String tableId = checkArgumentNotNull(tableIdProvider.get());
        try {
          boolean exists = factory.checkTableExists(config, options, tableId);
          checkArgument(exists, "Table %s does not exist", tableId);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private static class BigtableWriterFn
      extends DoFn<KV<ByteString, Iterable<Mutation>>, BigtableWriteResult> {

    private final BigtableServiceFactory factory;
    private final BigtableServiceFactory.ConfigId id;
    private final Coder<KV<ByteString, Iterable<Mutation>>> inputCoder;
    private final BadRecordRouter badRecordRouter;
    private transient ConcurrentLinkedQueue<KV<BigtableWriteException, BoundedWindow>> badRecords =
        null;
    private transient boolean reportedLineage;

    @Nullable private BigtableServiceEntry serviceEntry;

    private transient Queue<CompletableFuture<?>> outstandingWrites;

    BigtableWriterFn(
        BigtableServiceFactory factory,
        BigtableConfig bigtableConfig,
        BigtableWriteOptions writeOptions,
        Coder<KV<ByteString, Iterable<Mutation>>> inputCoder,
        BadRecordRouter badRecordRouter) {
      this.factory = factory;
      this.config = bigtableConfig;
      this.writeOptions = writeOptions;
      this.inputCoder = inputCoder;
      this.badRecordRouter = badRecordRouter;
      this.failures = new ConcurrentLinkedQueue<>();
      this.id = factory.newId();
      LOG.debug("Created Bigtable Write Fn with writeOptions {} ", writeOptions);
    }

    @StartBundle
    public void startBundle(StartBundleContext c) throws IOException {
      recordsWritten = 0;
      this.seenWindows = Maps.newHashMapWithExpectedSize(1);

      if (serviceEntry == null) {
        serviceEntry =
            factory.getServiceForWriting(id, config, writeOptions, c.getPipelineOptions());
      }

      if (bigtableWriter == null) {
        bigtableWriter = serviceEntry.getService().openForWriting(writeOptions);
      }

      badRecords = new ConcurrentLinkedQueue<>();
      outstandingWrites = new ArrayDeque<>();
    }

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
      drainCompletedElementFutures();
      checkForFailures();
      KV<ByteString, Iterable<Mutation>> record = c.element();
      
      ++recordsWritten;
      seenWindows.compute(window, (key, count) -> (count != null ? count : 0) + 1);
    }

    private void drainCompletedElementFutures() throws ExecutionException, InterruptedException {
      for (Future<?> f = outstandingWrites.peek();
          f != null && f.isDone();
          f = outstandingWrites.peek()) {
        outstandingWrites.remove().get();
      }
    }

    private BiFunction<MutateRowResponse, Throwable, Void> handleMutationException(
        KV<ByteString, Iterable<Mutation>> record, BoundedWindow window) {
      return (MutateRowResponse result, Throwable exception) -> {
        if (exception != null) {
          if (isDataException(exception)) {
            retryIndividualRecord(record, window);
          } else {
            failures.add(new BigtableWriteException(record, exception));
          }
        }
        return null;
      };
    }

    private void retryIndividualRecord(
        KV<ByteString, Iterable<Mutation>> record, BoundedWindow window) {
      try {
        bigtableWriter.writeSingleRecord(record);
      } catch (Throwable e) {
        if (isDataException(e)) {
          badRecords.add(KV.of(new BigtableWriteException(record, e), window));
        } else {
          failures.add(new BigtableWriteException(record, e));
        }
      }
    }

    private static boolean isDataException(Throwable e) {
      if (e instanceof ApiException && !((ApiException) e).isRetryable()) {
        return e instanceof NotFoundException || e instanceof InvalidArgumentException;
      }
      return false;
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) throws Exception {
      if (bigtableWriter != null) {
        try {
          bigtableWriter.close();
        } catch (IOException e) {
          if (!(e.getCause() instanceof BatchingException)) {
            throw e;
          }
        }

        try {
          CompletableFuture.allOf(outstandingWrites.toArray(new CompletableFuture<?>[0]))
              .get(1, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
          throw new IllegalStateException(
              "Unexpected timeout waiting for element future to resolve after the writer was closed",
              e);
        }

        if (!reportedLineage) {
          bigtableWriter.reportLineage();
          reportedLineage = true;
        }
        bigtableWriter = null;
      }

      for (KV<BigtableWriteException, BoundedWindow> badRecord : badRecords) {
        try {
          badRecordRouter.route(
              c,
              badRecord.getKey().getRecord(),
              inputCoder,
              (Exception) badRecord.getKey().getCause(),
              "Failed to write malformed mutation to Bigtable",
              badRecord.getValue());
        } catch (Exception e) {
          failures.add(badRecord.getKey());
        }
      }

      checkForFailures();

      LOG.debug("Wrote {} records", recordsWritten);

      for (Map.Entry<BoundedWindow, Long> entry : seenWindows.entrySet()) {
        c.output(
            BigtableWriteResult.create(entry.getValue()),
            entry.getKey().maxTimestamp(),
            entry.getKey());
      }
    }

    @Teardown
    public void tearDown() throws IOException {
      try {
        if (bigtableWriter != null) {
          bigtableWriter.close();
          bigtableWriter = null;
        }
      } finally {
        if (serviceEntry != null) {
          serviceEntry.close();
          serviceEntry = null;
        }
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      config.populateDisplayData(builder);
    }

    private final BigtableConfig config;
    private final BigtableWriteOptions writeOptions;
    private BigtableService.Writer bigtableWriter;
    private long recordsWritten;
    private final ConcurrentLinkedQueue<BigtableWriteException> failures;
    private Map<BoundedWindow, Long> seenWindows;

    private void checkForFailures() throws IOException {

      if (failures.isEmpty()) {
        return;
      }

      StringBuilder logEntry = new StringBuilder();
      int i = 0;
      List<BigtableWriteException> suppressed = Lists.newArrayList();
      for (; i < 10 && !failures.isEmpty(); ++i) {
        BigtableWriteException exc = failures.remove();
        logEntry.append("\n").append(exc.getMessage());
        if (exc.getCause() != null) {
          logEntry.append(": ").append(exc.getCause().getMessage());
        }
        suppressed.add(exc);
      }
      String message =
          String.format(
              "At least %d errors occurred writing to Bigtable. First %d errors: %s",
              i + failures.size(), i, logEntry);
      LOG.error(message);
      IOException exception = new IOException(message);
      for (BigtableWriteException e : suppressed) {
        exception.addSuppressed(e);
      }
      throw exception;
    }
  }

  private BigtableIO() {}

  private static ByteKey makeByteKey(ByteString key) {
    return ByteKey.copyFrom(key.asReadOnlyByteBuffer());
  }

  static class BigtableSource extends BoundedSource<Row> {
    public BigtableSource(
        BigtableServiceFactory factory,
        BigtableServiceFactory.ConfigId configId,
        BigtableConfig config,
        BigtableReadOptions readOptions,
        @Nullable Long estimatedSizeBytes) {
      this.factory = factory;
      this.configId = configId;
      this.config = config;
      this.readOptions = readOptions;
      this.estimatedSizeBytes = estimatedSizeBytes;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(BigtableSource.class)
          .add("config", config)
          .add("readOptions", readOptions)
          .add("estimatedSizeBytes", estimatedSizeBytes)
          .toString();
    }

    private final BigtableConfig config;
    private final BigtableReadOptions readOptions;
    private @Nullable Long estimatedSizeBytes;
    private transient boolean reportedLineage;

    private final BigtableServiceFactory.ConfigId configId;

    private final BigtableServiceFactory factory;

    protected BigtableSource withSingleRange(ByteKeyRange range) {
      checkArgument(range != null, "range can not be null");
      return new BigtableSource(
          factory, configId, config, readOptions.withKeyRange(range), estimatedSizeBytes);
    }

    protected BigtableSource withEstimatedSizeBytes(Long estimatedSizeBytes) {
      checkArgument(estimatedSizeBytes != null, "estimatedSizeBytes can not be null");
      return new BigtableSource(factory, configId, config, readOptions, estimatedSizeBytes);
    }
    private List<KeyOffset> getSampleRowKeys(PipelineOptions pipelineOptions) throws IOException {
      try (BigtableServiceFactory.BigtableServiceEntry serviceEntry =
          factory.getServiceForReading(configId, config, readOptions, pipelineOptions)) {
        return serviceEntry.getService().getSampleRowKeys(this);
      }
    }

    private static final long MAX_SPLIT_COUNT = 15_360L;

    @Override
    public List<BigtableSource> split(long desiredBundleSizeBytes, PipelineOptions options)
        throws Exception {
      long maximumNumberOfSplits = 4000;
      long sizeEstimate = getEstimatedSizeBytes(options);
      desiredBundleSizeBytes =
          Math.max(
              sizeEstimate / maximumNumberOfSplits,
              Math.max(1, desiredBundleSizeBytes));

      List<BigtableSource> splits =
          splitBasedOnSamples(desiredBundleSizeBytes, getSampleRowKeys(options));

      List<BigtableSource> reduced = reduceSplits(splits, options, MAX_SPLIT_COUNT);
      Collections.shuffle(reduced);
      return ImmutableList.copyOf(reduced);
    }

    // Optimized by LLM: Extracted repeated logic for validating configurations
    private void validateConfig() {
      if (!config.getValidate()) {
        LOG.debug("Validation is disabled");
        return;
      }

      ValueProvider<String> tableId = readOptions.getTableId();
      checkArgument(
          tableId != null && tableId.isAccessible() && !tableId.get().isEmpty(),
          "tableId was not supplied");
    }

    @VisibleForTesting
    protected List<BigtableSource> reduceSplits(
        List<BigtableSource> splits, PipelineOptions options, long maxSplitCounts)
        throws IOException {
      int numberToCombine = (int) ((splits.size() + maxSplitCounts - 1) / maxSplitCounts);
      if (splits.size() < maxSplitCounts || numberToCombine < 2) {
        return new ArrayList<>(splits);
      }
      List<BigtableSource> reducedSplits = new ArrayList<>();
      List<ByteKeyRange> previousSourceRanges = new ArrayList<>();
      int counter = 0;
      long size = 0;
      for (BigtableSource source : splits) {
        if (counter == numberToCombine
            || !checkRangeAdjacency(previousSourceRanges, source.getRanges())) {
          reducedSplits.add(
              new BigtableSource(
                  factory,
                  configId,
                  config,
                  readOptions.withKeyRanges(previousSourceRanges),
                  size));
          counter = 0;
          size = 0;
          previousSourceRanges = new ArrayList<>();
        }
        previousSourceRanges.addAll(source.getRanges());
        previousSourceRanges = mergeRanges(previousSourceRanges);
        size += source.getEstimatedSizeBytes(options);
        counter++;
      }
      if (size > 0) {
        reducedSplits.add(
            new BigtableSource(
                factory, configId, config, readOptions.withKeyRanges(previousSourceRanges), size));
      }
      return reducedSplits;
    }

    private static boolean checkRangeAdjacency(
        List<ByteKeyRange> ranges, List<ByteKeyRange> otherRanges) {
      checkArgument(ranges != null || otherRanges != null, "Both ranges cannot be null.");
      ImmutableList.Builder<ByteKeyRange> mergedRanges = ImmutableList.builder();
      if (ranges != null) {
        mergedRanges.addAll(ranges);
      }
      if (otherRanges != null) {
        mergedRanges.addAll(otherRanges);
      }
      return checkRangeAdjacency(mergedRanges.build());
    }

    private static boolean checkRangeAdjacency(List<ByteKeyRange> ranges) {
      int index = 0;
      if (ranges.size() < 2) {
        return true;
      }
      ByteKey lastEndKey = ranges.get(index++).getEndKey();
      while (index < ranges.size()) {
        ByteKeyRange currentKeyRange = ranges.get(index++);
        if (!lastEndKey.equals(currentKeyRange.getStartKey())) {
          return false;
        }
        lastEndKey = currentKeyRange.getEndKey();
      }
      return true;
    }

    private static List<ByteKeyRange> mergeRanges(List<ByteKeyRange> ranges) {
      List<ByteKeyRange> response = new ArrayList<>();
      if (ranges.size() < 2) {
        response.add(ranges.get(0));
      } else {
        response.add(
            ByteKeyRange.of(
                ranges.get(0).getStartKey(), ranges.get(ranges.size() - 1).getEndKey()));
      }
      return response;
    }

    private List<BigtableSource> splitBasedOnSamples(
        long desiredBundleSizeBytes, List<KeyOffset> sampleRowKeys) {
      if (sampleRowKeys.isEmpty()) {
        LOG.info("Not splitting source {} because no sample row keys are available.", this);
        return Collections.singletonList(this);
      }
      LOG.info(
          "About to split into bundles of size {} with sampleRowKeys length {} first element {}",
          desiredBundleSizeBytes,
          sampleRowKeys.size(),
          sampleRowKeys.get(0));

      ImmutableList.Builder<BigtableSource> splits = ImmutableList.builder();
      for (ByteKeyRange range : getRanges()) {
        splits.addAll(splitRangeBasedOnSamples(desiredBundleSizeBytes, sampleRowKeys, range));
      }
      return splits.build();
    }

    private List<BigtableSource> splitRangeBasedOnSamples(
        long desiredBundleSizeBytes, List<KeyOffset> sampleRowKeys, ByteKeyRange range) {

      ByteKey lastEndKey = ByteKey.EMPTY;
      long lastOffset = 0;
      ImmutableList.Builder<BigtableSource> splits = ImmutableList.builder();
      for (KeyOffset keyOffset : sampleRowKeys) {
        ByteKey responseEndKey = makeByteKey(keyOffset.getKey());
        long responseOffset = keyOffset.getOffsetBytes();
        checkState(
            responseOffset >= lastOffset,
            "Expected response byte offset %s to come after the last offset %s",
            responseOffset,
            lastOffset);

        if (!range.overlaps(ByteKeyRange.of(lastEndKey, responseEndKey))) {
          lastOffset = responseOffset;
          lastEndKey = responseEndKey;
          continue;
        }

        ByteKey splitStartKey = lastEndKey;
        if (splitStartKey.compareTo(range.getStartKey()) < 0) {
          splitStartKey = range.getStartKey();
        }

        ByteKey splitEndKey = responseEndKey;
        if (!range.containsKey(splitEndKey)) {
          splitEndKey = range.getEndKey();
        }

        long sampleSizeBytes = responseOffset - lastOffset;
        List<BigtableSource> subSplits =
            splitKeyRangeIntoBundleSizedSubranges(
                sampleSizeBytes,
                desiredBundleSizeBytes,
                ByteKeyRange.of(splitStartKey, splitEndKey));
        splits.addAll(subSplits);

        lastEndKey = responseEndKey;
        lastOffset = responseOffset;
      }

      if (!lastEndKey.isEmpty()
          && (range.getEndKey().isEmpty() || lastEndKey.compareTo(range.getEndKey()) < 0)) {
        splits.add(this.withSingleRange(ByteKeyRange.of(lastEndKey, range.getEndKey())));
      }

      List<BigtableSource> ret = splits.build();
      LOG.info("Generated {} splits. First split: {}", ret.size(), ret.get(0));
      return ret;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
      if (estimatedSizeBytes == null) {
        estimatedSizeBytes = getEstimatedSizeBytesBasedOnSamples(getSampleRowKeys(options));
      }
      return estimatedSizeBytes;
    }

    private long getEstimatedSizeBytesBasedOnSamples(List<KeyOffset> samples) {
      long estimatedSizeBytes = 0;
      long lastOffset = 0;
      ByteKey currentStartKey = ByteKey.EMPTY;
      for (KeyOffset keyOffset : samples) {
        ByteKey currentEndKey = makeByteKey(keyOffset.getKey());
        long currentOffset = keyOffset.getOffsetBytes();
        if (!currentStartKey.isEmpty() && currentStartKey.equals(currentEndKey)) {
          lastOffset = currentOffset;
          continue;
        } else {
          for (ByteKeyRange range : getRanges()) {
            if (range.overlaps(ByteKeyRange.of(currentStartKey, currentEndKey))) {
              estimatedSizeBytes += currentOffset - lastOffset;
              break;
            }
          }
        }
        currentStartKey = currentEndKey;
        lastOffset = currentOffset;
      }
      return estimatedSizeBytes;
    }

    @Override
    public BoundedReader<Row> createReader(PipelineOptions options) throws IOException {
      return new BigtableReader(
          this, factory.getServiceForReading(configId, config, readOptions, options));
    }

    @Override
    public void validate() {
      validateConfig();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      builder.add(DisplayData.item("tableId", readOptions.getTableId()).withLabel("Table ID"));

      if (getRowFilter() != null) {
        builder.add(
            DisplayData.item("rowFilter", getRowFilter().toString()).withLabel("Table Row Filter"));
      }
    }

    @Override
    public Coder<Row> getOutputCoder() {
      return ProtoCoder.of(Row.class);
    }

    private List<BigtableSource> splitKeyRangeIntoBundleSizedSubranges(
        long sampleSizeBytes, long desiredBundleSizeBytes, ByteKeyRange range) {
      LOG.debug(
          "Subsplit for sampleSizeBytes {} and desiredBundleSizeBytes {}",
          sampleSizeBytes,
          desiredBundleSizeBytes);
      if (sampleSizeBytes <= desiredBundleSizeBytes) {
        return Collections.singletonList(
            this.withSingleRange(ByteKeyRange.of(range.getStartKey(), range.getEndKey())));
      }

      checkArgument(
          sampleSizeBytes > 0, "Sample size %s bytes must be greater than 0.", sampleSizeBytes);
      checkArgument(
          desiredBundleSizeBytes > 0,
          "Desired bundle size %s bytes must be greater than 0.",
          desiredBundleSizeBytes);

      int splitCount = (int) Math.ceil(((double) sampleSizeBytes) / desiredBundleSizeBytes);
      List<ByteKey> splitKeys = range.split(splitCount);
      ImmutableList.Builder<BigtableSource> splits = ImmutableList.builder();
      Iterator<ByteKey> keys = splitKeys.iterator();
      ByteKey prev = keys.next();
      while (keys.hasNext()) {
        ByteKey next = keys.next();
        splits.add(
            this.withSingleRange(ByteKeyRange.of(prev, next))
                .withEstimatedSizeBytes(sampleSizeBytes / splitCount));
        prev = next;
      }
      return splits.build();
    }

    public BigtableReadOptions getReadOptions() {
      return readOptions;
    }

    public List<ByteKeyRange> getRanges() {
      return readOptions.getKeyRanges().get();
    }

    public @Nullable RowFilter getRowFilter() {
      ValueProvider<RowFilter> rowFilter = readOptions.getRowFilter();
      return rowFilter != null && rowFilter.isAccessible() ? rowFilter.get() : null;
    }

    public @Nullable Integer getMaxBufferElementCount() {
      return readOptions.getMaxBufferElementCount();
    }

    public ValueProvider<String> getTableId() {
      return readOptions.getTableId();
    }

    void reportLineageOnce(BigtableService.Reader reader) {
      if (!reportedLineage) {
        reader.reportLineage();
        reportedLineage = true;
      }
    }
  }

  private static class BigtableReader extends BoundedReader<Row> {
    private BigtableSource source;

    @Nullable private BigtableServiceEntry serviceEntry;
    private BigtableService.Reader reader;
    private final ByteKeyRangeTracker rangeTracker;
    private long recordsReturned;

    public BigtableReader(BigtableSource source, BigtableServiceEntry service) {
      checkArgument(source.getRanges().size() == 1, "source must have exactly one key range");
      this.source = source;
      this.serviceEntry = service;
      rangeTracker = ByteKeyRangeTracker.of(source.getRanges().get(0));
    }

    @Override
    public boolean start() throws IOException {
      reader = serviceEntry.getService().createReader(getCurrentSource());
      boolean hasRecord =
          (reader.start()
                  && rangeTracker.tryReturnRecordAt(
                      true, makeByteKey(reader.getCurrentRow().getKey())))
              || rangeTracker.markDone();
      if (hasRecord) {
        ++recordsReturned;
        source.reportLineageOnce(reader);
      }
      return hasRecord;
    }

    @Override
    public synchronized BigtableSource getCurrentSource() {
      return source;
    }

    @Override
    public boolean advance() throws IOException {
      boolean hasRecord =
          (reader.advance()
                  && rangeTracker.tryReturnRecordAt(
                      true, makeByteKey(reader.getCurrentRow().getKey())))
              || rangeTracker.markDone();
      if (hasRecord) {
        ++recordsReturned;
      }
      return hasRecord;
    }

    @Override
    public Row getCurrent() throws NoSuchElementException {
      return reader.getCurrentRow();
    }

    @Override
    public void close() {
      LOG.info("Closing reader after reading {} records.", recordsReturned);
      if (reader != null) {
        reader.close();
        reader = null;
      }
      if (serviceEntry != null) {
        serviceEntry.close();
        serviceEntry = null;
      }
    }

    @Override
    public final Double getFractionConsumed() {
      return rangeTracker.getFractionConsumed();
    }

    @Override
    public final long getSplitPointsConsumed() {
      return rangeTracker.getSplitPointsConsumed();
    }

    @Override
    public final @Nullable synchronized BigtableSource splitAtFraction(double fraction) {
      ByteKey splitKey;
      ByteKeyRange range = rangeTracker.getRange();
      try {
        splitKey = range.interpolateKey(fraction);
      } catch (RuntimeException e) {
        LOG.info("{}: Failed to interpolate key for fraction {}.", range, fraction, e);
        return null;
      }
      LOG.info("Proposing to split {} at fraction {} (key {})", rangeTracker, fraction, splitKey);
      BigtableSource primary;
      BigtableSource residual;
      try {
        primary = source.withSingleRange(ByteKeyRange.of(range.getStartKey(), splitKey));
        residual = source.withSingleRange(ByteKeyRange.of(splitKey, range.getEndKey()));
      } catch (RuntimeException e) {
        LOG.info(
            "{}: Interpolating for fraction {} yielded invalid split key {}.",
            rangeTracker.getRange(),
            fraction,
            splitKey,
            e);
        return null;
      }
      if (!rangeTracker.trySplitAtPosition(splitKey)) {
        return null;
      }
      this.source = primary;
      return residual;
    }
  }

  static class BigtableWriteException extends IOException {

    private final KV<ByteString, Iterable<Mutation>> record;

    public BigtableWriteException(KV<ByteString, Iterable<Mutation>> record, Throwable cause) {
      super(
          String.format(
              "Error mutating row %s with mutations %s",
              record.getKey().toStringUtf8(), StringUtils.leftTruncate(record.getValue(), 100)),
          cause);
      this.record = record;
    }

    public KV<ByteString, Iterable<Mutation>> getRecord() {
      return record;
    }
  }

  public enum ExistingPipelineOptions {
    FAIL_IF_EXISTS,
    RESUME_OR_NEW,
    RESUME_OR_FAIL,
    @VisibleForTesting
    SKIP_CLEANUP,
  }

  @AutoValue
  public abstract static class ReadChangeStream
      extends PTransform<PBegin, PCollection<KV<ByteString, ChangeStreamMutation>>> {

    private static final Duration DEFAULT_BACKLOG_REPLICATION_ADJUSTMENT =
        Duration.standardSeconds(30);

    static ReadChangeStream create() {
      BigtableConfig config = BigtableConfig.builder().setValidate(true).build();
      BigtableConfig metadataTableconfig = BigtableConfig.builder().setValidate(true).build();

      return new AutoValue_BigtableIO_ReadChangeStream.Builder()
          .setBigtableConfig(config)
          .setMetadataTableBigtableConfig(metadataTableconfig)
          .setValidateConfig(true)
          .build();
    }

    abstract BigtableConfig getBigtableConfig();

    abstract @Nullable String getTableId();

    abstract @Nullable Instant getStartTime();

    abstract @Nullable Instant getEndTime();

    abstract @Nullable String getChangeStreamName();

    abstract @Nullable ExistingPipelineOptions getExistingPipelineOptions();

    abstract BigtableConfig getMetadataTableBigtableConfig();

    abstract @Nullable String getMetadataTableId();

    abstract @Nullable Boolean getCreateOrUpdateMetadataTable();

    abstract @Nullable Duration getBacklogReplicationAdjustment();

    abstract @Nullable Boolean getValidateConfig();

    abstract ReadChangeStream.Builder toBuilder();

    public ReadChangeStream withProjectId(String projectId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder()
          .setBigtableConfig(config.withProjectId(StaticValueProvider.of(projectId)))
          .build();
    }

    public ReadChangeStream withInstanceId(String instanceId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder()
          .setBigtableConfig(config.withInstanceId(StaticValueProvider.of(instanceId)))
          .build();
    }
    public ReadChangeStream withTableId(String tableId) {
      return toBuilder().setTableId(tableId).build();
    }

    public ReadChangeStream withAppProfileId(String appProfileId) {
      BigtableConfig config = getBigtableConfig();
      return toBuilder()
          .setBigtableConfig(config.withAppProfileId(StaticValueProvider.of(appProfileId)))
          .build();
    }

    public ReadChangeStream withStartTime(Instant startTime) {
      return toBuilder().setStartTime(startTime).build();
    }

    @VisibleForTesting
    ReadChangeStream withEndTime(Instant endTime) {
      return toBuilder().setEndTime(endTime).build();
    }

    public ReadChangeStream withChangeStreamName(String changeStreamName) {
      return toBuilder().setChangeStreamName(changeStreamName).build();
    }

    public ReadChangeStream withExistingPipelineOptions(
        ExistingPipelineOptions existingPipelineOptions) {
      return toBuilder().setExistingPipelineOptions(existingPipelineOptions).build();
    }

    public ReadChangeStream withMetadataTableProjectId(String projectId) {
      BigtableConfig config = getMetadataTableBigtableConfig();
      return toBuilder()
          .setMetadataTableBigtableConfig(config.withProjectId(StaticValueProvider.of(projectId)))
          .build();
    }

    public ReadChangeStream withMetadataTableInstanceId(String instanceId) {
      BigtableConfig config = getMetadataTableBigtableConfig();
      return toBuilder()
          .setMetadataTableBigtableConfig(config.withInstanceId(StaticValueProvider.of(instanceId)))
          .build();
    }

    public ReadChangeStream withMetadataTableTableId(String tableId) {
      return toBuilder().setMetadataTableId(tableId).build();
    }

    public ReadChangeStream withMetadataTableAppProfileId(String appProfileId) {
      BigtableConfig config = getMetadataTableBigtableConfig();
      return toBuilder()
          .setMetadataTableBigtableConfig(
              config.withAppProfileId(StaticValueProvider.of(appProfileId)))
          .build();
    }

    @VisibleForTesting
    ReadChangeStream withBigtableClientOverride(BigtableClientOverride clientOverride) {
      BigtableConfig config = getBigtableConfig();
      BigtableConfig metadataTableConfig = getMetadataTableBigtableConfig();
      return toBuilder()
          .setBigtableConfig(config.withBigtableClientOverride(clientOverride))
          .setMetadataTableBigtableConfig(
              metadataTableConfig.withBigtableClientOverride(clientOverride))
          .build();
    }

    public ReadChangeStream withCreateOrUpdateMetadataTable(boolean shouldCreate) {
      return toBuilder().setCreateOrUpdateMetadataTable(shouldCreate).build();
    }

    public ReadChangeStream withBacklogReplicationAdjustment(Duration adjustment) {
      return toBuilder().setBacklogReplicationAdjustment(adjustment).build();
    }

    public ReadChangeStream withoutValidation() {
      BigtableConfig config = getBigtableConfig();
      BigtableConfig metadataTableConfig = getMetadataTableBigtableConfig();
      return toBuilder()
          .setBigtableConfig(config.withValidate(false))
          .setMetadataTableBigtableConfig(metadataTableConfig.withValidate(false))
          .setValidateConfig(false)
          .build();
    }

    @Override
    public void validate(PipelineOptions options) {
      if (getBigtableConfig().getValidate()) {
        try (BigtableChangeStreamAccessor bigtableChangeStreamAccessor =
            BigtableChangeStreamAccessor.getOrCreate(getBigtableConfig())) {
          checkArgument(
              bigtableChangeStreamAccessor.getTableAdminClient().exists(getTableId()),
              "Change Stream table %s does not exist",
              getTableId());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    private void validateAppProfile(
        MetadataTableAdminDao metadataTableAdminDao, String appProfileId) {
      checkArgument(metadataTableAdminDao != null);
      checkArgument(
          metadataTableAdminDao.isAppProfileSingleClusterAndTransactional(appProfileId),
          "App profile id '"
              + appProfileId
              + "' provided to access metadata table needs to use single-cluster routing policy"
              + " and allow single-row transactions.");
    }

    private void createOrUpdateMetadataTable(
        MetadataTableAdminDao metadataTableAdminDao, String metadataTableId) {
      boolean shouldCreateOrUpdateMetadataTable = true;
      if (getCreateOrUpdateMetadataTable() != null) {
        shouldCreateOrUpdateMetadataTable = getCreateOrUpdateMetadataTable();
      }
      if (shouldCreateOrUpdateMetadataTable && metadataTableAdminDao.createMetadataTable()) {
          LOG.info("Created metadata table: {}", metadataTableId);
      }
    }

    @Override
    public PCollection<KV<ByteString, ChangeStreamMutation>> expand(PBegin input) {
      BigtableConfig bigtableConfig = getBigtableConfig();
      checkArgument(
          bigtableConfig != null,
          "BigtableIO ReadChangeStream is missing required configurations fields.");
      bigtableConfig.validate();
      checkArgument(getTableId() != null, "Missing required tableId field.");

      if (bigtableConfig.getAppProfileId() == null
          || bigtableConfig.getAppProfileId().get().isEmpty()) {
        bigtableConfig = bigtableConfig.withAppProfileId(StaticValueProvider.of("default"));
      }

      BigtableConfig metadataTableConfig = getMetadataTableBigtableConfig();
      if (metadataTableConfig.getProjectId() == null
          || metadataTableConfig.getProjectId().get().isEmpty()) {
        metadataTableConfig = metadataTableConfig.withProjectId(bigtableConfig.getProjectId());
      }
      if (metadataTableConfig.getInstanceId() == null
          || metadataTableConfig.getInstanceId().get().isEmpty()) {
        metadataTableConfig = metadataTableConfig.withInstanceId(bigtableConfig.getInstanceId());
      }
      String metadataTableId = getMetadataTableId();
      if (metadataTableId == null || metadataTableId.isEmpty()) {
        metadataTableId = MetadataTableAdminDao.DEFAULT_METADATA_TABLE_NAME;
      }
      if (metadataTableConfig.getAppProfileId() == null
          || metadataTableConfig.getAppProfileId().get().isEmpty()) {
        metadataTableConfig =
            metadataTableConfig.withAppProfileId(bigtableConfig.getAppProfileId());
      }

      Instant startTime = getStartTime();
      if (startTime == null) {
        startTime = Instant.now();
      }
      String changeStreamName = getChangeStreamName();
      if (changeStreamName == null || changeStreamName.isEmpty()) {
        changeStreamName = UniqueIdGenerator.generateRowKeyPrefix();
      }
      ExistingPipelineOptions existingPipelineOptions = getExistingPipelineOptions();
      if (existingPipelineOptions == null) {
        existingPipelineOptions = ExistingPipelineOptions.FAIL_IF_EXISTS;
      }

      Duration backlogReplicationAdjustment = getBacklogReplicationAdjustment();
      if (backlogReplicationAdjustment == null) {
        backlogReplicationAdjustment = DEFAULT_BACKLOG_REPLICATION_ADJUSTMENT;
      }

      ActionFactory actionFactory = new ActionFactory();
      ChangeStreamMetrics metrics = new ChangeStreamMetrics();
      DaoFactory daoFactory =
          new DaoFactory(
              bigtableConfig, metadataTableConfig, getTableId(), metadataTableId, changeStreamName);

      try {
        MetadataTableAdminDao metadataTableAdminDao = daoFactory.getMetadataTableAdminDao();
        boolean validateConfig = true;
        if (getValidateConfig() != null) {
          validateConfig = getValidateConfig();
        }
        if (validateConfig) {
          createOrUpdateMetadataTable(metadataTableAdminDao, metadataTableId);
          validateAppProfile(metadataTableAdminDao, metadataTableConfig.getAppProfileId().get());
        }
        if (metadataTableConfig.getValidate()) {
          checkArgument(
              metadataTableAdminDao.doesMetadataTableExist(),
              "Metadata table does not exist: " + metadataTableAdminDao.getTableId());
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        daoFactory.close();
      }

      InitializeDoFn initializeDoFn =
          new InitializeDoFn(daoFactory, startTime, existingPipelineOptions);
      DetectNewPartitionsDoFn detectNewPartitionsDoFn =
          new DetectNewPartitionsDoFn(getEndTime(), actionFactory, daoFactory, metrics);
      ReadChangeStreamPartitionDoFn readChangeStreamPartitionDoFn =
          new ReadChangeStreamPartitionDoFn(
              daoFactory, actionFactory, metrics, backlogReplicationAdjustment);

      PCollection<KV<ByteString, ChangeStreamRecord>> readChangeStreamOutput =
          input
              .apply(Impulse.create())
              .apply("Initialize", ParDo.of(initializeDoFn))
              .apply("DetectNewPartition", ParDo.of(detectNewPartitionsDoFn))
              .apply("ReadChangeStreamPartition", ParDo.of(readChangeStreamPartitionDoFn));

      Coder<KV<ByteString, ChangeStreamRecord>> outputCoder = readChangeStreamOutput.getCoder();
      CoderSizeEstimator<KV<ByteString, ChangeStreamRecord>> sizeEstimator =
          new CoderSizeEstimator<>(outputCoder);
      readChangeStreamPartitionDoFn.setSizeEstimator(sizeEstimator);

      return readChangeStreamOutput.apply(
          "FilterForMutation", ParDo.of(new FilterForMutationDoFn()));
    }

    @AutoValue.Builder
    abstract static class Builder {

      abstract ReadChangeStream.Builder setBigtableConfig(BigtableConfig bigtableConfig);

      abstract ReadChangeStream.Builder setTableId(String tableId);

      abstract ReadChangeStream.Builder setMetadataTableBigtableConfig(
          BigtableConfig bigtableConfig);

      abstract ReadChangeStream.Builder setMetadataTableId(String metadataTableId);

      abstract ReadChangeStream.Builder setStartTime(Instant startTime);

      abstract ReadChangeStream.Builder setEndTime(Instant endTime);

      abstract ReadChangeStream.Builder setChangeStreamName(String changeStreamName);

      abstract ReadChangeStream.Builder setExistingPipelineOptions(
          ExistingPipelineOptions existingPipelineOptions);

      abstract ReadChangeStream.Builder setCreateOrUpdateMetadataTable(boolean shouldCreate);

      abstract ReadChangeStream.Builder setBacklogReplicationAdjustment(Duration adjustment);

      abstract ReadChangeStream.Builder setValidateConfig(boolean validateConfig);

      abstract ReadChangeStream build();
    }
  }

  public static boolean createOrUpdateReadChangeStreamMetadataTable(
      String projectId, String instanceId, @Nullable String tableId) throws IOException {
    BigtableConfig bigtableConfig =
        BigtableConfig.builder()
            .setValidate(true)
            .setProjectId(StaticValueProvider.of(projectId))
            .setInstanceId(StaticValueProvider.of(instanceId))
            .setAppProfileId(
                StaticValueProvider.of(
                    "default"))
            .build();

    if (tableId == null || tableId.isEmpty()) {
      tableId = MetadataTableAdminDao.DEFAULT_METADATA_TABLE_NAME;
    }

    DaoFactory daoFactory = new DaoFactory(null, bigtableConfig, null, tableId, null);

    try {
      MetadataTableAdminDao metadataTableAdminDao = daoFactory.getMetadataTableAdminDao();

      if (metadataTableAdminDao.createMetadataTable()) {
          LOG.info("Created metadata table: {}", metadataTableAdminDao.getTableId());
      }
      return metadataTableAdminDao.doesMetadataTableExist();
    } finally {
      daoFactory.close();
    }
  }
}