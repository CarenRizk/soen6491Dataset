package org.apache.beam.sdk.io.gcp.spanner;

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.gcp.spanner.MutationUtils.isPointDelete;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants.DEFAULT_CHANGE_STREAM_NAME;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants.DEFAULT_INCLUSIVE_END_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants.DEFAULT_INCLUSIVE_START_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants.DEFAULT_RPC_PRIORITY;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants.MAX_INCLUSIVE_END_AT;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants.THROUGHPUT_WINDOW_SECONDS;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.NameGenerator.generatePartitionMetadataTableName;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.auth.Credentials;
import com.google.auto.value.AutoValue;
import com.google.cloud.ServiceFactory;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.Op;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.ServiceCallMetric;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamsConstants;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.MetadataSpannerConfigFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.ActionFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.CleanUpReadChangeStreamDoFn;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.DetectNewPartitionsDoFn;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.InitializeDoFn;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.PostProcessingMetricsDoFn;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.ReadChangeStreamPartitionDoFn;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.estimator.BytesThroughputEstimator;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.estimator.SizeEstimator;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.MapperFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Stopwatch;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedBytes;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({
  "nullness" 
})
public class SpannerIO {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerIO.class);

  private static final long DEFAULT_BATCH_SIZE_BYTES = 1024L * 1024L; 
  
  private static final int DEFAULT_MAX_NUM_MUTATIONS = 5000;
  
  private static final int DEFAULT_MAX_NUM_ROWS = 500;
  
  private static final int DEFAULT_GROUPING_FACTOR = 1000;

  
  
  
  
  static final int METRICS_CACHE_SIZE = 100;

  
  public static Read read() {
    return new AutoValue_SpannerIO_Read.Builder()
        .setSpannerConfig(SpannerConfig.create())
        .setTimestampBound(TimestampBound.strong())
        .setReadOperation(ReadOperation.create())
        .setBatching(true)
        .build();
  }

  
  public static ReadAll readAll() {
    return new AutoValue_SpannerIO_ReadAll.Builder()
        .setSpannerConfig(SpannerConfig.create())
        .setTimestampBound(TimestampBound.strong())
        .setBatching(true)
        .build();
  }

  
  public static CreateTransaction createTransaction() {
    return new AutoValue_SpannerIO_CreateTransaction.Builder()
        .setSpannerConfig(SpannerConfig.create())
        .setTimestampBound(TimestampBound.strong())
        .build();
  }

  
  public static Write write() {
    return new AutoValue_SpannerIO_Write.Builder()
        .setSpannerConfig(SpannerConfig.create())
        .setBatchSizeBytes(DEFAULT_BATCH_SIZE_BYTES)
        .setMaxNumMutations(DEFAULT_MAX_NUM_MUTATIONS)
        .setMaxNumRows(DEFAULT_MAX_NUM_ROWS)
        .setFailureMode(FailureMode.FAIL_FAST)
        .build();
  }

  
  public static ReadChangeStream readChangeStream() {
    return new AutoValue_SpannerIO_ReadChangeStream.Builder()
        .setSpannerConfig(SpannerConfig.create())
        .setChangeStreamName(DEFAULT_CHANGE_STREAM_NAME)
        .setRpcPriority(DEFAULT_RPC_PRIORITY)
        .setInclusiveStartAt(DEFAULT_INCLUSIVE_START_AT)
        .setInclusiveEndAt(DEFAULT_INCLUSIVE_END_AT)
        .build();
  }

  
  @AutoValue
  public abstract static class ReadAll
      extends PTransform<PCollection<ReadOperation>, PCollection<Struct>> {

    abstract SpannerConfig getSpannerConfig();

    abstract @Nullable PCollectionView<Transaction> getTransaction();

    abstract @Nullable TimestampBound getTimestampBound();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

      abstract Builder setTransaction(PCollectionView<Transaction> transaction);

      abstract Builder setTimestampBound(TimestampBound timestampBound);

      abstract Builder setBatching(Boolean batching);

      abstract ReadAll build();
    }

    
    public ReadAll withSpannerConfig(SpannerConfig spannerConfig) {
      return toBuilder().setSpannerConfig(spannerConfig).build();
    }

    
    public ReadAll withProjectId(String projectId) {
      return withProjectId(ValueProvider.StaticValueProvider.of(projectId));
    }

    
    public ReadAll withProjectId(ValueProvider<String> projectId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withProjectId(projectId));
    }

    
    public ReadAll withInstanceId(String instanceId) {
      return withInstanceId(ValueProvider.StaticValueProvider.of(instanceId));
    }

    
    public ReadAll withInstanceId(ValueProvider<String> instanceId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withInstanceId(instanceId));
    }

    
    public ReadAll withDatabaseId(String databaseId) {
      return withDatabaseId(ValueProvider.StaticValueProvider.of(databaseId));
    }

    
    public ReadAll withHost(ValueProvider<String> host) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withHost(host));
    }

    public ReadAll withHost(String host) {
      return withHost(ValueProvider.StaticValueProvider.of(host));
    }

    
    public ReadAll withEmulatorHost(ValueProvider<String> emulatorHost) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withEmulatorHost(emulatorHost));
    }

    public ReadAll withEmulatorHost(String emulatorHost) {
      return withEmulatorHost(ValueProvider.StaticValueProvider.of(emulatorHost));
    }

    
    public ReadAll withDatabaseId(ValueProvider<String> databaseId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withDatabaseId(databaseId));
    }

    @VisibleForTesting
    ReadAll withServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withServiceFactory(serviceFactory));
    }

    public ReadAll withTransaction(PCollectionView<Transaction> transaction) {
      return toBuilder().setTransaction(transaction).build();
    }

    public ReadAll withTimestamp(Timestamp timestamp) {
      return withTimestampBound(TimestampBound.ofReadTimestamp(timestamp));
    }

    public ReadAll withTimestampBound(TimestampBound timestampBound) {
      return toBuilder().setTimestampBound(timestampBound).build();
    }

    
    public ReadAll withBatching(boolean batching) {
      return toBuilder().setBatching(batching).build();
    }

    public ReadAll withLowPriority() {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withRpcPriority(RpcPriority.LOW));
    }

    public ReadAll withHighPriority() {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withRpcPriority(RpcPriority.HIGH));
    }

    abstract Boolean getBatching();

    @Override
    public PCollection<Struct> expand(PCollection<ReadOperation> input) {

      if (PCollection.IsBounded.UNBOUNDED == input.isBounded()) {
        
        LOG.warn(
            "SpannerIO.ReadAll({}) is being applied to an unbounded input. "
                + "This is not supported and can lead to runtime failures.",
            this.getName());
      }

      PTransform<PCollection<ReadOperation>, PCollection<Struct>> readTransform;
      if (getBatching()) {
        readTransform =
            BatchSpannerRead.create(getSpannerConfig(), getTransaction(), getTimestampBound());
      } else {
        readTransform =
            NaiveSpannerRead.create(getSpannerConfig(), getTransaction(), getTimestampBound());
      }
      return input
          .apply("Reshuffle", Reshuffle.viaRandomKey())
          .apply("Read from Cloud Spanner", readTransform);
    }

    
    static ServiceCallMetric buildServiceCallMetricForReadOp(
        SpannerConfig config, ReadOperation op) {

      HashMap<String, String> baseLabels = buildServiceCallMetricLabels(config);
      baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "Read");

      if (op.getQuery() != null) {
        String queryName = op.getQueryName();
        if (queryName == null || queryName.isEmpty()) {
          
          queryName = String.format("UNNAMED_QUERY#%08x", op.getQuery().getSql().hashCode());
        }

        baseLabels.put(
            MonitoringInfoConstants.Labels.RESOURCE,
            GcpResourceIdentifiers.spannerQuery(
                baseLabels.get(MonitoringInfoConstants.Labels.SPANNER_PROJECT_ID),
                config.getInstanceId().get(),
                config.getDatabaseId().get(),
                queryName));
        baseLabels.put(MonitoringInfoConstants.Labels.SPANNER_QUERY_NAME, queryName);
      } else {
        baseLabels.put(
            MonitoringInfoConstants.Labels.RESOURCE,
            GcpResourceIdentifiers.spannerTable(
                baseLabels.get(MonitoringInfoConstants.Labels.SPANNER_PROJECT_ID),
                config.getInstanceId().get(),
                config.getDatabaseId().get(),
                op.getTable()));
        baseLabels.put(MonitoringInfoConstants.Labels.TABLE_ID, op.getTable());
      }
      return new ServiceCallMetric(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);
    }
  }

  
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<Struct>> {

    interface ToBeamRowFunction
        extends SerializableFunction<Schema, SerializableFunction<Struct, Row>> {}

    interface FromBeamRowFunction
        extends SerializableFunction<Schema, SerializableFunction<Row, Struct>> {}

    abstract SpannerConfig getSpannerConfig();

    abstract ReadOperation getReadOperation();

    abstract @Nullable TimestampBound getTimestampBound();

    abstract @Nullable PCollectionView<Transaction> getTransaction();

    abstract @Nullable PartitionOptions getPartitionOptions();

    abstract Boolean getBatching();

    abstract @Nullable TypeDescriptor<Struct> getTypeDescriptor();

    abstract @Nullable ToBeamRowFunction getToBeamRowFn();

    abstract @Nullable FromBeamRowFunction getFromBeamRowFn();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

      abstract Builder setReadOperation(ReadOperation readOperation);

      abstract Builder setTimestampBound(TimestampBound timestampBound);

      abstract Builder setTransaction(PCollectionView<Transaction> transaction);

      abstract Builder setPartitionOptions(PartitionOptions partitionOptions);

      abstract Builder setBatching(Boolean batching);

      abstract Builder setTypeDescriptor(TypeDescriptor<Struct> typeDescriptor);

      abstract Builder setToBeamRowFn(ToBeamRowFunction toRowFn);

      abstract Builder setFromBeamRowFn(FromBeamRowFunction fromRowFn);

      abstract Read build();
    }

    public Read withBeamRowConverters(
        TypeDescriptor<Struct> typeDescriptor,
        ToBeamRowFunction toRowFn,
        FromBeamRowFunction fromRowFn) {
      return toBuilder()
          .setTypeDescriptor(typeDescriptor)
          .setToBeamRowFn(toRowFn)
          .setFromBeamRowFn(fromRowFn)
          .build();
    }

    
    public Read withSpannerConfig(SpannerConfig spannerConfig) {
      return toBuilder().setSpannerConfig(spannerConfig).build();
    }

    
    public Read withProjectId(String projectId) {
      return withProjectId(ValueProvider.StaticValueProvider.of(projectId));
    }

    
    public Read withProjectId(ValueProvider<String> projectId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withProjectId(projectId));
    }

    
    public Read withInstanceId(String instanceId) {
      return withInstanceId(ValueProvider.StaticValueProvider.of(instanceId));
    }

    
    public Read withInstanceId(ValueProvider<String> instanceId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withInstanceId(instanceId));
    }

    
    public Read withDatabaseId(String databaseId) {
      return withDatabaseId(ValueProvider.StaticValueProvider.of(databaseId));
    }

    
    public Read withDatabaseId(ValueProvider<String> databaseId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withDatabaseId(databaseId));
    }

    
    public Read withHost(ValueProvider<String> host) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withHost(host));
    }

    public Read withHost(String host) {
      return withHost(ValueProvider.StaticValueProvider.of(host));
    }

    
    public Read withEmulatorHost(ValueProvider<String> emulatorHost) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withEmulatorHost(emulatorHost));
    }

    public Read withEmulatorHost(String emulatorHost) {
      return withEmulatorHost(ValueProvider.StaticValueProvider.of(emulatorHost));
    }

    
    public Read withBatching(boolean batching) {
      return toBuilder().setBatching(batching).build();
    }

    @VisibleForTesting
    Read withServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withServiceFactory(serviceFactory));
    }

    public Read withTransaction(PCollectionView<Transaction> transaction) {
      return toBuilder().setTransaction(transaction).build();
    }

    public Read withTimestamp(Timestamp timestamp) {
      return withTimestampBound(TimestampBound.ofReadTimestamp(timestamp));
    }

    public Read withTimestampBound(TimestampBound timestampBound) {
      return toBuilder().setTimestampBound(timestampBound).build();
    }

    public Read withTable(String table) {
      return withReadOperation(getReadOperation().withTable(table));
    }

    public Read withReadOperation(ReadOperation operation) {
      return toBuilder().setReadOperation(operation).build();
    }

    public Read withColumns(String... columns) {
      return withColumns(Arrays.asList(columns));
    }

    public Read withColumns(List<String> columns) {
      return withReadOperation(getReadOperation().withColumns(columns));
    }

    public Read withQuery(Statement statement) {
      return withReadOperation(getReadOperation().withQuery(statement));
    }

    public Read withQuery(String sql) {
      return withQuery(Statement.of(sql));
    }

    public Read withQueryName(String queryName) {
      return withReadOperation(getReadOperation().withQueryName(queryName));
    }

    public Read withKeySet(KeySet keySet) {
      return withReadOperation(getReadOperation().withKeySet(keySet));
    }

    public Read withIndex(String index) {
      return withReadOperation(getReadOperation().withIndex(index));
    }

    
    public Read withPartitionOptions(PartitionOptions partitionOptions) {
      return withReadOperation(getReadOperation().withPartitionOptions(partitionOptions));
    }

    public Read withLowPriority() {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withRpcPriority(RpcPriority.LOW));
    }

    public Read withHighPriority() {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withRpcPriority(RpcPriority.HIGH));
    }

    @Override
    public PCollection<Struct> expand(PBegin input) {
      getSpannerConfig().validate();
      checkArgument(
          getTimestampBound() != null,
          "SpannerIO.read() runs in a read only transaction and requires timestamp to be set "
              + "with withTimestampBound or withTimestamp method");

      if (getReadOperation().getQuery() != null) {
        
        if (getReadOperation().getTable() != null) {
          throw new IllegalArgumentException(
              "Both query and table cannot be specified at the same time for SpannerIO.read().");
        }
      } else if (getReadOperation().getTable() != null) {
        
        checkNotNull(
            getReadOperation().getColumns(),
            "For a read operation SpannerIO.read() requires a list of "
                + "columns to set with withColumns method");
        checkArgument(
            !getReadOperation().getColumns().isEmpty(),
            "For a read operation SpannerIO.read() requires a non-empty"
                + " list of columns to set with withColumns method");
      } else {
        throw new IllegalArgumentException(
            "SpannerIO.read() requires query OR table to set with withTable OR withQuery method.");
      }

      Schema beamSchema = null;

      ReadAll readAll =
          readAll()
              .withSpannerConfig(getSpannerConfig())
              .withTimestampBound(getTimestampBound())
              .withBatching(getBatching())
              .withTransaction(getTransaction());

      PCollection<Struct> rows =
          input.apply(Create.of(getReadOperation())).apply("Execute query", readAll);

      if (beamSchema != null) {
        rows.setSchema(
            beamSchema,
            getTypeDescriptor(),
            getToBeamRowFn().apply(beamSchema),
            getFromBeamRowFn().apply(beamSchema));
      }

      return rows;
    }

    SerializableFunction<Struct, Row> getFormatFn() {
      return input ->
          Row.withSchema(Schema.builder().addInt64Field("Key").build())
              .withFieldValue("Key", 3L)
              .build();
    }
  }

  static class ReadRows extends PTransform<PBegin, PCollection<Row>> {

    final Read read;
    final Schema schema;

    public ReadRows(Read read, Schema schema) {
      super("Read rows");
      this.read = read;
      this.schema = schema;
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      return input
          .apply(read)
          .apply(
              MapElements.into(TypeDescriptor.of(Row.class))
                  .via(
                      (SerializableFunction<Struct, Row>)
                          struct -> StructUtils.structToBeamRow(struct, schema)))
          .setRowSchema(schema);
    }
  }

  
  @AutoValue
  public abstract static class CreateTransaction
      extends PTransform<PInput, PCollectionView<Transaction>> {

    abstract SpannerConfig getSpannerConfig();

    abstract @Nullable TimestampBound getTimestampBound();

    abstract Builder toBuilder();

    @Override
    public PCollectionView<Transaction> expand(PInput input) {
      getSpannerConfig().validate();

      PCollection<?> collection = input.getPipeline().apply(Create.of(1));

      if (input instanceof PCollection) {
        collection = collection.apply(Wait.on((PCollection<?>) input));
      } else if (!(input instanceof PBegin)) {
        throw new RuntimeException("input must be PBegin or PCollection");
      }

      return collection
          .apply(
              "Create transaction",
              ParDo.of(new CreateTransactionFn(this.getSpannerConfig(), this.getTimestampBound())))
          .apply("As PCollectionView", View.asSingleton());
    }

    
    public CreateTransaction withSpannerConfig(SpannerConfig spannerConfig) {
      return toBuilder().setSpannerConfig(spannerConfig).build();
    }

    
    public CreateTransaction withProjectId(String projectId) {
      return withProjectId(ValueProvider.StaticValueProvider.of(projectId));
    }

    
    public CreateTransaction withProjectId(ValueProvider<String> projectId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withProjectId(projectId));
    }

    
    public CreateTransaction withInstanceId(String instanceId) {
      return withInstanceId(ValueProvider.StaticValueProvider.of(instanceId));
    }

    
    public CreateTransaction withInstanceId(ValueProvider<String> instanceId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withInstanceId(instanceId));
    }

    
    public CreateTransaction withDatabaseId(String databaseId) {
      return withDatabaseId(ValueProvider.StaticValueProvider.of(databaseId));
    }

    
    public CreateTransaction withDatabaseId(ValueProvider<String> databaseId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withDatabaseId(databaseId));
    }

    
    public CreateTransaction withHost(ValueProvider<String> host) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withHost(host));
    }

    public CreateTransaction withHost(String host) {
      return withHost(ValueProvider.StaticValueProvider.of(host));
    }

    
    public CreateTransaction withEmulatorHost(ValueProvider<String> emulatorHost) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withEmulatorHost(emulatorHost));
    }

    public CreateTransaction withEmulatorHost(String emulatorHost) {
      return withEmulatorHost(ValueProvider.StaticValueProvider.of(emulatorHost));
    }

    @VisibleForTesting
    CreateTransaction withServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withServiceFactory(serviceFactory));
    }

    public CreateTransaction withTimestampBound(TimestampBound timestampBound) {
      return toBuilder().setTimestampBound(timestampBound).build();
    }

    
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

      public abstract Builder setTimestampBound(TimestampBound newTimestampBound);

      public abstract CreateTransaction build();
    }
  }

  
  public enum FailureMode {
    
    FAIL_FAST,
    
    REPORT_FAILURES
  }

  
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<Mutation>, SpannerWriteResult> {

    abstract SpannerConfig getSpannerConfig();

    abstract long getBatchSizeBytes();

    abstract long getMaxNumMutations();

    abstract long getMaxNumRows();

    abstract FailureMode getFailureMode();

    abstract @Nullable PCollection<?> getSchemaReadySignal();

    abstract OptionalInt getGroupingFactor();

    abstract @Nullable PCollectionView<Dialect> getDialectView();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

      abstract Builder setBatchSizeBytes(long batchSizeBytes);

      abstract Builder setMaxNumMutations(long maxNumMutations);

      abstract Builder setMaxNumRows(long maxNumRows);

      abstract Builder setFailureMode(FailureMode failureMode);

      abstract Builder setSchemaReadySignal(PCollection<?> schemaReadySignal);

      abstract Builder setGroupingFactor(int groupingFactor);

      abstract Builder setDialectView(PCollectionView<Dialect> dialect);

      abstract Write build();
    }

    
    public Write withSpannerConfig(SpannerConfig spannerConfig) {
      return toBuilder().setSpannerConfig(spannerConfig).build();
    }

    
    public Write withProjectId(String projectId) {
      return withProjectId(ValueProvider.StaticValueProvider.of(projectId));
    }

    
    public Write withProjectId(ValueProvider<String> projectId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withProjectId(projectId));
    }

    
    public Write withInstanceId(String instanceId) {
      return withInstanceId(ValueProvider.StaticValueProvider.of(instanceId));
    }

    
    public Write withInstanceId(ValueProvider<String> instanceId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withInstanceId(instanceId));
    }

    
    public Write withDatabaseId(String databaseId) {
      return withDatabaseId(ValueProvider.StaticValueProvider.of(databaseId));
    }

    
    public Write withDatabaseId(ValueProvider<String> databaseId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withDatabaseId(databaseId));
    }

    
    public Write withHost(ValueProvider<String> host) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withHost(host));
    }

    
    public Write withHost(String host) {
      return withHost(ValueProvider.StaticValueProvider.of(host));
    }

    
    public Write withEmulatorHost(ValueProvider<String> emulatorHost) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withEmulatorHost(emulatorHost));
    }

    public Write withEmulatorHost(String emulatorHost) {
      return withEmulatorHost(ValueProvider.StaticValueProvider.of(emulatorHost));
    }

    public Write withDialectView(PCollectionView<Dialect> dialect) {
      return toBuilder().setDialectView(dialect).build();
    }

    
    public Write withCommitDeadline(Duration commitDeadline) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withCommitDeadline(commitDeadline));
    }

    
    public Write withMaxCommitDelay(long millis) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withMaxCommitDelay(millis));
    }

    
    public Write withMaxCumulativeBackoff(Duration maxCumulativeBackoff) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withMaxCumulativeBackoff(maxCumulativeBackoff));
    }

    @VisibleForTesting
    Write withServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withServiceFactory(serviceFactory));
    }

    
    public WriteGrouped grouped() {
      return new WriteGrouped(this);
    }

    
    public Write withBatchSizeBytes(long batchSizeBytes) {
      return toBuilder().setBatchSizeBytes(batchSizeBytes).build();
    }

    
    public Write withFailureMode(FailureMode failureMode) {
      return toBuilder().setFailureMode(failureMode).build();
    }

    
    public Write withMaxNumMutations(long maxNumMutations) {
      return toBuilder().setMaxNumMutations(maxNumMutations).build();
    }

    
    public Write withMaxNumRows(long maxNumRows) {
      return toBuilder().setMaxNumRows(maxNumRows).build();
    }

    
    public Write withSchemaReadySignal(PCollection<?> signal) {
      return toBuilder().setSchemaReadySignal(signal).build();
    }

    
    public Write withGroupingFactor(int groupingFactor) {
      return toBuilder().setGroupingFactor(groupingFactor).build();
    }

    public Write withLowPriority() {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withRpcPriority(RpcPriority.LOW));
    }

    public Write withHighPriority() {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withRpcPriority(RpcPriority.HIGH));
    }

    @Override
    public SpannerWriteResult expand(PCollection<Mutation> input) {
      getSpannerConfig().validate();

      return input
          .apply("To mutation group", ParDo.of(new ToMutationGroupFn()))
          .apply("Write mutations to Cloud Spanner", new WriteGrouped(this));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      populateDisplayDataWithParamaters(builder);
    }

    private void populateDisplayDataWithParamaters(DisplayData.Builder builder) {
      getSpannerConfig().populateDisplayData(builder);
      builder.add(
          DisplayData.item("batchSizeBytes", getBatchSizeBytes())
              .withLabel("Max batch size in bytes"));
      builder.add(
          DisplayData.item("maxNumMutations", getMaxNumMutations())
              .withLabel("Max number of mutated cells in each batch"));
      builder.add(
          DisplayData.item("maxNumRows", getMaxNumRows())
              .withLabel("Max number of rows in each batch"));
      
      
      builder.add(
          DisplayData.item(
                  "groupingFactor",
                  (getGroupingFactor().isPresent()
                      ? Integer.toString(getGroupingFactor().getAsInt())
                      : "DEFAULT"))
              .withLabel("Number of batches to sort over"));
    }
  }

  static class WriteRows extends PTransform<PCollection<Row>, PDone> {

    private final Write write;
    private final Op operation;
    private final String table;

    private WriteRows(Write write, Op operation, String table) {
      this.write = write;
      this.operation = operation;
      this.table = table;
    }

    public static WriteRows of(Write write, Op operation, String table) {
      return new WriteRows(write, operation, table);
    }

    @Override
    public PDone expand(PCollection<Row> input) {
      input
          .apply(
              MapElements.into(TypeDescriptor.of(Mutation.class))
                  .via(MutationUtils.beamRowToMutationFn(operation, table)))
          .apply(write);
      return PDone.in(input.getPipeline());
    }
  }

  
  public static class WriteGrouped
      extends PTransform<PCollection<MutationGroup>, SpannerWriteResult> {

    private final Write spec;
    private static final TupleTag<MutationGroup> BATCHABLE_MUTATIONS_TAG =
        new TupleTag<MutationGroup>("batchableMutations") {};
    private static final TupleTag<Iterable<MutationGroup>> UNBATCHABLE_MUTATIONS_TAG =
        new TupleTag<Iterable<MutationGroup>>("unbatchableMutations") {};

    private static final TupleTag<Void> MAIN_OUT_TAG = new TupleTag<Void>("mainOut") {};
    private static final TupleTag<MutationGroup> FAILED_MUTATIONS_TAG =
        new TupleTag<MutationGroup>("failedMutations") {};
    private static final SerializableCoder<MutationGroup> CODER =
        SerializableCoder.of(MutationGroup.class);

    public WriteGrouped(Write spec) {
      this.spec = spec;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      spec.populateDisplayDataWithParamaters(builder);
    }

    @Override
    public SpannerWriteResult expand(PCollection<MutationGroup> input) {
      PCollection<Iterable<MutationGroup>> batches;
      PCollectionView<Dialect> dialectView = spec.getDialectView();

      if (dialectView == null) {
        dialectView =
            input
                .getPipeline()
                .apply("CreateSingleton", Create.of(Dialect.GOOGLE_STANDARD_SQL))
                .apply("As PCollectionView", View.asSingleton());
      }

      if (spec.getBatchSizeBytes() <= 1
          || spec.getMaxNumMutations() <= 1
          || spec.getMaxNumRows() <= 1) {
        LOG.info("Batching of mutationGroups is disabled");
        TypeDescriptor<Iterable<MutationGroup>> descriptor =
            new TypeDescriptor<Iterable<MutationGroup>>() {};
        batches = input.apply(MapElements.into(descriptor).via(ImmutableList::of));
      } else {

        
        PCollection<Void> schemaSeed =
            input.getPipeline().apply("Create Seed", Create.of((Void) null));
        if (spec.getSchemaReadySignal() != null) {
          
          schemaSeed = schemaSeed.apply("Wait for schema", Wait.on(spec.getSchemaReadySignal()));
        }
        final PCollectionView<SpannerSchema> schemaView =
            schemaSeed
                .apply(
                    "Read information schema",
                    ParDo.of(new ReadSpannerSchema(spec.getSpannerConfig(), dialectView))
                        .withSideInputs(dialectView))
                .apply("Schema View", View.asSingleton());

        
        
        PCollectionTuple filteredMutations =
            input
                .apply(
                    "RewindowIntoGlobal",
                    Window.<MutationGroup>into(new GlobalWindows())
                        .triggering(DefaultTrigger.of())
                        .discardingFiredPanes())
                .apply(
                    "Filter Unbatchable Mutations",
                    ParDo.of(
                            new BatchableMutationFilterFn(
                                schemaView,
                                UNBATCHABLE_MUTATIONS_TAG,
                                spec.getBatchSizeBytes(),
                                spec.getMaxNumMutations(),
                                spec.getMaxNumRows()))
                        .withSideInputs(schemaView)
                        .withOutputTags(
                            BATCHABLE_MUTATIONS_TAG, TupleTagList.of(UNBATCHABLE_MUTATIONS_TAG)));

        
        
        PCollection<Iterable<MutationGroup>> batchedMutations =
            filteredMutations
                .get(BATCHABLE_MUTATIONS_TAG)
                .apply(
                    "Gather Sort And Create Batches",
                    ParDo.of(
                            new GatherSortCreateBatchesFn(
                                spec.getBatchSizeBytes(),
                                spec.getMaxNumMutations(),
                                spec.getMaxNumRows(),
                                
                                spec.getGroupingFactor()
                                    .orElse(
                                        input.isBounded() == IsBounded.BOUNDED
                                            ? DEFAULT_GROUPING_FACTOR
                                            : 1),
                                schemaView))
                        .withSideInputs(schemaView));

        
        batches =
            PCollectionList.of(filteredMutations.get(UNBATCHABLE_MUTATIONS_TAG))
                .and(batchedMutations)
                .apply("Merge", Flatten.pCollections());
      }

      PCollectionTuple result =
          batches.apply(
              "Write batches to Spanner",
              ParDo.of(
                      new WriteToSpannerFn(
                          spec.getSpannerConfig(), spec.getFailureMode()))
                  .withOutputTags(MAIN_OUT_TAG, TupleTagList.of(FAILED_MUTATIONS_TAG)));

      return new SpannerWriteResult(
          input.getPipeline(),
          result.get(MAIN_OUT_TAG),
          result.get(FAILED_MUTATIONS_TAG),
          FAILED_MUTATIONS_TAG);
    }

    @VisibleForTesting
    static MutationGroup decode(byte[] bytes) {
      ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
      try {
        return CODER.decode(bis);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @VisibleForTesting
    static byte[] encode(MutationGroup g) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try {
        CODER.encode(g, bos);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return bos.toByteArray();
    }
  }

  @AutoValue
  public abstract static class ReadChangeStream
      extends PTransform<PBegin, PCollection<DataChangeRecord>> {

    abstract SpannerConfig getSpannerConfig();

    abstract String getChangeStreamName();

    abstract @Nullable String getMetadataInstance();

    abstract @Nullable String getMetadataDatabase();

    abstract @Nullable String getMetadataTable();

    abstract Timestamp getInclusiveStartAt();

    abstract @Nullable Timestamp getInclusiveEndAt();

    abstract @Nullable RpcPriority getRpcPriority();

    
    @Deprecated
    abstract @Nullable Double getTraceSampleProbability();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

      abstract Builder setChangeStreamName(String changeStreamName);

      abstract Builder setMetadataInstance(String metadataInstance);

      abstract Builder setMetadataDatabase(String metadataDatabase);

      abstract Builder setMetadataTable(String metadataTable);

      abstract Builder setInclusiveStartAt(Timestamp inclusiveStartAt);

      abstract Builder setInclusiveEndAt(Timestamp inclusiveEndAt);

      abstract Builder setRpcPriority(RpcPriority rpcPriority);

      abstract Builder setTraceSampleProbability(Double probability);

      abstract ReadChangeStream build();
    }

    
    public ReadChangeStream withSpannerConfig(SpannerConfig spannerConfig) {
      return toBuilder().setSpannerConfig(spannerConfig).build();
    }

    
    public ReadChangeStream withProjectId(String projectId) {
      return withProjectId(ValueProvider.StaticValueProvider.of(projectId));
    }

    
    public ReadChangeStream withProjectId(ValueProvider<String> projectId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withProjectId(projectId));
    }

    
    public ReadChangeStream withInstanceId(String instanceId) {
      return withInstanceId(ValueProvider.StaticValueProvider.of(instanceId));
    }

    
    public ReadChangeStream withInstanceId(ValueProvider<String> instanceId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withInstanceId(instanceId));
    }

    
    public ReadChangeStream withDatabaseId(String databaseId) {
      return withDatabaseId(ValueProvider.StaticValueProvider.of(databaseId));
    }

    
    public ReadChangeStream withDatabaseId(ValueProvider<String> databaseId) {
      SpannerConfig config = getSpannerConfig();
      return withSpannerConfig(config.withDatabaseId(databaseId));
    }

    
    public ReadChangeStream withChangeStreamName(String changeStreamName) {
      return toBuilder().setChangeStreamName(changeStreamName).build();
    }

    
    public ReadChangeStream withMetadataInstance(String metadataInstance) {
      return toBuilder().setMetadataInstance(metadataInstance).build();
    }

    
    public ReadChangeStream withMetadataDatabase(String metadataDatabase) {
      return toBuilder().setMetadataDatabase(metadataDatabase).build();
    }

    
    public ReadChangeStream withMetadataTable(String metadataTable) {
      return toBuilder().setMetadataTable(metadataTable).build();
    }

    
    public ReadChangeStream withInclusiveStartAt(Timestamp timestamp) {
      return toBuilder().setInclusiveStartAt(timestamp).build();
    }

    
    public ReadChangeStream withInclusiveEndAt(Timestamp timestamp) {
      return toBuilder().setInclusiveEndAt(timestamp).build();
    }

    
    public ReadChangeStream withRpcPriority(RpcPriority rpcPriority) {
      return toBuilder().setRpcPriority(rpcPriority).build();
    }

    
    @Deprecated
    public ReadChangeStream withTraceSampleProbability(Double probability) {
      return toBuilder().setTraceSampleProbability(probability).build();
    }

    @Override
    public PCollection<DataChangeRecord> expand(PBegin input) {
      checkArgument(
          getSpannerConfig() != null,
          "SpannerIO.readChangeStream() requires the spanner config to be set.");
      checkArgument(
          getSpannerConfig().getProjectId() != null,
          "SpannerIO.readChangeStream() requires the project ID to be set.");
      checkArgument(
          getSpannerConfig().getInstanceId() != null,
          "SpannerIO.readChangeStream() requires the instance ID to be set.");
      checkArgument(
          getSpannerConfig().getDatabaseId() != null,
          "SpannerIO.readChangeStream() requires the database ID to be set.");
      checkArgument(
          getChangeStreamName() != null,
          "SpannerIO.readChangeStream() requires the name of the change stream to be set.");
      checkArgument(
          getInclusiveStartAt() != null,
          "SpannerIO.readChangeStream() requires the start time to be set.");
      
      checkArgument(
          getInclusiveEndAt() != null,
          "SpannerIO.readChangeStream() requires the end time to be set. If you'd like to process the stream without an end time, you can omit this parameter.");
      if (getMetadataInstance() != null) {
        checkArgument(
            getMetadataDatabase() != null,
            "SpannerIO.readChangeStream() requires the metadata database to be set if metadata instance is set.");
      }

      
      if (getInclusiveEndAt() != null
          && getInclusiveStartAt().toSqlTimestamp().after(getInclusiveEndAt().toSqlTimestamp())) {
        throw new IllegalArgumentException("Start time cannot be after end time.");
      }

      final DatabaseId changeStreamDatabaseId =
          DatabaseId.of(
              getSpannerConfig().getProjectId().get(),
              getSpannerConfig().getInstanceId().get(),
              getSpannerConfig().getDatabaseId().get());
      final String partitionMetadataInstanceId =
          MoreObjects.firstNonNull(
              getMetadataInstance(), changeStreamDatabaseId.getInstanceId().getInstance());
      final String partitionMetadataDatabaseId =
          MoreObjects.firstNonNull(getMetadataDatabase(), changeStreamDatabaseId.getDatabase());
      final DatabaseId fullPartitionMetadataDatabaseId =
          DatabaseId.of(
              getSpannerConfig().getProjectId().get(),
              partitionMetadataInstanceId,
              partitionMetadataDatabaseId);

      final SpannerConfig changeStreamSpannerConfig = buildChangeStreamSpannerConfig();
      final SpannerConfig partitionMetadataSpannerConfig =
          MetadataSpannerConfigFactory.create(
              changeStreamSpannerConfig, partitionMetadataInstanceId, partitionMetadataDatabaseId);
      final Dialect changeStreamDatabaseDialect =
          getDialect(changeStreamSpannerConfig, input.getPipeline().getOptions());
      final Dialect metadataDatabaseDialect =
          getDialect(partitionMetadataSpannerConfig, input.getPipeline().getOptions());
        LOG.info("The Spanner database {} has dialect {}", changeStreamDatabaseId, changeStreamDatabaseDialect);
        LOG.info("The Spanner database {} has dialect {}", fullPartitionMetadataDatabaseId, metadataDatabaseDialect);
      final String partitionMetadataTableName =
          MoreObjects.firstNonNull(
              getMetadataTable(), generatePartitionMetadataTableName(partitionMetadataDatabaseId));
      final String changeStreamName = getChangeStreamName();
      final Timestamp startTimestamp = getInclusiveStartAt();
      
      
      final Timestamp endTimestamp =
          getInclusiveEndAt().compareTo(MAX_INCLUSIVE_END_AT) > 0
              ? MAX_INCLUSIVE_END_AT
              : getInclusiveEndAt();
      final MapperFactory mapperFactory = new MapperFactory(changeStreamDatabaseDialect);
      final ChangeStreamMetrics metrics = new ChangeStreamMetrics();
      final RpcPriority rpcPriority = MoreObjects.firstNonNull(getRpcPriority(), RpcPriority.HIGH);
      final DaoFactory daoFactory =
          new DaoFactory(
              changeStreamSpannerConfig,
              changeStreamName,
              partitionMetadataSpannerConfig,
              partitionMetadataTableName,
              rpcPriority,
              input.getPipeline().getOptions().getJobName(),
              changeStreamDatabaseDialect,
              metadataDatabaseDialect);
      final ActionFactory actionFactory = new ActionFactory();

      final InitializeDoFn initializeDoFn =
          new InitializeDoFn(daoFactory, mapperFactory, startTimestamp, endTimestamp);
      final DetectNewPartitionsDoFn detectNewPartitionsDoFn =
          new DetectNewPartitionsDoFn(daoFactory, mapperFactory, actionFactory, metrics);
      final ReadChangeStreamPartitionDoFn readChangeStreamPartitionDoFn =
          new ReadChangeStreamPartitionDoFn(daoFactory, mapperFactory, actionFactory, metrics);
      final PostProcessingMetricsDoFn postProcessingMetricsDoFn =
          new PostProcessingMetricsDoFn(metrics);

        LOG.info("Partition metadata table that will be used is {}", partitionMetadataTableName);

      final PCollection<byte[]> impulseOut = input.apply(Impulse.create());
      final PCollection<PartitionMetadata> partitionsOut =
          impulseOut
              .apply("Initialize the connector", ParDo.of(initializeDoFn))
              .apply("Detect new partitions", ParDo.of(detectNewPartitionsDoFn));
      final Coder<PartitionMetadata> partitionMetadataCoder = partitionsOut.getCoder();
      final SizeEstimator<PartitionMetadata> partitionMetadataSizeEstimator =
          new SizeEstimator<>(partitionMetadataCoder);
      final long averagePartitionBytesSize =
          partitionMetadataSizeEstimator.sizeOf(ChangeStreamsConstants.SAMPLE_PARTITION);
      detectNewPartitionsDoFn.setAveragePartitionBytesSize(averagePartitionBytesSize);

      final PCollection<DataChangeRecord> dataChangeRecordsOut =
          partitionsOut
              .apply("Read change stream partition", ParDo.of(readChangeStreamPartitionDoFn))
              .apply("Gather metrics", ParDo.of(postProcessingMetricsDoFn));
      final Coder<DataChangeRecord> dataChangeRecordCoder = dataChangeRecordsOut.getCoder();
      final SizeEstimator<DataChangeRecord> dataChangeRecordSizeEstimator =
          new SizeEstimator<>(dataChangeRecordCoder);
      final BytesThroughputEstimator<DataChangeRecord> throughputEstimator =
          new BytesThroughputEstimator<>(THROUGHPUT_WINDOW_SECONDS, dataChangeRecordSizeEstimator);
      readChangeStreamPartitionDoFn.setThroughputEstimator(throughputEstimator);

      impulseOut
          .apply(WithTimestamps.of(e -> GlobalWindow.INSTANCE.maxTimestamp()))
          .apply(Wait.on(dataChangeRecordsOut))
          .apply(ParDo.of(new CleanUpReadChangeStreamDoFn(daoFactory)));
      return dataChangeRecordsOut;
    }

    @VisibleForTesting
    SpannerConfig buildChangeStreamSpannerConfig() {
      SpannerConfig changeStreamSpannerConfig = getSpannerConfig();
      
      if (changeStreamSpannerConfig.getRetryableCodes() == null) {
        ImmutableSet<Code> defaultRetryableCodes = ImmutableSet.of(Code.UNAVAILABLE, Code.ABORTED);
        changeStreamSpannerConfig =
            changeStreamSpannerConfig.toBuilder().setRetryableCodes(defaultRetryableCodes).build();
      }
      
      if (changeStreamSpannerConfig.getExecuteStreamingSqlRetrySettings() == null) {
        changeStreamSpannerConfig =
            changeStreamSpannerConfig
                .toBuilder()
                .setExecuteStreamingSqlRetrySettings(
                    RetrySettings.newBuilder()
                        .setTotalTimeout(org.threeten.bp.Duration.ofMinutes(5))
                        .setInitialRpcTimeout(org.threeten.bp.Duration.ofMinutes(1))
                        .setMaxRpcTimeout(org.threeten.bp.Duration.ofMinutes(1))
                        .build())
                .build();
      }
      return changeStreamSpannerConfig;
    }
  }

  
  @VisibleForTesting
  static SpannerConfig buildSpannerConfigWithCredential(
      SpannerConfig spannerConfig, PipelineOptions pipelineOptions) {
    if (spannerConfig.getCredentials() == null && pipelineOptions != null) {
      final Credentials credentials = pipelineOptions.as(GcpOptions.class).getGcpCredential();
      if (credentials != null) {
        spannerConfig = spannerConfig.withCredentials(credentials);
      }
    }
    return spannerConfig;
  }

  private static Dialect getDialect(SpannerConfig spannerConfig, PipelineOptions pipelineOptions) {
    
    SpannerConfig spannerConfigWithCredential =
        buildSpannerConfigWithCredential(spannerConfig, pipelineOptions);
    DatabaseClient databaseClient =
        SpannerAccessor.getOrCreate(spannerConfigWithCredential).getDatabaseClient();
    return databaseClient.getDialect();
  }

  
  public interface SpannerChangeStreamOptions extends StreamingOptions {

    
    String getMetadataTable();

    
    void setMetadataTable(String table);
  }

  private static class ToMutationGroupFn extends DoFn<Mutation, MutationGroup> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      Mutation value = c.element();
      c.output(MutationGroup.create(value));
    }
  }

  
  @VisibleForTesting
  static class GatherSortCreateBatchesFn extends DoFn<MutationGroup, Iterable<MutationGroup>> {

    private final long maxBatchSizeBytes;
    private final long maxBatchNumMutations;
    private final long maxBatchNumRows;
    private final long maxSortableSizeBytes;
    private final long maxSortableNumMutations;
    private final long maxSortableNumRows;
    private final PCollectionView<SpannerSchema> schemaView;
    private final ArrayList<MutationGroupContainer> mutationsToSort = new ArrayList<>();

    
    private long sortableSizeBytes = 0;
    
    private long sortableNumCells = 0;
    
    private long sortableNumRows = 0;

    GatherSortCreateBatchesFn(
        long maxBatchSizeBytes,
        long maxNumMutations,
        long maxNumRows,
        long groupingFactor,
        PCollectionView<SpannerSchema> schemaView) {
      this.maxBatchSizeBytes = maxBatchSizeBytes;
      this.maxBatchNumMutations = maxNumMutations;
      this.maxBatchNumRows = maxNumRows;

      if (groupingFactor <= 0) {
        groupingFactor = 1;
      }

      this.maxSortableSizeBytes = maxBatchSizeBytes * groupingFactor;
      this.maxSortableNumMutations = maxNumMutations * groupingFactor;
      this.maxSortableNumRows = maxNumRows * groupingFactor;
      this.schemaView = schemaView;

      initSorter();
    }

    private synchronized void initSorter() {
      mutationsToSort.clear();
      sortableSizeBytes = 0;
      sortableNumCells = 0;
      sortableNumRows = 0;
    }

    @FinishBundle
    public synchronized void finishBundle(FinishBundleContext c) {
      sortAndOutputBatches(new OutputReceiverForFinishBundle(c));
    }

    private synchronized void sortAndOutputBatches(OutputReceiver<Iterable<MutationGroup>> out) {
      try {
        if (mutationsToSort.isEmpty()) {
          
          return;
        }

        if (maxSortableNumMutations == maxBatchNumMutations) {
          
          outputBatch(out, 0, mutationsToSort.size());
          return;
        }

        
        mutationsToSort.sort(Comparator.naturalOrder());
        int batchStart = 0;
        int batchEnd = 0;

        
        long batchSizeBytes = 0;
        
        long batchCells = 0;
        
        long batchRows = 0;

        
        while (batchEnd < mutationsToSort.size()) {
          MutationGroupContainer mg = mutationsToSort.get(batchEnd);

          if (((batchCells + mg.numCells) > maxBatchNumMutations)
              || ((batchSizeBytes + mg.sizeBytes) > maxBatchSizeBytes
                  || (batchRows + mg.numRows > maxBatchNumRows))) {
            
            outputBatch(out, batchStart, batchEnd);
            batchStart = batchEnd;
            batchSizeBytes = 0;
            batchCells = 0;
            batchRows = 0;
          }

          batchEnd++;
          batchSizeBytes += mg.sizeBytes;
          batchCells += mg.numCells;
          batchRows += mg.numRows;
        }

        if (batchStart < batchEnd) {
          
          outputBatch(out, batchStart, mutationsToSort.size());
        }
      } finally {
        initSorter();
      }
    }

    private void outputBatch(
        OutputReceiver<Iterable<MutationGroup>> out, int batchStart, int batchEnd) {
      out.output(
          mutationsToSort.subList(batchStart, batchEnd).stream()
              .map(o -> o.mutationGroup)
              .collect(toList()));
    }

    @ProcessElement
    public synchronized void processElement(
        ProcessContext c, OutputReceiver<Iterable<MutationGroup>> out) {
      SpannerSchema spannerSchema = c.sideInput(schemaView);
      MutationKeyEncoder encoder = new MutationKeyEncoder(spannerSchema);
      MutationGroup mg = c.element();
      long groupSize = MutationSizeEstimator.sizeOf(mg);
      long groupCells = MutationCellCounter.countOf(spannerSchema, mg);
      long groupRows = mg.size();

      synchronized (this) {
        if (((sortableNumCells + groupCells) > maxSortableNumMutations)
            || (sortableSizeBytes + groupSize) > maxSortableSizeBytes
            || (sortableNumRows + groupRows) > maxSortableNumRows) {
          sortAndOutputBatches(out);
        }

        mutationsToSort.add(
            new MutationGroupContainer(
                mg, groupSize, groupCells, groupRows, encoder.encodeTableNameAndKey(mg.primary())));
        sortableSizeBytes += groupSize;
        sortableNumCells += groupCells;
        sortableNumRows += groupRows;
      }
    }

    
    private static final class MutationGroupContainer
        implements Comparable<MutationGroupContainer> {

      final MutationGroup mutationGroup;
      final long sizeBytes;
      final long numCells;
      final long numRows;
      final byte[] encodedKey;

      MutationGroupContainer(
          MutationGroup mutationGroup,
          long sizeBytes,
          long numCells,
          long numRows,
          byte[] encodedKey) {
        this.mutationGroup = mutationGroup;
        this.sizeBytes = sizeBytes;
        this.numCells = numCells;
        this.numRows = numRows;
        this.encodedKey = encodedKey;
      }

      @Override
      public int compareTo(MutationGroupContainer o) {
        return UnsignedBytes.lexicographicalComparator().compare(this.encodedKey, o.encodedKey);
      }
    }

    
    
    private static class OutputReceiverForFinishBundle
        implements OutputReceiver<Iterable<MutationGroup>> {

      private final FinishBundleContext c;

      OutputReceiverForFinishBundle(FinishBundleContext c) {
        this.c = c;
      }

      @Override
      public void output(Iterable<MutationGroup> output) {
        outputWithTimestamp(output, Instant.now());
      }

      @Override
      public void outputWithTimestamp(Iterable<MutationGroup> output, Instant timestamp) {
        c.output(output, timestamp, GlobalWindow.INSTANCE);
      }
    }
  }

  
  @VisibleForTesting
  static class BatchableMutationFilterFn extends DoFn<MutationGroup, MutationGroup> {

    private final PCollectionView<SpannerSchema> schemaView;
    private final TupleTag<Iterable<MutationGroup>> unbatchableMutationsTag;
    private final long batchSizeBytes;
    private final long maxNumMutations;
    private final long maxNumRows;
    private final Counter batchableMutationGroupsCounter =
        Metrics.counter(WriteGrouped.class, "batchable_mutation_groups");
    private final Counter unBatchableMutationGroupsCounter =
        Metrics.counter(WriteGrouped.class, "unbatchable_mutation_groups");

    BatchableMutationFilterFn(
        PCollectionView<SpannerSchema> schemaView,
        TupleTag<Iterable<MutationGroup>> unbatchableMutationsTag,
        long batchSizeBytes,
        long maxNumMutations,
        long maxNumRows) {
      this.schemaView = schemaView;
      this.unbatchableMutationsTag = unbatchableMutationsTag;
      this.batchSizeBytes = batchSizeBytes;
      this.maxNumMutations = maxNumMutations;
      this.maxNumRows = maxNumRows;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      MutationGroup mg = c.element();
      if (mg.primary().getOperation() == Op.DELETE && !isPointDelete(mg.primary())) {
        
        c.output(unbatchableMutationsTag, Collections.singletonList(mg));
        unBatchableMutationGroupsCounter.inc();
        return;
      }

      SpannerSchema spannerSchema = c.sideInput(schemaView);
      long groupSize = MutationSizeEstimator.sizeOf(mg);
      long groupCells = MutationCellCounter.countOf(spannerSchema, mg);
      long groupRows = Iterables.size(mg);

      if (groupSize >= batchSizeBytes || groupCells >= maxNumMutations || groupRows >= maxNumRows) {
        c.output(unbatchableMutationsTag, Collections.singletonList(mg));
        unBatchableMutationGroupsCounter.inc();
      } else {
        c.output(mg);
        batchableMutationGroupsCounter.inc();
      }
    }
  }

  @VisibleForTesting
  static class WriteToSpannerFn extends DoFn<Iterable<MutationGroup>, Void> {

    private final SpannerConfig spannerConfig;
    private final FailureMode failureMode;

    
    private transient SpannerAccessor spannerAccessor;
      
    private static final int ABORTED_RETRY_ATTEMPTS = 5;
    
    private final String schemaChangeErrString =
        "Transaction aborted. "
            + "Database schema probably changed during transaction, retry may succeed.";

    
    private final String emulatorErrorString =
        "The emulator only supports one transaction at a time.";

    @VisibleForTesting static final Sleeper sleeper = Sleeper.DEFAULT;

    private final Counter mutationGroupBatchesReceived =
        Metrics.counter(WriteGrouped.class, "mutation_group_batches_received");
    private final Counter mutationGroupBatchesWriteSuccess =
        Metrics.counter(WriteGrouped.class, "mutation_group_batches_write_success");
    private final Counter mutationGroupBatchesWriteFail =
        Metrics.counter(WriteGrouped.class, "mutation_group_batches_write_fail");

    private final Counter mutationGroupsReceived =
        Metrics.counter(WriteGrouped.class, "mutation_groups_received");
    private final Counter mutationGroupsWriteSuccess =
        Metrics.counter(WriteGrouped.class, "mutation_groups_write_success");
    private final Counter mutationGroupsWriteFail =
        Metrics.counter(WriteGrouped.class, "mutation_groups_write_fail");

    private final Counter spannerWriteSuccess =
        Metrics.counter(WriteGrouped.class, "spanner_write_success");
    private final Counter spannerWriteFail =
        Metrics.counter(WriteGrouped.class, "spanner_write_fail");
    private final Distribution spannerWriteLatency =
        Metrics.distribution(WriteGrouped.class, "spanner_write_latency_ms");
    private final Counter spannerWriteTimeouts =
        Metrics.counter(WriteGrouped.class, "spanner_write_timeouts");
    private final Counter spannerWriteRetries =
        Metrics.counter(WriteGrouped.class, "spanner_write_retries");

    private final TupleTag<MutationGroup> failedTag;

    
    private transient FluentBackoff bundleWriteBackoff;
    private transient LoadingCache<String, ServiceCallMetric> writeMetricsByTableName;

    WriteToSpannerFn(
        SpannerConfig spannerConfig, FailureMode failureMode) {
      this.spannerConfig = spannerConfig;
      this.failureMode = failureMode;
      this.failedTag = WriteGrouped.FAILED_MUTATIONS_TAG;
    }

    @Setup
    public void setup() {
      spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
      bundleWriteBackoff =
          FluentBackoff.DEFAULT
              .withMaxCumulativeBackoff(spannerConfig.getMaxCumulativeBackoff().get())
              .withInitialBackoff(spannerConfig.getMaxCumulativeBackoff().get().dividedBy(60));

      
      
      
      writeMetricsByTableName =
          CacheBuilder.newBuilder()
              .maximumSize(METRICS_CACHE_SIZE)
              .build(
                  new CacheLoader<String, ServiceCallMetric>() {
                    @Override
                    public ServiceCallMetric load(String tableName) {
                      return buildWriteServiceCallMetric(spannerConfig, tableName);
                    }
                  });

        
        String projectId = resolveSpannerProjectId(spannerConfig);
    }

    @Teardown
    public void teardown() {
      spannerAccessor.close();
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      List<MutationGroup> mutations = ImmutableList.copyOf(c.element());

      
      try {
        mutationGroupBatchesReceived.inc();
        mutationGroupsReceived.inc(mutations.size());
        Iterable<Mutation> batch = Iterables.concat(mutations);
        writeMutations(batch);
        mutationGroupBatchesWriteSuccess.inc();
        mutationGroupsWriteSuccess.inc(mutations.size());
        return;
      } catch (SpannerException e) {
        mutationGroupBatchesWriteFail.inc();
        if (failureMode == FailureMode.REPORT_FAILURES) {
          
        } else if (failureMode == FailureMode.FAIL_FAST) {
          mutationGroupsWriteFail.inc(mutations.size());
          LOG.error("Failed to write a batch of mutation groups", e);
          throw e;
        } else {
          throw new IllegalArgumentException("Unknown failure mode " + failureMode);
        }
      }

      
      for (MutationGroup mg : mutations) {
        try {
          spannerWriteRetries.inc();
          writeMutations(mg);
          mutationGroupsWriteSuccess.inc();
        } catch (SpannerException e) {
          mutationGroupsWriteFail.inc();
            LOG.warn("Failed to write the mutation group: {}", mg, e);
          c.output(failedTag, mg);
        }
      }
    }

    private Options.TransactionOption[] getTransactionOptions() {
      return Stream.of(
              spannerConfig.getRpcPriority() != null && spannerConfig.getRpcPriority().get() != null
                  ? Options.priority(spannerConfig.getRpcPriority().get())
                  : null,
              spannerConfig.getMaxCommitDelay() != null
                      && spannerConfig.getMaxCommitDelay().get() != null
                  ? Options.maxCommitDelay(
                      java.time.Duration.ofMillis(
                          spannerConfig.getMaxCommitDelay().get().getMillis()))
                  : null)
          .filter(Objects::nonNull)
          .toArray(Options.TransactionOption[]::new);
    }

    private void reportServiceCallMetricsForBatch(Set<String> tableNames, String statusCode) {
      for (String tableName : tableNames) {
        writeMetricsByTableName.getUnchecked(tableName).call(statusCode);
      }
    }

    private static ServiceCallMetric buildWriteServiceCallMetric(
        SpannerConfig config, String tableId) {
      HashMap<String, String> baseLabels = buildServiceCallMetricLabels(config);
      baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "Write");
      baseLabels.put(
          MonitoringInfoConstants.Labels.RESOURCE,
          GcpResourceIdentifiers.spannerTable(
              baseLabels.get(MonitoringInfoConstants.Labels.SPANNER_PROJECT_ID),
              config.getInstanceId().get(),
              config.getDatabaseId().get(),
              tableId));
      baseLabels.put(MonitoringInfoConstants.Labels.TABLE_ID, tableId);
      return new ServiceCallMetric(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);
    }

    
    private void writeMutations(Iterable<Mutation> mutationIterable)
        throws SpannerException, IOException {
      BackOff backoff = bundleWriteBackoff.backoff();
      List<Mutation> mutations = ImmutableList.copyOf(mutationIterable);

      while (true) {
        Stopwatch timer = Stopwatch.createStarted();
        
        try {
          spannerWriteSuccess.inc();
          return;
        } catch (SpannerException exception) {
          if (exception.getErrorCode() == ErrorCode.DEADLINE_EXCEEDED) {
            spannerWriteTimeouts.inc();

            
            long sleepTimeMsecs = backoff.nextBackOffMillis();
            if (sleepTimeMsecs == BackOff.STOP) {
              LOG.error(
                  "DEADLINE_EXCEEDED writing batch of {} mutations to Cloud Spanner. "
                      + "Aborting after too many retries.",
                  mutations.size());
              spannerWriteFail.inc();
              throw exception;
            }
            LOG.info(
                "DEADLINE_EXCEEDED writing batch of {} mutations to Cloud Spanner, "
                    + "retrying after backoff of {}ms\n"
                    + "({})",
                mutations.size(),
                sleepTimeMsecs,
                exception.getMessage());
            spannerWriteRetries.inc();
            try {
              sleeper.sleep(sleepTimeMsecs);
            } catch (InterruptedException e) {
              
            }
          } else {
            
            spannerWriteFail.inc();
            throw exception;
          }
        } finally {
          spannerWriteLatency.update(timer.elapsed(TimeUnit.MILLISECONDS));
        }
      }
    }
  }

  private SpannerIO() {} 

  private static HashMap<String, String> buildServiceCallMetricLabels(SpannerConfig config) {
    HashMap<String, String> baseLabels = new HashMap<>();
    baseLabels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
    baseLabels.put(MonitoringInfoConstants.Labels.SERVICE, "Spanner");
    baseLabels.put(
        MonitoringInfoConstants.Labels.SPANNER_PROJECT_ID, resolveSpannerProjectId(config));
    baseLabels.put(
        MonitoringInfoConstants.Labels.SPANNER_INSTANCE_ID, config.getInstanceId().get());
    baseLabels.put(
        MonitoringInfoConstants.Labels.SPANNER_DATABASE_ID, config.getDatabaseId().get());
    return baseLabels;
  }

  static String resolveSpannerProjectId(SpannerConfig config) {
    return config.getProjectId() == null
            || config.getProjectId().get() == null
            || config.getProjectId().get().isEmpty()
        ? SpannerOptions.getDefaultProjectId()
        : config.getProjectId().get();
  }
}