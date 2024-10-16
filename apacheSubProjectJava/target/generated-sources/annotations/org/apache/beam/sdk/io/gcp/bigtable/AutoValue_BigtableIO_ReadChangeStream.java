package org.apache.beam.sdk.io.gcp.bigtable;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_BigtableIO_ReadChangeStream extends BigtableIO.ReadChangeStream {

  private final BigtableConfig bigtableConfig;

  private final @Nullable String tableId;

  private final @Nullable Instant startTime;

  private final @Nullable Instant endTime;

  private final @Nullable String changeStreamName;

  private final BigtableIO.@Nullable ExistingPipelineOptions existingPipelineOptions;

  private final BigtableConfig metadataTableBigtableConfig;

  private final @Nullable String metadataTableId;

  private final @Nullable Boolean createOrUpdateMetadataTable;

  private final @Nullable Duration backlogReplicationAdjustment;

  private final @Nullable Boolean validateConfig;

  private AutoValue_BigtableIO_ReadChangeStream(
      BigtableConfig bigtableConfig,
      @Nullable String tableId,
      @Nullable Instant startTime,
      @Nullable Instant endTime,
      @Nullable String changeStreamName,
      BigtableIO.@Nullable ExistingPipelineOptions existingPipelineOptions,
      BigtableConfig metadataTableBigtableConfig,
      @Nullable String metadataTableId,
      @Nullable Boolean createOrUpdateMetadataTable,
      @Nullable Duration backlogReplicationAdjustment,
      @Nullable Boolean validateConfig) {
    this.bigtableConfig = bigtableConfig;
    this.tableId = tableId;
    this.startTime = startTime;
    this.endTime = endTime;
    this.changeStreamName = changeStreamName;
    this.existingPipelineOptions = existingPipelineOptions;
    this.metadataTableBigtableConfig = metadataTableBigtableConfig;
    this.metadataTableId = metadataTableId;
    this.createOrUpdateMetadataTable = createOrUpdateMetadataTable;
    this.backlogReplicationAdjustment = backlogReplicationAdjustment;
    this.validateConfig = validateConfig;
  }

  @Override
  BigtableConfig getBigtableConfig() {
    return bigtableConfig;
  }

  @Override
  @Nullable String getTableId() {
    return tableId;
  }

  @Override
  @Nullable Instant getStartTime() {
    return startTime;
  }

  @Override
  @Nullable Instant getEndTime() {
    return endTime;
  }

  @Override
  @Nullable String getChangeStreamName() {
    return changeStreamName;
  }

  @Override
  BigtableIO.@Nullable ExistingPipelineOptions getExistingPipelineOptions() {
    return existingPipelineOptions;
  }

  @Override
  BigtableConfig getMetadataTableBigtableConfig() {
    return metadataTableBigtableConfig;
  }

  @Override
  @Nullable String getMetadataTableId() {
    return metadataTableId;
  }

  @Override
  @Nullable Boolean getCreateOrUpdateMetadataTable() {
    return createOrUpdateMetadataTable;
  }

  @Override
  @Nullable Duration getBacklogReplicationAdjustment() {
    return backlogReplicationAdjustment;
  }

  @Override
  @Nullable Boolean getValidateConfig() {
    return validateConfig;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof BigtableIO.ReadChangeStream) {
      BigtableIO.ReadChangeStream that = (BigtableIO.ReadChangeStream) o;
      return this.bigtableConfig.equals(that.getBigtableConfig())
          && (this.tableId == null ? that.getTableId() == null : this.tableId.equals(that.getTableId()))
          && (this.startTime == null ? that.getStartTime() == null : this.startTime.equals(that.getStartTime()))
          && (this.endTime == null ? that.getEndTime() == null : this.endTime.equals(that.getEndTime()))
          && (this.changeStreamName == null ? that.getChangeStreamName() == null : this.changeStreamName.equals(that.getChangeStreamName()))
          && (this.existingPipelineOptions == null ? that.getExistingPipelineOptions() == null : this.existingPipelineOptions.equals(that.getExistingPipelineOptions()))
          && this.metadataTableBigtableConfig.equals(that.getMetadataTableBigtableConfig())
          && (this.metadataTableId == null ? that.getMetadataTableId() == null : this.metadataTableId.equals(that.getMetadataTableId()))
          && (this.createOrUpdateMetadataTable == null ? that.getCreateOrUpdateMetadataTable() == null : this.createOrUpdateMetadataTable.equals(that.getCreateOrUpdateMetadataTable()))
          && (this.backlogReplicationAdjustment == null ? that.getBacklogReplicationAdjustment() == null : this.backlogReplicationAdjustment.equals(that.getBacklogReplicationAdjustment()))
          && (this.validateConfig == null ? that.getValidateConfig() == null : this.validateConfig.equals(that.getValidateConfig()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= bigtableConfig.hashCode();
    h$ *= 1000003;
    h$ ^= (tableId == null) ? 0 : tableId.hashCode();
    h$ *= 1000003;
    h$ ^= (startTime == null) ? 0 : startTime.hashCode();
    h$ *= 1000003;
    h$ ^= (endTime == null) ? 0 : endTime.hashCode();
    h$ *= 1000003;
    h$ ^= (changeStreamName == null) ? 0 : changeStreamName.hashCode();
    h$ *= 1000003;
    h$ ^= (existingPipelineOptions == null) ? 0 : existingPipelineOptions.hashCode();
    h$ *= 1000003;
    h$ ^= metadataTableBigtableConfig.hashCode();
    h$ *= 1000003;
    h$ ^= (metadataTableId == null) ? 0 : metadataTableId.hashCode();
    h$ *= 1000003;
    h$ ^= (createOrUpdateMetadataTable == null) ? 0 : createOrUpdateMetadataTable.hashCode();
    h$ *= 1000003;
    h$ ^= (backlogReplicationAdjustment == null) ? 0 : backlogReplicationAdjustment.hashCode();
    h$ *= 1000003;
    h$ ^= (validateConfig == null) ? 0 : validateConfig.hashCode();
    return h$;
  }

  @Override
  BigtableIO.ReadChangeStream.Builder toBuilder() {
    return new AutoValue_BigtableIO_ReadChangeStream.Builder(this);
  }

  static final class Builder extends BigtableIO.ReadChangeStream.Builder {
    private @Nullable BigtableConfig bigtableConfig;
    private @Nullable String tableId;
    private @Nullable Instant startTime;
    private @Nullable Instant endTime;
    private @Nullable String changeStreamName;
    private BigtableIO.@Nullable ExistingPipelineOptions existingPipelineOptions;
    private @Nullable BigtableConfig metadataTableBigtableConfig;
    private @Nullable String metadataTableId;
    private @Nullable Boolean createOrUpdateMetadataTable;
    private @Nullable Duration backlogReplicationAdjustment;
    private @Nullable Boolean validateConfig;
    Builder() {
    }
    Builder(BigtableIO.ReadChangeStream source) {
      this.bigtableConfig = source.getBigtableConfig();
      this.tableId = source.getTableId();
      this.startTime = source.getStartTime();
      this.endTime = source.getEndTime();
      this.changeStreamName = source.getChangeStreamName();
      this.existingPipelineOptions = source.getExistingPipelineOptions();
      this.metadataTableBigtableConfig = source.getMetadataTableBigtableConfig();
      this.metadataTableId = source.getMetadataTableId();
      this.createOrUpdateMetadataTable = source.getCreateOrUpdateMetadataTable();
      this.backlogReplicationAdjustment = source.getBacklogReplicationAdjustment();
      this.validateConfig = source.getValidateConfig();
    }
    @Override
    BigtableIO.ReadChangeStream.Builder setBigtableConfig(BigtableConfig bigtableConfig) {
      if (bigtableConfig == null) {
        throw new NullPointerException("Null bigtableConfig");
      }
      this.bigtableConfig = bigtableConfig;
      return this;
    }
    @Override
    BigtableIO.ReadChangeStream.Builder setTableId(String tableId) {
      this.tableId = tableId;
      return this;
    }
    @Override
    BigtableIO.ReadChangeStream.Builder setStartTime(Instant startTime) {
      this.startTime = startTime;
      return this;
    }
    @Override
    BigtableIO.ReadChangeStream.Builder setEndTime(Instant endTime) {
      this.endTime = endTime;
      return this;
    }
    @Override
    BigtableIO.ReadChangeStream.Builder setChangeStreamName(String changeStreamName) {
      this.changeStreamName = changeStreamName;
      return this;
    }
    @Override
    BigtableIO.ReadChangeStream.Builder setExistingPipelineOptions(BigtableIO.ExistingPipelineOptions existingPipelineOptions) {
      this.existingPipelineOptions = existingPipelineOptions;
      return this;
    }
    @Override
    BigtableIO.ReadChangeStream.Builder setMetadataTableBigtableConfig(BigtableConfig metadataTableBigtableConfig) {
      if (metadataTableBigtableConfig == null) {
        throw new NullPointerException("Null metadataTableBigtableConfig");
      }
      this.metadataTableBigtableConfig = metadataTableBigtableConfig;
      return this;
    }
    @Override
    BigtableIO.ReadChangeStream.Builder setMetadataTableId(String metadataTableId) {
      this.metadataTableId = metadataTableId;
      return this;
    }
    @Override
    BigtableIO.ReadChangeStream.Builder setCreateOrUpdateMetadataTable(boolean createOrUpdateMetadataTable) {
      this.createOrUpdateMetadataTable = createOrUpdateMetadataTable;
      return this;
    }
    @Override
    BigtableIO.ReadChangeStream.Builder setBacklogReplicationAdjustment(Duration backlogReplicationAdjustment) {
      this.backlogReplicationAdjustment = backlogReplicationAdjustment;
      return this;
    }
    @Override
    BigtableIO.ReadChangeStream.Builder setValidateConfig(boolean validateConfig) {
      this.validateConfig = validateConfig;
      return this;
    }
    @Override
    BigtableIO.ReadChangeStream build() {
      if (this.bigtableConfig == null
          || this.metadataTableBigtableConfig == null) {
        StringBuilder missing = new StringBuilder();
        if (this.bigtableConfig == null) {
          missing.append(" bigtableConfig");
        }
        if (this.metadataTableBigtableConfig == null) {
          missing.append(" metadataTableBigtableConfig");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_BigtableIO_ReadChangeStream(
          this.bigtableConfig,
          this.tableId,
          this.startTime,
          this.endTime,
          this.changeStreamName,
          this.existingPipelineOptions,
          this.metadataTableBigtableConfig,
          this.metadataTableId,
          this.createOrUpdateMetadataTable,
          this.backlogReplicationAdjustment,
          this.validateConfig);
    }
  }

}
