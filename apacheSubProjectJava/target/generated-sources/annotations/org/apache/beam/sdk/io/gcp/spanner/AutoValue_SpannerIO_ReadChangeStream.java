package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Options;
import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_SpannerIO_ReadChangeStream extends SpannerIO.ReadChangeStream {

  private final SpannerConfig spannerConfig;

  private final String changeStreamName;

  private final @Nullable String metadataInstance;

  private final @Nullable String metadataDatabase;

  private final @Nullable String metadataTable;

  private final Timestamp inclusiveStartAt;

  private final @Nullable Timestamp inclusiveEndAt;

  private final Options.@Nullable RpcPriority rpcPriority;

  private final @Nullable Double traceSampleProbability;

  private AutoValue_SpannerIO_ReadChangeStream(
      SpannerConfig spannerConfig,
      String changeStreamName,
      @Nullable String metadataInstance,
      @Nullable String metadataDatabase,
      @Nullable String metadataTable,
      Timestamp inclusiveStartAt,
      @Nullable Timestamp inclusiveEndAt,
      Options.@Nullable RpcPriority rpcPriority,
      @Nullable Double traceSampleProbability) {
    this.spannerConfig = spannerConfig;
    this.changeStreamName = changeStreamName;
    this.metadataInstance = metadataInstance;
    this.metadataDatabase = metadataDatabase;
    this.metadataTable = metadataTable;
    this.inclusiveStartAt = inclusiveStartAt;
    this.inclusiveEndAt = inclusiveEndAt;
    this.rpcPriority = rpcPriority;
    this.traceSampleProbability = traceSampleProbability;
  }

  @Override
  SpannerConfig getSpannerConfig() {
    return spannerConfig;
  }

  @Override
  String getChangeStreamName() {
    return changeStreamName;
  }

  @Override
  @Nullable String getMetadataInstance() {
    return metadataInstance;
  }

  @Override
  @Nullable String getMetadataDatabase() {
    return metadataDatabase;
  }

  @Override
  @Nullable String getMetadataTable() {
    return metadataTable;
  }

  @Override
  Timestamp getInclusiveStartAt() {
    return inclusiveStartAt;
  }

  @Override
  @Nullable Timestamp getInclusiveEndAt() {
    return inclusiveEndAt;
  }

  @Override
  Options.@Nullable RpcPriority getRpcPriority() {
    return rpcPriority;
  }

  @Deprecated
  @Override
  @Nullable Double getTraceSampleProbability() {
    return traceSampleProbability;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof SpannerIO.ReadChangeStream) {
      SpannerIO.ReadChangeStream that = (SpannerIO.ReadChangeStream) o;
      return this.spannerConfig.equals(that.getSpannerConfig())
          && this.changeStreamName.equals(that.getChangeStreamName())
          && (this.metadataInstance == null ? that.getMetadataInstance() == null : this.metadataInstance.equals(that.getMetadataInstance()))
          && (this.metadataDatabase == null ? that.getMetadataDatabase() == null : this.metadataDatabase.equals(that.getMetadataDatabase()))
          && (this.metadataTable == null ? that.getMetadataTable() == null : this.metadataTable.equals(that.getMetadataTable()))
          && this.inclusiveStartAt.equals(that.getInclusiveStartAt())
          && (this.inclusiveEndAt == null ? that.getInclusiveEndAt() == null : this.inclusiveEndAt.equals(that.getInclusiveEndAt()))
          && (this.rpcPriority == null ? that.getRpcPriority() == null : this.rpcPriority.equals(that.getRpcPriority()))
          && (this.traceSampleProbability == null ? that.getTraceSampleProbability() == null : this.traceSampleProbability.equals(that.getTraceSampleProbability()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= spannerConfig.hashCode();
    h$ *= 1000003;
    h$ ^= changeStreamName.hashCode();
    h$ *= 1000003;
    h$ ^= (metadataInstance == null) ? 0 : metadataInstance.hashCode();
    h$ *= 1000003;
    h$ ^= (metadataDatabase == null) ? 0 : metadataDatabase.hashCode();
    h$ *= 1000003;
    h$ ^= (metadataTable == null) ? 0 : metadataTable.hashCode();
    h$ *= 1000003;
    h$ ^= inclusiveStartAt.hashCode();
    h$ *= 1000003;
    h$ ^= (inclusiveEndAt == null) ? 0 : inclusiveEndAt.hashCode();
    h$ *= 1000003;
    h$ ^= (rpcPriority == null) ? 0 : rpcPriority.hashCode();
    h$ *= 1000003;
    h$ ^= (traceSampleProbability == null) ? 0 : traceSampleProbability.hashCode();
    return h$;
  }

  @Override
  SpannerIO.ReadChangeStream.Builder toBuilder() {
    return new AutoValue_SpannerIO_ReadChangeStream.Builder(this);
  }

  static final class Builder extends SpannerIO.ReadChangeStream.Builder {
    private @Nullable SpannerConfig spannerConfig;
    private @Nullable String changeStreamName;
    private @Nullable String metadataInstance;
    private @Nullable String metadataDatabase;
    private @Nullable String metadataTable;
    private @Nullable Timestamp inclusiveStartAt;
    private @Nullable Timestamp inclusiveEndAt;
    private Options.@Nullable RpcPriority rpcPriority;
    private @Nullable Double traceSampleProbability;
    Builder() {
    }
    Builder(SpannerIO.ReadChangeStream source) {
      this.spannerConfig = source.getSpannerConfig();
      this.changeStreamName = source.getChangeStreamName();
      this.metadataInstance = source.getMetadataInstance();
      this.metadataDatabase = source.getMetadataDatabase();
      this.metadataTable = source.getMetadataTable();
      this.inclusiveStartAt = source.getInclusiveStartAt();
      this.inclusiveEndAt = source.getInclusiveEndAt();
      this.rpcPriority = source.getRpcPriority();
      this.traceSampleProbability = source.getTraceSampleProbability();
    }
    @Override
    SpannerIO.ReadChangeStream.Builder setSpannerConfig(SpannerConfig spannerConfig) {
      if (spannerConfig == null) {
        throw new NullPointerException("Null spannerConfig");
      }
      this.spannerConfig = spannerConfig;
      return this;
    }
    @Override
    SpannerIO.ReadChangeStream.Builder setChangeStreamName(String changeStreamName) {
      if (changeStreamName == null) {
        throw new NullPointerException("Null changeStreamName");
      }
      this.changeStreamName = changeStreamName;
      return this;
    }
    @Override
    SpannerIO.ReadChangeStream.Builder setMetadataInstance(String metadataInstance) {
      this.metadataInstance = metadataInstance;
      return this;
    }
    @Override
    SpannerIO.ReadChangeStream.Builder setMetadataDatabase(String metadataDatabase) {
      this.metadataDatabase = metadataDatabase;
      return this;
    }
    @Override
    SpannerIO.ReadChangeStream.Builder setMetadataTable(String metadataTable) {
      this.metadataTable = metadataTable;
      return this;
    }
    @Override
    SpannerIO.ReadChangeStream.Builder setInclusiveStartAt(Timestamp inclusiveStartAt) {
      if (inclusiveStartAt == null) {
        throw new NullPointerException("Null inclusiveStartAt");
      }
      this.inclusiveStartAt = inclusiveStartAt;
      return this;
    }
    @Override
    SpannerIO.ReadChangeStream.Builder setInclusiveEndAt(Timestamp inclusiveEndAt) {
      this.inclusiveEndAt = inclusiveEndAt;
      return this;
    }
    @Override
    SpannerIO.ReadChangeStream.Builder setRpcPriority(Options.RpcPriority rpcPriority) {
      this.rpcPriority = rpcPriority;
      return this;
    }
    @Override
    SpannerIO.ReadChangeStream.Builder setTraceSampleProbability(Double traceSampleProbability) {
      this.traceSampleProbability = traceSampleProbability;
      return this;
    }
    @Override
    SpannerIO.ReadChangeStream build() {
      if (this.spannerConfig == null
          || this.changeStreamName == null
          || this.inclusiveStartAt == null) {
        StringBuilder missing = new StringBuilder();
        if (this.spannerConfig == null) {
          missing.append(" spannerConfig");
        }
        if (this.changeStreamName == null) {
          missing.append(" changeStreamName");
        }
        if (this.inclusiveStartAt == null) {
          missing.append(" inclusiveStartAt");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_SpannerIO_ReadChangeStream(
          this.spannerConfig,
          this.changeStreamName,
          this.metadataInstance,
          this.metadataDatabase,
          this.metadataTable,
          this.inclusiveStartAt,
          this.inclusiveEndAt,
          this.rpcPriority,
          this.traceSampleProbability);
    }
  }

}
