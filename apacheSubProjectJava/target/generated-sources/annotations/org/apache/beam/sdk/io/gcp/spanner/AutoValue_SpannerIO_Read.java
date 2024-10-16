package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import javax.annotation.Generated;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_SpannerIO_Read extends SpannerIO.Read {

  private final SpannerConfig spannerConfig;

  private final ReadOperation readOperation;

  private final @Nullable TimestampBound timestampBound;

  private final @Nullable PCollectionView<Transaction> transaction;

  private final @Nullable PartitionOptions partitionOptions;

  private final Boolean batching;

  private final @Nullable TypeDescriptor<Struct> typeDescriptor;

  private final SpannerIO.Read.@Nullable ToBeamRowFunction toBeamRowFn;

  private final SpannerIO.Read.@Nullable FromBeamRowFunction fromBeamRowFn;

  private AutoValue_SpannerIO_Read(
      SpannerConfig spannerConfig,
      ReadOperation readOperation,
      @Nullable TimestampBound timestampBound,
      @Nullable PCollectionView<Transaction> transaction,
      @Nullable PartitionOptions partitionOptions,
      Boolean batching,
      @Nullable TypeDescriptor<Struct> typeDescriptor,
      SpannerIO.Read.@Nullable ToBeamRowFunction toBeamRowFn,
      SpannerIO.Read.@Nullable FromBeamRowFunction fromBeamRowFn) {
    this.spannerConfig = spannerConfig;
    this.readOperation = readOperation;
    this.timestampBound = timestampBound;
    this.transaction = transaction;
    this.partitionOptions = partitionOptions;
    this.batching = batching;
    this.typeDescriptor = typeDescriptor;
    this.toBeamRowFn = toBeamRowFn;
    this.fromBeamRowFn = fromBeamRowFn;
  }

  @Override
  SpannerConfig getSpannerConfig() {
    return spannerConfig;
  }

  @Override
  ReadOperation getReadOperation() {
    return readOperation;
  }

  @Override
  @Nullable TimestampBound getTimestampBound() {
    return timestampBound;
  }

  @Override
  @Nullable PCollectionView<Transaction> getTransaction() {
    return transaction;
  }

  @Override
  @Nullable PartitionOptions getPartitionOptions() {
    return partitionOptions;
  }

  @Override
  Boolean getBatching() {
    return batching;
  }

  @Override
  @Nullable TypeDescriptor<Struct> getTypeDescriptor() {
    return typeDescriptor;
  }

  @Override
  SpannerIO.Read.@Nullable ToBeamRowFunction getToBeamRowFn() {
    return toBeamRowFn;
  }

  @Override
  SpannerIO.Read.@Nullable FromBeamRowFunction getFromBeamRowFn() {
    return fromBeamRowFn;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof SpannerIO.Read) {
      SpannerIO.Read that = (SpannerIO.Read) o;
      return this.spannerConfig.equals(that.getSpannerConfig())
          && this.readOperation.equals(that.getReadOperation())
          && (this.timestampBound == null ? that.getTimestampBound() == null : this.timestampBound.equals(that.getTimestampBound()))
          && (this.transaction == null ? that.getTransaction() == null : this.transaction.equals(that.getTransaction()))
          && (this.partitionOptions == null ? that.getPartitionOptions() == null : this.partitionOptions.equals(that.getPartitionOptions()))
          && this.batching.equals(that.getBatching())
          && (this.typeDescriptor == null ? that.getTypeDescriptor() == null : this.typeDescriptor.equals(that.getTypeDescriptor()))
          && (this.toBeamRowFn == null ? that.getToBeamRowFn() == null : this.toBeamRowFn.equals(that.getToBeamRowFn()))
          && (this.fromBeamRowFn == null ? that.getFromBeamRowFn() == null : this.fromBeamRowFn.equals(that.getFromBeamRowFn()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= spannerConfig.hashCode();
    h$ *= 1000003;
    h$ ^= readOperation.hashCode();
    h$ *= 1000003;
    h$ ^= (timestampBound == null) ? 0 : timestampBound.hashCode();
    h$ *= 1000003;
    h$ ^= (transaction == null) ? 0 : transaction.hashCode();
    h$ *= 1000003;
    h$ ^= (partitionOptions == null) ? 0 : partitionOptions.hashCode();
    h$ *= 1000003;
    h$ ^= batching.hashCode();
    h$ *= 1000003;
    h$ ^= (typeDescriptor == null) ? 0 : typeDescriptor.hashCode();
    h$ *= 1000003;
    h$ ^= (toBeamRowFn == null) ? 0 : toBeamRowFn.hashCode();
    h$ *= 1000003;
    h$ ^= (fromBeamRowFn == null) ? 0 : fromBeamRowFn.hashCode();
    return h$;
  }

  @Override
  SpannerIO.Read.Builder toBuilder() {
    return new AutoValue_SpannerIO_Read.Builder(this);
  }

  static final class Builder extends SpannerIO.Read.Builder {
    private @Nullable SpannerConfig spannerConfig;
    private @Nullable ReadOperation readOperation;
    private @Nullable TimestampBound timestampBound;
    private @Nullable PCollectionView<Transaction> transaction;
    private @Nullable PartitionOptions partitionOptions;
    private @Nullable Boolean batching;
    private @Nullable TypeDescriptor<Struct> typeDescriptor;
    private SpannerIO.Read.@Nullable ToBeamRowFunction toBeamRowFn;
    private SpannerIO.Read.@Nullable FromBeamRowFunction fromBeamRowFn;
    Builder() {
    }
    Builder(SpannerIO.Read source) {
      this.spannerConfig = source.getSpannerConfig();
      this.readOperation = source.getReadOperation();
      this.timestampBound = source.getTimestampBound();
      this.transaction = source.getTransaction();
      this.partitionOptions = source.getPartitionOptions();
      this.batching = source.getBatching();
      this.typeDescriptor = source.getTypeDescriptor();
      this.toBeamRowFn = source.getToBeamRowFn();
      this.fromBeamRowFn = source.getFromBeamRowFn();
    }
    @Override
    SpannerIO.Read.Builder setSpannerConfig(SpannerConfig spannerConfig) {
      if (spannerConfig == null) {
        throw new NullPointerException("Null spannerConfig");
      }
      this.spannerConfig = spannerConfig;
      return this;
    }
    @Override
    SpannerIO.Read.Builder setReadOperation(ReadOperation readOperation) {
      if (readOperation == null) {
        throw new NullPointerException("Null readOperation");
      }
      this.readOperation = readOperation;
      return this;
    }
    @Override
    SpannerIO.Read.Builder setTimestampBound(TimestampBound timestampBound) {
      this.timestampBound = timestampBound;
      return this;
    }
    @Override
    SpannerIO.Read.Builder setTransaction(PCollectionView<Transaction> transaction) {
      this.transaction = transaction;
      return this;
    }
    @Override
    SpannerIO.Read.Builder setPartitionOptions(PartitionOptions partitionOptions) {
      this.partitionOptions = partitionOptions;
      return this;
    }
    @Override
    SpannerIO.Read.Builder setBatching(Boolean batching) {
      if (batching == null) {
        throw new NullPointerException("Null batching");
      }
      this.batching = batching;
      return this;
    }
    @Override
    SpannerIO.Read.Builder setTypeDescriptor(TypeDescriptor<Struct> typeDescriptor) {
      this.typeDescriptor = typeDescriptor;
      return this;
    }
    @Override
    SpannerIO.Read.Builder setToBeamRowFn(SpannerIO.Read.ToBeamRowFunction toBeamRowFn) {
      this.toBeamRowFn = toBeamRowFn;
      return this;
    }
    @Override
    SpannerIO.Read.Builder setFromBeamRowFn(SpannerIO.Read.FromBeamRowFunction fromBeamRowFn) {
      this.fromBeamRowFn = fromBeamRowFn;
      return this;
    }
    @Override
    SpannerIO.Read build() {
      if (this.spannerConfig == null
          || this.readOperation == null
          || this.batching == null) {
        StringBuilder missing = new StringBuilder();
        if (this.spannerConfig == null) {
          missing.append(" spannerConfig");
        }
        if (this.readOperation == null) {
          missing.append(" readOperation");
        }
        if (this.batching == null) {
          missing.append(" batching");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_SpannerIO_Read(
          this.spannerConfig,
          this.readOperation,
          this.timestampBound,
          this.transaction,
          this.partitionOptions,
          this.batching,
          this.typeDescriptor,
          this.toBeamRowFn,
          this.fromBeamRowFn);
    }
  }

}
