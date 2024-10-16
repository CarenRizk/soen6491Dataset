package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.TimestampBound;
import javax.annotation.Generated;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_SpannerIO_ReadAll extends SpannerIO.ReadAll {

  private final SpannerConfig spannerConfig;

  private final @Nullable PCollectionView<Transaction> transaction;

  private final @Nullable TimestampBound timestampBound;

  private final Boolean batching;

  private AutoValue_SpannerIO_ReadAll(
      SpannerConfig spannerConfig,
      @Nullable PCollectionView<Transaction> transaction,
      @Nullable TimestampBound timestampBound,
      Boolean batching) {
    this.spannerConfig = spannerConfig;
    this.transaction = transaction;
    this.timestampBound = timestampBound;
    this.batching = batching;
  }

  @Override
  SpannerConfig getSpannerConfig() {
    return spannerConfig;
  }

  @Override
  @Nullable PCollectionView<Transaction> getTransaction() {
    return transaction;
  }

  @Override
  @Nullable TimestampBound getTimestampBound() {
    return timestampBound;
  }

  @Override
  Boolean getBatching() {
    return batching;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof SpannerIO.ReadAll) {
      SpannerIO.ReadAll that = (SpannerIO.ReadAll) o;
      return this.spannerConfig.equals(that.getSpannerConfig())
          && (this.transaction == null ? that.getTransaction() == null : this.transaction.equals(that.getTransaction()))
          && (this.timestampBound == null ? that.getTimestampBound() == null : this.timestampBound.equals(that.getTimestampBound()))
          && this.batching.equals(that.getBatching());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= spannerConfig.hashCode();
    h$ *= 1000003;
    h$ ^= (transaction == null) ? 0 : transaction.hashCode();
    h$ *= 1000003;
    h$ ^= (timestampBound == null) ? 0 : timestampBound.hashCode();
    h$ *= 1000003;
    h$ ^= batching.hashCode();
    return h$;
  }

  @Override
  SpannerIO.ReadAll.Builder toBuilder() {
    return new AutoValue_SpannerIO_ReadAll.Builder(this);
  }

  static final class Builder extends SpannerIO.ReadAll.Builder {
    private @Nullable SpannerConfig spannerConfig;
    private @Nullable PCollectionView<Transaction> transaction;
    private @Nullable TimestampBound timestampBound;
    private @Nullable Boolean batching;
    Builder() {
    }
    Builder(SpannerIO.ReadAll source) {
      this.spannerConfig = source.getSpannerConfig();
      this.transaction = source.getTransaction();
      this.timestampBound = source.getTimestampBound();
      this.batching = source.getBatching();
    }
    @Override
    SpannerIO.ReadAll.Builder setSpannerConfig(SpannerConfig spannerConfig) {
      if (spannerConfig == null) {
        throw new NullPointerException("Null spannerConfig");
      }
      this.spannerConfig = spannerConfig;
      return this;
    }
    @Override
    SpannerIO.ReadAll.Builder setTransaction(PCollectionView<Transaction> transaction) {
      this.transaction = transaction;
      return this;
    }
    @Override
    SpannerIO.ReadAll.Builder setTimestampBound(TimestampBound timestampBound) {
      this.timestampBound = timestampBound;
      return this;
    }
    @Override
    SpannerIO.ReadAll.Builder setBatching(Boolean batching) {
      if (batching == null) {
        throw new NullPointerException("Null batching");
      }
      this.batching = batching;
      return this;
    }
    @Override
    SpannerIO.ReadAll build() {
      if (this.spannerConfig == null
          || this.batching == null) {
        StringBuilder missing = new StringBuilder();
        if (this.spannerConfig == null) {
          missing.append(" spannerConfig");
        }
        if (this.batching == null) {
          missing.append(" batching");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_SpannerIO_ReadAll(
          this.spannerConfig,
          this.transaction,
          this.timestampBound,
          this.batching);
    }
  }

}
