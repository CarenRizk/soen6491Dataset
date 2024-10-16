package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.TimestampBound;
import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_SpannerIO_CreateTransaction extends SpannerIO.CreateTransaction {

  private final SpannerConfig spannerConfig;

  private final @Nullable TimestampBound timestampBound;

  private AutoValue_SpannerIO_CreateTransaction(
      SpannerConfig spannerConfig,
      @Nullable TimestampBound timestampBound) {
    this.spannerConfig = spannerConfig;
    this.timestampBound = timestampBound;
  }

  @Override
  SpannerConfig getSpannerConfig() {
    return spannerConfig;
  }

  @Override
  @Nullable TimestampBound getTimestampBound() {
    return timestampBound;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof SpannerIO.CreateTransaction) {
      SpannerIO.CreateTransaction that = (SpannerIO.CreateTransaction) o;
      return this.spannerConfig.equals(that.getSpannerConfig())
          && (this.timestampBound == null ? that.getTimestampBound() == null : this.timestampBound.equals(that.getTimestampBound()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= spannerConfig.hashCode();
    h$ *= 1000003;
    h$ ^= (timestampBound == null) ? 0 : timestampBound.hashCode();
    return h$;
  }

  @Override
  SpannerIO.CreateTransaction.Builder toBuilder() {
    return new AutoValue_SpannerIO_CreateTransaction.Builder(this);
  }

  static final class Builder extends SpannerIO.CreateTransaction.Builder {
    private @Nullable SpannerConfig spannerConfig;
    private @Nullable TimestampBound timestampBound;
    Builder() {
    }
    Builder(SpannerIO.CreateTransaction source) {
      this.spannerConfig = source.getSpannerConfig();
      this.timestampBound = source.getTimestampBound();
    }
    @Override
    public SpannerIO.CreateTransaction.Builder setSpannerConfig(SpannerConfig spannerConfig) {
      if (spannerConfig == null) {
        throw new NullPointerException("Null spannerConfig");
      }
      this.spannerConfig = spannerConfig;
      return this;
    }
    @Override
    public SpannerIO.CreateTransaction.Builder setTimestampBound(TimestampBound timestampBound) {
      this.timestampBound = timestampBound;
      return this;
    }
    @Override
    public SpannerIO.CreateTransaction build() {
      if (this.spannerConfig == null) {
        String missing = " spannerConfig";
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_SpannerIO_CreateTransaction(
          this.spannerConfig,
          this.timestampBound);
    }
  }

}
