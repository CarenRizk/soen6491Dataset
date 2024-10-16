package org.apache.beam.sdk.io.gcp.spanner;

import com.google.cloud.spanner.Dialect;
import java.util.OptionalInt;
import javax.annotation.Generated;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_SpannerIO_Write extends SpannerIO.Write {

  private final SpannerConfig spannerConfig;

  private final long batchSizeBytes;

  private final long maxNumMutations;

  private final long maxNumRows;

  private final SpannerIO.FailureMode failureMode;

  private final @Nullable PCollection<?> schemaReadySignal;

  private final OptionalInt groupingFactor;

  private final @Nullable PCollectionView<Dialect> dialectView;

  private AutoValue_SpannerIO_Write(
      SpannerConfig spannerConfig,
      long batchSizeBytes,
      long maxNumMutations,
      long maxNumRows,
      SpannerIO.FailureMode failureMode,
      @Nullable PCollection<?> schemaReadySignal,
      OptionalInt groupingFactor,
      @Nullable PCollectionView<Dialect> dialectView) {
    this.spannerConfig = spannerConfig;
    this.batchSizeBytes = batchSizeBytes;
    this.maxNumMutations = maxNumMutations;
    this.maxNumRows = maxNumRows;
    this.failureMode = failureMode;
    this.schemaReadySignal = schemaReadySignal;
    this.groupingFactor = groupingFactor;
    this.dialectView = dialectView;
  }

  @Override
  SpannerConfig getSpannerConfig() {
    return spannerConfig;
  }

  @Override
  long getBatchSizeBytes() {
    return batchSizeBytes;
  }

  @Override
  long getMaxNumMutations() {
    return maxNumMutations;
  }

  @Override
  long getMaxNumRows() {
    return maxNumRows;
  }

  @Override
  SpannerIO.FailureMode getFailureMode() {
    return failureMode;
  }

  @Override
  @Nullable PCollection<?> getSchemaReadySignal() {
    return schemaReadySignal;
  }

  @Override
  OptionalInt getGroupingFactor() {
    return groupingFactor;
  }

  @Override
  @Nullable PCollectionView<Dialect> getDialectView() {
    return dialectView;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof SpannerIO.Write) {
      SpannerIO.Write that = (SpannerIO.Write) o;
      return this.spannerConfig.equals(that.getSpannerConfig())
          && this.batchSizeBytes == that.getBatchSizeBytes()
          && this.maxNumMutations == that.getMaxNumMutations()
          && this.maxNumRows == that.getMaxNumRows()
          && this.failureMode.equals(that.getFailureMode())
          && (this.schemaReadySignal == null ? that.getSchemaReadySignal() == null : this.schemaReadySignal.equals(that.getSchemaReadySignal()))
          && this.groupingFactor.equals(that.getGroupingFactor())
          && (this.dialectView == null ? that.getDialectView() == null : this.dialectView.equals(that.getDialectView()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= spannerConfig.hashCode();
    h$ *= 1000003;
    h$ ^= (int) ((batchSizeBytes >>> 32) ^ batchSizeBytes);
    h$ *= 1000003;
    h$ ^= (int) ((maxNumMutations >>> 32) ^ maxNumMutations);
    h$ *= 1000003;
    h$ ^= (int) ((maxNumRows >>> 32) ^ maxNumRows);
    h$ *= 1000003;
    h$ ^= failureMode.hashCode();
    h$ *= 1000003;
    h$ ^= (schemaReadySignal == null) ? 0 : schemaReadySignal.hashCode();
    h$ *= 1000003;
    h$ ^= groupingFactor.hashCode();
    h$ *= 1000003;
    h$ ^= (dialectView == null) ? 0 : dialectView.hashCode();
    return h$;
  }

  @Override
  SpannerIO.Write.Builder toBuilder() {
    return new AutoValue_SpannerIO_Write.Builder(this);
  }

  static final class Builder extends SpannerIO.Write.Builder {
    private @Nullable SpannerConfig spannerConfig;
    private long batchSizeBytes;
    private long maxNumMutations;
    private long maxNumRows;
    private SpannerIO.@Nullable FailureMode failureMode;
    private @Nullable PCollection<?> schemaReadySignal;
    private OptionalInt groupingFactor = OptionalInt.empty();
    private @Nullable PCollectionView<Dialect> dialectView;
    private byte set$0;
    Builder() {
    }
    Builder(SpannerIO.Write source) {
      this.spannerConfig = source.getSpannerConfig();
      this.batchSizeBytes = source.getBatchSizeBytes();
      this.maxNumMutations = source.getMaxNumMutations();
      this.maxNumRows = source.getMaxNumRows();
      this.failureMode = source.getFailureMode();
      this.schemaReadySignal = source.getSchemaReadySignal();
      this.groupingFactor = source.getGroupingFactor();
      this.dialectView = source.getDialectView();
      set$0 = (byte) 7;
    }
    @Override
    SpannerIO.Write.Builder setSpannerConfig(SpannerConfig spannerConfig) {
      if (spannerConfig == null) {
        throw new NullPointerException("Null spannerConfig");
      }
      this.spannerConfig = spannerConfig;
      return this;
    }
    @Override
    SpannerIO.Write.Builder setBatchSizeBytes(long batchSizeBytes) {
      this.batchSizeBytes = batchSizeBytes;
      set$0 |= (byte) 1;
      return this;
    }
    @Override
    SpannerIO.Write.Builder setMaxNumMutations(long maxNumMutations) {
      this.maxNumMutations = maxNumMutations;
      set$0 |= (byte) 2;
      return this;
    }
    @Override
    SpannerIO.Write.Builder setMaxNumRows(long maxNumRows) {
      this.maxNumRows = maxNumRows;
      set$0 |= (byte) 4;
      return this;
    }
    @Override
    SpannerIO.Write.Builder setFailureMode(SpannerIO.FailureMode failureMode) {
      if (failureMode == null) {
        throw new NullPointerException("Null failureMode");
      }
      this.failureMode = failureMode;
      return this;
    }
    @Override
    SpannerIO.Write.Builder setSchemaReadySignal(PCollection<?> schemaReadySignal) {
      this.schemaReadySignal = schemaReadySignal;
      return this;
    }
    @Override
    SpannerIO.Write.Builder setGroupingFactor(int groupingFactor) {
      this.groupingFactor = OptionalInt.of(groupingFactor);
      return this;
    }
    @Override
    SpannerIO.Write.Builder setDialectView(PCollectionView<Dialect> dialectView) {
      this.dialectView = dialectView;
      return this;
    }
    @Override
    SpannerIO.Write build() {
      if (set$0 != 7
          || this.spannerConfig == null
          || this.failureMode == null) {
        StringBuilder missing = new StringBuilder();
        if (this.spannerConfig == null) {
          missing.append(" spannerConfig");
        }
        if ((set$0 & 1) == 0) {
          missing.append(" batchSizeBytes");
        }
        if ((set$0 & 2) == 0) {
          missing.append(" maxNumMutations");
        }
        if ((set$0 & 4) == 0) {
          missing.append(" maxNumRows");
        }
        if (this.failureMode == null) {
          missing.append(" failureMode");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_SpannerIO_Write(
          this.spannerConfig,
          this.batchSizeBytes,
          this.maxNumMutations,
          this.maxNumRows,
          this.failureMode,
          this.schemaReadySignal,
          this.groupingFactor,
          this.dialectView);
    }
  }

}
