package org.apache.beam.sdk.extensions.avro.io;

import javax.annotation.Generated;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.ReadAllViaFileBasedSource;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_AvroIO_ParseFiles<T> extends AvroIO.ParseFiles<T> {

  private final SerializableFunction<GenericRecord, T> parseFn;

  private final @Nullable Coder<T> coder;

  private final boolean usesReshuffle;

  private final ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler fileExceptionHandler;

  private final long desiredBundleSizeBytes;

  private AutoValue_AvroIO_ParseFiles(
      SerializableFunction<GenericRecord, T> parseFn,
      @Nullable Coder<T> coder,
      boolean usesReshuffle,
      ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler fileExceptionHandler,
      long desiredBundleSizeBytes) {
    this.parseFn = parseFn;
    this.coder = coder;
    this.usesReshuffle = usesReshuffle;
    this.fileExceptionHandler = fileExceptionHandler;
    this.desiredBundleSizeBytes = desiredBundleSizeBytes;
  }

  @Override
  SerializableFunction<GenericRecord, T> getParseFn() {
    return parseFn;
  }

  @Override
  @Nullable Coder<T> getCoder() {
    return coder;
  }

  @Override
  boolean getUsesReshuffle() {
    return usesReshuffle;
  }

  @Override
  ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler getFileExceptionHandler() {
    return fileExceptionHandler;
  }

  @Override
  long getDesiredBundleSizeBytes() {
    return desiredBundleSizeBytes;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof AvroIO.ParseFiles) {
      AvroIO.ParseFiles<?> that = (AvroIO.ParseFiles<?>) o;
      return this.parseFn.equals(that.getParseFn())
          && (this.coder == null ? that.getCoder() == null : this.coder.equals(that.getCoder()))
          && this.usesReshuffle == that.getUsesReshuffle()
          && this.fileExceptionHandler.equals(that.getFileExceptionHandler())
          && this.desiredBundleSizeBytes == that.getDesiredBundleSizeBytes();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= parseFn.hashCode();
    h$ *= 1000003;
    h$ ^= (coder == null) ? 0 : coder.hashCode();
    h$ *= 1000003;
    h$ ^= usesReshuffle ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= fileExceptionHandler.hashCode();
    h$ *= 1000003;
    h$ ^= (int) ((desiredBundleSizeBytes >>> 32) ^ desiredBundleSizeBytes);
    return h$;
  }

  @Override
  AvroIO.ParseFiles.Builder<T> toBuilder() {
    return new AutoValue_AvroIO_ParseFiles.Builder<T>(this);
  }

  static final class Builder<T> extends AvroIO.ParseFiles.Builder<T> {
    private @Nullable SerializableFunction<GenericRecord, T> parseFn;
    private @Nullable Coder<T> coder;
    private boolean usesReshuffle;
    private ReadAllViaFileBasedSource.@Nullable ReadFileRangesFnExceptionHandler fileExceptionHandler;
    private long desiredBundleSizeBytes;
    private byte set$0;
    Builder() {
    }
    Builder(AvroIO.ParseFiles<T> source) {
      this.parseFn = source.getParseFn();
      this.coder = source.getCoder();
      this.usesReshuffle = source.getUsesReshuffle();
      this.fileExceptionHandler = source.getFileExceptionHandler();
      this.desiredBundleSizeBytes = source.getDesiredBundleSizeBytes();
      set$0 = (byte) 3;
    }
    @Override
    AvroIO.ParseFiles.Builder<T> setParseFn(SerializableFunction<GenericRecord, T> parseFn) {
      if (parseFn == null) {
        throw new NullPointerException("Null parseFn");
      }
      this.parseFn = parseFn;
      return this;
    }
    @Override
    AvroIO.ParseFiles.Builder<T> setCoder(Coder<T> coder) {
      this.coder = coder;
      return this;
    }
    @Override
    AvroIO.ParseFiles.Builder<T> setUsesReshuffle(boolean usesReshuffle) {
      this.usesReshuffle = usesReshuffle;
      set$0 |= (byte) 1;
      return this;
    }
    @Override
    AvroIO.ParseFiles.Builder<T> setFileExceptionHandler(ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler fileExceptionHandler) {
      if (fileExceptionHandler == null) {
        throw new NullPointerException("Null fileExceptionHandler");
      }
      this.fileExceptionHandler = fileExceptionHandler;
      return this;
    }
    @Override
    AvroIO.ParseFiles.Builder<T> setDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      this.desiredBundleSizeBytes = desiredBundleSizeBytes;
      set$0 |= (byte) 2;
      return this;
    }
    @Override
    AvroIO.ParseFiles<T> build() {
      if (set$0 != 3
          || this.parseFn == null
          || this.fileExceptionHandler == null) {
        StringBuilder missing = new StringBuilder();
        if (this.parseFn == null) {
          missing.append(" parseFn");
        }
        if ((set$0 & 1) == 0) {
          missing.append(" usesReshuffle");
        }
        if (this.fileExceptionHandler == null) {
          missing.append(" fileExceptionHandler");
        }
        if ((set$0 & 2) == 0) {
          missing.append(" desiredBundleSizeBytes");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_AvroIO_ParseFiles<T>(
          this.parseFn,
          this.coder,
          this.usesReshuffle,
          this.fileExceptionHandler,
          this.desiredBundleSizeBytes);
    }
  }

}
