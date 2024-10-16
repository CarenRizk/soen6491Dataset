package org.apache.beam.sdk.extensions.avro.io;

import javax.annotation.Generated;
import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.ReadAllViaFileBasedSource;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_AvroIO_ReadFiles<T> extends AvroIO.ReadFiles<T> {

  private final @Nullable Class<T> recordClass;

  private final @Nullable Schema schema;

  private final boolean usesReshuffle;

  private final ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler fileExceptionHandler;

  private final long desiredBundleSizeBytes;

  private final boolean inferBeamSchema;

  private final @Nullable Coder<T> coder;

  private final AvroSource.@Nullable DatumReaderFactory<T> datumReaderFactory;

  private AutoValue_AvroIO_ReadFiles(
      @Nullable Class<T> recordClass,
      @Nullable Schema schema,
      boolean usesReshuffle,
      ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler fileExceptionHandler,
      long desiredBundleSizeBytes,
      boolean inferBeamSchema,
      @Nullable Coder<T> coder,
      AvroSource.@Nullable DatumReaderFactory<T> datumReaderFactory) {
    this.recordClass = recordClass;
    this.schema = schema;
    this.usesReshuffle = usesReshuffle;
    this.fileExceptionHandler = fileExceptionHandler;
    this.desiredBundleSizeBytes = desiredBundleSizeBytes;
    this.inferBeamSchema = inferBeamSchema;
    this.coder = coder;
    this.datumReaderFactory = datumReaderFactory;
  }

  @Override
  @Nullable Class<T> getRecordClass() {
    return recordClass;
  }

  @Override
  @Nullable Schema getSchema() {
    return schema;
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
  boolean getInferBeamSchema() {
    return inferBeamSchema;
  }

  @Override
  @Nullable Coder<T> getCoder() {
    return coder;
  }

  @Override
  AvroSource.@Nullable DatumReaderFactory<T> getDatumReaderFactory() {
    return datumReaderFactory;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof AvroIO.ReadFiles) {
      AvroIO.ReadFiles<?> that = (AvroIO.ReadFiles<?>) o;
      return (this.recordClass == null ? that.getRecordClass() == null : this.recordClass.equals(that.getRecordClass()))
          && (this.schema == null ? that.getSchema() == null : this.schema.equals(that.getSchema()))
          && this.usesReshuffle == that.getUsesReshuffle()
          && this.fileExceptionHandler.equals(that.getFileExceptionHandler())
          && this.desiredBundleSizeBytes == that.getDesiredBundleSizeBytes()
          && this.inferBeamSchema == that.getInferBeamSchema()
          && (this.coder == null ? that.getCoder() == null : this.coder.equals(that.getCoder()))
          && (this.datumReaderFactory == null ? that.getDatumReaderFactory() == null : this.datumReaderFactory.equals(that.getDatumReaderFactory()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (recordClass == null) ? 0 : recordClass.hashCode();
    h$ *= 1000003;
    h$ ^= (schema == null) ? 0 : schema.hashCode();
    h$ *= 1000003;
    h$ ^= usesReshuffle ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= fileExceptionHandler.hashCode();
    h$ *= 1000003;
    h$ ^= (int) ((desiredBundleSizeBytes >>> 32) ^ desiredBundleSizeBytes);
    h$ *= 1000003;
    h$ ^= inferBeamSchema ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= (coder == null) ? 0 : coder.hashCode();
    h$ *= 1000003;
    h$ ^= (datumReaderFactory == null) ? 0 : datumReaderFactory.hashCode();
    return h$;
  }

  @Override
  AvroIO.ReadFiles.Builder<T> toBuilder() {
    return new AutoValue_AvroIO_ReadFiles.Builder<T>(this);
  }

  static final class Builder<T> extends AvroIO.ReadFiles.Builder<T> {
    private @Nullable Class<T> recordClass;
    private @Nullable Schema schema;
    private boolean usesReshuffle;
    private ReadAllViaFileBasedSource.@Nullable ReadFileRangesFnExceptionHandler fileExceptionHandler;
    private long desiredBundleSizeBytes;
    private boolean inferBeamSchema;
    private @Nullable Coder<T> coder;
    private AvroSource.@Nullable DatumReaderFactory<T> datumReaderFactory;
    private byte set$0;
    Builder() {
    }
    Builder(AvroIO.ReadFiles<T> source) {
      this.recordClass = source.getRecordClass();
      this.schema = source.getSchema();
      this.usesReshuffle = source.getUsesReshuffle();
      this.fileExceptionHandler = source.getFileExceptionHandler();
      this.desiredBundleSizeBytes = source.getDesiredBundleSizeBytes();
      this.inferBeamSchema = source.getInferBeamSchema();
      this.coder = source.getCoder();
      this.datumReaderFactory = source.getDatumReaderFactory();
      set$0 = (byte) 7;
    }
    @Override
    AvroIO.ReadFiles.Builder<T> setRecordClass(Class<T> recordClass) {
      this.recordClass = recordClass;
      return this;
    }
    @Override
    AvroIO.ReadFiles.Builder<T> setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }
    @Override
    AvroIO.ReadFiles.Builder<T> setUsesReshuffle(boolean usesReshuffle) {
      this.usesReshuffle = usesReshuffle;
      set$0 |= (byte) 1;
      return this;
    }
    @Override
    AvroIO.ReadFiles.Builder<T> setFileExceptionHandler(ReadAllViaFileBasedSource.ReadFileRangesFnExceptionHandler fileExceptionHandler) {
      if (fileExceptionHandler == null) {
        throw new NullPointerException("Null fileExceptionHandler");
      }
      this.fileExceptionHandler = fileExceptionHandler;
      return this;
    }
    @Override
    AvroIO.ReadFiles.Builder<T> setDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      this.desiredBundleSizeBytes = desiredBundleSizeBytes;
      set$0 |= (byte) 2;
      return this;
    }
    @Override
    AvroIO.ReadFiles.Builder<T> setInferBeamSchema(boolean inferBeamSchema) {
      this.inferBeamSchema = inferBeamSchema;
      set$0 |= (byte) 4;
      return this;
    }
    @Override
    AvroIO.ReadFiles.Builder<T> setCoder(Coder<T> coder) {
      this.coder = coder;
      return this;
    }
    @Override
    AvroIO.ReadFiles.Builder<T> setDatumReaderFactory(AvroSource.DatumReaderFactory<T> datumReaderFactory) {
      this.datumReaderFactory = datumReaderFactory;
      return this;
    }
    @Override
    AvroIO.ReadFiles<T> build() {
      if (set$0 != 7
          || this.fileExceptionHandler == null) {
        StringBuilder missing = new StringBuilder();
        if ((set$0 & 1) == 0) {
          missing.append(" usesReshuffle");
        }
        if (this.fileExceptionHandler == null) {
          missing.append(" fileExceptionHandler");
        }
        if ((set$0 & 2) == 0) {
          missing.append(" desiredBundleSizeBytes");
        }
        if ((set$0 & 4) == 0) {
          missing.append(" inferBeamSchema");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_AvroIO_ReadFiles<T>(
          this.recordClass,
          this.schema,
          this.usesReshuffle,
          this.fileExceptionHandler,
          this.desiredBundleSizeBytes,
          this.inferBeamSchema,
          this.coder,
          this.datumReaderFactory);
    }
  }

}
