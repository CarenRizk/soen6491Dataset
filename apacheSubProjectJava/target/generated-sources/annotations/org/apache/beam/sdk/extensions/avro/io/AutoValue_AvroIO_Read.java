package org.apache.beam.sdk.extensions.avro.io;

import javax.annotation.Generated;
import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_AvroIO_Read<T> extends AvroIO.Read<T> {

  private final @Nullable ValueProvider<String> filepattern;

  private final FileIO.MatchConfiguration matchConfiguration;

  private final @Nullable Class<T> recordClass;

  private final @Nullable Schema schema;

  private final boolean inferBeamSchema;

  private final boolean hintMatchesManyFiles;

  private final @Nullable Coder<T> coder;

  private final AvroSource.@Nullable DatumReaderFactory<T> datumReaderFactory;

  private AutoValue_AvroIO_Read(
      @Nullable ValueProvider<String> filepattern,
      FileIO.MatchConfiguration matchConfiguration,
      @Nullable Class<T> recordClass,
      @Nullable Schema schema,
      boolean inferBeamSchema,
      boolean hintMatchesManyFiles,
      @Nullable Coder<T> coder,
      AvroSource.@Nullable DatumReaderFactory<T> datumReaderFactory) {
    this.filepattern = filepattern;
    this.matchConfiguration = matchConfiguration;
    this.recordClass = recordClass;
    this.schema = schema;
    this.inferBeamSchema = inferBeamSchema;
    this.hintMatchesManyFiles = hintMatchesManyFiles;
    this.coder = coder;
    this.datumReaderFactory = datumReaderFactory;
  }

  @Override
  @Nullable ValueProvider<String> getFilepattern() {
    return filepattern;
  }

  @Override
  FileIO.MatchConfiguration getMatchConfiguration() {
    return matchConfiguration;
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
  boolean getInferBeamSchema() {
    return inferBeamSchema;
  }

  @Override
  boolean getHintMatchesManyFiles() {
    return hintMatchesManyFiles;
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
    if (o instanceof AvroIO.Read) {
      AvroIO.Read<?> that = (AvroIO.Read<?>) o;
      return (this.filepattern == null ? that.getFilepattern() == null : this.filepattern.equals(that.getFilepattern()))
          && this.matchConfiguration.equals(that.getMatchConfiguration())
          && (this.recordClass == null ? that.getRecordClass() == null : this.recordClass.equals(that.getRecordClass()))
          && (this.schema == null ? that.getSchema() == null : this.schema.equals(that.getSchema()))
          && this.inferBeamSchema == that.getInferBeamSchema()
          && this.hintMatchesManyFiles == that.getHintMatchesManyFiles()
          && (this.coder == null ? that.getCoder() == null : this.coder.equals(that.getCoder()))
          && (this.datumReaderFactory == null ? that.getDatumReaderFactory() == null : this.datumReaderFactory.equals(that.getDatumReaderFactory()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (filepattern == null) ? 0 : filepattern.hashCode();
    h$ *= 1000003;
    h$ ^= matchConfiguration.hashCode();
    h$ *= 1000003;
    h$ ^= (recordClass == null) ? 0 : recordClass.hashCode();
    h$ *= 1000003;
    h$ ^= (schema == null) ? 0 : schema.hashCode();
    h$ *= 1000003;
    h$ ^= inferBeamSchema ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= hintMatchesManyFiles ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= (coder == null) ? 0 : coder.hashCode();
    h$ *= 1000003;
    h$ ^= (datumReaderFactory == null) ? 0 : datumReaderFactory.hashCode();
    return h$;
  }

  @Override
  AvroIO.Read.Builder<T> toBuilder() {
    return new AutoValue_AvroIO_Read.Builder<T>(this);
  }

  static final class Builder<T> extends AvroIO.Read.Builder<T> {
    private @Nullable ValueProvider<String> filepattern;
    private FileIO.@Nullable MatchConfiguration matchConfiguration;
    private @Nullable Class<T> recordClass;
    private @Nullable Schema schema;
    private boolean inferBeamSchema;
    private boolean hintMatchesManyFiles;
    private @Nullable Coder<T> coder;
    private AvroSource.@Nullable DatumReaderFactory<T> datumReaderFactory;
    private byte set$0;
    Builder() {
    }
    Builder(AvroIO.Read<T> source) {
      this.filepattern = source.getFilepattern();
      this.matchConfiguration = source.getMatchConfiguration();
      this.recordClass = source.getRecordClass();
      this.schema = source.getSchema();
      this.inferBeamSchema = source.getInferBeamSchema();
      this.hintMatchesManyFiles = source.getHintMatchesManyFiles();
      this.coder = source.getCoder();
      this.datumReaderFactory = source.getDatumReaderFactory();
      set$0 = (byte) 3;
    }
    @Override
    AvroIO.Read.Builder<T> setFilepattern(ValueProvider<String> filepattern) {
      this.filepattern = filepattern;
      return this;
    }
    @Override
    AvroIO.Read.Builder<T> setMatchConfiguration(FileIO.MatchConfiguration matchConfiguration) {
      if (matchConfiguration == null) {
        throw new NullPointerException("Null matchConfiguration");
      }
      this.matchConfiguration = matchConfiguration;
      return this;
    }
    @Override
    AvroIO.Read.Builder<T> setRecordClass(Class<T> recordClass) {
      this.recordClass = recordClass;
      return this;
    }
    @Override
    AvroIO.Read.Builder<T> setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }
    @Override
    AvroIO.Read.Builder<T> setInferBeamSchema(boolean inferBeamSchema) {
      this.inferBeamSchema = inferBeamSchema;
      set$0 |= (byte) 1;
      return this;
    }
    @Override
    AvroIO.Read.Builder<T> setHintMatchesManyFiles(boolean hintMatchesManyFiles) {
      this.hintMatchesManyFiles = hintMatchesManyFiles;
      set$0 |= (byte) 2;
      return this;
    }
    @Override
    AvroIO.Read.Builder<T> setCoder(Coder<T> coder) {
      this.coder = coder;
      return this;
    }
    @Override
    AvroIO.Read.Builder<T> setDatumReaderFactory(AvroSource.DatumReaderFactory<T> datumReaderFactory) {
      this.datumReaderFactory = datumReaderFactory;
      return this;
    }
    @Override
    AvroIO.Read<T> build() {
      if (set$0 != 3
          || this.matchConfiguration == null) {
        StringBuilder missing = new StringBuilder();
        if (this.matchConfiguration == null) {
          missing.append(" matchConfiguration");
        }
        if ((set$0 & 1) == 0) {
          missing.append(" inferBeamSchema");
        }
        if ((set$0 & 2) == 0) {
          missing.append(" hintMatchesManyFiles");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_AvroIO_Read<T>(
          this.filepattern,
          this.matchConfiguration,
          this.recordClass,
          this.schema,
          this.inferBeamSchema,
          this.hintMatchesManyFiles,
          this.coder,
          this.datumReaderFactory);
    }
  }

}
