package org.apache.beam.sdk.extensions.avro.io;

import javax.annotation.Generated;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_AvroIO_TypedWrite<UserT, DestinationT, OutputT> extends AvroIO.TypedWrite<UserT, DestinationT, OutputT> {

  private final @Nullable SerializableFunction<UserT, OutputT> formatFunction;

  private final @Nullable ValueProvider<ResourceId> filenamePrefix;

  private final @Nullable String shardTemplate;

  private final @Nullable String filenameSuffix;

  private final @Nullable ValueProvider<ResourceId> tempDirectory;

  private final int numShards;

  private final Class<OutputT> recordClass;

  private final int syncInterval;

  private final @Nullable Schema schema;

  private final boolean windowedWrites;

  private final boolean noSpilling;

  private final FileBasedSink.@Nullable FilenamePolicy filenamePolicy;

  private final @Nullable DynamicAvroDestinations<UserT, DestinationT, OutputT> dynamicDestinations;

  private final AvroSink.@Nullable DatumWriterFactory<OutputT> datumWriterFactory;

  private final @Nullable ErrorHandler<BadRecord, ?> badRecordErrorHandler;

  private final SerializableAvroCodecFactory codec;

  private final ImmutableMap<String, Object> metadata;

  private AutoValue_AvroIO_TypedWrite(
      @Nullable SerializableFunction<UserT, OutputT> formatFunction,
      @Nullable ValueProvider<ResourceId> filenamePrefix,
      @Nullable String shardTemplate,
      @Nullable String filenameSuffix,
      @Nullable ValueProvider<ResourceId> tempDirectory,
      int numShards,
      Class<OutputT> recordClass,
      int syncInterval,
      @Nullable Schema schema,
      boolean windowedWrites,
      boolean noSpilling,
      FileBasedSink.@Nullable FilenamePolicy filenamePolicy,
      @Nullable DynamicAvroDestinations<UserT, DestinationT, OutputT> dynamicDestinations,
      AvroSink.@Nullable DatumWriterFactory<OutputT> datumWriterFactory,
      @Nullable ErrorHandler<BadRecord, ?> badRecordErrorHandler,
      SerializableAvroCodecFactory codec,
      ImmutableMap<String, Object> metadata) {
    this.formatFunction = formatFunction;
    this.filenamePrefix = filenamePrefix;
    this.shardTemplate = shardTemplate;
    this.filenameSuffix = filenameSuffix;
    this.tempDirectory = tempDirectory;
    this.numShards = numShards;
    this.recordClass = recordClass;
    this.syncInterval = syncInterval;
    this.schema = schema;
    this.windowedWrites = windowedWrites;
    this.noSpilling = noSpilling;
    this.filenamePolicy = filenamePolicy;
    this.dynamicDestinations = dynamicDestinations;
    this.datumWriterFactory = datumWriterFactory;
    this.badRecordErrorHandler = badRecordErrorHandler;
    this.codec = codec;
    this.metadata = metadata;
  }

  @Override
  @Nullable SerializableFunction<UserT, OutputT> getFormatFunction() {
    return formatFunction;
  }

  @Override
  @Nullable ValueProvider<ResourceId> getFilenamePrefix() {
    return filenamePrefix;
  }

  @Override
  @Nullable String getShardTemplate() {
    return shardTemplate;
  }

  @Override
  @Nullable String getFilenameSuffix() {
    return filenameSuffix;
  }

  @Override
  @Nullable ValueProvider<ResourceId> getTempDirectory() {
    return tempDirectory;
  }

  @Override
  int getNumShards() {
    return numShards;
  }

  @Override
  Class<OutputT> getRecordClass() {
    return recordClass;
  }

  @Override
  int getSyncInterval() {
    return syncInterval;
  }

  @Override
  @Nullable Schema getSchema() {
    return schema;
  }

  @Override
  boolean getWindowedWrites() {
    return windowedWrites;
  }

  @Override
  boolean getNoSpilling() {
    return noSpilling;
  }

  @Override
  FileBasedSink.@Nullable FilenamePolicy getFilenamePolicy() {
    return filenamePolicy;
  }

  @Override
  @Nullable DynamicAvroDestinations<UserT, DestinationT, OutputT> getDynamicDestinations() {
    return dynamicDestinations;
  }

  @Override
  AvroSink.@Nullable DatumWriterFactory<OutputT> getDatumWriterFactory() {
    return datumWriterFactory;
  }

  @Override
  @Nullable ErrorHandler<BadRecord, ?> getBadRecordErrorHandler() {
    return badRecordErrorHandler;
  }

  @Override
  SerializableAvroCodecFactory getCodec() {
    return codec;
  }

  @Override
  ImmutableMap<String, Object> getMetadata() {
    return metadata;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof AvroIO.TypedWrite) {
      AvroIO.TypedWrite<?, ?, ?> that = (AvroIO.TypedWrite<?, ?, ?>) o;
      return (this.formatFunction == null ? that.getFormatFunction() == null : this.formatFunction.equals(that.getFormatFunction()))
          && (this.filenamePrefix == null ? that.getFilenamePrefix() == null : this.filenamePrefix.equals(that.getFilenamePrefix()))
          && (this.shardTemplate == null ? that.getShardTemplate() == null : this.shardTemplate.equals(that.getShardTemplate()))
          && (this.filenameSuffix == null ? that.getFilenameSuffix() == null : this.filenameSuffix.equals(that.getFilenameSuffix()))
          && (this.tempDirectory == null ? that.getTempDirectory() == null : this.tempDirectory.equals(that.getTempDirectory()))
          && this.numShards == that.getNumShards()
          && this.recordClass.equals(that.getRecordClass())
          && this.syncInterval == that.getSyncInterval()
          && (this.schema == null ? that.getSchema() == null : this.schema.equals(that.getSchema()))
          && this.windowedWrites == that.getWindowedWrites()
          && this.noSpilling == that.getNoSpilling()
          && (this.filenamePolicy == null ? that.getFilenamePolicy() == null : this.filenamePolicy.equals(that.getFilenamePolicy()))
          && (this.dynamicDestinations == null ? that.getDynamicDestinations() == null : this.dynamicDestinations.equals(that.getDynamicDestinations()))
          && (this.datumWriterFactory == null ? that.getDatumWriterFactory() == null : this.datumWriterFactory.equals(that.getDatumWriterFactory()))
          && (this.badRecordErrorHandler == null ? that.getBadRecordErrorHandler() == null : this.badRecordErrorHandler.equals(that.getBadRecordErrorHandler()))
          && this.codec.equals(that.getCodec())
          && this.metadata.equals(that.getMetadata());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (formatFunction == null) ? 0 : formatFunction.hashCode();
    h$ *= 1000003;
    h$ ^= (filenamePrefix == null) ? 0 : filenamePrefix.hashCode();
    h$ *= 1000003;
    h$ ^= (shardTemplate == null) ? 0 : shardTemplate.hashCode();
    h$ *= 1000003;
    h$ ^= (filenameSuffix == null) ? 0 : filenameSuffix.hashCode();
    h$ *= 1000003;
    h$ ^= (tempDirectory == null) ? 0 : tempDirectory.hashCode();
    h$ *= 1000003;
    h$ ^= numShards;
    h$ *= 1000003;
    h$ ^= recordClass.hashCode();
    h$ *= 1000003;
    h$ ^= syncInterval;
    h$ *= 1000003;
    h$ ^= (schema == null) ? 0 : schema.hashCode();
    h$ *= 1000003;
    h$ ^= windowedWrites ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= noSpilling ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= (filenamePolicy == null) ? 0 : filenamePolicy.hashCode();
    h$ *= 1000003;
    h$ ^= (dynamicDestinations == null) ? 0 : dynamicDestinations.hashCode();
    h$ *= 1000003;
    h$ ^= (datumWriterFactory == null) ? 0 : datumWriterFactory.hashCode();
    h$ *= 1000003;
    h$ ^= (badRecordErrorHandler == null) ? 0 : badRecordErrorHandler.hashCode();
    h$ *= 1000003;
    h$ ^= codec.hashCode();
    h$ *= 1000003;
    h$ ^= metadata.hashCode();
    return h$;
  }

  @Override
  AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> toBuilder() {
    return new AutoValue_AvroIO_TypedWrite.Builder<UserT, DestinationT, OutputT>(this);
  }

  static final class Builder<UserT, DestinationT, OutputT> extends AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> {
    private @Nullable SerializableFunction<UserT, OutputT> formatFunction;
    private @Nullable ValueProvider<ResourceId> filenamePrefix;
    private @Nullable String shardTemplate;
    private @Nullable String filenameSuffix;
    private @Nullable ValueProvider<ResourceId> tempDirectory;
    private int numShards;
    private @Nullable Class<OutputT> recordClass;
    private int syncInterval;
    private @Nullable Schema schema;
    private boolean windowedWrites;
    private boolean noSpilling;
    private FileBasedSink.@Nullable FilenamePolicy filenamePolicy;
    private @Nullable DynamicAvroDestinations<UserT, DestinationT, OutputT> dynamicDestinations;
    private AvroSink.@Nullable DatumWriterFactory<OutputT> datumWriterFactory;
    private @Nullable ErrorHandler<BadRecord, ?> badRecordErrorHandler;
    private @Nullable SerializableAvroCodecFactory codec;
    private @Nullable ImmutableMap<String, Object> metadata;
    private byte set$0;
    Builder() {
    }
    Builder(AvroIO.TypedWrite<UserT, DestinationT, OutputT> source) {
      this.formatFunction = source.getFormatFunction();
      this.filenamePrefix = source.getFilenamePrefix();
      this.shardTemplate = source.getShardTemplate();
      this.filenameSuffix = source.getFilenameSuffix();
      this.tempDirectory = source.getTempDirectory();
      this.numShards = source.getNumShards();
      this.recordClass = source.getRecordClass();
      this.syncInterval = source.getSyncInterval();
      this.schema = source.getSchema();
      this.windowedWrites = source.getWindowedWrites();
      this.noSpilling = source.getNoSpilling();
      this.filenamePolicy = source.getFilenamePolicy();
      this.dynamicDestinations = source.getDynamicDestinations();
      this.datumWriterFactory = source.getDatumWriterFactory();
      this.badRecordErrorHandler = source.getBadRecordErrorHandler();
      this.codec = source.getCodec();
      this.metadata = source.getMetadata();
      set$0 = (byte) 0xf;
    }
    @Override
    AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> setFormatFunction(@Nullable SerializableFunction<UserT, OutputT> formatFunction) {
      this.formatFunction = formatFunction;
      return this;
    }
    @Override
    AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> setFilenamePrefix(ValueProvider<ResourceId> filenamePrefix) {
      this.filenamePrefix = filenamePrefix;
      return this;
    }
    @Override
    AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> setShardTemplate(@Nullable String shardTemplate) {
      this.shardTemplate = shardTemplate;
      return this;
    }
    @Override
    AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> setFilenameSuffix(@Nullable String filenameSuffix) {
      this.filenameSuffix = filenameSuffix;
      return this;
    }
    @Override
    AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> setTempDirectory(ValueProvider<ResourceId> tempDirectory) {
      this.tempDirectory = tempDirectory;
      return this;
    }
    @Override
    AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> setNumShards(int numShards) {
      this.numShards = numShards;
      set$0 |= (byte) 1;
      return this;
    }
    @Override
    AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> setRecordClass(Class<OutputT> recordClass) {
      if (recordClass == null) {
        throw new NullPointerException("Null recordClass");
      }
      this.recordClass = recordClass;
      return this;
    }
    @Override
    AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> setSyncInterval(int syncInterval) {
      this.syncInterval = syncInterval;
      set$0 |= (byte) 2;
      return this;
    }
    @Override
    AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }
    @Override
    AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> setWindowedWrites(boolean windowedWrites) {
      this.windowedWrites = windowedWrites;
      set$0 |= (byte) 4;
      return this;
    }
    @Override
    AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> setNoSpilling(boolean noSpilling) {
      this.noSpilling = noSpilling;
      set$0 |= (byte) 8;
      return this;
    }
    @Override
    AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> setFilenamePolicy(FileBasedSink.FilenamePolicy filenamePolicy) {
      this.filenamePolicy = filenamePolicy;
      return this;
    }
    @Override
    AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> setDynamicDestinations(DynamicAvroDestinations<UserT, DestinationT, OutputT> dynamicDestinations) {
      this.dynamicDestinations = dynamicDestinations;
      return this;
    }
    @Override
    AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> setDatumWriterFactory(AvroSink.DatumWriterFactory<OutputT> datumWriterFactory) {
      this.datumWriterFactory = datumWriterFactory;
      return this;
    }
    @Override
    AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> setBadRecordErrorHandler(@Nullable ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
      this.badRecordErrorHandler = badRecordErrorHandler;
      return this;
    }
    @Override
    AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> setCodec(SerializableAvroCodecFactory codec) {
      if (codec == null) {
        throw new NullPointerException("Null codec");
      }
      this.codec = codec;
      return this;
    }
    @Override
    AvroIO.TypedWrite.Builder<UserT, DestinationT, OutputT> setMetadata(ImmutableMap<String, Object> metadata) {
      if (metadata == null) {
        throw new NullPointerException("Null metadata");
      }
      this.metadata = metadata;
      return this;
    }
    @Override
    AvroIO.TypedWrite<UserT, DestinationT, OutputT> build() {
      if (set$0 != 0xf
          || this.recordClass == null
          || this.codec == null
          || this.metadata == null) {
        StringBuilder missing = new StringBuilder();
        if ((set$0 & 1) == 0) {
          missing.append(" numShards");
        }
        if (this.recordClass == null) {
          missing.append(" recordClass");
        }
        if ((set$0 & 2) == 0) {
          missing.append(" syncInterval");
        }
        if ((set$0 & 4) == 0) {
          missing.append(" windowedWrites");
        }
        if ((set$0 & 8) == 0) {
          missing.append(" noSpilling");
        }
        if (this.codec == null) {
          missing.append(" codec");
        }
        if (this.metadata == null) {
          missing.append(" metadata");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_AvroIO_TypedWrite<UserT, DestinationT, OutputT>(
          this.formatFunction,
          this.filenamePrefix,
          this.shardTemplate,
          this.filenameSuffix,
          this.tempDirectory,
          this.numShards,
          this.recordClass,
          this.syncInterval,
          this.schema,
          this.windowedWrites,
          this.noSpilling,
          this.filenamePolicy,
          this.dynamicDestinations,
          this.datumWriterFactory,
          this.badRecordErrorHandler,
          this.codec,
          this.metadata);
    }
  }

}
