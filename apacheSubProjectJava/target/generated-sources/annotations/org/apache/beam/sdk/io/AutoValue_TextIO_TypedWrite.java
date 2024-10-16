package org.apache.beam.sdk.io;

import java.util.Arrays;
import javax.annotation.Generated;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TextIO_TypedWrite<UserT, DestinationT> extends TextIO.TypedWrite<UserT, DestinationT> {

  private final @Nullable ValueProvider<ResourceId> filenamePrefix;

  private final @Nullable String filenameSuffix;

  private final @Nullable ValueProvider<ResourceId> tempDirectory;

  private final char[] delimiter;

  private final @Nullable String header;

  private final @Nullable String footer;

  private final @Nullable ValueProvider<Integer> numShards;

  private final @Nullable String shardTemplate;

  private final FileBasedSink.@Nullable FilenamePolicy filenamePolicy;

  private final FileBasedSink.@Nullable DynamicDestinations<UserT, DestinationT, String> dynamicDestinations;

  private final @Nullable SerializableFunction<UserT, DefaultFilenamePolicy.Params> destinationFunction;

  private final DefaultFilenamePolicy.@Nullable Params emptyDestination;

  private final @Nullable SerializableFunction<UserT, String> formatFunction;

  private final @Nullable Integer batchSize;

  private final @Nullable Integer batchSizeBytes;

  private final @Nullable Duration batchMaxBufferingDuration;

  private final boolean windowedWrites;

  private final boolean autoSharding;

  private final boolean noSpilling;

  private final boolean skipIfEmpty;

  private final FileBasedSink.WritableByteChannelFactory writableByteChannelFactory;

  private final @Nullable ErrorHandler<BadRecord, ?> badRecordErrorHandler;

  private AutoValue_TextIO_TypedWrite(
      @Nullable ValueProvider<ResourceId> filenamePrefix,
      @Nullable String filenameSuffix,
      @Nullable ValueProvider<ResourceId> tempDirectory,
      char[] delimiter,
      @Nullable String header,
      @Nullable String footer,
      @Nullable ValueProvider<Integer> numShards,
      @Nullable String shardTemplate,
      FileBasedSink.@Nullable FilenamePolicy filenamePolicy,
      FileBasedSink.@Nullable DynamicDestinations<UserT, DestinationT, String> dynamicDestinations,
      @Nullable SerializableFunction<UserT, DefaultFilenamePolicy.Params> destinationFunction,
      DefaultFilenamePolicy.@Nullable Params emptyDestination,
      @Nullable SerializableFunction<UserT, String> formatFunction,
      @Nullable Integer batchSize,
      @Nullable Integer batchSizeBytes,
      @Nullable Duration batchMaxBufferingDuration,
      boolean windowedWrites,
      boolean autoSharding,
      boolean noSpilling,
      boolean skipIfEmpty,
      FileBasedSink.WritableByteChannelFactory writableByteChannelFactory,
      @Nullable ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
    this.filenamePrefix = filenamePrefix;
    this.filenameSuffix = filenameSuffix;
    this.tempDirectory = tempDirectory;
    this.delimiter = delimiter;
    this.header = header;
    this.footer = footer;
    this.numShards = numShards;
    this.shardTemplate = shardTemplate;
    this.filenamePolicy = filenamePolicy;
    this.dynamicDestinations = dynamicDestinations;
    this.destinationFunction = destinationFunction;
    this.emptyDestination = emptyDestination;
    this.formatFunction = formatFunction;
    this.batchSize = batchSize;
    this.batchSizeBytes = batchSizeBytes;
    this.batchMaxBufferingDuration = batchMaxBufferingDuration;
    this.windowedWrites = windowedWrites;
    this.autoSharding = autoSharding;
    this.noSpilling = noSpilling;
    this.skipIfEmpty = skipIfEmpty;
    this.writableByteChannelFactory = writableByteChannelFactory;
    this.badRecordErrorHandler = badRecordErrorHandler;
  }

  @Override
  @Nullable ValueProvider<ResourceId> getFilenamePrefix() {
    return filenamePrefix;
  }

  @Override
  @Nullable String getFilenameSuffix() {
    return filenameSuffix;
  }

  @Override
  @Nullable ValueProvider<ResourceId> getTempDirectory() {
    return tempDirectory;
  }

  @SuppressWarnings("mutable")
  @Override
  char[] getDelimiter() {
    return delimiter;
  }

  @Override
  @Nullable String getHeader() {
    return header;
  }

  @Override
  @Nullable String getFooter() {
    return footer;
  }

  @Override
  @Nullable ValueProvider<Integer> getNumShards() {
    return numShards;
  }

  @Override
  @Nullable String getShardTemplate() {
    return shardTemplate;
  }

  @Override
  FileBasedSink.@Nullable FilenamePolicy getFilenamePolicy() {
    return filenamePolicy;
  }

  @Override
  FileBasedSink.@Nullable DynamicDestinations<UserT, DestinationT, String> getDynamicDestinations() {
    return dynamicDestinations;
  }

  @Override
  @Nullable SerializableFunction<UserT, DefaultFilenamePolicy.Params> getDestinationFunction() {
    return destinationFunction;
  }

  @Override
  DefaultFilenamePolicy.@Nullable Params getEmptyDestination() {
    return emptyDestination;
  }

  @Override
  @Nullable SerializableFunction<UserT, String> getFormatFunction() {
    return formatFunction;
  }

  @Override
  @Nullable Integer getBatchSize() {
    return batchSize;
  }

  @Override
  @Nullable Integer getBatchSizeBytes() {
    return batchSizeBytes;
  }

  @Override
  @Nullable Duration getBatchMaxBufferingDuration() {
    return batchMaxBufferingDuration;
  }

  @Override
  boolean getWindowedWrites() {
    return windowedWrites;
  }

  @Override
  boolean getAutoSharding() {
    return autoSharding;
  }

  @Override
  boolean getNoSpilling() {
    return noSpilling;
  }

  @Override
  boolean getSkipIfEmpty() {
    return skipIfEmpty;
  }

  @Override
  FileBasedSink.WritableByteChannelFactory getWritableByteChannelFactory() {
    return writableByteChannelFactory;
  }

  @Override
  @Nullable ErrorHandler<BadRecord, ?> getBadRecordErrorHandler() {
    return badRecordErrorHandler;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TextIO.TypedWrite) {
      TextIO.TypedWrite<?, ?> that = (TextIO.TypedWrite<?, ?>) o;
      return (this.filenamePrefix == null ? that.getFilenamePrefix() == null : this.filenamePrefix.equals(that.getFilenamePrefix()))
          && (this.filenameSuffix == null ? that.getFilenameSuffix() == null : this.filenameSuffix.equals(that.getFilenameSuffix()))
          && (this.tempDirectory == null ? that.getTempDirectory() == null : this.tempDirectory.equals(that.getTempDirectory()))
          && Arrays.equals(this.delimiter, (that instanceof AutoValue_TextIO_TypedWrite) ? ((AutoValue_TextIO_TypedWrite<?, ?>) that).delimiter : that.getDelimiter())
          && (this.header == null ? that.getHeader() == null : this.header.equals(that.getHeader()))
          && (this.footer == null ? that.getFooter() == null : this.footer.equals(that.getFooter()))
          && (this.numShards == null ? that.getNumShards() == null : this.numShards.equals(that.getNumShards()))
          && (this.shardTemplate == null ? that.getShardTemplate() == null : this.shardTemplate.equals(that.getShardTemplate()))
          && (this.filenamePolicy == null ? that.getFilenamePolicy() == null : this.filenamePolicy.equals(that.getFilenamePolicy()))
          && (this.dynamicDestinations == null ? that.getDynamicDestinations() == null : this.dynamicDestinations.equals(that.getDynamicDestinations()))
          && (this.destinationFunction == null ? that.getDestinationFunction() == null : this.destinationFunction.equals(that.getDestinationFunction()))
          && (this.emptyDestination == null ? that.getEmptyDestination() == null : this.emptyDestination.equals(that.getEmptyDestination()))
          && (this.formatFunction == null ? that.getFormatFunction() == null : this.formatFunction.equals(that.getFormatFunction()))
          && (this.batchSize == null ? that.getBatchSize() == null : this.batchSize.equals(that.getBatchSize()))
          && (this.batchSizeBytes == null ? that.getBatchSizeBytes() == null : this.batchSizeBytes.equals(that.getBatchSizeBytes()))
          && (this.batchMaxBufferingDuration == null ? that.getBatchMaxBufferingDuration() == null : this.batchMaxBufferingDuration.equals(that.getBatchMaxBufferingDuration()))
          && this.windowedWrites == that.getWindowedWrites()
          && this.autoSharding == that.getAutoSharding()
          && this.noSpilling == that.getNoSpilling()
          && this.skipIfEmpty == that.getSkipIfEmpty()
          && this.writableByteChannelFactory.equals(that.getWritableByteChannelFactory())
          && (this.badRecordErrorHandler == null ? that.getBadRecordErrorHandler() == null : this.badRecordErrorHandler.equals(that.getBadRecordErrorHandler()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (filenamePrefix == null) ? 0 : filenamePrefix.hashCode();
    h$ *= 1000003;
    h$ ^= (filenameSuffix == null) ? 0 : filenameSuffix.hashCode();
    h$ *= 1000003;
    h$ ^= (tempDirectory == null) ? 0 : tempDirectory.hashCode();
    h$ *= 1000003;
    h$ ^= Arrays.hashCode(delimiter);
    h$ *= 1000003;
    h$ ^= (header == null) ? 0 : header.hashCode();
    h$ *= 1000003;
    h$ ^= (footer == null) ? 0 : footer.hashCode();
    h$ *= 1000003;
    h$ ^= (numShards == null) ? 0 : numShards.hashCode();
    h$ *= 1000003;
    h$ ^= (shardTemplate == null) ? 0 : shardTemplate.hashCode();
    h$ *= 1000003;
    h$ ^= (filenamePolicy == null) ? 0 : filenamePolicy.hashCode();
    h$ *= 1000003;
    h$ ^= (dynamicDestinations == null) ? 0 : dynamicDestinations.hashCode();
    h$ *= 1000003;
    h$ ^= (destinationFunction == null) ? 0 : destinationFunction.hashCode();
    h$ *= 1000003;
    h$ ^= (emptyDestination == null) ? 0 : emptyDestination.hashCode();
    h$ *= 1000003;
    h$ ^= (formatFunction == null) ? 0 : formatFunction.hashCode();
    h$ *= 1000003;
    h$ ^= (batchSize == null) ? 0 : batchSize.hashCode();
    h$ *= 1000003;
    h$ ^= (batchSizeBytes == null) ? 0 : batchSizeBytes.hashCode();
    h$ *= 1000003;
    h$ ^= (batchMaxBufferingDuration == null) ? 0 : batchMaxBufferingDuration.hashCode();
    h$ *= 1000003;
    h$ ^= windowedWrites ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= autoSharding ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= noSpilling ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= skipIfEmpty ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= writableByteChannelFactory.hashCode();
    h$ *= 1000003;
    h$ ^= (badRecordErrorHandler == null) ? 0 : badRecordErrorHandler.hashCode();
    return h$;
  }

  @Override
  TextIO.TypedWrite.Builder<UserT, DestinationT> toBuilder() {
    return new AutoValue_TextIO_TypedWrite.Builder<UserT, DestinationT>(this);
  }

  static final class Builder<UserT, DestinationT> extends TextIO.TypedWrite.Builder<UserT, DestinationT> {
    private @Nullable ValueProvider<ResourceId> filenamePrefix;
    private @Nullable String filenameSuffix;
    private @Nullable ValueProvider<ResourceId> tempDirectory;
    private char @Nullable [] delimiter;
    private @Nullable String header;
    private @Nullable String footer;
    private @Nullable ValueProvider<Integer> numShards;
    private @Nullable String shardTemplate;
    private FileBasedSink.@Nullable FilenamePolicy filenamePolicy;
    private FileBasedSink.@Nullable DynamicDestinations<UserT, DestinationT, String> dynamicDestinations;
    private @Nullable SerializableFunction<UserT, DefaultFilenamePolicy.Params> destinationFunction;
    private DefaultFilenamePolicy.@Nullable Params emptyDestination;
    private @Nullable SerializableFunction<UserT, String> formatFunction;
    private @Nullable Integer batchSize;
    private @Nullable Integer batchSizeBytes;
    private @Nullable Duration batchMaxBufferingDuration;
    private boolean windowedWrites;
    private boolean autoSharding;
    private boolean noSpilling;
    private boolean skipIfEmpty;
    private FileBasedSink.@Nullable WritableByteChannelFactory writableByteChannelFactory;
    private @Nullable ErrorHandler<BadRecord, ?> badRecordErrorHandler;
    private byte set$0;
    Builder() {
    }
    Builder(TextIO.TypedWrite<UserT, DestinationT> source) {
      this.filenamePrefix = source.getFilenamePrefix();
      this.filenameSuffix = source.getFilenameSuffix();
      this.tempDirectory = source.getTempDirectory();
      this.delimiter = source.getDelimiter();
      this.header = source.getHeader();
      this.footer = source.getFooter();
      this.numShards = source.getNumShards();
      this.shardTemplate = source.getShardTemplate();
      this.filenamePolicy = source.getFilenamePolicy();
      this.dynamicDestinations = source.getDynamicDestinations();
      this.destinationFunction = source.getDestinationFunction();
      this.emptyDestination = source.getEmptyDestination();
      this.formatFunction = source.getFormatFunction();
      this.batchSize = source.getBatchSize();
      this.batchSizeBytes = source.getBatchSizeBytes();
      this.batchMaxBufferingDuration = source.getBatchMaxBufferingDuration();
      this.windowedWrites = source.getWindowedWrites();
      this.autoSharding = source.getAutoSharding();
      this.noSpilling = source.getNoSpilling();
      this.skipIfEmpty = source.getSkipIfEmpty();
      this.writableByteChannelFactory = source.getWritableByteChannelFactory();
      this.badRecordErrorHandler = source.getBadRecordErrorHandler();
      set$0 = (byte) 0xf;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setFilenamePrefix(@Nullable ValueProvider<ResourceId> filenamePrefix) {
      this.filenamePrefix = filenamePrefix;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setFilenameSuffix(@Nullable String filenameSuffix) {
      this.filenameSuffix = filenameSuffix;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setTempDirectory(@Nullable ValueProvider<ResourceId> tempDirectory) {
      this.tempDirectory = tempDirectory;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setDelimiter(char[] delimiter) {
      if (delimiter == null) {
        throw new NullPointerException("Null delimiter");
      }
      this.delimiter = delimiter;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setHeader(@Nullable String header) {
      this.header = header;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setFooter(@Nullable String footer) {
      this.footer = footer;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setNumShards(@Nullable ValueProvider<Integer> numShards) {
      this.numShards = numShards;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setShardTemplate(@Nullable String shardTemplate) {
      this.shardTemplate = shardTemplate;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setFilenamePolicy(FileBasedSink.@Nullable FilenamePolicy filenamePolicy) {
      this.filenamePolicy = filenamePolicy;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setDynamicDestinations(FileBasedSink.@Nullable DynamicDestinations<UserT, DestinationT, String> dynamicDestinations) {
      this.dynamicDestinations = dynamicDestinations;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setDestinationFunction(@Nullable SerializableFunction<UserT, DefaultFilenamePolicy.Params> destinationFunction) {
      this.destinationFunction = destinationFunction;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setEmptyDestination(DefaultFilenamePolicy.Params emptyDestination) {
      this.emptyDestination = emptyDestination;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setFormatFunction(@Nullable SerializableFunction<UserT, String> formatFunction) {
      this.formatFunction = formatFunction;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setBatchSize(@Nullable Integer batchSize) {
      this.batchSize = batchSize;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setBatchSizeBytes(@Nullable Integer batchSizeBytes) {
      this.batchSizeBytes = batchSizeBytes;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setBatchMaxBufferingDuration(@Nullable Duration batchMaxBufferingDuration) {
      this.batchMaxBufferingDuration = batchMaxBufferingDuration;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setWindowedWrites(boolean windowedWrites) {
      this.windowedWrites = windowedWrites;
      set$0 |= (byte) 1;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setAutoSharding(boolean autoSharding) {
      this.autoSharding = autoSharding;
      set$0 |= (byte) 2;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setNoSpilling(boolean noSpilling) {
      this.noSpilling = noSpilling;
      set$0 |= (byte) 4;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setSkipIfEmpty(boolean skipIfEmpty) {
      this.skipIfEmpty = skipIfEmpty;
      set$0 |= (byte) 8;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setWritableByteChannelFactory(FileBasedSink.WritableByteChannelFactory writableByteChannelFactory) {
      if (writableByteChannelFactory == null) {
        throw new NullPointerException("Null writableByteChannelFactory");
      }
      this.writableByteChannelFactory = writableByteChannelFactory;
      return this;
    }
    @Override
    TextIO.TypedWrite.Builder<UserT, DestinationT> setBadRecordErrorHandler(@Nullable ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
      this.badRecordErrorHandler = badRecordErrorHandler;
      return this;
    }
    @Override
    TextIO.TypedWrite<UserT, DestinationT> build() {
      if (set$0 != 0xf
          || this.delimiter == null
          || this.writableByteChannelFactory == null) {
        StringBuilder missing = new StringBuilder();
        if (this.delimiter == null) {
          missing.append(" delimiter");
        }
        if ((set$0 & 1) == 0) {
          missing.append(" windowedWrites");
        }
        if ((set$0 & 2) == 0) {
          missing.append(" autoSharding");
        }
        if ((set$0 & 4) == 0) {
          missing.append(" noSpilling");
        }
        if ((set$0 & 8) == 0) {
          missing.append(" skipIfEmpty");
        }
        if (this.writableByteChannelFactory == null) {
          missing.append(" writableByteChannelFactory");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_TextIO_TypedWrite<UserT, DestinationT>(
          this.filenamePrefix,
          this.filenameSuffix,
          this.tempDirectory,
          this.delimiter,
          this.header,
          this.footer,
          this.numShards,
          this.shardTemplate,
          this.filenamePolicy,
          this.dynamicDestinations,
          this.destinationFunction,
          this.emptyDestination,
          this.formatFunction,
          this.batchSize,
          this.batchSizeBytes,
          this.batchMaxBufferingDuration,
          this.windowedWrites,
          this.autoSharding,
          this.noSpilling,
          this.skipIfEmpty,
          this.writableByteChannelFactory,
          this.badRecordErrorHandler);
    }
  }

}
