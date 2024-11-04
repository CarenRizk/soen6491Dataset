package org.apache.beam.sdk.io;

import static org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.commons.compress.utils.CharsetNames.UTF_8;

import com.google.auto.value.AutoValue;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Params;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.WritableByteChannelFactory;
import org.apache.beam.sdk.io.FileIO.MatchConfiguration;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;


@SuppressWarnings({
  "nullness" 
})
public class TextIO {
  private static final long DEFAULT_BUNDLE_SIZE_BYTES = 64 * 1024 * 1024L;

  
  public static Read read() {
    return new AutoValue_TextIO_Read.Builder()
        .setCompression(Compression.AUTO)
        .setHintMatchesManyFiles(false)
        .setSkipHeaderLines(0)
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
        .build();
  }

  
  @Deprecated
  public static ReadAll readAll() {
    return new AutoValue_TextIO_ReadAll.Builder()
        .setCompression(Compression.AUTO)
        .setSkipHeaderLines(0)
        .setMatchConfiguration(MatchConfiguration.create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
        .build();
  }

  
  public static ReadFiles readFiles() {
    return new AutoValue_TextIO_ReadFiles.Builder()
        
        
        
        .setDesiredBundleSizeBytes(DEFAULT_BUNDLE_SIZE_BYTES)
        .setSkipHeaderLines(0)
        .build();
  }

  
  public static Write write() {
    return new TextIO.Write();
  }

  
  public static <UserT> TypedWrite<UserT, Void> writeCustomType() {
    return new AutoValue_TextIO_TypedWrite.Builder<UserT, Void>()
        .setFilenamePrefix(null)
        .setTempDirectory(null)
        .setShardTemplate(null)
        .setFilenameSuffix(null)
        .setFilenamePolicy(null)
        .setDynamicDestinations(null)
        .setDelimiter(new char[] {'\n'})
        .setWritableByteChannelFactory(FileBasedSink.CompressionType.UNCOMPRESSED)
        .setWindowedWrites(false)
        .setNoSpilling(false)
        .setSkipIfEmpty(false)
        .setAutoSharding(false)
        .build();
  }

  
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {

    abstract @Nullable ValueProvider<String> getFilepattern();

    abstract MatchConfiguration getMatchConfiguration();

    abstract boolean getHintMatchesManyFiles();

    abstract Compression getCompression();

    @SuppressWarnings("mutable") 
    abstract byte @Nullable [] getDelimiter();

    abstract int getSkipHeaderLines();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);

      abstract Builder setMatchConfiguration(MatchConfiguration matchConfiguration);

      abstract Builder setHintMatchesManyFiles(boolean hintManyFiles);

      abstract Builder setCompression(Compression compression);

      abstract Builder setDelimiter(byte @Nullable [] delimiter);

      abstract Builder setSkipHeaderLines(int skipHeaderLines);

      abstract Read build();
    }

    
    public Read from(String filepattern) {
      checkArgument(filepattern != null, "filepattern can not be null");
      return from(StaticValueProvider.of(filepattern));
    }

    
    public Read from(ValueProvider<String> filepattern) {
      checkArgument(filepattern != null, "filepattern can not be null");
      return toBuilder().setFilepattern(filepattern).build();
    }

    
    public Read withMatchConfiguration(MatchConfiguration matchConfiguration) {
      return toBuilder().setMatchConfiguration(matchConfiguration).build();
    }

    
    @Deprecated
    public Read withCompressionType(TextIO.CompressionType compressionType) {
      return withCompression(compressionType.canonical);
    }

    
    public Read withCompression(Compression compression) {
      return toBuilder().setCompression(compression).build();
    }

    
    public Read watchForNewFiles(
        Duration pollInterval,
        TerminationCondition<String, ?> terminationCondition,
        boolean matchUpdatedFiles) {
      return withMatchConfiguration(
          getMatchConfiguration()
              .continuously(pollInterval, terminationCondition, matchUpdatedFiles));
    }

    
    public Read watchForNewFiles(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return watchForNewFiles(pollInterval, terminationCondition, false);
    }

    
    public Read withHintMatchesManyFiles() {
      return toBuilder().setHintMatchesManyFiles(true).build();
    }

    
    public Read withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
    }

    
    public Read withDelimiter(byte[] delimiter) {
      checkArgument(delimiter != null, "delimiter can not be null");
      checkArgument(!isSelfOverlapping(delimiter), "delimiter must not self-overlap");
      return toBuilder().setDelimiter(delimiter).build();
    }

    public Read withSkipHeaderLines(int skipHeaderLines) {
      return toBuilder().setSkipHeaderLines(skipHeaderLines).build();
    }

    static boolean isSelfOverlapping(byte[] s) {
      
      for (int i = 1; i < s.length - 1; ++i) {
        if (ByteBuffer.wrap(s, 0, i).equals(ByteBuffer.wrap(s, s.length - i, i))) {
          return true;
        }
      }
      return false;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      checkNotNull(getFilepattern(), "need to set the filepattern of a TextIO.Read transform");
      if (getMatchConfiguration().getWatchInterval() == null && !getHintMatchesManyFiles()) {
        return input.apply("Read", org.apache.beam.sdk.io.Read.from(getSource()));
      }

      
      return input
          .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
          .apply("Match All", FileIO.matchAll().withConfiguration(getMatchConfiguration()))
          .apply(
              "Read Matches",
              FileIO.readMatches()
                  .withCompression(getCompression())
                  .withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
          .apply(
              "Via ReadFiles",
              readFiles().withDelimiter(getDelimiter()).withSkipHeaderLines(getSkipHeaderLines()));
    }

    
    protected FileBasedSource<String> getSource() {
      return CompressedSource.from(
              new TextSource(
                  getFilepattern(),
                  getMatchConfiguration().getEmptyMatchTreatment(),
                  getDelimiter(),
                  getSkipHeaderLines()))
          .withCompression(getCompression());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(
              DisplayData.item("compressionType", getCompression().toString())
                  .withLabel("Compression Type"))
          .addIfNotNull(DisplayData.item("filePattern", getFilepattern()).withLabel("File Pattern"))
          .include("matchConfiguration", getMatchConfiguration())
          .addIfNotNull(
              DisplayData.item("delimiter", Arrays.toString(getDelimiter()))
                  .withLabel("Custom delimiter to split records"));
    }
  }

  

  
  @Deprecated
  @AutoValue
  public abstract static class ReadAll
      extends PTransform<PCollection<String>, PCollection<String>> {
    abstract MatchConfiguration getMatchConfiguration();

    abstract Compression getCompression();

    @SuppressWarnings("mutable") 
    abstract byte @Nullable [] getDelimiter();

    abstract int getSkipHeaderLines();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setMatchConfiguration(MatchConfiguration matchConfiguration);

      abstract Builder setCompression(Compression compression);

      abstract Builder setDelimiter(byte @Nullable [] delimiter);

      abstract Builder setSkipHeaderLines(int skipHeaderLines);

      abstract ReadAll build();
    }

    
    public ReadAll withMatchConfiguration(MatchConfiguration configuration) {
      return toBuilder().setMatchConfiguration(configuration).build();
    }

    
    @Deprecated
    public ReadAll withCompressionType(TextIO.CompressionType compressionType) {
      return withCompression(compressionType.canonical);
    }

    
    public ReadAll withCompression(Compression compression) {
      return toBuilder().setCompression(compression).build();
    }

    
    public ReadAll withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
    }

    
    public ReadAll watchForNewFiles(
        Duration pollInterval,
        TerminationCondition<String, ?> terminationCondition,
        boolean matchUpdatedFiles) {
      return withMatchConfiguration(
          getMatchConfiguration()
              .continuously(pollInterval, terminationCondition, matchUpdatedFiles));
    }

    
    public ReadAll watchForNewFiles(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return watchForNewFiles(pollInterval, terminationCondition, false);
    }

    ReadAll withDelimiter(byte[] delimiter) {
      return toBuilder().setDelimiter(delimiter).build();
    }

    @Override
    public PCollection<String> expand(PCollection<String> input) {
      return input
          .apply(FileIO.matchAll().withConfiguration(getMatchConfiguration()))
          .apply(
              FileIO.readMatches()
                  .withCompression(getCompression())
                  .withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
          .apply(readFiles().withDelimiter(getDelimiter()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(
              DisplayData.item("compressionType", getCompression().toString())
                  .withLabel("Compression Type"))
          .addIfNotNull(
              DisplayData.item("delimiter", Arrays.toString(getDelimiter()))
                  .withLabel("Custom delimiter to split records"))
          .include("matchConfiguration", getMatchConfiguration());
    }
  }

  
  @AutoValue
  public abstract static class ReadFiles
      extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<String>> {
    abstract long getDesiredBundleSizeBytes();

    @SuppressWarnings("mutable") 
    abstract byte @Nullable [] getDelimiter();

    abstract int getSkipHeaderLines();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setDesiredBundleSizeBytes(long desiredBundleSizeBytes);

      abstract Builder setDelimiter(byte @Nullable [] delimiter);

      abstract Builder setSkipHeaderLines(int skipHeaderLines);

      abstract ReadFiles build();
    }

    @VisibleForTesting
    ReadFiles withDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      return toBuilder().setDesiredBundleSizeBytes(desiredBundleSizeBytes).build();
    }

    
    public ReadFiles withDelimiter(byte[] delimiter) {
      return toBuilder().setDelimiter(delimiter).build();
    }

    public ReadFiles withSkipHeaderLines(int skipHeaderLines) {
      return toBuilder().setSkipHeaderLines(skipHeaderLines).build();
    }

    @Override
    public PCollection<String> expand(PCollection<FileIO.ReadableFile> input) {
      return input.apply(
          "Read all via FileBasedSource",
          new ReadAllViaFileBasedSource<>(
              getDesiredBundleSizeBytes(),
              new CreateTextSourceFn(getDelimiter(), getSkipHeaderLines()),
              StringUtf8Coder.of()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(
          DisplayData.item("delimiter", Arrays.toString(getDelimiter()))
              .withLabel("Custom delimiter to split records"));
    }

    private static class CreateTextSourceFn
        implements SerializableFunction<String, FileBasedSource<String>> {
      private byte[] delimiter;
      private int skipHeaderLines;

      private CreateTextSourceFn(byte[] delimiter, int skipHeaderLines) {
        this.delimiter = delimiter;
        this.skipHeaderLines = skipHeaderLines;
      }

      @Override
      public FileBasedSource<String> apply(String input) {
        return new TextSource(
            StaticValueProvider.of(input),
            EmptyMatchTreatment.DISALLOW,
            delimiter,
            skipHeaderLines);
      }
    }
  }

  

  
  @AutoValue
  public abstract static class TypedWrite<UserT, DestinationT>
      extends PTransform<PCollection<UserT>, WriteFilesResult<DestinationT>> {

    
    abstract @Nullable ValueProvider<ResourceId> getFilenamePrefix();

    
    abstract @Nullable String getFilenameSuffix();

    
    abstract @Nullable ValueProvider<ResourceId> getTempDirectory();

    
    @SuppressWarnings("mutable") 
    abstract char[] getDelimiter();

    
    abstract @Nullable String getHeader();

    
    abstract @Nullable String getFooter();

    
    abstract @Nullable ValueProvider<Integer> getNumShards();

    
    abstract @Nullable String getShardTemplate();

    
    abstract @Nullable FilenamePolicy getFilenamePolicy();

    
    abstract @Nullable DynamicDestinations<UserT, DestinationT, String> getDynamicDestinations();

    
    abstract @Nullable SerializableFunction<UserT, Params> getDestinationFunction();

    
    abstract @Nullable Params getEmptyDestination();

    
    abstract @Nullable SerializableFunction<UserT, String> getFormatFunction();

    
    abstract @Nullable Integer getBatchSize();

    
    abstract @Nullable Integer getBatchSizeBytes();

    
    abstract @Nullable Duration getBatchMaxBufferingDuration();

    
    abstract boolean getWindowedWrites();

    
    abstract boolean getAutoSharding();

    
    abstract boolean getNoSpilling();

    
    abstract boolean getSkipIfEmpty();

    
    abstract WritableByteChannelFactory getWritableByteChannelFactory();

    abstract @Nullable ErrorHandler<BadRecord, ?> getBadRecordErrorHandler();

    abstract Builder<UserT, DestinationT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<UserT, DestinationT> {
      abstract Builder<UserT, DestinationT> setFilenamePrefix(
          @Nullable ValueProvider<ResourceId> filenamePrefix);

      abstract Builder<UserT, DestinationT> setTempDirectory(
          @Nullable ValueProvider<ResourceId> tempDirectory);

      abstract Builder<UserT, DestinationT> setShardTemplate(@Nullable String shardTemplate);

      abstract Builder<UserT, DestinationT> setFilenameSuffix(@Nullable String filenameSuffix);

      abstract Builder<UserT, DestinationT> setHeader(@Nullable String header);

      abstract Builder<UserT, DestinationT> setFooter(@Nullable String footer);

      abstract Builder<UserT, DestinationT> setDelimiter(char[] delimiter);

      abstract Builder<UserT, DestinationT> setFilenamePolicy(
          @Nullable FilenamePolicy filenamePolicy);

      abstract Builder<UserT, DestinationT> setDynamicDestinations(
          @Nullable DynamicDestinations<UserT, DestinationT, String> dynamicDestinations);

      abstract Builder<UserT, DestinationT> setDestinationFunction(
          @Nullable SerializableFunction<UserT, Params> destinationFunction);

      abstract Builder<UserT, DestinationT> setEmptyDestination(Params emptyDestination);

      abstract Builder<UserT, DestinationT> setFormatFunction(
          @Nullable SerializableFunction<UserT, String> formatFunction);

      abstract Builder<UserT, DestinationT> setNumShards(
          @Nullable ValueProvider<Integer> numShards);

      abstract Builder<UserT, DestinationT> setBatchSize(@Nullable Integer batchSize);

      abstract Builder<UserT, DestinationT> setBatchSizeBytes(@Nullable Integer batchSizeBytes);

      abstract Builder<UserT, DestinationT> setBatchMaxBufferingDuration(
          @Nullable Duration batchMaxBufferingDuration);

      abstract Builder<UserT, DestinationT> setWindowedWrites(boolean windowedWrites);

      abstract Builder<UserT, DestinationT> setAutoSharding(boolean windowedWrites);

      abstract Builder<UserT, DestinationT> setNoSpilling(boolean noSpilling);

      abstract Builder<UserT, DestinationT> setSkipIfEmpty(boolean noSpilling);

      abstract Builder<UserT, DestinationT> setWritableByteChannelFactory(
          WritableByteChannelFactory writableByteChannelFactory);

      abstract Builder<UserT, DestinationT> setBadRecordErrorHandler(
          @Nullable ErrorHandler<BadRecord, ?> badRecordErrorHandler);

      abstract TypedWrite<UserT, DestinationT> build();
    }

    
    public TypedWrite<UserT, DestinationT> to(String filenamePrefix) {
      return to(FileBasedSink.convertToFileResourceIfPossible(filenamePrefix));
    }

    
    public TypedWrite<UserT, DestinationT> to(ResourceId filenamePrefix) {
      return toResource(StaticValueProvider.of(filenamePrefix));
    }

    
    public TypedWrite<UserT, DestinationT> to(ValueProvider<String> outputPrefix) {
      return toResource(
          NestedValueProvider.of(outputPrefix, FileBasedSink::convertToFileResourceIfPossible));
    }

    
    public TypedWrite<UserT, DestinationT> to(FilenamePolicy filenamePolicy) {
      return toBuilder().setFilenamePolicy(filenamePolicy).build();
    }

    
    @Deprecated
    public <NewDestinationT> TypedWrite<UserT, NewDestinationT> to(
        DynamicDestinations<UserT, NewDestinationT, String> dynamicDestinations) {
      return (TypedWrite)
          toBuilder().setDynamicDestinations((DynamicDestinations) dynamicDestinations).build();
    }

    
    @Deprecated
    public TypedWrite<UserT, Params> to(
        SerializableFunction<UserT, Params> destinationFunction, Params emptyDestination) {
      return (TypedWrite)
          toBuilder()
              .setDestinationFunction(destinationFunction)
              .setEmptyDestination(emptyDestination)
              .build();
    }

    
    public TypedWrite<UserT, DestinationT> toResource(ValueProvider<ResourceId> filenamePrefix) {
      return toBuilder().setFilenamePrefix(filenamePrefix).build();
    }

    
    @Deprecated
    public TypedWrite<UserT, DestinationT> withFormatFunction(
        @Nullable SerializableFunction<UserT, String> formatFunction) {
      return toBuilder().setFormatFunction(formatFunction).build();
    }

    
    public TypedWrite<UserT, DestinationT> withBatchSize(@Nullable Integer batchSize) {
      return toBuilder().setBatchSize(batchSize).build();
    }

    
    public TypedWrite<UserT, DestinationT> withBatchSizeBytes(@Nullable Integer batchSizeBytes) {
      return toBuilder().setBatchSizeBytes(batchSizeBytes).build();
    }

    
    public TypedWrite<UserT, DestinationT> withBatchMaxBufferingDuration(
        @Nullable Duration batchMaxBufferingDuration) {
      return toBuilder().setBatchMaxBufferingDuration(batchMaxBufferingDuration).build();
    }

    
    public TypedWrite<UserT, DestinationT> withTempDirectory(
        ValueProvider<ResourceId> tempDirectory) {
      return toBuilder().setTempDirectory(tempDirectory).build();
    }

    
    public TypedWrite<UserT, DestinationT> withTempDirectory(ResourceId tempDirectory) {
      return withTempDirectory(StaticValueProvider.of(tempDirectory));
    }

    
    public TypedWrite<UserT, DestinationT> withShardNameTemplate(String shardTemplate) {
      return toBuilder().setShardTemplate(shardTemplate).build();
    }

    
    public TypedWrite<UserT, DestinationT> withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(filenameSuffix).build();
    }

    
    public TypedWrite<UserT, DestinationT> withNumShards(int numShards) {
      checkArgument(numShards >= 0);
      if (numShards == 0) {
        
        
        
        return withNumShards(null);
      } else {
        return withNumShards(StaticValueProvider.of(numShards));
      }
    }

    
    public TypedWrite<UserT, DestinationT> withNumShards(
        @Nullable ValueProvider<Integer> numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    
    public TypedWrite<UserT, DestinationT> withoutSharding() {
      return withNumShards(1).withShardNameTemplate("");
    }

    
    public TypedWrite<UserT, DestinationT> withDelimiter(char[] delimiter) {
      return toBuilder().setDelimiter(delimiter).build();
    }

    
    public TypedWrite<UserT, DestinationT> withHeader(@Nullable String header) {
      return toBuilder().setHeader(header).build();
    }

    
    public TypedWrite<UserT, DestinationT> withFooter(@Nullable String footer) {
      return toBuilder().setFooter(footer).build();
    }

    
    public TypedWrite<UserT, DestinationT> withWritableByteChannelFactory(
        WritableByteChannelFactory writableByteChannelFactory) {
      return toBuilder().setWritableByteChannelFactory(writableByteChannelFactory).build();
    }

    
    public TypedWrite<UserT, DestinationT> withCompression(Compression compression) {
      checkArgument(compression != null, "compression can not be null");
      return withWritableByteChannelFactory(
          FileBasedSink.CompressionType.fromCanonical(compression));
    }

    
    public TypedWrite<UserT, DestinationT> withWindowedWrites() {
      return toBuilder().setWindowedWrites(true).build();
    }

    public TypedWrite<UserT, DestinationT> withAutoSharding() {
      return toBuilder().setAutoSharding(true).build();
    }

    
    public TypedWrite<UserT, DestinationT> withNoSpilling() {
      return toBuilder().setNoSpilling(true).build();
    }

    
    public TypedWrite<UserT, DestinationT> withBadRecordErrorHandler(
        ErrorHandler<BadRecord, ?> errorHandler) {
      return toBuilder().setBadRecordErrorHandler(errorHandler).build();
    }

    
    public TypedWrite<UserT, DestinationT> skipIfEmpty() {
      return toBuilder().setSkipIfEmpty(true).build();
    }

    private DynamicDestinations<UserT, DestinationT, String> resolveDynamicDestinations() {
      DynamicDestinations<UserT, DestinationT, String> dynamicDestinations =
          getDynamicDestinations();
      if (dynamicDestinations == null) {
        if (getDestinationFunction() != null) {
          
          dynamicDestinations =
              (DynamicDestinations)
                  DynamicFileDestinations.toDefaultPolicies(
                      getDestinationFunction(), getEmptyDestination(), getFormatFunction());
        } else {
          
          FilenamePolicy usedFilenamePolicy = getFilenamePolicy();
          if (usedFilenamePolicy == null) {
            usedFilenamePolicy =
                DefaultFilenamePolicy.fromStandardParameters(
                    getFilenamePrefix(),
                    getShardTemplate(),
                    getFilenameSuffix(),
                    getWindowedWrites());
          }
          dynamicDestinations =
              (DynamicDestinations)
                  DynamicFileDestinations.constant(usedFilenamePolicy, getFormatFunction());
        }
      }
      return dynamicDestinations;
    }

    @Override
    public WriteFilesResult<DestinationT> expand(PCollection<UserT> input) {
      checkState(
          getFilenamePrefix() != null || getTempDirectory() != null,
          "Need to set either the filename prefix or the tempDirectory of a TextIO.Write "
              + "transform.");

      List<?> allToArgs =
          Lists.newArrayList(
              getFilenamePolicy(),
              getDynamicDestinations(),
              getFilenamePrefix(),
              getDestinationFunction());
      checkArgument(
          1
              == Iterables.size(
                  allToArgs.stream()
                      .filter(Predicates.notNull()::apply)
                      .collect(Collectors.toList())),
          "Exactly one of filename policy, dynamic destinations, filename prefix, or destination "
              + "function must be set");

      if (getDynamicDestinations() != null) {
        checkArgument(
            getFormatFunction() == null,
            "A format function should not be specified "
                + "with DynamicDestinations. Use DynamicDestinations.formatRecord instead");
      }
      if (getFilenamePolicy() != null || getDynamicDestinations() != null) {
        checkState(
            getShardTemplate() == null && getFilenameSuffix() == null,
            "shardTemplate and filenameSuffix should only be used with the default "
                + "filename policy");
      }
      ValueProvider<ResourceId> tempDirectory = getTempDirectory();
      if (tempDirectory == null) {
        tempDirectory = getFilenamePrefix();
      }
      WriteFiles<UserT, DestinationT, String> write =
          WriteFiles.to(
              new TextSink<>(
                  tempDirectory,
                  resolveDynamicDestinations(),
                  getDelimiter(),
                  getHeader(),
                  getFooter(),
                  getWritableByteChannelFactory()));
      if (getNumShards() != null) {
        write = write.withNumShards(getNumShards());
      }
      if (getWindowedWrites()) {
        write = write.withWindowedWrites();
      }
      if (getAutoSharding()) {
        write = write.withAutoSharding();
      }
      if (getNoSpilling()) {
        write = write.withNoSpilling();
      }
      if (getBadRecordErrorHandler() != null) {
        write = write.withBadRecordErrorHandler(getBadRecordErrorHandler());
      }
      if (getSkipIfEmpty()) {
        write = write.withSkipIfEmpty();
      }
      if (getBatchSize() != null) {
        write = write.withBatchSize(getBatchSize());
      }
      if (getBatchSizeBytes() != null) {
        write = write.withBatchSizeBytes(getBatchSizeBytes());
      }
      if (getBatchMaxBufferingDuration() != null) {
        write = write.withBatchMaxBufferingDuration(getBatchMaxBufferingDuration());
      }
      return input.apply("WriteFiles", write);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      resolveDynamicDestinations().populateDisplayData(builder);
      builder
          .addIfNotNull(
              DisplayData.item("numShards", getNumShards()).withLabel("Maximum Output Shards"))
          .addIfNotNull(
              DisplayData.item("tempDirectory", getTempDirectory())
                  .withLabel("Directory for temporary files"))
          .addIfNotNull(DisplayData.item("fileHeader", getHeader()).withLabel("File Header"))
          .addIfNotNull(DisplayData.item("fileFooter", getFooter()).withLabel("File Footer"))
          .add(
              DisplayData.item(
                      "writableByteChannelFactory", getWritableByteChannelFactory().toString())
                  .withLabel("Compression/Transformation Type"));
    }
  }

  
  public static class Write extends PTransform<PCollection<String>, PDone> {
    @VisibleForTesting TypedWrite<String, ?> inner;

    Write() {
      this(TextIO.writeCustomType());
    }

    Write(TypedWrite<String, ?> inner) {
      this.inner = inner;
    }

    
    public Write to(String filenamePrefix) {
      return new Write(
          inner.to(filenamePrefix).withFormatFunction(SerializableFunctions.identity()));
    }

    
    public Write to(ResourceId filenamePrefix) {
      return new Write(
          inner.to(filenamePrefix).withFormatFunction(SerializableFunctions.identity()));
    }

    
    public Write to(ValueProvider<String> outputPrefix) {
      return new Write(inner.to(outputPrefix).withFormatFunction(SerializableFunctions.identity()));
    }

    
    public Write toResource(ValueProvider<ResourceId> filenamePrefix) {
      return new Write(
          inner.toResource(filenamePrefix).withFormatFunction(SerializableFunctions.identity()));
    }

    
    public Write to(FilenamePolicy filenamePolicy) {
      return new Write(
          inner.to(filenamePolicy).withFormatFunction(SerializableFunctions.identity()));
    }

    
    @Deprecated
    public Write to(DynamicDestinations<String, ?, String> dynamicDestinations) {
      return new Write(
          inner.to((DynamicDestinations) dynamicDestinations).withFormatFunction(null));
    }

    
    @Deprecated
    public Write to(
        SerializableFunction<String, Params> destinationFunction, Params emptyDestination) {
      return new Write(
          inner
              .to(destinationFunction, emptyDestination)
              .withFormatFunction(SerializableFunctions.identity()));
    }

    
    public Write withTempDirectory(ValueProvider<ResourceId> tempDirectory) {
      return new Write(inner.withTempDirectory(tempDirectory));
    }

    
    public Write withTempDirectory(ResourceId tempDirectory) {
      return new Write(inner.withTempDirectory(tempDirectory));
    }

    
    public Write withShardNameTemplate(String shardTemplate) {
      return new Write(inner.withShardNameTemplate(shardTemplate));
    }

    
    public Write withSuffix(String filenameSuffix) {
      return new Write(inner.withSuffix(filenameSuffix));
    }

    
    public Write withNumShards(int numShards) {
      return new Write(inner.withNumShards(numShards));
    }

    
    public Write withNumShards(@Nullable ValueProvider<Integer> numShards) {
      return new Write(inner.withNumShards(numShards));
    }

    
    public Write withoutSharding() {
      return new Write(inner.withoutSharding());
    }

    
    public Write withDelimiter(char[] delimiter) {
      return new Write(inner.withDelimiter(delimiter));
    }

    
    public Write withHeader(@Nullable String header) {
      return new Write(inner.withHeader(header));
    }

    
    public Write withFooter(@Nullable String footer) {
      return new Write(inner.withFooter(footer));
    }

    
    public Write withWritableByteChannelFactory(
        WritableByteChannelFactory writableByteChannelFactory) {
      return new Write(inner.withWritableByteChannelFactory(writableByteChannelFactory));
    }

    
    public Write withCompression(Compression compression) {
      return new Write(inner.withCompression(compression));
    }

    
    public Write withWindowedWrites() {
      return new Write(inner.withWindowedWrites());
    }

    
    public Write withAutoSharding() {
      return new Write(inner.withAutoSharding());
    }

    
    public Write withNoSpilling() {
      return new Write(inner.withNoSpilling());
    }

    
    public Write withBatchSize(@Nullable Integer batchSize) {
      return new Write(inner.withBatchSize(batchSize));
    }

    
    public Write withBatchSizeBytes(@Nullable Integer batchSizeBytes) {
      return new Write(inner.withBatchSizeBytes(batchSizeBytes));
    }

    
    public Write withBatchMaxBufferingDuration(@Nullable Duration batchMaxBufferingDuration) {
      return new Write(inner.withBatchMaxBufferingDuration(batchMaxBufferingDuration));
    }

    
    public <DestinationT> TypedWrite<String, DestinationT> withOutputFilenames() {
      return (TypedWrite) inner;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      inner.populateDisplayData(builder);
    }

    @Override
    public PDone expand(PCollection<String> input) {
      inner.expand(input);
      return PDone.in(input.getPipeline());
    }
  }

  
  @Deprecated
  public enum CompressionType {
    
    AUTO(Compression.AUTO),

    
    UNCOMPRESSED(Compression.UNCOMPRESSED),

    
    GZIP(Compression.GZIP),

    
    BZIP2(Compression.BZIP2),

    
    ZIP(Compression.ZIP),

    
    ZSTD(Compression.ZSTD),

    
    DEFLATE(Compression.DEFLATE);

    private final Compression canonical;

    CompressionType(Compression canonical) {
      this.canonical = canonical;
    }

    
    public boolean matches(String filename) {
      return canonical.matches(filename);
    }
  }

  

  
  public static Sink sink() {
    return new AutoValue_TextIO_Sink.Builder().build();
  }

  
  @AutoValue
  public abstract static class Sink implements FileIO.Sink<String> {

    abstract @Nullable String getHeader();

    abstract @Nullable String getFooter();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setHeader(String header);

      abstract Builder setFooter(String footer);

      abstract Sink build();
    }

    public Sink withHeader(String header) {
      checkArgument(header != null, "header can not be null");
      return toBuilder().setHeader(header).build();
    }

    public Sink withFooter(String footer) {
      checkArgument(footer != null, "footer can not be null");
      return toBuilder().setFooter(footer).build();
    }

    private transient @Nullable PrintWriter writer;

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      writer =
          new PrintWriter(
              new BufferedWriter(new OutputStreamWriter(Channels.newOutputStream(channel), UTF_8)));
      if (getHeader() != null) {
        writer.println(getHeader());
      }
    }

    @Override
    public void write(String element) throws IOException {
      writer.println(element);
    }

    @Override
    public void flush() throws IOException {
      if (getFooter() != null) {
        writer.println(getFooter());
      }
      
      writer.flush();
    }
  }

  
  private TextIO() {}
}
