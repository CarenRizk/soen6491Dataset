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
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Params;
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.WritableByteChannelFactory;
import org.apache.beam.sdk.io.FileIO.MatchConfiguration;
import org.apache.beam.sdk.io.TextIO.Write;
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
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
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

    abstract @UnknownKeyFor @NonNull @Initialized ValueProvider<@UnknownKeyFor @NonNull @Initialized String> getFilepattern();

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

    
    // Optimized by LLM: Consolidated deprecated and non-deprecated compression methods
    public Read withCompression(TextIO.CompressionType compressionType) {
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

    private PCollection<String> expandRead(PBegin input) {
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

    @Override
    public PCollection<String> expand(PBegin input) {
      return expandRead(input);
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

    
    // Optimized by LLM: Consolidated deprecated and non-deprecated compression methods
    public ReadAll withCompression(TextIO.CompressionType compressionType) {
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

    
    abstract Optional<ValueProvider<ResourceId>> getFilenamePrefix();

    
    abstract Optional<String> getFilenameSuffix();

    
    abstract Optional<ValueProvider<ResourceId>> getTempDirectory();

    
    @SuppressWarnings("mutable") 
    abstract char[] getDelimiter();

    
    abstract Optional<String> getHeader();

    
    abstract Optional<String> getFooter();

    
    abstract Optional<ValueProvider<Integer>> getNumShards();

    
    abstract Optional<String> getShardTemplate();

    
    abstract Optional<FilenamePolicy> getFilenamePolicy();

    
    abstract Optional<DynamicDestinations<UserT, DestinationT, String>> getDynamicDestinations();

    
    abstract Optional<SerializableFunction<UserT, Params>> getDestinationFunction();

    
    abstract Optional<Params> getEmptyDestination();

    
    abstract Optional<SerializableFunction<UserT, String>> getFormatFunction();

    
    abstract Optional<Integer> getBatchSize();

    
    abstract Optional<Integer> getBatchSizeBytes();

    
    abstract Optional<Duration> getBatchMaxBufferingDuration();

    
    abstract boolean getWindowedWrites();

    
    abstract boolean getAutoSharding();

    
    abstract boolean getNoSpilling();

    
    abstract boolean getSkipIfEmpty();

    
    abstract WritableByteChannelFactory getWritableByteChannelFactory();

    abstract Optional<ErrorHandler<BadRecord, ?>> getBadRecordErrorHandler();

    abstract Builder<UserT, DestinationT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<UserT, DestinationT> {
      abstract Builder<UserT, DestinationT> setFilenamePrefix(
          Optional<ValueProvider<ResourceId>> filenamePrefix);

      abstract Builder<UserT, DestinationT> setTempDirectory(
          Optional<ValueProvider<ResourceId>> tempDirectory);

      abstract Builder<UserT, DestinationT> setShardTemplate(Optional<String> shardTemplate);

      abstract Builder<UserT, DestinationT> setFilenameSuffix(Optional<String> filenameSuffix);

      abstract Builder<UserT, DestinationT> setHeader(String header);

      abstract Builder<UserT, DestinationT> setFooter(String footer);

      abstract Builder<UserT, DestinationT> setDelimiter(char[] delimiter);

      abstract Builder<UserT, DestinationT> setFilenamePolicy(
          Optional<FilenamePolicy> filenamePolicy);

      abstract Builder<UserT, DestinationT> setDynamicDestinations(
          DynamicDestinations dynamicDestinations);

      abstract Builder<UserT, DestinationT> setDestinationFunction(
          Optional<SerializableFunction<UserT, Params>> destinationFunction);

      abstract Builder<UserT, DestinationT> setEmptyDestination(Params emptyDestination);

      abstract Builder<UserT, DestinationT> setFormatFunction(
          Optional<SerializableFunction<UserT, String>> formatFunction);

      abstract Builder<UserT, DestinationT> setNumShards(
          Optional<ValueProvider<Integer>> numShards);

      abstract Builder<UserT, DestinationT> setBatchSize(Optional<Integer> batchSize);

      abstract Builder<UserT, DestinationT> setBatchSizeBytes(Optional<Integer> batchSizeBytes);

      abstract Builder<UserT, DestinationT> setBatchMaxBufferingDuration(
          Optional<Duration> batchMaxBufferingDuration);

      abstract Builder<UserT, DestinationT> setWindowedWrites(boolean windowedWrites);

      abstract Builder<UserT, DestinationT> setAutoSharding(boolean windowedWrites);

      abstract Builder<UserT, DestinationT> setNoSpilling(boolean noSpilling);

      abstract Builder<UserT, DestinationT> setSkipIfEmpty(boolean noSpilling);

      abstract Builder<UserT, DestinationT> setWritableByteChannelFactory(
          WritableByteChannelFactory writableByteChannelFactory);

      abstract Builder<UserT, DestinationT> setBadRecordErrorHandler(
          Optional<ErrorHandler<BadRecord, ?>> badRecordErrorHandler);

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
      return toBuilder().setFilenamePolicy(Optional.of(filenamePolicy)).build();
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
              .setDestinationFunction(Optional.of(destinationFunction))
              .setEmptyDestination(emptyDestination)
              .build();
    }

    
    public TypedWrite<UserT, DestinationT> toResource(ValueProvider<ResourceId> filenamePrefix) {
      return toBuilder().setFilenamePrefix(Optional.of(filenamePrefix)).build();
    }

    
    @Deprecated
    public TypedWrite<UserT, DestinationT> withFormatFunction(
        Optional<SerializableFunction<UserT, String>> formatFunction) {
      return toBuilder().setFormatFunction(formatFunction).build();
    }

    
    public TypedWrite<UserT, DestinationT> withBatchSize(Optional<Integer> batchSize) {
      return toBuilder().setBatchSize(batchSize).build();
    }

    
    public TypedWrite<UserT, DestinationT> withBatchSizeBytes(Optional<Integer> batchSizeBytes) {
      return toBuilder().setBatchSizeBytes(batchSizeBytes).build();
    }

    
    public TypedWrite<UserT, DestinationT> withBatchMaxBufferingDuration(
        Optional<Duration> batchMaxBufferingDuration) {
      return toBuilder().setBatchMaxBufferingDuration(batchMaxBufferingDuration).build();
    }

    
    public TypedWrite<UserT, DestinationT> withTempDirectory(
        ValueProvider<ResourceId> tempDirectory) {
      return toBuilder().setTempDirectory(Optional.of(tempDirectory)).build();
    }

    
    public TypedWrite<UserT, DestinationT> withTempDirectory(ResourceId tempDirectory) {
      return withTempDirectory(StaticValueProvider.of(tempDirectory));
    }

    
    public TypedWrite<UserT, DestinationT> withShardNameTemplate(String shardTemplate) {
      return toBuilder().setShardTemplate(Optional.of(shardTemplate)).build();
    }

    
    public TypedWrite<UserT, DestinationT> withSuffix(String filenameSuffix) {
      return toBuilder().setFilenameSuffix(Optional.of(filenameSuffix)).build();
    }

    
    public TypedWrite<UserT, DestinationT> withNumShards(int numShards) {
      checkArgument(numShards >= 0);
      if (numShards == 0) {
        return withNumShards(Optional.empty());
      } else {
        return withNumShards(Optional.of(StaticValueProvider.of(numShards)));
      }
    }

    
    public TypedWrite<UserT, DestinationT> withNumShards(
        Optional<ValueProvider<Integer>> numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    
    public TypedWrite<UserT, DestinationT> withoutSharding() {
      return withNumShards(1).withShardNameTemplate("");
    }

    
    public TypedWrite<UserT, DestinationT> withDelimiter(char[] delimiter) {
      return toBuilder().setDelimiter(delimiter).build();
    }

    
    public TypedWrite<UserT, DestinationT> withHeader(String header) {
      return toBuilder().setHeader(header).build();
    }

    
    public TypedWrite<UserT, DestinationT> withFooter(String footer) {
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
      return toBuilder().setBadRecordErrorHandler(Optional.of(errorHandler)).build();
    }

    
    public TypedWrite<UserT, DestinationT> skipIfEmpty() {
      return toBuilder().setSkipIfEmpty(true).build();
    }

    private DynamicDestinations<UserT, DestinationT, String> resolveDynamicDestinations() {
      DynamicDestinations<UserT, DestinationT, String> dynamicDestinations =
          getDynamicDestinations().orElse(null);
      if (dynamicDestinations == null) {
        if (getDestinationFunction().isPresent()) {
          
          dynamicDestinations =
              (DynamicDestinations)
                  DynamicFileDestinations.toDefaultPolicies(
                      getDestinationFunction().get(), getEmptyDestination().orElse(null), getFormatFunction().orElse(null));
        } else {
          
          FilenamePolicy usedFilenamePolicy = getFilenamePolicy().orElse(null);
          if (usedFilenamePolicy == null) {
            usedFilenamePolicy =
                DefaultFilenamePolicy.fromStandardParameters(
                    getFilenamePrefix().orElse(null),
                    getShardTemplate().orElse(null),
                    getFilenameSuffix().orElse(null),
                    getWindowedWrites());
          }
          dynamicDestinations =
              (DynamicDestinations)
                  DynamicFileDestinations.constant(usedFilenamePolicy, getFormatFunction().orElse(null));
        }
      }
      return dynamicDestinations;
    }

    @Override
    public WriteFilesResult<DestinationT> expand(PCollection<UserT> input) {
      checkState(
          getFilenamePrefix().isPresent() || getTempDirectory().isPresent(),
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

      if (getDynamicDestinations().isPresent()) {
        checkArgument(
            !getFormatFunction().isPresent(),
            "A format function should not be specified "
                + "with DynamicDestinations. Use DynamicDestinations.formatRecord instead");
      }
      if (getFilenamePolicy().isPresent() || getDynamicDestinations().isPresent()) {
        checkState(
            !getShardTemplate().isPresent() && !getFilenameSuffix().isPresent(),
            "shardTemplate and filenameSuffix should only be used with the default "
                + "filename policy");
      }
      ValueProvider<ResourceId> tempDirectory = getTempDirectory().orElse(getFilenamePrefix().orElse(null));
      WriteFiles<UserT, DestinationT, String> write =
          WriteFiles.to(
              new TextSink<>(
                  tempDirectory,
                  resolveDynamicDestinations(),
                  getDelimiter(),
                  getHeader().orElse(null),
                  getFooter().orElse(null),
                  getWritableByteChannelFactory()));
      if (getNumShards().isPresent()) {
        write = write.withNumShards(getNumShards().get());
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
      if (getBadRecordErrorHandler().isPresent()) {
        write = write.withBadRecordErrorHandler(getBadRecordErrorHandler().get());
      }
      if (getSkipIfEmpty()) {
        write = write.withSkipIfEmpty();
      }
      if (getBatchSize().isPresent()) {
        write = write.withBatchSize(getBatchSize().get());
      }
      if (getBatchSizeBytes().isPresent()) {
        write = write.withBatchSizeBytes(getBatchSizeBytes().get());
      }
      if (getBatchMaxBufferingDuration().isPresent()) {
        write = write.withBatchMaxBufferingDuration(getBatchMaxBufferingDuration().get());
      }
      return input.apply("WriteFiles", write);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      resolveDynamicDestinations().populateDisplayData(builder);
      builder
          .addIfNotNull(
              DisplayData.item("numShards", getNumShards().orElse(null)).withLabel("Maximum Output Shards"))
          .addIfNotNull(
              DisplayData.item("tempDirectory", getTempDirectory().orElse(null))
                  .withLabel("Directory for temporary files"))
          .addIfNotNull(DisplayData.item("fileHeader", getHeader().orElse(null)).withLabel("File Header"))
          .addIfNotNull(DisplayData.item("fileFooter", getFooter().orElse(null)).withLabel("File Footer"))
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
            inner.to(filenamePrefix).withFormatFunction(Optional.empty()));
      }
    
    public Write to(ResourceId filenamePrefix) {
      return new Write(
          inner.to(filenamePrefix).withFormatFunction(Optional.empty()));
    }

    
    public Write to(ValueProvider<String> outputPrefix) {
      return new Write(inner.to(outputPrefix).withFormatFunction(Optional.empty()));
    }

    
    public Write toResource(ValueProvider<ResourceId> filenamePrefix) {
      return new Write(
          inner.toResource(filenamePrefix).withFormatFunction(Optional.empty()));
    }

    
    public Write to(FilenamePolicy filenamePolicy) {
      return new Write(
          inner.to(filenamePolicy).withFormatFunction(Optional.empty()));
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
              .withFormatFunction(Optional.empty()));
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

    
    public Write withNumShards(Optional<ValueProvider<Integer>> numShards) {
      return new Write(inner.withNumShards(numShards));
    }

    
    public Write withoutSharding() {
      return new Write(inner.withoutSharding());
    }

    
    public Write withDelimiter(char[] delimiter) {
      return new Write(inner.withDelimiter(delimiter));
    }

    
    public Write withHeader(String header) {
      return new Write(inner.withHeader(header));
    }

    
    public Write withFooter(String footer) {
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

    
    public Write withBatchSize(Optional<Integer> batchSize) {
      return new Write(inner.withBatchSize(batchSize));
    }

    
    public Write withBatchSizeBytes(Optional<Integer> batchSizeBytes) {
      return new Write(inner.withBatchSizeBytes(batchSizeBytes));
    }

    
    public Write withBatchMaxBufferingDuration(Optional<Duration> batchMaxBufferingDuration) {
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

    abstract Optional<String> getHeader();

    abstract Optional<String> getFooter();

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

    private transient Optional<PrintWriter> writer = Optional.empty();

    @Override
    public void open(WritableByteChannel channel) throws IOException {
      writer =
          Optional.of(
              new PrintWriter(
                  new BufferedWriter(new OutputStreamWriter(Channels.newOutputStream(channel), UTF_8))));
      if (getHeader().isPresent()) {
        writer.get().println(getHeader().get());
      }
    }

    @Override
    public void write(String element) throws IOException {
      writer.get().println(element);
    }

    @Override
    public void flush() throws IOException {
      if (getFooter().isPresent()) {
        writer.get().println(getFooter().get());
      }
      
      writer.get().flush();
    }
  }

  
  private TextIO() {}
}