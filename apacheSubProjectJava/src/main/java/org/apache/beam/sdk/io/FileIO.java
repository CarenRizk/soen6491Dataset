/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io;

import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_FILE;
import static org.apache.beam.sdk.transforms.Contextful.fn;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MetadataCoderV2;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Requirements;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.Watch.Growth.PollFn;
import org.apache.beam.sdk.transforms.Watch.Growth.TerminationCondition;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class FileIO {
  private static final Logger LOG = LoggerFactory.getLogger(FileIO.class);

  
  public static Match match() {
    return new AutoValue_FileIO_Match.Builder()
        .setConfiguration(MatchConfiguration.create(EmptyMatchTreatment.DISALLOW))
        .build();
  }

  
  public static MatchAll matchAll() {
    return new AutoValue_FileIO_MatchAll.Builder()
        .setConfiguration(MatchConfiguration.create(EmptyMatchTreatment.ALLOW_IF_WILDCARD))
        .build();
  }

  
  public static ReadMatches readMatches() {
    return new AutoValue_FileIO_ReadMatches.Builder()
        .setCompression(Compression.AUTO)
        .setDirectoryTreatment(ReadMatches.DirectoryTreatment.SKIP)
        .build();
  }

  
  public static <InputT> Write<Void, InputT> write() {
    return new AutoValue_FileIO_Write.Builder<Void, InputT>()
        .setDynamic(false)
        .setCompression(Compression.UNCOMPRESSED)
        .setIgnoreWindowing(false)
        .setAutoSharding(false)
        .setNoSpilling(false)
        .build();
  }

  
  public static <DestT, InputT> Write<DestT, InputT> writeDynamic() {
    return new AutoValue_FileIO_Write.Builder<DestT, InputT>()
        .setDynamic(true)
        .setCompression(Compression.UNCOMPRESSED)
        .setIgnoreWindowing(false)
        .setAutoSharding(false)
        .setNoSpilling(false)
        .build();
  }

  
  public static final class ReadableFile {
    private final MatchResult.Metadata metadata;
    private final Compression compression;

    ReadableFile(MatchResult.Metadata metadata, Compression compression) {
      this.metadata = metadata;
      this.compression = compression;
    }

    
    public MatchResult.Metadata getMetadata() {
      return metadata;
    }

    
    public Compression getCompression() {
      return compression;
    }

    
    public ReadableByteChannel open() throws IOException {
      return compression.readDecompressed(FileSystems.open(metadata.resourceId()));
    }

    
    public SeekableByteChannel openSeekable() throws IOException {
      checkState(
          getMetadata().isReadSeekEfficient(),
          "The file %s is not seekable",
          metadata.resourceId());
      return (SeekableByteChannel) open();
    }

    
    public byte[] readFullyAsBytes() throws IOException {
      try (InputStream stream = Channels.newInputStream(open())) {
        return StreamUtils.getBytesWithoutClosing(stream);
      }
    }

    
    public String readFullyAsUTF8String() throws IOException {
      return new String(readFullyAsBytes(), StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
      return "ReadableFile{metadata=" + metadata + ", compression=" + compression + '}';
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ReadableFile that = (ReadableFile) o;
      return Objects.equal(metadata, that.metadata) && compression == that.compression;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(metadata, compression);
    }
  }

  
  @AutoValue
  public abstract static class MatchConfiguration implements HasDisplayData, Serializable {
    
    public static MatchConfiguration create(EmptyMatchTreatment emptyMatchTreatment) {
      return new AutoValue_FileIO_MatchConfiguration.Builder()
          .setEmptyMatchTreatment(emptyMatchTreatment)
          .setMatchUpdatedFiles(false)
          .build();
    }

    public abstract EmptyMatchTreatment getEmptyMatchTreatment();

    public abstract boolean getMatchUpdatedFiles();

    public abstract @Nullable Duration getWatchInterval();

    abstract @Nullable TerminationCondition<String, ?> getWatchTerminationCondition();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setEmptyMatchTreatment(EmptyMatchTreatment treatment);

      abstract Builder setMatchUpdatedFiles(boolean matchUpdatedFiles);

      abstract Builder setWatchInterval(Duration watchInterval);

      abstract Builder setWatchTerminationCondition(TerminationCondition<String, ?> condition);

      abstract MatchConfiguration build();
    }

    
    public MatchConfiguration withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return toBuilder().setEmptyMatchTreatment(treatment).build();
    }

    
    public MatchConfiguration continuously(
        Duration interval, TerminationCondition<String, ?> condition, boolean matchUpdatedFiles) {
      LOG.warn(
          "Matching Continuously is stateful, and can scale poorly. Consider using Pub/Sub "
              + "Notifications (https://cloud.google.com/storage/docs/pubsub-notifications) if possible");
      return toBuilder()
          .setWatchInterval(interval)
          .setWatchTerminationCondition(condition)
          .setMatchUpdatedFiles(matchUpdatedFiles)
          .build();
    }

    
    public MatchConfiguration continuously(
        Duration interval, TerminationCondition<String, ?> condition) {
      return continuously(interval, condition, false);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(
          DisplayData.item("emptyMatchTreatment", getEmptyMatchTreatment().toString())
              .withLabel("Treatment of filepatterns that match no files"));
      if (getWatchInterval() != null) {
        builder
            .add(
                DisplayData.item("watchForNewFilesInterval", getWatchInterval())
                    .withLabel("Interval to watch for new files"))
            .add(
                DisplayData.item("isMatchUpdatedFiles", getMatchUpdatedFiles())
                    .withLabel("If also match for files with timestamp change"));
      }
    }
  }

  
  @AutoValue
  public abstract static class Match extends PTransform<PBegin, PCollection<MatchResult.Metadata>> {

    abstract @Nullable ValueProvider<String> getFilepattern();

    abstract MatchConfiguration getConfiguration();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilepattern(ValueProvider<String> filepattern);

      abstract Builder setConfiguration(MatchConfiguration configuration);

      abstract Match build();
    }

    
    public Match filepattern(String filepattern) {
      return this.filepattern(StaticValueProvider.of(filepattern));
    }

    
    public Match filepattern(ValueProvider<String> filepattern) {
      return toBuilder().setFilepattern(filepattern).build();
    }

    
    public Match withConfiguration(MatchConfiguration configuration) {
      return toBuilder().setConfiguration(configuration).build();
    }

    
    public Match withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withConfiguration(getConfiguration().withEmptyMatchTreatment(treatment));
    }

    
    public Match continuously(
        Duration pollInterval,
        TerminationCondition<String, ?> terminationCondition,
        boolean matchUpdatedFiles) {
      return withConfiguration(
          getConfiguration().continuously(pollInterval, terminationCondition, matchUpdatedFiles));
    }

    
    public Match continuously(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return continuously(pollInterval, terminationCondition, false);
    }

    @Override
    public PCollection<MatchResult.Metadata> expand(PBegin input) {
      return input
          .apply("Create filepattern", Create.ofProvider(getFilepattern(), StringUtf8Coder.of()))
          .apply("Via MatchAll", matchAll().withConfiguration(getConfiguration()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .addIfNotNull(
              DisplayData.item("filePattern", getFilepattern()).withLabel("Input File Pattern"))
          .include("configuration", getConfiguration());
    }
  }

  
  @AutoValue
  public abstract static class MatchAll
      extends PTransform<PCollection<String>, PCollection<MatchResult.Metadata>> {
    abstract MatchConfiguration getConfiguration();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConfiguration(MatchConfiguration configuration);

      abstract MatchAll build();
    }

    
    public MatchAll withConfiguration(MatchConfiguration configuration) {
      return toBuilder().setConfiguration(configuration).build();
    }

    
    public MatchAll withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
      return withConfiguration(getConfiguration().withEmptyMatchTreatment(treatment));
    }

    
    public MatchAll continuously(
        Duration pollInterval,
        TerminationCondition<String, ?> terminationCondition,
        boolean matchUpdatedFiles) {
      return withConfiguration(
          getConfiguration().continuously(pollInterval, terminationCondition, matchUpdatedFiles));
    }

    
    public MatchAll continuously(
        Duration pollInterval, TerminationCondition<String, ?> terminationCondition) {
      return continuously(pollInterval, terminationCondition, false);
    }

    @Override
    public PCollection<MatchResult.Metadata> expand(PCollection<String> input) {
      PCollection<MatchResult.Metadata> res;
      if (getConfiguration().getWatchInterval() == null) {
        res =
            input.apply(
                "Match filepatterns",
                ParDo.of(new MatchFn(getConfiguration().getEmptyMatchTreatment())));
      } else {
        if (getConfiguration().getMatchUpdatedFiles()) {
          res =
              input
                  .apply(createWatchTransform(new ExtractFilenameAndLastUpdateFn()))
                  .apply(Values.create())
                  .setCoder(MetadataCoderV2.of());
        } else {
          res = input.apply(createWatchTransform(new ExtractFilenameFn())).apply(Values.create());
        }
      }
      return res.apply(Reshuffle.viaRandomKey());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.include("configuration", getConfiguration());
    }

    
    private <KeyT> Watch.Growth<String, MatchResult.Metadata, KeyT> createWatchTransform(
        SerializableFunction<MatchResult.Metadata, KeyT> outputKeyFn) {
      return Watch.growthOf(Contextful.of(new MatchPollFn(), Requirements.empty()), outputKeyFn)
          .withPollInterval(getConfiguration().getWatchInterval())
          .withTerminationPerInput(getConfiguration().getWatchTerminationCondition());
    }

    private static class MatchFn extends DoFn<String, MatchResult.Metadata> {
      private final EmptyMatchTreatment emptyMatchTreatment;

      public MatchFn(EmptyMatchTreatment emptyMatchTreatment) {
        this.emptyMatchTreatment = emptyMatchTreatment;
      }

      @ProcessElement
      public void process(ProcessContext c) throws Exception {
        String filepattern = c.element();
        MatchResult match = FileSystems.match(filepattern, emptyMatchTreatment);
        LOG.info("Matched {} files for pattern {}", match.metadata().size(), filepattern);
        for (MatchResult.Metadata metadata : match.metadata()) {
          c.output(metadata);
        }
      }
    }

    private static class MatchPollFn extends PollFn<String, MatchResult.Metadata> {
      @Override
      public Watch.Growth.PollResult<MatchResult.Metadata> apply(String element, Context c)
          throws Exception {
        Instant now = Instant.now();
        return Watch.Growth.PollResult.incomplete(
                now, FileSystems.match(element, EmptyMatchTreatment.ALLOW).metadata())
            .withWatermark(now);
      }
    }

    private static class ExtractFilenameFn
        implements SerializableFunction<MatchResult.Metadata, String> {
      @Override
      public String apply(MatchResult.Metadata input) {
        return input.resourceId().toString();
      }
    }

    private static class ExtractFilenameAndLastUpdateFn
        implements SerializableFunction<MatchResult.Metadata, KV<String, Long>> {
      @Override
      public KV<String, Long> apply(MatchResult.Metadata input) throws RuntimeException {
        long timestamp = input.lastModifiedMillis();
        if (0L == timestamp) {
          throw new RuntimeException("Extract file timestamp failed: got file timestamp == 0.");
        }
        return KV.of(input.resourceId().toString(), timestamp);
      }
    }
  }

  
  @AutoValue
  public abstract static class ReadMatches
      extends PTransform<PCollection<MatchResult.Metadata>, PCollection<ReadableFile>> {
    
    public enum DirectoryTreatment {
      SKIP,
      PROHIBIT
    }

    abstract Compression getCompression();

    abstract DirectoryTreatment getDirectoryTreatment();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setCompression(Compression compression);

      abstract Builder setDirectoryTreatment(DirectoryTreatment directoryTreatment);

      abstract ReadMatches build();
    }

    
    public ReadMatches withCompression(Compression compression) {
      checkArgument(compression != null, "compression can not be null");
      return toBuilder().setCompression(compression).build();
    }

    
    public ReadMatches withDirectoryTreatment(DirectoryTreatment directoryTreatment) {
      checkArgument(directoryTreatment != null, "directoryTreatment can not be null");
      return toBuilder().setDirectoryTreatment(directoryTreatment).build();
    }

    @Override
    public PCollection<ReadableFile> expand(PCollection<MatchResult.Metadata> input) {
      return input.apply(ParDo.of(new ToReadableFileFn(this)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("compression", getCompression().toString()));
      builder.add(DisplayData.item("directoryTreatment", getDirectoryTreatment().toString()));
    }

    
    static boolean shouldSkipDirectory(
        MatchResult.Metadata metadata, DirectoryTreatment directoryTreatment) {
      if (metadata.resourceId().isDirectory()) {
        switch (directoryTreatment) {
          case SKIP:
            return true;
          case PROHIBIT:
            throw new IllegalArgumentException(
                "Trying to read " + metadata.resourceId() + " which is a directory");

          default:
            throw new UnsupportedOperationException(
                "Unknown DirectoryTreatment: " + directoryTreatment);
        }
      }

      return false;
    }

    
    static ReadableFile matchToReadableFile(
        MatchResult.Metadata metadata, Compression compression) {

      compression =
          (compression == Compression.AUTO)
              ? Compression.detect(metadata.resourceId().getFilename())
              : compression;
      return new ReadableFile(
          MatchResult.Metadata.builder()
              .setResourceId(metadata.resourceId())
              .setSizeBytes(metadata.sizeBytes())
              .setLastModifiedMillis(metadata.lastModifiedMillis())
              .setIsReadSeekEfficient(
                  metadata.isReadSeekEfficient() && compression == Compression.UNCOMPRESSED)
              .build(),
          compression);
    }

    private static class ToReadableFileFn extends DoFn<MatchResult.Metadata, ReadableFile> {
      private final ReadMatches spec;

      private ToReadableFileFn(ReadMatches spec) {
        this.spec = spec;
      }

      @ProcessElement
      public void process(ProcessContext c) {
        if (shouldSkipDirectory(c.element(), spec.getDirectoryTreatment())) {
          return;
        }
        ReadableFile r = matchToReadableFile(c.element(), spec.getCompression());
        c.output(r);
      }
    }
  }

  
  public interface Sink<ElementT> extends Serializable {
    
    void open(WritableByteChannel channel) throws IOException;

    
    void write(ElementT element) throws IOException;

    
    void flush() throws IOException;
  }

  
  @AutoValue
  public abstract static class Write<DestinationT, UserT>
      extends PTransform<PCollection<UserT>, WriteFilesResult<DestinationT>> {
    
    public interface FileNaming extends Serializable {
      
      String getFilename(
          BoundedWindow window,
          PaneInfo pane,
          int numShards,
          int shardIndex,
          Compression compression);
    }

    public static FileNaming defaultNaming(final String prefix, final String suffix) {
      return defaultNaming(StaticValueProvider.of(prefix), StaticValueProvider.of(suffix));
    }

    
    public static FileNaming defaultNaming(
        final ValueProvider<String> prefix, final ValueProvider<String> suffix) {
      return (window, pane, numShards, shardIndex, compression) -> {
        checkArgument(window != null, "window can not be null");
        checkArgument(pane != null, "pane can not be null");
        checkArgument(compression != null, "compression can not be null");
        StringBuilder res = new StringBuilder(prefix.get());
        if (window != GlobalWindow.INSTANCE) {
          if (res.length() > 0) {
            res.append("-");
          }
          checkArgument(
              window instanceof IntervalWindow,
              "defaultNaming() supports only windows of type %s, " + "but got window %s of type %s",
              IntervalWindow.class.getSimpleName(),
              window,
              window.getClass().getSimpleName());
          IntervalWindow iw = (IntervalWindow) window;
          res.append(iw.start().toString()).append("-").append(iw.end().toString());
        }
        boolean isOnlyFiring = pane.isFirst() && pane.isLast();
        if (!isOnlyFiring) {
          if (res.length() > 0) {
            res.append("-");
          }
          res.append(pane.getIndex());
        }
        if (res.length() > 0) {
          res.append("-");
        }
        String numShardsStr = String.valueOf(numShards);
        // A trillion shards per window per pane ought to be enough for everybody.
        DecimalFormat df =
            new DecimalFormat("000000000000".substring(0, Math.max(5, numShardsStr.length())));
        res.append(df.format(shardIndex)).append("-of-").append(df.format(numShards));
        res.append(suffix.get());
        res.append(compression.getSuggestedSuffix());
        return res.toString();
      };
    }

    public static FileNaming relativeFileNaming(
        final ValueProvider<String> baseDirectory, final FileNaming innerNaming) {
      return (window, pane, numShards, shardIndex, compression) ->
          FileSystems.matchNewResource(baseDirectory.get(), true /* isDirectory */)
              .resolve(
                  innerNaming.getFilename(window, pane, numShards, shardIndex, compression),
                  RESOLVE_FILE)
              .toString();
    }

    abstract boolean getDynamic();

    abstract @Nullable Contextful<Fn<DestinationT, Sink<?>>> getSinkFn();

    abstract @Nullable Contextful<Fn<UserT, ?>> getOutputFn();

    abstract @Nullable Contextful<Fn<UserT, DestinationT>> getDestinationFn();

    abstract @Nullable ValueProvider<String> getOutputDirectory();

    abstract @Nullable ValueProvider<String> getFilenamePrefix();

    abstract @Nullable ValueProvider<String> getFilenameSuffix();

    abstract @Nullable FileNaming getConstantFileNaming();

    abstract @Nullable Contextful<Fn<DestinationT, FileNaming>> getFileNamingFn();

    abstract @Nullable DestinationT getEmptyWindowDestination();

    abstract @Nullable Coder<DestinationT> getDestinationCoder();

    abstract @Nullable ValueProvider<String> getTempDirectory();

    abstract Compression getCompression();

    abstract @Nullable ValueProvider<Integer> getNumShards();

    abstract @Nullable PTransform<PCollection<UserT>, PCollectionView<Integer>> getSharding();

    abstract boolean getIgnoreWindowing();

    abstract boolean getAutoSharding();

    abstract boolean getNoSpilling();

    abstract @Nullable ErrorHandler<BadRecord, ?> getBadRecordErrorHandler();

    abstract Builder<DestinationT, UserT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<DestinationT, UserT> {
      abstract Builder<DestinationT, UserT> setDynamic(boolean dynamic);

      abstract Builder<DestinationT, UserT> setSinkFn(Contextful<Fn<DestinationT, Sink<?>>> sink);

      abstract Builder<DestinationT, UserT> setOutputFn(Contextful<Fn<UserT, ?>> outputFn);

      abstract Builder<DestinationT, UserT> setDestinationFn(
          Contextful<Fn<UserT, DestinationT>> destinationFn);

      abstract Builder<DestinationT, UserT> setOutputDirectory(
          ValueProvider<String> outputDirectory);

      abstract Builder<DestinationT, UserT> setFilenamePrefix(ValueProvider<String> filenamePrefix);

      abstract Builder<DestinationT, UserT> setFilenameSuffix(ValueProvider<String> filenameSuffix);

      abstract Builder<DestinationT, UserT> setConstantFileNaming(FileNaming constantFileNaming);

      abstract Builder<DestinationT, UserT> setFileNamingFn(
          Contextful<Fn<DestinationT, FileNaming>> namingFn);

      abstract Builder<DestinationT, UserT> setEmptyWindowDestination(
          DestinationT emptyWindowDestination);

      abstract Builder<DestinationT, UserT> setDestinationCoder(
          Coder<DestinationT> destinationCoder);

      abstract Builder<DestinationT, UserT> setTempDirectory(
          ValueProvider<String> tempDirectoryProvider);

      abstract Builder<DestinationT, UserT> setCompression(Compression compression);

      abstract Builder<DestinationT, UserT> setNumShards(
          @Nullable ValueProvider<Integer> numShards);

      abstract Builder<DestinationT, UserT> setSharding(
          PTransform<PCollection<UserT>, PCollectionView<Integer>> sharding);

      abstract Builder<DestinationT, UserT> setIgnoreWindowing(boolean ignoreWindowing);

      abstract Builder<DestinationT, UserT> setAutoSharding(boolean autosharding);

      abstract Builder<DestinationT, UserT> setNoSpilling(boolean noSpilling);

      abstract Builder<DestinationT, UserT> setBadRecordErrorHandler(
          @Nullable ErrorHandler<BadRecord, ?> badRecordErrorHandler);

      abstract Write<DestinationT, UserT> build();
    }

    
    public Write<DestinationT, UserT> by(SerializableFunction<UserT, DestinationT> destinationFn) {
      checkArgument(destinationFn != null, "destinationFn can not be null");
      return by(fn(destinationFn));
    }

    
    public Write<DestinationT, UserT> by(Contextful<Fn<UserT, DestinationT>> destinationFn) {
      checkArgument(destinationFn != null, "destinationFn can not be null");
      return toBuilder().setDestinationFn(destinationFn).build();
    }

    
    public <OutputT> Write<DestinationT, UserT> via(
        Contextful<Fn<UserT, OutputT>> outputFn,
        Contextful<Fn<DestinationT, Sink<OutputT>>> sinkFn) {
      checkArgument(sinkFn != null, "sinkFn can not be null");
      checkArgument(outputFn != null, "outputFn can not be null");
      return toBuilder().setSinkFn((Contextful) sinkFn).setOutputFn(outputFn).build();
    }

    
    public <OutputT> Write<DestinationT, UserT> via(
        Contextful<Fn<UserT, OutputT>> outputFn, final Sink<OutputT> sink) {
      checkArgument(sink != null, "sink can not be null");
      checkArgument(outputFn != null, "outputFn can not be null");
      return via(outputFn, fn(SerializableFunctions.clonesOf(sink)));
    }

    
    public Write<DestinationT, UserT> via(Contextful<Fn<DestinationT, Sink<UserT>>> sinkFn) {
      checkArgument(sinkFn != null, "sinkFn can not be null");
      return toBuilder()
          .setSinkFn((Contextful) sinkFn)
          .setOutputFn(fn(SerializableFunctions.<UserT>identity()))
          .build();
    }

    
    public Write<DestinationT, UserT> via(Sink<UserT> sink) {
      checkArgument(sink != null, "sink can not be null");
      return via(fn(SerializableFunctions.clonesOf(sink)));
    }

    
    public Write<DestinationT, UserT> to(String directory) {
      checkArgument(directory != null, "directory can not be null");
      return to(StaticValueProvider.of(directory));
    }

    
    public Write<DestinationT, UserT> to(ValueProvider<String> directory) {
      checkArgument(directory != null, "directory can not be null");
      return toBuilder().setOutputDirectory(directory).build();
    }

    
    public Write<DestinationT, UserT> withPrefix(String prefix) {
      checkArgument(prefix != null, "prefix can not be null");
      return withPrefix(StaticValueProvider.of(prefix));
    }

    
    public Write<DestinationT, UserT> withPrefix(ValueProvider<String> prefix) {
      checkArgument(prefix != null, "prefix can not be null");
      return toBuilder().setFilenamePrefix(prefix).build();
    }

    
    public Write<DestinationT, UserT> withSuffix(String suffix) {
      checkArgument(suffix != null, "suffix can not be null");
      return withSuffix(StaticValueProvider.of(suffix));
    }

    
    public Write<DestinationT, UserT> withSuffix(ValueProvider<String> suffix) {
      checkArgument(suffix != null, "suffix can not be null");
      return toBuilder().setFilenameSuffix(suffix).build();
    }

    
    public Write<DestinationT, UserT> withNaming(FileNaming naming) {
      checkArgument(naming != null, "naming can not be null");
      return toBuilder().setConstantFileNaming(naming).build();
    }

    
    public Write<DestinationT, UserT> withNaming(
        SerializableFunction<DestinationT, FileNaming> namingFn) {
      checkArgument(namingFn != null, "namingFn can not be null");
      return withNaming(fn(namingFn));
    }

    
    public Write<DestinationT, UserT> withNaming(
        Contextful<Fn<DestinationT, FileNaming>> namingFn) {
      checkArgument(namingFn != null, "namingFn can not be null");
      return toBuilder().setFileNamingFn(namingFn).build();
    }

    
    public Write<DestinationT, UserT> withTempDirectory(String tempDirectory) {
      checkArgument(tempDirectory != null, "tempDirectory can not be null");
      return withTempDirectory(StaticValueProvider.of(tempDirectory));
    }

    
    public Write<DestinationT, UserT> withTempDirectory(ValueProvider<String> tempDirectory) {
      checkArgument(tempDirectory != null, "tempDirectory can not be null");
      return toBuilder().setTempDirectory(tempDirectory).build();
    }

    
    public Write<DestinationT, UserT> withCompression(Compression compression) {
      checkArgument(compression != null, "compression can not be null");
      checkArgument(
          compression != Compression.AUTO, "AUTO compression is not supported for writing");
      return toBuilder().setCompression(compression).build();
    }

    
    public Write<DestinationT, UserT> withEmptyGlobalWindowDestination(
        DestinationT emptyWindowDestination) {
      return toBuilder().setEmptyWindowDestination(emptyWindowDestination).build();
    }

    
    public Write<DestinationT, UserT> withDestinationCoder(Coder<DestinationT> destinationCoder) {
      checkArgument(destinationCoder != null, "destinationCoder can not be null");
      return toBuilder().setDestinationCoder(destinationCoder).build();
    }

    
    public Write<DestinationT, UserT> withNumShards(int numShards) {
      checkArgument(numShards >= 0, "numShards must be non-negative, but was: %s", numShards);
      if (numShards == 0) {
        return withNumShards(null);
      }
      return withNumShards(StaticValueProvider.of(numShards));
    }

    
    public Write<DestinationT, UserT> withNumShards(@Nullable ValueProvider<Integer> numShards) {
      return toBuilder().setNumShards(numShards).build();
    }

    
    public Write<DestinationT, UserT> withSharding(
        PTransform<PCollection<UserT>, PCollectionView<Integer>> sharding) {
      checkArgument(sharding != null, "sharding can not be null");
      return toBuilder().setSharding(sharding).build();
    }

    
    @Deprecated
    public Write<DestinationT, UserT> withIgnoreWindowing() {
      return toBuilder().setIgnoreWindowing(true).build();
    }

    public Write<DestinationT, UserT> withAutoSharding() {
      return toBuilder().setAutoSharding(true).build();
    }

    
    public Write<DestinationT, UserT> withNoSpilling() {
      return toBuilder().setNoSpilling(true).build();
    }

    
    public Write<DestinationT, UserT> withBadRecordErrorHandler(
        ErrorHandler<BadRecord, ?> errorHandler) {
      return toBuilder().setBadRecordErrorHandler(errorHandler).build();
    }

    @VisibleForTesting
    Contextful<Fn<DestinationT, FileNaming>> resolveFileNamingFn() {
      if (getDynamic()) {
        checkArgument(
            getConstantFileNaming() == null,
            "when using writeDynamic(), must use versions of .withNaming() "
                + "that take functions from DestinationT");
        checkArgument(getFilenamePrefix() == null, ".withPrefix() requires write()");
        checkArgument(getFilenameSuffix() == null, ".withSuffix() requires write()");
        checkArgument(
            getFileNamingFn() != null,
            "when using writeDynamic(), must specify "
                + ".withNaming() taking a function form DestinationT");
        return fn(
            (element, c) -> {
              FileNaming naming = getFileNamingFn().getClosure().apply(element, c);
              return getOutputDirectory() == null
                  ? naming
                  : relativeFileNaming(getOutputDirectory(), naming);
            },
            getFileNamingFn().getRequirements());
      } else {
        checkArgument(
            getFileNamingFn() == null,
            ".withNaming() taking a function from DestinationT requires writeDynamic()");
        FileNaming constantFileNaming;
        if (getConstantFileNaming() == null) {
          constantFileNaming =
              defaultNaming(
                  MoreObjects.firstNonNull(getFilenamePrefix(), StaticValueProvider.of("output")),
                  MoreObjects.firstNonNull(getFilenameSuffix(), StaticValueProvider.of("")));
        } else {
          checkArgument(
              getFilenamePrefix() == null, ".to(FileNaming) is incompatible with .withSuffix()");
          checkArgument(
              getFilenameSuffix() == null, ".to(FileNaming) is incompatible with .withPrefix()");
          constantFileNaming = getConstantFileNaming();
        }
        if (getOutputDirectory() != null) {
          constantFileNaming = relativeFileNaming(getOutputDirectory(), constantFileNaming);
        }
        return fn(SerializableFunctions.<DestinationT, FileNaming>constant(constantFileNaming));
      }
    }

    @Override
    public WriteFilesResult<DestinationT> expand(PCollection<UserT> input) {
      Write.Builder<DestinationT, UserT> resolvedSpec = new AutoValue_FileIO_Write.Builder<>();

      resolvedSpec.setDynamic(getDynamic());

      checkArgument(getSinkFn() != null, ".via() is required");
      resolvedSpec.setSinkFn(getSinkFn());

      checkArgument(getOutputFn() != null, "outputFn should have been set by .via()");
      resolvedSpec.setOutputFn(getOutputFn());

      // Resolve destinationFn
      if (getDynamic()) {
        checkArgument(getDestinationFn() != null, "when using writeDynamic(), .by() is required");
        resolvedSpec.setDestinationFn(getDestinationFn());
        resolvedSpec.setDestinationCoder(resolveDestinationCoder(input));
      } else {
        checkArgument(getDestinationFn() == null, ".by() requires writeDynamic()");
        checkArgument(
            getDestinationCoder() == null, ".withDestinationCoder() requires writeDynamic()");
        resolvedSpec.setDestinationFn(fn(SerializableFunctions.constant(null)));
        resolvedSpec.setDestinationCoder((Coder) VoidCoder.of());
      }

      resolvedSpec.setFileNamingFn(resolveFileNamingFn());
      resolvedSpec.setEmptyWindowDestination(getEmptyWindowDestination());
      if (getTempDirectory() == null) {
        checkArgument(
            getOutputDirectory() != null, "must specify either .withTempDirectory() or .to()");
        resolvedSpec.setTempDirectory(getOutputDirectory());
      } else {
        resolvedSpec.setTempDirectory(getTempDirectory());
      }

      resolvedSpec.setCompression(getCompression());
      resolvedSpec.setNumShards(getNumShards());
      resolvedSpec.setSharding(getSharding());
      resolvedSpec.setIgnoreWindowing(getIgnoreWindowing());
      resolvedSpec.setAutoSharding(getAutoSharding());
      resolvedSpec.setNoSpilling(getNoSpilling());

      Write<DestinationT, UserT> resolved = resolvedSpec.build();
      WriteFiles<UserT, DestinationT, ?> writeFiles =
          WriteFiles.to(new ViaFileBasedSink<>(resolved))
              .withSideInputs(Lists.newArrayList(resolved.getAllSideInputs()));
      if (getNumShards() != null) {
        writeFiles = writeFiles.withNumShards(getNumShards());
      } else if (getSharding() != null) {
        writeFiles = writeFiles.withSharding(getSharding());
      } else {
        writeFiles = writeFiles.withRunnerDeterminedSharding();
      }
      if (!getIgnoreWindowing()) {
        writeFiles = writeFiles.withWindowedWrites();
      }
      if (getAutoSharding()) {
        writeFiles = writeFiles.withAutoSharding();
      }
      if (getNoSpilling()) {
        writeFiles = writeFiles.withNoSpilling();
      }
      if (getBadRecordErrorHandler() != null) {
        writeFiles = writeFiles.withBadRecordErrorHandler(getBadRecordErrorHandler());
      }
      return input.apply(writeFiles);
    }

    private Coder<DestinationT> resolveDestinationCoder(PCollection<UserT> input) {
      Coder<DestinationT> destinationCoder = getDestinationCoder();
      if (destinationCoder == null) {
        TypeDescriptor<DestinationT> destinationT =
            TypeDescriptors.outputOf(getDestinationFn().getClosure());
        try {
          destinationCoder = input.getPipeline().getCoderRegistry().getCoder(destinationT);
        } catch (CannotProvideCoderException e) {
          throw new IllegalArgumentException(
              "Unable to infer a coder for destination type (inferred from .by() as \""
                  + destinationT
                  + "\") - specify it explicitly using .withDestinationCoder()");
        }
      }
      return destinationCoder;
    }

    private Collection<PCollectionView<?>> getAllSideInputs() {
      return Requirements.union(getDestinationFn(), getOutputFn(), getSinkFn(), getFileNamingFn())
          .getSideInputs();
    }

    private static class ViaFileBasedSink<UserT, DestinationT, OutputT>
        extends FileBasedSink<UserT, DestinationT, OutputT> {
      private final Write<DestinationT, UserT> spec;

      private ViaFileBasedSink(Write<DestinationT, UserT> spec) {
        super(
            ValueProvider.NestedValueProvider.of(
                spec.getTempDirectory(),
                input -> FileSystems.matchNewResource(input, true /* isDirectory */)),
            new DynamicDestinationsAdapter<>(spec),
            spec.getCompression());
        this.spec = spec;
      }

      @Override
      public WriteOperation<DestinationT, OutputT> createWriteOperation() {
        return new WriteOperation<DestinationT, OutputT>(this) {
          @Override
          public Writer<DestinationT, OutputT> createWriter() throws Exception {
            return new Writer<DestinationT, OutputT>(this, "") {
              private @Nullable Sink<OutputT> sink;

              @Override
              protected void prepareWrite(WritableByteChannel channel) throws Exception {
                Fn<DestinationT, Sink<OutputT>> sinkFn = (Fn) spec.getSinkFn().getClosure();
                sink =
                    sinkFn.apply(
                        getDestination(),
                        new Fn.Context() {
                          @Override
                          public <T> T sideInput(PCollectionView<T> view) {
                            return getWriteOperation()
                                .getSink()
                                .getDynamicDestinations()
                                .sideInput(view);
                          }
                        });
                sink.open(channel);
              }

              @Override
              public void write(OutputT value) throws Exception {
                sink.write(value);
              }

              @Override
              protected void finishWrite() throws Exception {
                sink.flush();
              }
            };
          }
        };
      }

      private static class DynamicDestinationsAdapter<UserT, DestinationT, OutputT>
          extends DynamicDestinations<UserT, DestinationT, OutputT> {
        private final Write<DestinationT, UserT> spec;
        private transient Fn.@Nullable Context context;

        private DynamicDestinationsAdapter(Write<DestinationT, UserT> spec) {
          this.spec = spec;
        }

        private Fn.Context getContext() {
          if (context == null) {
            context =
                new Fn.Context() {
                  @Override
                  public <T> T sideInput(PCollectionView<T> view) {
                    return DynamicDestinationsAdapter.this.sideInput(view);
                  }
                };
          }
          return context;
        }

        @Override
        public OutputT formatRecord(UserT record) {
          try {
            return ((Fn<UserT, OutputT>) spec.getOutputFn().getClosure())
                .apply(record, getContext());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public DestinationT getDestination(UserT element) {
          try {
            return spec.getDestinationFn().getClosure().apply(element, getContext());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public DestinationT getDefaultDestination() {
          return spec.getEmptyWindowDestination();
        }

        @Override
        public FilenamePolicy getFilenamePolicy(final DestinationT destination) {
          final FileNaming namingFn;
          try {
            namingFn = spec.getFileNamingFn().getClosure().apply(destination, getContext());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return new FilenamePolicy() {
            @Override
            public ResourceId windowedFilename(
                int shardNumber,
                int numShards,
                BoundedWindow window,
                PaneInfo paneInfo,
                OutputFileHints outputFileHints) {
              // We ignore outputFileHints because it will always be the same as
              // spec.getCompression() because we control the FileBasedSink.
              return FileSystems.matchNewResource(
                  namingFn.getFilename(
                      window, paneInfo, numShards, shardNumber, spec.getCompression()),
                  false /* isDirectory */);
            }

            @Override
            public @Nullable ResourceId unwindowedFilename(
                int shardNumber, int numShards, OutputFileHints outputFileHints) {
              return FileSystems.matchNewResource(
                  namingFn.getFilename(
                      GlobalWindow.INSTANCE,
                      PaneInfo.NO_FIRING,
                      numShards,
                      shardNumber,
                      spec.getCompression()),
                  false /* isDirectory */);
            }
          };
        }

        @Override
        public List<PCollectionView<?>> getSideInputs() {
          return Lists.newArrayList(spec.getAllSideInputs());
        }

        @Override
        public @Nullable Coder<DestinationT> getDestinationCoder() {
          return spec.getDestinationCoder();
        }
      }
    }
  }
}
