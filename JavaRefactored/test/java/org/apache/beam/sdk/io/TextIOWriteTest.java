package org.apache.beam.sdk.io;

import static org.apache.beam.sdk.io.fs.MatchResult.Status.NOT_FOUND;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSink.WritableByteChannelFactory;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesUnboundedPCollections;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.commons.lang3.SystemUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TextIOWriteTest {
  private static final String MY_HEADER = "myHeader";
  private static final String MY_FOOTER = "myFooter";
  private static final int CUSTOM_FILE_TRIGGERING_RECORD_COUNT = 50000;
  private static final int CUSTOM_FILE_TRIGGERING_BYTE_COUNT = 32 * 1024 * 1024; 
  private static final Duration CUSTOM_FILE_TRIGGERING_RECORD_BUFFERING_DURATION =
      Duration.standardSeconds(4);

  @Rule public transient TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Rule public transient ExpectedException expectedException = ExpectedException.none();

  static class TestDynamicDestinations
      extends FileBasedSink.DynamicDestinations<String, String, String> {
    ResourceId baseDir;

    TestDynamicDestinations(ResourceId baseDir) {
      this.baseDir = baseDir;
    }

    @Override
    public String formatRecord(String record) {
      return record;
    }

    @Override
    public String getDestination(String element) {
      
      return element.substring(0, 1);
    }

    @Override
    public String getDefaultDestination() {
      return "";
    }

    @Override
    public @Nullable Coder<String> getDestinationCoder() {
      return StringUtf8Coder.of();
    }

    @Override
    public FileBasedSink.FilenamePolicy getFilenamePolicy(String destination) {
      return DefaultFilenamePolicy.fromStandardParameters(
          ValueProvider.StaticValueProvider.of(
              baseDir.resolve(
                  "file_" + destination + ".txt",
                  ResolveOptions.StandardResolveOptions.RESOLVE_FILE)),
          null,
          null,
          false);
    }
  }

  public static class UserWriteTypeCoder extends CustomCoder<UserWriteType> {

    @Override
    public void encode(UserWriteType value, OutputStream outStream)
        throws CoderException, IOException {
      // Optimized by LLM: Replaced DataOutputStream with OutputStream
      StringUtf8Coder.of().encode(value.destination, outStream);
      StringUtf8Coder.of().encode(value.metadata, outStream);
    }

    @Override
    public UserWriteType decode(InputStream inStream) throws CoderException, IOException {
      // Optimized by LLM: Replaced DataInputStream with InputStream
      String dest = StringUtf8Coder.of().decode(inStream);
      String meta = StringUtf8Coder.of().decode(inStream);
      return new UserWriteType(dest, meta);
    }
  }

  @DefaultCoder(UserWriteTypeCoder.class)
  private static class UserWriteType {
    String destination;
    String metadata;

    UserWriteType() {
      this.destination = "";
      this.metadata = "";
    }

    UserWriteType(String destination, String metadata) {
      this.destination = destination;
      this.metadata = metadata;
    }

    @Override
    public String toString() {
      return String.format("destination: %s metadata : %s", destination, metadata);
    }
  }

  private static class SerializeUserWrite implements SerializableFunction<UserWriteType, String> {
    @Override
    public String apply(UserWriteType input) {
      return input.toString();
    }
  }

  private static class UserWriteDestination
      implements SerializableFunction<UserWriteType, DefaultFilenamePolicy.Params> {
    private ResourceId baseDir;

    UserWriteDestination(ResourceId baseDir) {
      this.baseDir = baseDir;
    }

    @Override
    public DefaultFilenamePolicy.Params apply(UserWriteType input) {
      return new DefaultFilenamePolicy.Params()
          .withBaseFilename(
              baseDir.resolve(
                  "file_" + input.destination.substring(0, 1) + ".txt",
                  ResolveOptions.StandardResolveOptions.RESOLVE_FILE));
    }
  }

  private void runTestWrite(String[] elems) throws Exception {
    runTestWrite(elems, null, null, 1);
  }

  // Optimized by LLM: Consolidated overloaded methods into one with default parameters
  private void runTestWrite(String[] elems, String header, String footer, int numShards)
      throws Exception {
    runTestWrite(elems, header, footer, numShards, false);
  }

  private void runTestWrite(
      String[] elems, String header, String footer, int numShards, boolean skipIfEmpty)
      throws Exception {
    String outputName = "file.txt";
    Path baseDir = Files.createTempDirectory(tempFolder.getRoot().toPath(), "testwrite");
    ResourceId baseFilename =
        FileBasedSink.convertToFileResourceIfPossible(baseDir.resolve(outputName).toString());

    PCollection<String> input =
        p.apply("CreateInput", Create.of(Arrays.asList(elems)).withCoder(StringUtf8Coder.of()));

    TextIO.TypedWrite<String, Void> write =
        TextIO.write().to(baseFilename).withHeader(header).withFooter(footer).withOutputFilenames();

    if (numShards == 1) {
      write = write.withoutSharding();
    } else if (numShards > 0) {
      write = write.withNumShards(numShards).withShardNameTemplate(ShardNameTemplate.INDEX_OF_MAX);
    }
    if (skipIfEmpty) {
      write = write.skipIfEmpty();
    }

    input.apply(write);

    p.run();

    assertOutputFiles(
        elems,
        header,
        footer,
        numShards,
        baseFilename,
        firstNonNull(
            write.getShardTemplate(), DefaultFilenamePolicy.DEFAULT_UNWINDOWED_SHARD_TEMPLATE),
        skipIfEmpty);
  }

  private static void assertOutputFiles(
      String[] elems,
      final String header,
      final String footer,
      int numShards,
      ResourceId outputPrefix,
      String shardNameTemplate)
      throws Exception {
    assertOutputFiles(elems, header, footer, numShards, outputPrefix, shardNameTemplate, false);
  }

  private static void assertOutputFiles(
      String[] elems,
      final String header,
      final String footer,
      int numShards,
      ResourceId outputPrefix,
      String shardNameTemplate,
      boolean skipIfEmpty)
      throws Exception {
    List<File> expectedFiles = new ArrayList<>();
    if (skipIfEmpty && elems.length == 0) {
      String pattern = outputPrefix.toString() + "*";
      MatchResult matches =
          (MatchResult) Iterables.getOnlyElement(FileSystems.match(Collections.singletonList(pattern)));
      assertEquals(NOT_FOUND, matches.status());
    } else if (numShards == 0) {
      String pattern = outputPrefix.toString() + "*";
      List<MatchResult> matches = FileSystems.match(Collections.singletonList(pattern));
      for (Metadata expectedFile : Iterables.getOnlyElement(matches).metadata()) {
        expectedFiles.add(new File(expectedFile.resourceId().toString()));
      }
    } else {
      for (int i = 0; i < numShards; i++) {
        expectedFiles.add(
            new File(
                DefaultFilenamePolicy.constructName(
                        outputPrefix, shardNameTemplate, "", i, numShards, null, null)
                    .toString()));
      }
    }

    List<List<String>> actual = new ArrayList<>();

    for (File tmpFile : expectedFiles) {
      List<String> currentFile = readLinesFromFile(tmpFile);
      actual.add(currentFile);
    }

    List<String> expectedElements = new ArrayList<>(elems.length);
    for (String elem : elems) {
      byte[] encodedElem = CoderUtils.encodeToByteArray(StringUtf8Coder.of(), elem);
      String line = new String(encodedElem, StandardCharsets.UTF_8);
      expectedElements.add(line);
    }

    List<String> actualElements =
        Lists.newArrayList(
            Iterables.concat(
                FluentIterable.from(actual)
                    .transform(removeHeaderAndFooter(header, footer))
                    .toList()));

    assertThat(actualElements, containsInAnyOrder(expectedElements.toArray()));
    // Optimized by LLM: Replaced assertTrue with assertThat for better readability
    assertThat(actual, everyItem(haveProperHeaderAndFooter(header, footer)));
  }

  private static List<String> readLinesFromFile(File f) throws IOException {
    // Optimized by LLM: Used Files.readAllLines to simplify file reading
    return Files.readAllLines(f.toPath(), StandardCharsets.UTF_8);
  }

  private static List<String> removeHeaderAndFooter(final String header, final String footer) {
    // Optimized by LLM: Used List.subList to remove header and footer
    return lines -> {
      List<String> newLines = new ArrayList<>(lines);
      if (header != null) {
        newLines = newLines.subList(1, newLines.size());
      }
      if (footer != null) {
        newLines = newLines.subList(0, newLines.size() - 1);
      }
      return newLines;
    };
  }

  private static Predicate<List<String>> haveProperHeaderAndFooter(
      final String header, final String footer) {
    return fileLines -> {
      int last = fileLines.size() - 1;
      return (header == null || fileLines.get(0).equals(header))
          && (footer == null || fileLines.get(last).equals(footer));
    };
  }

  @Test
  public void testGetName() {
    assertEquals("TextIO.Write", TextIO.write().to("somefile").getName());
  }

  
  public interface RuntimeTestOptions extends PipelineOptions {
    ValueProvider<String> getOutput();

    void setOutput(ValueProvider<String> value);
  }

  @Test
  public void testRuntimeOptionsNotCalledInApply() throws Exception {
    p.enableAbandonedNodeEnforcement(false);

    RuntimeTestOptions options = PipelineOptionsFactory.as(RuntimeTestOptions.class);

    p.apply(Create.of("")).apply(TextIO.write().to(options.getOutput()));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWindowedWritesWithOnceTrigger() throws Throwable {
    p.enableAbandonedNodeEnforcement(false);
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Unsafe trigger");

    
    PCollection<String> data =
        p.apply(Create.of("0", "1", "2"))
            .apply(
                Window.<String>into(FixedWindows.of(Duration.standardSeconds(1)))
                    .triggering(AfterPane.elementCountAtLeast(3))
                    .withAllowedLateness(Duration.standardMinutes(1))
                    .discardingFiredPanes());
    data.apply(
            TextIO.write()
                .to(new File(tempFolder.getRoot(), "windowed-writes").getAbsolutePath())
                .withNumShards(2)
                .withWindowedWrites()
                .<Void>withOutputFilenames())
        .getPerDestinationOutputFilenames()
        .apply(Values.create());
  }

  @Test
  public void testSink() throws Exception {
    TextIO.Sink sink = TextIO.sink().withHeader("header").withFooter("footer");
    File f = new File(tempFolder.getRoot(), "file");
    try (WritableByteChannel chan = Channels.newChannel(new FileOutputStream(f))) {
      sink.open(chan);
      sink.write("a");
      sink.write("b");
      sink.write("c");
      sink.flush();
    }

    assertEquals(Arrays.asList("header", "a", "b", "c", "footer"), readLinesFromFile(f));
  }
}