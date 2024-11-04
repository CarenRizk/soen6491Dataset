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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.io.Compression.AUTO;
import static org.apache.beam.sdk.io.Compression.BZIP2;
import static org.apache.beam.sdk.io.Compression.DEFLATE;
import static org.apache.beam.sdk.io.Compression.GZIP;
import static org.apache.beam.sdk.io.Compression.UNCOMPRESSED;
import static org.apache.beam.sdk.io.Compression.ZIP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSource.FileBasedReader;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesUnboundedSplittableParDo;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.apache.commons.lang3.SystemUtils;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;


@RunWith(Enclosed.class)
public class TextIOReadTest {
  private static final int LINES_NUMBER_FOR_LARGE = 1000;
  private static final List<String> EMPTY = Collections.emptyList();
  private static final List<String> TINY =
      Arrays.asList("Irritable eagle", "Optimistic jay", "Fanciful hawk");

  private static final List<String> LARGE = makeLines(LINES_NUMBER_FOR_LARGE);

  private static File writeToFile(
      List<String> lines, TemporaryFolder folder, String fileName, Compression compression)
      throws IOException {
    File file = folder.getRoot().toPath().resolve(fileName).toFile();
    OutputStream output = new FileOutputStream(file);
    switch (compression) {
      case UNCOMPRESSED:
        break;
      case GZIP:
        output = new GZIPOutputStream(output);
        break;
      case BZIP2:
        output = new BZip2CompressorOutputStream(output);
        break;
      case ZIP:
        ZipOutputStream zipOutput = new ZipOutputStream(output);
        zipOutput.putNextEntry(new ZipEntry("entry"));
        output = zipOutput;
        break;
      case DEFLATE:
        output = new DeflateCompressorOutputStream(output);
        break;
      default:
        throw new UnsupportedOperationException(compression.toString());
    }
    writeToStreamAndClose(lines, output);
    return file;
  }

  
  private static void writeToStreamAndClose(List<String> lines, OutputStream outputStream) {
    try (PrintStream writer = new PrintStream(outputStream)) {
      for (String line : lines) {
        writer.println(line);
      }
    }
  }

  
  private static List<String> makeLines(int n) {
    List<String> ret = new ArrayList<>();
    for (int i = 0; i < n; ++i) {
      ret.add("word" + i);
    }
    return ret;
  }

  
  private static void assertReadingCompressedFileMatchesExpected(
      File file, Compression compression, List<String> expected, Pipeline p) {

    TextIO.Read read = TextIO.read().from(file.getPath()).withCompression(compression);

    PAssert.that(p.apply("Read_" + file + "_" + compression.toString(), read))
        .containsInAnyOrder(expected);
    PAssert.that(
            p.apply(
                "Read_" + file + "_" + compression.toString() + "_many",
                read.withHintMatchesManyFiles()))
        .containsInAnyOrder(expected);

    PAssert.that(
            p.apply("Create_Paths_ReadFiles_" + file, Create.of(file.getPath()))
                .apply("Match_" + file, FileIO.matchAll())
                .apply("ReadMatches_" + file, FileIO.readMatches().withCompression(compression))
                .apply("ReadFiles_" + compression.toString(), TextIO.readFiles()))
        .containsInAnyOrder(expected);

    PAssert.that(
            p.apply("Create_Paths_ReadAll_" + file, Create.of(file.getPath()))
                .apply(
                    "ReadAll_" + compression.toString(),
                    TextIO.readAll().withCompression(compression)))
        .containsInAnyOrder(expected);
  }

  
  private static File createZipFile(
      List<String> expected, TemporaryFolder folder, String filename, String[]... fieldsEntries)
      throws Exception {
    File tmpFile = folder.getRoot().toPath().resolve(filename).toFile();

    ZipOutputStream out = new ZipOutputStream(new FileOutputStream(tmpFile));
    PrintStream writer = new PrintStream(out, true /* auto-flush on write */);

    int index = 0;
    for (String[] entry : fieldsEntries) {
      out.putNextEntry(new ZipEntry(Integer.toString(index)));
      for (String field : entry) {
        writer.println(field);
        expected.add(field);
      }
      out.closeEntry();
      index++;
    }

    writer.close();
    out.close();

    return tmpFile;
  }

  private static TextSource prepareSource(
      TemporaryFolder temporaryFolder, byte[] data, @Nullable byte[] delimiter, int skipHeaderLines)
      throws IOException {
    Path path = temporaryFolder.newFile().toPath();
    Files.write(path, data);
    return getTextSource(path.toString(), delimiter, skipHeaderLines);
  }

  public static TextSource getTextSource(
      String path, @Nullable byte[] delimiter, int skipHeaderLines) {
    return new TextSource(
        ValueProvider.StaticValueProvider.of(path),
        EmptyMatchTreatment.DISALLOW,
        delimiter,
        skipHeaderLines);
  }

  public static TextSource getTextSource(String path, @Nullable byte[] delimiter) {
    return getTextSource(path, delimiter, 0);
  }

  private static String getFileSuffix(Compression compression) {
    switch (compression) {
      case UNCOMPRESSED:
        return ".txt";
      case GZIP:
        return ".gz";
      case BZIP2:
        return ".bz2";
      case ZIP:
        return ".zip";
      case DEFLATE:
        return ".deflate";
      default:
        return "";
    }
  }
  
  private static void verifyReaderInitialStateAndProgress(BoundedSource.BoundedReader<String> reader)
		throws @UnknownKeyFor @NonNull @Initialized IOException {
	// Check preconditions before starting
	assertReaderInitialState(reader);

	// Line 2
	assertTrue(reader.advance());
	assertEquals(1, reader.getSplitPointsConsumed());
}

private static void assertReaderInitialState(BoundedSource.BoundedReader<String> reader)
		throws @UnknownKeyFor @NonNull @Initialized IOException {
	assertEquals(0.0, reader.getFractionConsumed(), 1e-6);
	assertEquals(0, reader.getSplitPointsConsumed());
	assertEquals(
	    BoundedSource.BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

	// Line 1
	assertTrue(reader.start());
	assertEquals(0, reader.getSplitPointsConsumed());
	assertEquals(
	    BoundedSource.BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());
}


  @RunWith(Parameterized.class)
  public static class ReadWithDefaultDelimiterTest {
    private static final ImmutableList<String> EXPECTED = ImmutableList.of("asdf", "hjkl", "xyz");
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> data() {
      return ImmutableList.<Object[]>builder()
          .add(new Object[] {"\n\n\n", ImmutableList.of("", "", "")})
          .add(new Object[] {"asdf\nhjkl\nxyz\n", EXPECTED})
          .add(new Object[] {"asdf\rhjkl\rxyz\r", EXPECTED})
          .add(new Object[] {"asdf\r\nhjkl\r\nxyz\r\n", EXPECTED})
          .add(new Object[] {"asdf\rhjkl\r\nxyz\n", EXPECTED})
          .add(new Object[] {"asdf\nhjkl\nxyz", EXPECTED})
          .add(new Object[] {"asdf\rhjkl\rxyz", EXPECTED})
          .add(new Object[] {"asdf\r\nhjkl\r\nxyz", EXPECTED})
          .add(new Object[] {"asdf\rhjkl\r\nxyz", EXPECTED})
          .build();
    }

    @Parameterized.Parameter(0)
    public String line;

    @Parameterized.Parameter(1)
    public ImmutableList<String> expected;

    @Test
    public void testReadLinesWithDefaultDelimiter() throws Exception {
      runTestReadWithData(line.getBytes(UTF_8), expected);
    }

    // Placeholder channel that only yields 0- and 1-length buffers.
    private static class SlowReadChannel implements ReadableByteChannel {
      int readCount = 0;
      InputStream stream;
      ReadableByteChannel channel;

      public SlowReadChannel(FileBasedSource source) throws IOException {
        channel =
            FileSystems.open(
                FileSystems.matchSingleFileSpec(source.getFileOrPatternSpec()).resourceId());
        stream = Channels.newInputStream(channel);
      }

      // Data is read at most one byte at a time from line parameter.
      @Override
      public int read(ByteBuffer dst) throws IOException {
        if (++readCount % 3 == 0) {
          if (dst.hasRemaining()) {
            int value = stream.read();
            if (value == -1) {
              return -1;
            }
            dst.put((byte) value);
            return 1;
          }
        }
        return 0;
      }

      @Override
      public boolean isOpen() {
        return channel.isOpen();
      }

      @Override
      public void close() throws IOException {
        stream.close();
      }
    }

    @Test
    public void testReadLinesWithDefaultDelimiterAndSlowReadChannel() throws Exception {
      Path path = tempFolder.newFile().toPath();
      Files.write(path, line.getBytes(UTF_8));
      Metadata metadata = FileSystems.matchSingleFileSpec(path.toString());
      FileBasedSource source =
          getTextSource(path.toString(), null, 0)
              .createForSubrangeOfFile(metadata, 0, metadata.sizeBytes());

      FileBasedReader<String> reader =
          source.createSingleFileReader(PipelineOptionsFactory.create());

      reader.startReading(new SlowReadChannel(source));
      assertEquals(expected, SourceTestUtils.readFromStartedReader(reader));
    }

    @Test
    public void testReadLinesWithDefaultDelimiterOnSplittingSourceAndSlowReadChannel()
        throws Exception {
      Path path = tempFolder.newFile().toPath();
      Files.write(path, line.getBytes(UTF_8));
      Metadata metadata = FileSystems.matchSingleFileSpec(path.toString());
      FileBasedSource<String> source =
          getTextSource(path.toString(), null, 0)
              .createForSubrangeOfFile(metadata, 0, metadata.sizeBytes());

      PipelineOptions options = PipelineOptionsFactory.create();

      // Check every possible split positions.
      for (int i = 0; i < line.length(); ++i) {
        double fraction = i * 1.0 / line.length();
        FileBasedReader<String> reader = source.createSingleFileReader(options);

        // Use a slow read channel to read the content byte by byte. This can simulate the scenario
        // of a certain character (in our case CR) occurring at the end of the read buffer.
        reader.startReading(new SlowReadChannel(source));

        // In order to get a successful split, we need to read at least one record before calling
        // splitAtFraction().
        List<String> totalItems = SourceTestUtils.readNItemsFromStartedReader(reader, 1);
        BoundedSource<String> residual = reader.splitAtFraction(fraction);
        List<String> primaryItems = SourceTestUtils.readFromStartedReader(reader);
        totalItems.addAll(primaryItems);

        if (residual != null) {
          List<String> residualItems = SourceTestUtils.readFromSource(residual, options);
          totalItems.addAll(residualItems);
        }
        assertEquals(expected, totalItems);
      }
    }

    @Test
    public void testSplittingSource() throws Exception {
      TextSource source = prepareSource(line.getBytes(UTF_8));
      SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
    }

    private TextSource prepareSource(byte[] data) throws IOException {
      return TextIOReadTest.prepareSource(tempFolder, data, null, 0);
    }

    private void runTestReadWithData(byte[] data, List<String> expectedResults) throws Exception {
      TextSource source = prepareSource(data);
      List<String> actual = SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create());
      assertThat(
          actual, containsInAnyOrder(new ArrayList<>(expectedResults).toArray(new String[0])));
    }
  }

  
  @RunWith(Parameterized.class)
  public static class SkippingHeaderTest {
    private static final ImmutableList<String> EXPECTED = ImmutableList.of("asdf", "hjkl", "xyz");
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> data() {
      return ImmutableList.<Object[]>builder()
          .add(new Object[] {"\n\n\n", ImmutableList.of("", ""), 1})
          .add(new Object[] {"\n", ImmutableList.of(), 1})
          .add(new Object[] {"header\nasdf\nhjkl\nxyz\n", EXPECTED, 1})
          .add(new Object[] {"header1\nheader2\nasdf\nhjkl\nxyz\n", EXPECTED, 2})
          .build();
    }

    @Parameterized.Parameter(0)
    public String line;

    @Parameterized.Parameter(1)
    public ImmutableList<String> expected;

    @Parameterized.Parameter(2)
    public int skipHeaderLines;

    @Test
    public void testReadLines() throws Exception {
      runTestReadWithData(line.getBytes(UTF_8), expected);
    }

    private TextSource prepareSource(byte[] data) throws IOException {
      return TextIOReadTest.prepareSource(tempFolder, data, null, skipHeaderLines);
    }

    private void runTestReadWithData(byte[] data, List<String> expectedResults) throws Exception {
      TextSource source = prepareSource(data);
      List<String> actual = SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create());
      assertThat(
          actual, containsInAnyOrder(new ArrayList<>(expectedResults).toArray(new String[0])));
    }
  }

  
  @RunWith(Parameterized.class)
  public static class ReadWithCustomDelimiterTest {
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> data() {
      return ImmutableList.<Object[]>builder()
          .add(new Object[] {"first|*second|*|*third"})
          .add(new Object[] {"first|*second|*|*third|"})
          .add(new Object[] {"first|*second|*|*third*"})
          .add(new Object[] {"first|*second|*|*third|*"})
          .add(new Object[] {"|first|*second|*|*third"})
          .add(new Object[] {"|first|*second|*|*third|"})
          .add(new Object[] {"|first|*second|*|*third*"})
          .add(new Object[] {"|first|*second|*|*third|*"})
          .add(new Object[] {"*first|*second|*|*third"})
          .add(new Object[] {"*first|*second|*|*third|"})
          .add(new Object[] {"*first|*second|*|*third*"})
          .add(new Object[] {"*first|*second|*|*third|*"})
          .add(new Object[] {"|*first|*second|*|*third"})
          .add(new Object[] {"|*first|*second|*|*third|"})
          .add(new Object[] {"|*first|*second|*|*third*"})
          .add(new Object[] {"|*first|*second|*|*third|*"})
          .build();
    }

    @Parameterized.Parameter(0)
    public String testCase;

    @Test
    public void testReadLinesWithCustomDelimiter() throws Exception {
      SourceTestUtils.assertSplitAtFractionExhaustive(
          TextIOReadTest.prepareSource(
              tempFolder, testCase.getBytes(UTF_8), new byte[] {'|', '*'}, 0),
          PipelineOptionsFactory.create());
    }

    @Test
    public void testReadLinesWithCustomDelimiterAndZeroAndOneLengthReturningChannel()
        throws Exception {
      byte[] delimiter = new byte[] {'|', '*'};
      Path path = tempFolder.newFile().toPath();
      Files.write(path, testCase.getBytes(UTF_8));
      Metadata metadata = FileSystems.matchSingleFileSpec(path.toString());
      FileBasedSource source =
          getTextSource(path.toString(), delimiter, 0)
              .createForSubrangeOfFile(metadata, 0, metadata.sizeBytes());
      FileBasedReader<String> reader =
          source.createSingleFileReader(PipelineOptionsFactory.create());
      ReadableByteChannel channel =
          FileSystems.open(
              FileSystems.matchSingleFileSpec(source.getFileOrPatternSpec()).resourceId());
      InputStream stream = Channels.newInputStream(channel);
      reader.startReading(
          // Placeholder channel that only yields 0- and 1-length buffers.
          // Data is read at most one byte at a time from testCase parameter.
          new ReadableByteChannel() {
            int readCount = 0;

            @Override
            public int read(ByteBuffer dst) throws IOException {
              if (++readCount % 3 == 0) {
                if (dst.hasRemaining()) {
                  int value = stream.read();
                  if (value == -1) {
                    return -1;
                  }
                  dst.put((byte) value);
                  return 1;
                }
              }
              return 0;
            }

            @Override
            public boolean isOpen() {
              return channel.isOpen();
            }

            @Override
            public void close() throws IOException {
              stream.close();
            }
          });
      assertEquals(
          SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create()),
          SourceTestUtils.readFromStartedReader(reader));
    }
  }

  
  @RunWith(JUnit4.class)
  public static class BasicIOTest {
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
    @Rule public TestPipeline p = TestPipeline.create();

    private void runTestRead(String[] expected) throws Exception {
      File tmpFile = tempFolder.newFile();
      String filename = tmpFile.getPath();

      try (PrintStream writer = new PrintStream(new FileOutputStream(tmpFile))) {
        for (String elem : expected) {
          byte[] encodedElem = CoderUtils.encodeToByteArray(StringUtf8Coder.of(), elem);
          String line = new String(encodedElem, StandardCharsets.UTF_8);
          writer.println(line);
        }
      }

      TextIO.Read read = TextIO.read().from(filename);
      PCollection<String> output = p.apply(read);

      PAssert.that(output).containsInAnyOrder(expected);
      p.run();
    }

    @Test
    public void testDelimiterSelfOverlaps() {
      assertFalse(TextIO.Read.isSelfOverlapping(new byte[] {'a', 'b', 'c'}));
      assertFalse(TextIO.Read.isSelfOverlapping(new byte[] {'c', 'a', 'b', 'd', 'a', 'b'}));
      assertFalse(TextIO.Read.isSelfOverlapping(new byte[] {'a', 'b', 'c', 'a', 'b', 'd'}));
      assertTrue(TextIO.Read.isSelfOverlapping(new byte[] {'a', 'b', 'a'}));
      assertTrue(TextIO.Read.isSelfOverlapping(new byte[] {'a', 'b', 'c', 'a', 'b'}));
    }

    @Test
    public void testReadNamed() throws Exception {
      File emptyFile = tempFolder.newFile();
      p.enableAbandonedNodeEnforcement(false);

      assertThat(p.apply(TextIO.read().from("somefile")).getName(), startsWith("TextIO.Read/Read"));
      assertThat(
          p.apply("MyRead", TextIO.read().from(emptyFile.getPath())).getName(),
          startsWith("MyRead/Read"));
    }

    
    public interface RuntimeTestOptions extends PipelineOptions {
      ValueProvider<String> getInput();

      void setInput(ValueProvider<String> value);
    }

    @Test
    public void testRuntimeOptionsNotCalledInApply() throws Exception {
      p.enableAbandonedNodeEnforcement(false);

      RuntimeTestOptions options = PipelineOptionsFactory.as(RuntimeTestOptions.class);

      p.apply(TextIO.read().from(options.getInput()));
    }

    @Test
    public void testCompressionIsSet() throws Exception {
      TextIO.Read read = TextIO.read().from("/tmp/test");
      assertEquals(AUTO, read.getCompression());
      read = TextIO.read().from("/tmp/test").withCompression(GZIP);
      assertEquals(GZIP, read.getCompression());
    }

    @Test
    public void testTextIOGetName() {
      assertEquals("TextIO.Read", TextIO.read().from("somefile").getName());
      assertEquals("TextIO.Read", TextIO.read().from("somefile").toString());
    }

    private TextSource prepareSource(byte[] data) throws IOException {
      return TextIOReadTest.prepareSource(tempFolder, data, null, 0);
    }

    @Test
    public void testProgressEmptyFile() throws IOException {
      try (BoundedSource.BoundedReader<String> reader =
          prepareSource(new byte[0]).createReader(PipelineOptionsFactory.create())) {
        // Check preconditions before starting.
        assertEquals(0.0, reader.getFractionConsumed(), 1e-6);
        assertEquals(0, reader.getSplitPointsConsumed());
        assertEquals(
            BoundedSource.BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

        // Assert empty
        assertFalse(reader.start());

        // Check postconditions after finishing
        assertEquals(1.0, reader.getFractionConsumed(), 1e-6);
        assertEquals(0, reader.getSplitPointsConsumed());
        assertEquals(0, reader.getSplitPointsRemaining());
      }
    }

    @Test
    public void testProgressTextFile() throws IOException {
      String file = "line1\nline2\nline3";
      try (BoundedSource.BoundedReader<String> reader =
          prepareSource(file.getBytes(StandardCharsets.UTF_8))
              .createReader(PipelineOptionsFactory.create())) {
        verifyReaderInitialStateAndProgress(reader);
        assertEquals(
            BoundedSource.BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

        // Line 3
        assertTrue(reader.advance());
        assertEquals(2, reader.getSplitPointsConsumed());
        assertReaderStateAfterAdvance(reader);
        assertEquals(3, reader.getSplitPointsConsumed());
        assertEquals(0, reader.getSplitPointsRemaining());
      }
    }

	private void assertReaderStateAfterAdvance(BoundedSource.BoundedReader<String> reader)
			throws @UnknownKeyFor @NonNull @Initialized IOException {
		assertEquals(1, reader.getSplitPointsRemaining());

        // Check postconditions after finishing
        assertFalse(reader.advance());
        assertEquals(1.0, reader.getFractionConsumed(), 1e-6);
	}

    @Test
    public void testProgressAfterSplitting() throws IOException {
      String file = "line1\nline2\nline3";
      BoundedSource<String> source = prepareSource(file.getBytes(StandardCharsets.UTF_8));
      BoundedSource<String> remainder;

      // Create the remainder, verifying properties pre- and post-splitting.
      try (BoundedSource.BoundedReader<String> readerOrig =
          source.createReader(PipelineOptionsFactory.create())) {
        // Preconditions.
    	assertReaderInitialState(readerOrig);
        // Split. 0.1 is in line1, so should now be able to detect last record.
        remainder = readerOrig.splitAtFraction(0.1);
        assertNotNull(remainder);

        // First record, after splitting.
        assertEquals(0, readerOrig.getSplitPointsConsumed());
        assertReaderStateAfterAdvance(readerOrig);
        assertEquals(1, readerOrig.getSplitPointsConsumed());
        assertEquals(0, readerOrig.getSplitPointsRemaining());
      }

      // Check the properties of the remainder.
      try (BoundedSource.BoundedReader<String> reader =
          remainder.createReader(PipelineOptionsFactory.create())) {
        verifyReaderInitialStateAndProgress(reader);
        assertReaderStateAfterAdvance(reader);
        assertEquals(2, reader.getSplitPointsConsumed());
        assertEquals(0, reader.getSplitPointsRemaining());
      }
    }

    @Test
    public void testInitialSplitAutoModeGz() throws Exception {
      // TODO: Java core test failing on windows, https://github.com/apache/beam/issues/20470
      assumeFalse(SystemUtils.IS_OS_WINDOWS);
      PipelineOptions options = TestPipeline.testingPipelineOptions();
      long desiredBundleSize = 1000;
      File largeGz = writeToFile(LARGE, tempFolder, "large.gz", GZIP);
      // Sanity check: file is at least 2 bundles long.
      assertThat(largeGz.length(), greaterThan(2 * desiredBundleSize));

      FileBasedSource<String> source = TextIO.read().from(largeGz.getPath()).getSource();
      List<? extends FileBasedSource<String>> splits = source.split(desiredBundleSize, options);

      // Exactly 1 split, even in AUTO mode, since it is a gzip file.
      assertThat(splits, hasSize(equalTo(1)));
      SourceTestUtils.assertSourcesEqualReferenceSource(source, splits, options);
    }

    private List<KV<String, String>> filenameKV(Path path, String fn, List<String> input) {
      return input.stream()
          .map(l -> KV.of(path.resolve(fn).toString(), l))
          .collect(Collectors.toList());
    }
  }

  
  @RunWith(JUnit4.class)
  public static class TextSourceTest {
    @Rule public transient TestPipeline pipeline = TestPipeline.create();

    @Test
    @Category(NeedsRunner.class)
    public void testRemoveUtf8BOM() throws Exception {
      Path p1 = createTestFile("test_txt_ascii", Charset.forName("US-ASCII"), "1,p1", "2,p1");
      Path p2 =
          createTestFile(
              "test_txt_utf8_no_bom",
              Charset.forName("UTF-8"),
              "1,p2-Japanese:テスト",
              "2,p2-Japanese:テスト");
      Path p3 =
          createTestFile(
              "test_txt_utf8_bom",
              Charset.forName("UTF-8"),
              "\uFEFF1,p3-テストBOM",
              "\uFEFF2,p3-テストBOM");
      PCollection<String> contents =
          pipeline
              .apply("Create", Create.of(p1.toString(), p2.toString(), p3.toString()))
              .setCoder(StringUtf8Coder.of())
              // PCollection<String>
              .apply("Read file", new TextIOReadTest.TextSourceTest.TextFileReadTransform());
      // PCollection<KV<String, String>>: tableName, line

      // Validate that the BOM bytes (\uFEFF) at the beginning of the first line have been removed.
      PAssert.that(contents)
          .containsInAnyOrder(
              "1,p1",
              "2,p1",
              "1,p2-Japanese:テスト",
              "2,p2-Japanese:テスト",
              "1,p3-テストBOM",
              "\uFEFF2,p3-テストBOM");

      pipeline.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testPreserveNonBOMBytes() throws Exception {
      // Contains \uFEFE, not UTF BOM.
      Path p1 =
          createTestFile(
              "test_txt_utf_bom", Charset.forName("UTF-8"), "\uFEFE1,p1テスト", "\uFEFE2,p1テスト");
      PCollection<String> contents =
          pipeline
              .apply("Create", Create.of(p1.toString()))
              .setCoder(StringUtf8Coder.of())
              // PCollection<String>
              .apply("Read file", new TextIOReadTest.TextSourceTest.TextFileReadTransform());

      PAssert.that(contents).containsInAnyOrder("\uFEFE1,p1テスト", "\uFEFE2,p1テスト");

      pipeline.run();
    }

    private static class FileReadDoFn extends DoFn<FileIO.ReadableFile, String> {

      @ProcessElement
      public void processElement(ProcessContext c) {
        FileIO.ReadableFile file = c.element();
        ValueProvider<String> filenameProvider =
            ValueProvider.StaticValueProvider.of(file.getMetadata().resourceId().getFilename());
        // Create a TextSource, passing null as the delimiter to use the default
        // delimiters ('\n', '\r', or '\r\n').
        TextSource textSource = new TextSource(filenameProvider, null, null, 0);
        try {
          BoundedSource.BoundedReader<String> reader =
              textSource
                  .createForSubrangeOfFile(file.getMetadata(), 0, file.getMetadata().sizeBytes())
                  .createReader(c.getPipelineOptions());
          for (boolean more = reader.start(); more; more = reader.advance()) {
            c.output(reader.getCurrent());
          }
        } catch (IOException e) {
          throw new RuntimeException(
              "Unable to readFile: " + file.getMetadata().resourceId().toString());
        }
      }
    }

    
    private static class TextFileReadTransform
        extends PTransform<PCollection<String>, PCollection<String>> {
      public TextFileReadTransform() {}

      @Override
      public PCollection<String> expand(PCollection<String> files) {
        return files
            // PCollection<String>
            .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
            // PCollection<Match.Metadata>
            .apply(FileIO.readMatches())
            // PCollection<FileIO.ReadableFile>
            .apply("Read lines", ParDo.of(new TextIOReadTest.TextSourceTest.FileReadDoFn()));
        // PCollection<String>: line
      }
    }

    private Path createTestFile(String filename, Charset charset, String... lines)
        throws IOException {
      Path path = Files.createTempFile(filename, ".csv");
      try (BufferedWriter writer = Files.newBufferedWriter(path, charset)) {
        for (String line : lines) {
          writer.write(line);
          writer.write('\n');
        }
      }
      return path;
    }
  }
}
