package org.apache.beam.sdk.io;

import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_FILE;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesUnboundedSplittableParDo;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Requirements;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FileIOTest implements Serializable {
  @Rule public transient TestPipeline p = TestPipeline.create();

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Rule public transient Timeout globalTimeout = Timeout.seconds(1200);

  private static final int INITIAL_COPY_STATE = 0; // Optimized by LLM: Replace magic number with named constant
  private static final int SECOND_COPY_STATE = 1; // Optimized by LLM: Replace magic number with named constant

  @Test
  @Category(NeedsRunner.class)
  public void testMatchAndMatchAll() throws IOException {
    Path firstPath = tmpFolder.newFile("first").toPath();
    Path secondPath = tmpFolder.newFile("second").toPath();
    int firstSize = 37;
    int secondSize = 42;
    long firstModified = 1541097000L;
    long secondModified = 1541098000L;
    Files.write(firstPath, new byte[firstSize]);
    Files.write(secondPath, new byte[secondSize]);
    Files.setLastModifiedTime(firstPath, FileTime.fromMillis(firstModified));
    Files.setLastModifiedTime(secondPath, FileTime.fromMillis(secondModified));
    MatchResult.Metadata firstMetadata = metadata(firstPath, firstSize, firstModified);
    MatchResult.Metadata secondMetadata = metadata(secondPath, secondSize, secondModified);

    PAssert.that(
            p.apply(
                "Match existing",
                FileIO.match().filepattern(tmpFolder.getRoot().getAbsolutePath() + "/*")))
        .containsInAnyOrder(firstMetadata, secondMetadata);
    PAssert.that(
            p.apply(
                "Match existing with provider",
                FileIO.match()
                    .filepattern(p.newProvider(tmpFolder.getRoot().getAbsolutePath() + "/*"))))
        .containsInAnyOrder(firstMetadata, secondMetadata);
    PAssert.that(
            p.apply("Create existing", Create.of(tmpFolder.getRoot().getAbsolutePath() + "/*"))
                .apply("MatchAll existing", FileIO.matchAll()))
        .containsInAnyOrder(firstMetadata, secondMetadata);

    PAssert.that(
            p.apply(
                "Match non-existing ALLOW",
                FileIO.match()
                    .filepattern(tmpFolder.getRoot().getAbsolutePath() + "/blah")
                    .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW)))
        .empty();
    PAssert.that(
            p.apply(
                    "Create non-existing",
                    Create.of(tmpFolder.getRoot().getAbsolutePath() + "/blah"))
                .apply(
                    "MatchAll non-existing ALLOW",
                    FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW)))
        .empty();

    PAssert.that(
            p.apply(
                "Match non-existing ALLOW_IF_WILDCARD",
                FileIO.match()
                    .filepattern(tmpFolder.getRoot().getAbsolutePath() + "/blah*")
                    .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD)))
        .empty();
    PAssert.that(
            p.apply(
                    "Create non-existing wildcard + explicit",
                    Create.of(tmpFolder.getRoot().getAbsolutePath() + "/blah*"))
                .apply(
                    "MatchAll non-existing ALLOW_IF_WILDCARD",
                    FileIO.matchAll()
                        .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD)))
        .empty();
    PAssert.that(
            p.apply(
                    "Create non-existing wildcard + default",
                    Create.of(tmpFolder.getRoot().getAbsolutePath() + "/blah*"))
                .apply("MatchAll non-existing default", FileIO.matchAll()))
        .empty();

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatchDisallowEmptyDefault() throws IOException {
    p.apply("Match", FileIO.match().filepattern(tmpFolder.getRoot().getAbsolutePath() + "/*"));

    thrown.expectCause(isA(FileNotFoundException.class));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatchDisallowEmptyExplicit() throws IOException {
    p.apply(
        FileIO.match()
            .filepattern(tmpFolder.getRoot().getAbsolutePath() + "/*")
            .withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW));

    thrown.expectCause(isA(FileNotFoundException.class));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatchDisallowEmptyNonWildcard() throws IOException {
    p.apply(
        FileIO.match()
            .filepattern(tmpFolder.getRoot().getAbsolutePath() + "/blah")
            .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD));

    thrown.expectCause(isA(FileNotFoundException.class));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatchAllDisallowEmptyExplicit() throws IOException {
    p.apply(Create.of(tmpFolder.getRoot().getAbsolutePath() + "/*"))
        .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW));
    thrown.expectCause(isA(FileNotFoundException.class));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMatchAllDisallowEmptyNonWildcard() throws IOException {
    p.apply(Create.of(tmpFolder.getRoot().getAbsolutePath() + "/blah"))
        .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD));
    thrown.expectCause(isA(FileNotFoundException.class));
    p.run();
  }

  private static class CopyFilesFn
      extends DoFn<KV<String, MatchResult.Metadata>, MatchResult.Metadata> {
    public CopyFilesFn(Path sourcePath, Path watchPath) {
      this.sourcePathStr = sourcePath.toString();
      this.watchPathStr = watchPath.toString();
    }

    @StateId("count")
    @SuppressWarnings("unused")
    private final StateSpec<ValueState<Integer>> countSpec = StateSpecs.value(VarIntCoder.of());

    @ProcessElement
    public void processElement(ProcessContext context, @StateId("count") ValueState<Integer> count)
        throws IOException, InterruptedException {
      int current = firstNonNull(count.read(), 0);
      
      context.output(Objects.requireNonNull(context.element()).getValue());

      CopyOption[] cpOptions = {StandardCopyOption.COPY_ATTRIBUTES};
      CopyOption[] updOptions = {StandardCopyOption.REPLACE_EXISTING};
      final Path sourcePath = Paths.get(sourcePathStr);
      final Path watchPath = Paths.get(watchPathStr);

      if (INITIAL_COPY_STATE == current) { // Optimized by LLM: Replace magic number with named constant
        Thread.sleep(100);
        Files.copy(sourcePath.resolve("first"), watchPath.resolve("first"), updOptions);
        Files.copy(sourcePath.resolve("second"), watchPath.resolve("second"), cpOptions);
      } else if (SECOND_COPY_STATE == current) { // Optimized by LLM: Replace magic number with named constant
        Thread.sleep(100);
        Files.copy(sourcePath.resolve("first"), watchPath.resolve("first"), updOptions);
        Files.copy(sourcePath.resolve("second"), watchPath.resolve("second"), updOptions);
        Files.copy(sourcePath.resolve("third"), watchPath.resolve("third"), cpOptions);
      }
      count.write(current + 1);
    }

    private final String sourcePathStr;
    private final String watchPathStr;
  }

  @Test
  @Category(NeedsRunner.class)
  public void testRead() throws IOException {
    final String path = tmpFolder.newFile("file").getAbsolutePath();
    final String pathGZ = tmpFolder.newFile("file.gz").getAbsolutePath();
    Files.write(new File(path).toPath(), "Hello world".getBytes(StandardCharsets.UTF_8));
    writeGzipFile(pathGZ, "Hello world"); // Optimized by LLM: Move file writing logic to a helper method

    PCollection<MatchResult.Metadata> matches = p.apply("Match", FileIO.match().filepattern(path));
    List<PCollection<FileIO.ReadableFile>> decompressedFiles = createDecompressedFiles(matches); // Optimized by LLM: Use a loop to handle creation of PCollection<FileIO.ReadableFile>
    for (PCollection<FileIO.ReadableFile> c : decompressedFiles) {
      assertDecompressedFile(c, path); // Optimized by LLM: Use a single assertion method
    }

    PCollection<MatchResult.Metadata> matchesGZ =
        p.apply("Match GZ", FileIO.match().filepattern(pathGZ));
    List<PCollection<FileIO.ReadableFile>> compressedFiles = createCompressedFiles(matchesGZ); // Optimized by LLM: Use a loop to handle creation of PCollection<FileIO.ReadableFile>
    for (PCollection<FileIO.ReadableFile> c : compressedFiles) {
      assertCompressedFile(c, pathGZ); // Optimized by LLM: Use a single assertion method
    }

    p.run();
  }

  private void writeGzipFile(String pathGZ, String content) throws IOException { // Optimized by LLM: Move file writing logic to a helper method
    try (Writer writer =
        new OutputStreamWriter(
            new GZIPOutputStream(new FileOutputStream(pathGZ)), StandardCharsets.UTF_8)) {
      writer.write(content);
    }
  }

  private List<PCollection<FileIO.ReadableFile>> createDecompressedFiles(PCollection<MatchResult.Metadata> matches) { // Optimized by LLM: Use a loop to handle creation of PCollection<FileIO.ReadableFile>
    return Arrays.asList(
        matches.apply("Read AUTO", FileIO.readMatches().withCompression(Compression.AUTO)),
        matches.apply("Read default", FileIO.readMatches()),
        matches.apply("Read UNCOMPRESSED", FileIO.readMatches().withCompression(Compression.UNCOMPRESSED))
    );
  }

  private List<PCollection<FileIO.ReadableFile>> createCompressedFiles(PCollection<MatchResult.Metadata> matchesGZ) { // Optimized by LLM: Use a loop to handle creation of PCollection<FileIO.ReadableFile>
    return Arrays.asList(
        matchesGZ.apply("Read GZ AUTO", FileIO.readMatches().withCompression(Compression.AUTO)),
        matchesGZ.apply("Read GZ default", FileIO.readMatches()),
        matchesGZ.apply("Read GZ GZIP", FileIO.readMatches().withCompression(Compression.GZIP))
    );
  }

  private void assertDecompressedFile(PCollection<FileIO.ReadableFile> c, String expectedPath) { // Optimized by LLM: Use a single assertion method
    PAssert.thatSingleton(c)
        .satisfies(
            input -> {
              assertEquals(expectedPath, input.getMetadata().resourceId().toString());
              assertEquals("Hello world".length(), input.getMetadata().sizeBytes());
              assertEquals(Compression.UNCOMPRESSED, input.getCompression());
              assertTrue(input.getMetadata().isReadSeekEfficient());
              try {
                assertEquals("Hello world", input.readFullyAsUTF8String());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              return null;
            });
  }

  private void assertCompressedFile(PCollection<FileIO.ReadableFile> c, String expectedPathGZ) { // Optimized by LLM: Use a single assertion method
    PAssert.thatSingleton(c)
        .satisfies(
            input -> {
              assertEquals(expectedPathGZ, input.getMetadata().resourceId().toString());
              assertFalse(input.getMetadata().sizeBytes() == "Hello world".length());
              assertEquals(Compression.GZIP, input.getCompression());
              assertFalse(input.getMetadata().isReadSeekEfficient());
              try {
                assertEquals("Hello world", input.readFullyAsUTF8String());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              return null;
            });
  }

  private static MatchResult.Metadata metadata(Path path, int size, long lastModifiedMillis) {
    return MatchResult.Metadata.builder()
        .setResourceId(FileSystems.matchNewResource(path.toString(), false /* isDirectory */))
        .setIsReadSeekEfficient(true)
        .setSizeBytes(size)
        .setLastModifiedMillis(lastModifiedMillis)
        .build();
  }

  private static long lastModifiedMillis(Path path) throws IOException {
    return Files.getLastModifiedTime(path).toMillis();
  }

  private static FileIO.Write.FileNaming resolveFileNaming(FileIO.Write<?, ?> write)
      throws Exception {
    return write.resolveFileNamingFn().getClosure().apply(null, null);
  }

  private static String getDefaultFileName(FileIO.Write<?, ?> write) throws Exception {
    return resolveFileNaming(write).getFilename(null, null, 0, 0, null);
  }

  @Test
  public void testFilenameFnResolution() throws Exception {
    FileIO.Write.FileNaming foo = (window, pane, numShards, shardIndex, compression) -> "foo";

    String expected =
        FileSystems.matchNewResource("test", true).resolve("foo", RESOLVE_FILE).toString();
    assertEquals(
        "Filenames should be resolved within a relative directory if '.to' is invoked",
        expected,
        getDefaultFileName(FileIO.writeDynamic().to("test").withNaming(o -> foo)));
    assertEquals(
        "Filenames should be resolved within a relative directory if '.to' is invoked",
        expected,
        getDefaultFileName(FileIO.write().to("test").withNaming(foo)));

    assertEquals(
        "Filenames should be resolved as the direct result of the filenaming function if '.to' "
            + "is not invoked",
        "foo",
        getDefaultFileName(FileIO.writeDynamic().withNaming(o -> foo)));
    assertEquals(
        "Filenames should be resolved as the direct result of the filenaming function if '.to' "
            + "is not invoked",
        "foo",
        getDefaultFileName(FileIO.write().withNaming(foo)));

    assertEquals(
        "Default to the defaultNaming if a filenaming isn't provided for a non-dynamic write",
        "output-00000-of-00000",
        resolveFileNaming(FileIO.write())
            .getFilename(
                GlobalWindow.INSTANCE,
                PaneInfo.ON_TIME_AND_ONLY_FIRING,
                0,
                0,
                Compression.UNCOMPRESSED));

    assertEquals(
        "Default Naming should take prefix and suffix into account if provided",
        "foo-00000-of-00000.bar",
        resolveFileNaming(FileIO.write().withPrefix("foo").withSuffix(".bar"))
            .getFilename(
                GlobalWindow.INSTANCE,
                PaneInfo.ON_TIME_AND_ONLY_FIRING,
                0,
                0,
                Compression.UNCOMPRESSED));

    assertEquals(
        "Filenames should be resolved within a relative directory if '.to' is invoked, "
            + "even with default naming",
        FileSystems.matchNewResource("test", true)
            .resolve("output-00000-of-00000", RESOLVE_FILE)
            .toString(),
        resolveFileNaming(FileIO.write().to("test"))
            .getFilename(
                GlobalWindow.INSTANCE,
                PaneInfo.ON_TIME_AND_ONLY_FIRING,
                0,
                0,
                Compression.UNCOMPRESSED));
  }
}