package org.apache.beam.sdk.transforms.windowing;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WindowingTest implements Serializable {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final Duration FIXED_WINDOW_DURATION = Duration.millis(10); // Optimized by LLM: Suggestion 3
  private static final Duration SLIDING_WINDOW_DURATION = Duration.millis(10); // Optimized by LLM: Suggestion 3
  private static final Duration SLIDE_DURATION = Duration.millis(5); // Optimized by LLM: Suggestion 3
  private static final Duration SESSION_GAP_DURATION = Duration.millis(10); // Optimized by LLM: Suggestion 3

  private static class WindowedCount extends PTransform<PCollection<String>, PCollection<String>> {

    private static final class FormatCountsDoFn extends DoFn<KV<String, Long>, String> {
      @ProcessElement
      public void processElement(ProcessContext c, BoundedWindow window) {
        c.output(
            c.element().getKey()
                + ":"
                + c.element().getValue()
                + ":"
                + c.timestamp().getMillis()
                + ":"
                + window);
      }
    }

    private WindowFn<? super String, ?> windowFn;

    public WindowedCount(WindowFn<? super String, ?> windowFn) {
      this.windowFn = windowFn;
    }

    @Override
    public PCollection<String> expand(PCollection<String> in) {
      return in.apply(
              "Window",
              Window.<String>into(windowFn).withTimestampCombiner(TimestampCombiner.EARLIEST))
          .apply(Count.perElement())
          .apply("FormatCounts", ParDo.of(new FormatCountsDoFn()))
          .setCoder(StringUtf8Coder.of());
    }
  }

  private String output(String value, int count, int timestamp, int windowStart, int windowEnd) {
    return value
        + ":"
        + count
        + ":"
        + timestamp
        + ":["
        + new Instant(windowStart)
        + ".."
        + new Instant(windowEnd)
        + ")";
  }

  private PCollection<String> createTimestampedInput(String... values) { // Optimized by LLM: Suggestion 4
	    List<TimestampedValue<String>> timestampedValues = new ArrayList<>();
	    for (int i = 0; i < values.length; i++) {
	        timestampedValues.add(TimestampedValue.of(values[i], new Instant(i + 1))); // Example timestamp
	    }
	    return p.apply(Create.timestamped(timestampedValues));
	}

  @Test
  @Category(ValidatesRunner.class)
  public void testPartitioningWindowing() {
    PCollection<String> input = createTimestampedInput("a", "b", "b", "c", "d"); // Optimized by LLM: Suggestion 4

    PCollection<String> output =
        input.apply(new WindowedCount(FixedWindows.of(FIXED_WINDOW_DURATION))); // Optimized by LLM: Suggestion 3

    PAssert.that(output)
        .containsInAnyOrder(
            output("a", 1, 1, 0, 10),
            output("b", 2, 2, 0, 10),
            output("c", 1, 11, 10, 20),
            output("d", 1, 11, 10, 20));

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testNonPartitioningWindowing() {
    PCollection<String> input = createTimestampedInput("a", "a", "b"); // Optimized by LLM: Suggestion 4

    PCollection<String> output =
        input.apply(
            new WindowedCount(SlidingWindows.of(SLIDING_WINDOW_DURATION).every(SLIDE_DURATION))); // Optimized by LLM: Suggestion 3

    PAssert.that(output)
        .containsInAnyOrder(
            output("a", 1, 1, -5, 5),
            output("a", 2, 1, 0, 10),
            output("a", 1, 7, 5, 15),
            output("b", 1, 8, 0, 10),
            output("b", 1, 8, 5, 15));

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testMergingWindowing() {
    PCollection<String> input = createTimestampedInput("a", "a", "a"); // Optimized by LLM: Suggestion 4

    PCollection<String> output =
        input.apply(new WindowedCount(Sessions.withGapDuration(SESSION_GAP_DURATION))); // Optimized by LLM: Suggestion 3

    PAssert.that(output).containsInAnyOrder(output("a", 2, 1, 1, 15), output("a", 1, 20, 20, 30));

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testWindowPreservation() {
    PCollection<String> input1 = createTimestampedInput("a", "b"); // Optimized by LLM: Suggestion 4
    PCollection<String> input2 = createTimestampedInput("a", "b"); // Optimized by LLM: Suggestion 4

    PCollectionList<String> input = PCollectionList.of(input1).and(input2);

    PCollection<String> output =
        input
            .apply(Flatten.pCollections())
            .apply(new WindowedCount(FixedWindows.of(Duration.millis(5))));

    PAssert.that(output).containsInAnyOrder(output("a", 2, 1, 0, 5), output("b", 2, 2, 0, 5));

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testEmptyInput() {
    PCollection<String> input = p.apply(Create.empty(StringUtf8Coder.of()));

    PCollection<String> output =
        input.apply(new WindowedCount(FixedWindows.of(FIXED_WINDOW_DURATION))); // Optimized by LLM: Suggestion 3

    PAssert.that(output).empty();

    p.run();
  }

  
  static class ExtractWordsWithTimestampsFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      List<String> words = Splitter.onPattern("[^a-zA-Z0-9']+").splitToList(c.element());
      if (words.size() == 2) {
        c.outputWithTimestamp(words.get(0), new Instant(Long.parseLong(words.get(1))));
      }
    }
  }
}