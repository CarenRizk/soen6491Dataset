package org.apache.beam.sdk.transforms.errorhandling;

import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;

public class ErrorHandlingTestUtils {
  private static final String COMBINE_TRANSFORM_NAME = "Combine"; // Optimized by LLM: Use constants for transform names
  private static final String WINDOW_TRANSFORM_NAME = "Window"; // Optimized by LLM: Use constants for transform names

  public static class ErrorSinkTransform
      extends PTransform<PCollection<BadRecord>, PCollection<Long>> {

    @Override
    public @UnknownKeyFor @NonNull @Initialized PCollection<Long> expand(
        PCollection<BadRecord> input) {
      return handleBoundedAndUnbounded(input); // Optimized by LLM: Extract the logic for handling bounded and unbounded inputs
    }

    // Optimized by LLM: Extract the logic for handling bounded and unbounded inputs
    private PCollection<Long> handleBoundedAndUnbounded(PCollection<BadRecord> input) {
      if (input.isBounded() == IsBounded.BOUNDED) {
        return input.apply(COMBINE_TRANSFORM_NAME, Combine.globally(Count.<BadRecord>combineFn()));
      } else {
        return input
            .apply(WINDOW_TRANSFORM_NAME, Window.into(FixedWindows.of(Duration.standardDays(1))))
            .apply(COMBINE_TRANSFORM_NAME, Combine.globally(Count.<BadRecord>combineFn()).withoutDefaults());
      }
    }
  }

  public static class EchoErrorTransform
      extends PTransform<PCollection<BadRecord>, PCollection<BadRecord>> {

    @Override
    public PCollection<BadRecord> expand(PCollection<BadRecord> input) {
      return input;
    }
  }
}