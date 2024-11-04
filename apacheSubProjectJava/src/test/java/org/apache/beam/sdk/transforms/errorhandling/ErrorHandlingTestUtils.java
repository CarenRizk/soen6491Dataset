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
  public static class ErrorSinkTransform
      extends PTransform<PCollection<BadRecord>, PCollection<Long>> {

    @Override
    public @UnknownKeyFor @NonNull @Initialized PCollection<Long> expand(
        PCollection<BadRecord> input) {
      if (input.isBounded() == IsBounded.BOUNDED) {
        return input.apply("Combine", Combine.globally(Count.<BadRecord>combineFn()));
      } else {
        return input
            .apply("Window", Window.into(FixedWindows.of(Duration.standardDays(1))))
            .apply("Combine", Combine.globally(Count.<BadRecord>combineFn()).withoutDefaults());
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
