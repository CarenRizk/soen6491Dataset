package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.UnboundedReadFromBoundedSource;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;

// Optimized by LLM: Consider making the `source` field final to ensure it is immutable after construction.
class StreamingBoundedRead<T> extends PTransform<PBegin, PCollection<T>> {

    private final BoundedSource<T> source;

    // Optimized by LLM: Add input validation in the constructor to check if the `transform` parameter is null before accessing it.
    public StreamingBoundedRead(Read.Bounded<T> transform) {
      if (transform == null) {
          throw new IllegalArgumentException("Transform cannot be null");
      }
      this.source = transform.getSource();
    }

    // Optimized by LLM: Consider using a more descriptive name for the `input` parameter in the `expand` method to clarify its purpose.
    @Override
    public final PCollection<T> expand(PBegin begin) {
      // Optimized by LLM: Handle potential exceptions that may arise from `source.validate()` to ensure robustness.
      try {
          source.validate();
      } catch (Exception e) {
          // Optimized by LLM: Consider using a logging framework to log the validation process or any potential issues during the execution.
          System.err.println("Validation failed: " + e.getMessage());
          throw e;
      }

      return applyTransformToPipeline(begin);
    }

    // Optimized by LLM: Extract the `Pipeline.applyTransform` call into a separate method to improve readability and maintainability.
    private PCollection<T> applyTransformToPipeline(PBegin begin) {
      return Pipeline.applyTransform(begin, new UnboundedReadFromBoundedSource<>(source))
          .setIsBoundedInternal(IsBounded.BOUNDED);
    }
}