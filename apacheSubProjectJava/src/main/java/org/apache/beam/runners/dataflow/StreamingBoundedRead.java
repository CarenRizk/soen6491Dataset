package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.UnboundedReadFromBoundedSource;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;

/**
   * Specialized implementation for {@link org.apache.beam.sdk.io.Read.Bounded Read.Bounded} for the
   * Dataflow runner in streaming mode.
   */
  class StreamingBoundedRead<T> extends PTransform<PBegin, PCollection<T>> {

    private final BoundedSource<T> source;

    public StreamingBoundedRead(Read.Bounded<T> transform) {
      this.source = transform.getSource();
    }

    @Override
    public final PCollection<T> expand(PBegin input) {
      source.validate();

      return Pipeline.applyTransform(input, new UnboundedReadFromBoundedSource<>(source))
          .setIsBoundedInternal(IsBounded.BOUNDED);
    }
  }