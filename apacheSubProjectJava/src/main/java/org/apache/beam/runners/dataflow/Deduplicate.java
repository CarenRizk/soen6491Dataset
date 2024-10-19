package org.apache.beam.runners.dataflow;

import java.util.Arrays;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.ValueWithRecordId;

/** Remove values with duplicate ids. */
  class Deduplicate<T>
      extends PTransform<PCollection<ValueWithRecordId<T>>, PCollection<T>> {

    // Use a finite set of keys to improve bundling.  Without this, the key space
    // will be the space of ids which is potentially very large, which results in much
    // more per-key overhead.
    private static final int NUM_RESHARD_KEYS = 10000;

    @Override
    public PCollection<T> expand(PCollection<ValueWithRecordId<T>> input) {
      return input
          .apply(
              WithKeys.of(
                      (ValueWithRecordId<T> value) ->
                          Arrays.hashCode(value.getId()) % NUM_RESHARD_KEYS)
                  .withKeyType(TypeDescriptors.integers()))
          // Reshuffle will dedup based on ids in ValueWithRecordId by passing the data through
          // WindmillSink.
          .apply(Reshuffle.of())
          .apply(
              "StripIds",
              ParDo.of(
                  new DoFn<KV<Integer, ValueWithRecordId<T>>, T>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                      c.output(c.element().getValue().getValue());
                    }
                  }));
    }
  }