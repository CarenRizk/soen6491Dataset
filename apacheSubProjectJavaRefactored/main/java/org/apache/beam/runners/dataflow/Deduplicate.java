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

class Deduplicate<T>
    extends PTransform<PCollection<ValueWithRecordId<T>>, PCollection<T>> {

    
    private static final int NUM_RESHARD_KEYS = 10000; // Optimized by LLM: Extract the magic number `10000` into a named constant for better readability and maintainability.

    @Override
    public PCollection<T> expand(PCollection<ValueWithRecordId<T>> input) {
      return input
          .apply(
              WithKeys.of(
                      (ValueWithRecordId<T> value) ->
                          Arrays.hashCode(value.getId()) % NUM_RESHARD_KEYS)
                  .withKeyType(TypeDescriptors.integers()))
          .apply(Reshuffle.of())
          .apply(
              "StripIds",
              ParDo.of(
                  new StrippingDoFn<T>())); // Optimized by LLM: Move the `DoFn` implementation for stripping IDs to a separate static class or method to improve readability and reduce the complexity of the `expand` method.
    }

    private static class StrippingDoFn<T> extends DoFn<KV<Integer, ValueWithRecordId<T>>, T> { // Optimized by LLM: Use a lambda expression instead of an anonymous inner class for the `DoFn` to simplify the code.
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().getValue().getValue());
        }
    }
}