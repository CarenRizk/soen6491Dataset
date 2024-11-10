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
                      this::hashValueWithRecordId) // Optimized by LLM: Use method references instead of lambda expressions where applicable, such as in the `WithKeys.of` method.
                  .withKeyType(TypeDescriptors.integers()))
          .apply(Reshuffle.of())
          .apply(
              "StripIds", // Optimized by LLM: Consider using a more descriptive name for the `StripIds` transform to clarify its purpose.
              ParDo.of(
                  new StripIdsDoFn<T>())); // Optimized by LLM: Move the inner `DoFn` class to a separate static class or make it a private static inner class to improve readability and reduce complexity in the `expand` method.
    }

    private int hashValueWithRecordId(ValueWithRecordId<T> value) { // Optimized by LLM: Use method references instead of lambda expressions where applicable, such as in the `WithKeys.of` method.
        return Arrays.hashCode(value.getId()) % NUM_RESHARD_KEYS;
    }

    private static class StripIdsDoFn<T> extends DoFn<KV<Integer, ValueWithRecordId<T>>, T> { // Optimized by LLM: Move the inner `DoFn` class to a separate static class or make it a private static inner class to improve readability and reduce complexity in the `expand` method.
        @ProcessElement
        public void processValueWithRecordId(ProcessContext c) { // Optimized by LLM: Use a more descriptive name for the `processElement` method to clarify its purpose, such as `processValueWithRecordId`.
            ValueWithRecordId<T> valueWithRecordId = c.element().getValue();
            if (valueWithRecordId != null) { // Optimized by LLM: Handle potential exceptions or edge cases in the `processElement` method, such as null checks for `ValueWithRecordId<T>`.
                c.output(valueWithRecordId.getValue());
            }
        }
    }
}