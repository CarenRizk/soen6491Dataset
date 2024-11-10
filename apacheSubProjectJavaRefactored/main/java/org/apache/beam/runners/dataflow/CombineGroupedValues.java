package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Combine.GroupedValues;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/** 
 * A PTransform that combines grouped values from a PCollection of KV pairs.
 * 
 * @param <K> The type of the key.
 * @param <InputT> The type of the input values.
 * @param <OutputT> The type of the output values.
 */
class CombineGroupedValues<K, InputT, OutputT>
      extends PTransform<PCollection<KV<K, Iterable<InputT>>>, PCollection<KV<K, OutputT>>> {

    private final Combine.GroupedValues<K, InputT, OutputT> original;
    private final Coder<KV<K, OutputT>> outputCoder;

    // Optimized by LLM: Suggestion 2 applied
    CombineGroupedValues(
        Combine.GroupedValues<K, InputT, OutputT> original, Coder<KV<K, OutputT>> outputCoder) {
      if (original == null) {
          throw new IllegalArgumentException("original cannot be null");
      }
      if (outputCoder == null) {
          throw new IllegalArgumentException("outputCoder cannot be null");
      }
      this.original = original;
      this.outputCoder = outputCoder;
    }

    // Optimized by LLM: Suggestion 4 applied
    @Override
    public PCollection<KV<K, OutputT>> expand(PCollection<KV<K, Iterable<InputT>>> inputCollection) {
      // Optimized by LLM: Suggestion 3 applied
      return PCollection.createPrimitiveOutputInternal(
          inputCollection.getPipeline(), inputCollection.getWindowingStrategy(), inputCollection.isBounded(), outputCoder);
    }

    public Combine.GroupedValues<K, InputT, OutputT> getOriginalCombine() {
      return original;
    }
}