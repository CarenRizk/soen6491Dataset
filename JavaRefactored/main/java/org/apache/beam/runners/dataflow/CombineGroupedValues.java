package org.apache.beam.runners.dataflow;

/** 
 * A PTransform that combines grouped values using a specified Combine operation. 
 * 
 * @param <K> The type of the key.
 * @param <InputT> The type of the input values.
 * @param <OutputT> The type of the output values.
 */
final class CombineGroupedValues<K, InputT, OutputT>
      extends PTransform<PCollection<KV<K, Iterable<InputT>>>, PCollection<KV<K, OutputT>>> {

    private final Combine.GroupedValues<K, InputT, OutputT> original;
    private final Coder<KV<K, OutputT>> outputCoder;

    // Optimized by LLM: Suggestion 2 applied
    CombineGroupedValues(
        Combine.GroupedValues<K, InputT, OutputT> original, Coder<KV<K, OutputT>> outputCoder) {
      if (original == null || outputCoder == null) {
          throw new IllegalArgumentException("original and outputCoder cannot be null");
      }
      this.original = original;
      this.outputCoder = outputCoder;
    }

    // Optimized by LLM: Suggestion 3 applied
    @Override
    public PCollection<KV<K, OutputT>> expand(PCollection<KV<K, Iterable<InputT>>> inputCollection) {
      // Optimized by LLM: Suggestion 4 applied
      return PCollection.createPrimitiveOutputInternal(
          inputCollection.getPipeline(), inputCollection.getWindowingStrategy(), inputCollection.isBounded(), outputCoder);
    }

    public Combine.GroupedValues<K, InputT, OutputT> getOriginalCombine() {
      return original;
    }

    // Optimized by LLM: Suggestion 7 applied
    @Override
    public String toString() {
      return "CombineGroupedValues{" +
              "original=" + original +
              ", outputCoder=" + outputCoder +
              '}';
    }
}