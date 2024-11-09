package org.apache.beam.runners.dataflow;

// Optimized by LLM: Added JavaDoc comments to the class and its methods
/**
 * A factory for overriding the streaming unbounded read transform.
 *
 * @param <T> the type of the elements in the input and output collections
 */
class StreamingUnboundedReadOverrideFactory<T>
      implements PTransformOverrideFactory<PBegin, PCollection<T>, Read.Unbounded<T>> {

    // Optimized by LLM: Added JavaDoc comments to the method
    /**
     * Returns a replacement transform for the given applied transform.
     *
     * @param transform the applied transform to replace
     * @return a replacement transform
     */
    @Override
    public PTransformReplacement<PBegin, PCollection<T>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<T>, Read.Unbounded<T>> transform) {
      return PTransformReplacement.of(
          transform.getPipeline().begin(), new StreamingUnboundedRead<>(transform.getTransform()));
    }

    // Optimized by LLM: Added validation for input parameters in mapOutputs method
    // Optimized by LLM: Used more descriptive variable names in mapOutputs method
    /**
     * Maps the outputs of the transform to the new output collection.
     *
     * @param outputCollections a map of output collections keyed by their TupleTags
     * @param newOutputCollection the new output collection to be added
     * @return a map of replacement outputs
     */
    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputCollections, PCollection<T> newOutputCollection) {
      if (outputCollections == null || newOutputCollection == null) {
          throw new IllegalArgumentException("Output collections and new output collection must not be null");
      }
      return ReplacementOutputs.singleton(outputCollections, newOutputCollection);
    }
}