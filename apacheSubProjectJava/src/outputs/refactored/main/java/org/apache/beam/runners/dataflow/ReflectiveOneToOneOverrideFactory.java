package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.sdk.values.PCollection;

class ReflectiveOneToOneOverrideFactory<
          InputT, OutputT, TransformT extends PTransform<PCollection<InputT>, PCollection<OutputT>>>
      extends SingleInputOutputOverrideFactory<
          PCollection<InputT>, PCollection<OutputT>, TransformT> {

    private final Class<PTransform<PCollection<InputT>, PCollection<OutputT>>> replacement;
    private final DataflowRunner runner;

    // Optimized by LLM: Consider making the constructor public or protected if this class is intended to be used outside of its package, to improve accessibility.
    protected ReflectiveOneToOneOverrideFactory(
        Class<PTransform<PCollection<InputT>, PCollection<OutputT>>> replacement,
        DataflowRunner runner) {
      if (replacement == null || runner == null) {
          throw new IllegalArgumentException("Replacement and runner cannot be null."); // Optimized by LLM: Validate the input parameters in the constructor to ensure that replacement and runner are not null, throwing an IllegalArgumentException if they are.
      }
      this.replacement = replacement;
      this.runner = runner;
    }

    // Optimized by LLM: Add JavaDoc comments to the class and its methods to provide better documentation and understanding of their purpose and usage.
    /**
     * Gets the replacement transform for the given applied transform.
     *
     * @param transform the applied transform
     * @return the replacement transform
     */
    @Override
    public PTransformReplacement<PCollection<InputT>, PCollection<OutputT>> getReplacementTransform(
        AppliedPTransform<PCollection<InputT>, PCollection<OutputT>, TransformT> transform) {
      PTransform<PCollection<InputT>, PCollection<OutputT>> replacementTransform = createReplacementTransform(transform); // Optimized by LLM: Extract the logic for creating the replacement transform into a separate private method to improve readability and maintainability.
      return PTransformReplacement.of(PTransformReplacements.getSingletonMainInput(transform), replacementTransform);
    }

    // Optimized by LLM: Extract the logic for creating the replacement transform into a separate private method to improve readability and maintainability.
    private PTransform<PCollection<InputT>, PCollection<OutputT>> createReplacementTransform(
        AppliedPTransform<PCollection<InputT>, PCollection<OutputT>, TransformT> transform) {
      @SuppressWarnings("unchecked") // Optimized by LLM: Use @SuppressWarnings("unchecked") annotation when casting transform.getTransform().getClass() to Class<TransformT> to avoid unchecked cast warnings.
      PTransform<PCollection<InputT>, PCollection<OutputT>> replacementTransform =
          InstanceBuilder.ofType(replacement)
              .withArg(DataflowRunner.class, runner)
              .withArg(
                  (Class<TransformT>) transform.getTransform().getClass(), transform.getTransform())
              .build();
      return replacementTransform;
    }
}