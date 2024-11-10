package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.sdk.values.PCollection;

/**
 * ReflectiveOneToOneOverrideFactory is responsible for creating a replacement transform
 * for a given PTransform. It ensures that the replacement transform is properly instantiated
 * with the required arguments.
 */
class ReflectiveOneToOneOverrideFactory<
          InputT, OutputT, TransformT extends PTransform<PCollection<InputT>, PCollection<OutputT>>>
      extends SingleInputOutputOverrideFactory<
          PCollection<InputT>, PCollection<OutputT>, TransformT> {

    private final Class<PTransform<PCollection<InputT>, PCollection<OutputT>>> replacement;
    private final DataflowRunner runner;

    // Optimized by LLM: Consider making the constructor public or protected if this class is intended to be used outside its package.
    protected ReflectiveOneToOneOverrideFactory(
        Class<PTransform<PCollection<InputT>, PCollection<OutputT>>> replacement,
        DataflowRunner runner) {
      // Optimized by LLM: Validate the input parameters in the constructor to ensure that `replacement` and `runner` are not null.
      if (replacement == null || runner == null) {
          throw new IllegalArgumentException("Replacement and runner cannot be null");
      }
      this.replacement = replacement;
      this.runner = runner;
    }

    // Optimized by LLM: Add JavaDoc comments to the class and its methods to provide better documentation for future developers.
    /**
     * Returns the replacement transform for the given applied transform.
     *
     * @param transform the applied transform for which to get the replacement
     * @return the replacement transform
     */
    @Override
    public PTransformReplacement<PCollection<InputT>, PCollection<OutputT>> getReplacementTransform(
        AppliedPTransform<PCollection<InputT>, PCollection<OutputT>, TransformT> transform) {
      // Optimized by LLM: Extract the logic for creating the replacement transform into a separate private method to improve readability and maintainability.
      PTransform<PCollection<InputT>, PCollection<OutputT>> replacementTransform = createReplacementTransform(transform);
      return PTransformReplacement.of(PTransformReplacements.getSingletonMainInput(transform), replacementTransform);
    }

    // Optimized by LLM: Consider using generics more effectively by defining a bounded type for `TransformT` to ensure it extends `PTransform`.
    @SuppressWarnings("unchecked") // Optimized by LLM: Use `@SuppressWarnings("unchecked")` on the cast to avoid potential warnings and clarify that the developer is aware of the unchecked cast.
    private PTransform<PCollection<InputT>, PCollection<OutputT>> createReplacementTransform(
        AppliedPTransform<PCollection<InputT>, PCollection<OutputT>, TransformT> transform) {
      // Optimized by LLM: Rename the variable `rep` to something more descriptive, such as `replacementTransform`, to improve code clarity.
      return InstanceBuilder.ofType(replacement)
          .withArg(DataflowRunner.class, runner)
          .withArg(
              (Class<TransformT>) transform.getTransform().getClass(), transform.getTransform())
          .build();
    }
}