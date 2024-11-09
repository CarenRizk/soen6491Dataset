package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

// This class provides a factory for creating overrides for Combine.GloballyAsSingletonView transforms.
class CombineGloballyAsSingletonViewOverrideFactory<InputT, ViewT>
    extends ReflectiveViewOverrideFactory<Object, Object> {

    // Optimized by LLM: Replace the raw type usage of Class in the constructor with a specific type to avoid unchecked conversion warnings.
    CombineGloballyAsSingletonViewOverrideFactory(DataflowRunner runner) {
      super((Class<BatchViewOverrides.BatchViewAsSingleton>) BatchViewOverrides.BatchViewAsSingleton.class, runner);
    }

    @Override
    public PTransformReplacement<PCollection<Object>, PCollectionView<Object>>
        getReplacementTransform(
            AppliedPTransform<PCollection<Object>, PCollectionView<Object>, PTransform<PCollection<Object>, PCollectionView<Object>>>
                transform) {
      // Optimized by LLM: Use a specific type for Combine.GloballyAsSingletonView instead of casting to improve type safety.
      Combine.GloballyAsSingletonView<InputT, ViewT> combineTransform =
          (Combine.GloballyAsSingletonView<InputT, ViewT>) transform.getTransform();
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          createBatchViewAsSingleton(runner, transform, combineTransform));
    }

    // Optimized by LLM: Extract the logic for creating the BatchViewOverrides.BatchViewAsSingleton into a separate method to improve readability and maintainability.
    private PTransform<PCollection<Object>, PCollectionView<Object>> createBatchViewAsSingleton(
        DataflowRunner runner,
        AppliedPTransform<PCollection<Object>, PCollectionView<Object>, PTransform<PCollection<Object>, PCollectionView<Object>>> transform,
        Combine.GloballyAsSingletonView<InputT, ViewT> combineTransform) {
      // Optimized by LLM: Replace the raw type usage of CombineFn with a specific type parameter to avoid unchecked conversion warnings.
      return new BatchViewOverrides.BatchViewAsSingleton(
          runner,
          findCreatePCollectionView(transform),
          (CombineFn<InputT, ViewT>) combineTransform.getCombineFn(),
          combineTransform.getFanout());
    }
}