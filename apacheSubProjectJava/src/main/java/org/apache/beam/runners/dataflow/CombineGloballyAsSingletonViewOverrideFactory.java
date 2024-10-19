package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
   * Replace the {@link Combine.GloballyAsSingletonView} transform with a specialization which
   * re-applies the {@link CombineFn} and adds a specialization specific to the Dataflow runner.
   */
  class CombineGloballyAsSingletonViewOverrideFactory<InputT, ViewT>
      extends ReflectiveViewOverrideFactory<Object, Object> {

    CombineGloballyAsSingletonViewOverrideFactory(DataflowRunner runner) {
      super((Class) BatchViewOverrides.BatchViewAsSingleton.class, runner);
    }

    @Override
    public PTransformReplacement<PCollection<Object>, PCollectionView<Object>>
        getReplacementTransform(
            AppliedPTransform<PCollection<Object>, PCollectionView<Object>, PTransform<PCollection<Object>, PCollectionView<Object>>>
                transform) {
      Combine.GloballyAsSingletonView<?, ?> combineTransform =
          (Combine.GloballyAsSingletonView) transform.getTransform();
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new BatchViewOverrides.BatchViewAsSingleton(
              runner,
              findCreatePCollectionView(transform),
              (CombineFn) combineTransform.getCombineFn(),
              combineTransform.getFanout()));
    }
  }