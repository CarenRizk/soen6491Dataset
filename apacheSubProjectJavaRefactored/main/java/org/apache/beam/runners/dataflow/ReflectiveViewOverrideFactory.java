package org.apache.beam.runners.dataflow;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.Map;
import java.util.Optional;

import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

  class ReflectiveViewOverrideFactory<InputT, ViewT>
      implements PTransformOverrideFactory<
          PCollection<InputT>,
          PCollectionView<ViewT>,
          PTransform<PCollection<InputT>, PCollectionView<ViewT>>> {

    final Class<PTransform<PCollection<InputT>, PCollectionView<ViewT>>> replacement;
    final DataflowRunner runner;

    protected ReflectiveViewOverrideFactory(
        Class<PTransform<PCollection<InputT>, PCollectionView<ViewT>>> replacement,
        DataflowRunner runner) {
      // Optimized by LLM: Validate input parameters to prevent NullPointerExceptions
      if (replacement == null || runner == null) {
        throw new IllegalArgumentException("Replacement and runner cannot be null");
      }
      this.replacement = replacement;
      this.runner = runner;
    }

    CreatePCollectionView findCreatePCollectionView(
        final AppliedPTransform<
                PCollection<InputT>,
                PCollectionView<ViewT>,
                PTransform<PCollection<InputT>, PCollectionView<ViewT>>>
            transform) {
      final Optional<CreatePCollectionView> viewTransformRef = Optional.empty();
      transform
          .getPipeline()
          .traverseTopologically(
              new PipelineVisitor.Defaults() {
                
                private boolean isTrackingTransform = false; // Optimized by LLM: Renamed variable for clarity

                @Override
                public CompositeBehavior enterCompositeTransform(Node node) {
                  if (transform.getTransform() == node.getTransform()) {
                    isTrackingTransform = true;
                  }
                  return super.enterCompositeTransform(node);
                }

                @Override
                public void visitPrimitiveTransform(Node node) {
                  if (isTrackingTransform && node.getTransform() instanceof CreatePCollectionView) {
                    // Optimized by LLM: Extracted logic to a separate method for readability
                    handlePrimitiveTransform(node, viewTransformRef);
                  }
                }

                private void handlePrimitiveTransform(Node node, Optional<CreatePCollectionView> viewTransformRef) {
                  checkState(
                      viewTransformRef.isEmpty() && viewTransformRef.equals(Optional.of((CreatePCollectionView) node.getTransform())),
                      "Found more than one instance of a CreatePCollectionView when "
                          + "attempting to replace %s, found [%s, %s]",
                      replacement,
                      viewTransformRef.orElse(null),
                      node.getTransform());
                }

                @Override
                public void leaveCompositeTransform(Node node) {
                  if (transform.getTransform() == node.getTransform()) {
                    isTrackingTransform = false;
                  }
                }
              });

      checkState(
          viewTransformRef.isPresent(),
          "Expected to find CreatePCollectionView contained within %s",
          transform.getTransform());
      return viewTransformRef.get();
    }

    @Override
    public PTransformReplacement<PCollection<InputT>, PCollectionView<ViewT>>
        getReplacementTransform(
            final AppliedPTransform<
                    PCollection<InputT>,
                    PCollectionView<ViewT>,
                    PTransform<PCollection<InputT>, PCollectionView<ViewT>>>
                transform) {

      PTransform<PCollection<InputT>, PCollectionView<ViewT>> rep =
          InstanceBuilder.ofType(replacement)
              .withArg(DataflowRunner.class, runner)
              .withArg(CreatePCollectionView.class, findCreatePCollectionView(transform))
              .build();
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform), (PTransform) rep);
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollectionView<ViewT> newOutput) {

      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }