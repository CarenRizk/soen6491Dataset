package org.apache.beam.runners.dataflow;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

/**
   * Replace the View.AsYYY transform with specialized view overrides for Dataflow. It is required
   * that the new replacement transform uses the supplied PCollectionView and does not create
   * another instance.
   */
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
      this.replacement = replacement;
      this.runner = runner;
    }

    CreatePCollectionView<ViewT, ViewT> findCreatePCollectionView(
        final AppliedPTransform<
                PCollection<InputT>,
                PCollectionView<ViewT>,
                PTransform<PCollection<InputT>, PCollectionView<ViewT>>>
            transform) {
      final AtomicReference<CreatePCollectionView> viewTransformRef = new AtomicReference<>();
      transform
          .getPipeline()
          .traverseTopologically(
              new PipelineVisitor.Defaults() {
                // Stores whether we have entered the expected composite view transform.
                private boolean tracking = false;

                @Override
                public CompositeBehavior enterCompositeTransform(Node node) {
                  if (transform.getTransform() == node.getTransform()) {
                    tracking = true;
                  }
                  return super.enterCompositeTransform(node);
                }

                @Override
                public void visitPrimitiveTransform(Node node) {
                  if (tracking && node.getTransform() instanceof CreatePCollectionView) {
                    checkState(
                        viewTransformRef.compareAndSet(
                            null, (CreatePCollectionView) node.getTransform()),
                        "Found more than one instance of a CreatePCollectionView when "
                            + "attempting to replace %s, found [%s, %s]",
                        replacement,
                        viewTransformRef.get(),
                        node.getTransform());
                  }
                }

                @Override
                public void leaveCompositeTransform(Node node) {
                  if (transform.getTransform() == node.getTransform()) {
                    tracking = false;
                  }
                }
              });

      checkState(
          viewTransformRef.get() != null,
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
      /*
      The output of View.AsXYZ is a PCollectionView that expands to the PCollection to be materialized.
      The PCollectionView itself must have the same tag since that tag may have been embedded in serialized DoFns
      previously and cannot easily be rewired. The PCollection may differ, so we rewire it, even if the rewiring
      is a noop.
      */
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }