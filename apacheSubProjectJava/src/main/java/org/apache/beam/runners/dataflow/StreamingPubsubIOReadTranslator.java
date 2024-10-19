package org.apache.beam.runners.dataflow;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.runners.dataflow.TransformTranslator.StepTranslationContext;
import org.apache.beam.runners.dataflow.util.PropertyNames;

/** Rewrite {@link StreamingPubsubIORead} to the appropriate internal node. */
  class StreamingPubsubIOReadTranslator
      implements TransformTranslator<StreamingPubsubIORead> {

    @Override
    public void translate(
        StreamingPubsubIORead transform, TransformTranslator.TranslationContext context) {
      checkArgument(
          context.getPipelineOptions().isStreaming(),
          "StreamingPubsubIORead is only for streaming pipelines.");
      StepTranslationContext stepContext = context.addStep(transform, "ParallelRead");
      DataflowRunner.translateOverriddenPubsubSourceStep(transform.getOverriddenTransform(), stepContext);
      stepContext.addOutput(PropertyNames.OUTPUT, context.getOutput(transform));
    }
  }