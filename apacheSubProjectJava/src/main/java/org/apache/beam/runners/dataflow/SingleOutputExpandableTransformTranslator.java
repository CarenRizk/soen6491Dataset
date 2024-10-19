package org.apache.beam.runners.dataflow;

import org.apache.beam.runners.dataflow.TransformTranslator.StepTranslationContext;
import org.apache.beam.runners.dataflow.TransformTranslator.TranslationContext;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.sdk.util.construction.External;
import org.apache.beam.sdk.util.construction.External.SingleOutputExpandableTransform;
import org.apache.beam.sdk.values.PCollection;

class SingleOutputExpandableTransformTranslator
      implements TransformTranslator<External.SingleOutputExpandableTransform> {
    @Override
    public void translate(
        External.SingleOutputExpandableTransform transform, TranslationContext context) {
      StepTranslationContext stepContext = context.addStep(transform, "ExternalTransform");
      PCollection<?> output = (PCollection<?>) context.getOutput(transform);
      stepContext.addOutput(PropertyNames.OUTPUT, output);
    }
  }