package org.apache.beam.runners.dataflow;

import java.util.Map;

import org.apache.beam.runners.dataflow.TransformTranslator.StepTranslationContext;
import org.apache.beam.runners.dataflow.TransformTranslator.TranslationContext;
import org.apache.beam.sdk.util.construction.External;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

class MultiOutputExpandableTransformTranslator
      implements TransformTranslator<External.MultiOutputExpandableTransform> {
    @Override
    public void translate(
        External.MultiOutputExpandableTransform transform, TranslationContext context) {
      StepTranslationContext stepContext = context.addStep(transform, "ExternalTransform");
      Map<TupleTag<?>, PCollection<?>> outputs = context.getOutputs(transform);
      for (Map.Entry<TupleTag<?>, PCollection<?>> taggedOutput : outputs.entrySet()) {
        TupleTag<?> tag = taggedOutput.getKey();
        stepContext.addOutput(tag.getId(), taggedOutput.getValue());
      }
    }
  }