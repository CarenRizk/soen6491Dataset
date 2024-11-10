package org.apache.beam.runners.dataflow;

import java.util.Map;

import org.apache.beam.runners.dataflow.TransformTranslator.StepTranslationContext;
import org.apache.beam.runners.dataflow.TransformTranslator.TranslationContext;
import org.apache.beam.sdk.util.construction.External;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

class MultiOutputExpandableTransformTranslator
      implements TransformTranslator<External.MultiOutputExpandableTransform> {
    // Optimized by LLM: Added comments to explain the purpose of the translate method and the overall functionality of the class.
    // This class translates the MultiOutputExpandableTransform into a format suitable for execution.
    // The translate method processes the outputs of the transform and adds them to the step context.
    @Override
    public void translate(
        External.MultiOutputExpandableTransform transform, TranslationContext context) {
      StepTranslationContext stepContext = context.addStep(transform, "ExternalTransform");
      Map<TupleTag<?>, PCollection<?>> outputs = context.getOutputs(transform);
      // Optimized by LLM: Validate that the outputs map is not null before iterating over it.
      if (outputs != null) {
        // Optimized by LLM: Extracted the logic inside the for loop into a separate method to improve readability and maintainability.
        processOutputs(outputs, stepContext);
      }
    }

    // Optimized by LLM: Extracted method to handle processing of outputs.
    private void processOutputs(Map<TupleTag<?>, PCollection<?>> outputs, StepTranslationContext stepContext) {
      // Optimized by LLM: Used a more descriptive variable name than taggedOutput for better clarity.
      outputs.entrySet().forEach(entry -> {
        TupleTag<?> tag = entry.getKey();
        stepContext.addOutput(tag.getId(), entry.getValue());
      });
    }
}