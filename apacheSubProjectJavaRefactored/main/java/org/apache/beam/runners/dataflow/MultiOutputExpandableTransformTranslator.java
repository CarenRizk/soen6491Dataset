package org.apache.beam.runners.dataflow;

import java.util.Map;

import org.apache.beam.sdk.util.construction.External;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

// Optimized by LLM: Added JavaDoc comments to the class and method
/**
 * Translates the MultiOutputExpandableTransform for the Dataflow runner.
 */
class MultiOutputExpandableTransformTranslator
      implements TransformTranslator<External.MultiOutputExpandableTransform> {
    // Optimized by LLM: Added JavaDoc comments to the method
    /**
     * Translates the given MultiOutputExpandableTransform into a step context.
     *
     * @param transform the MultiOutputExpandableTransform to translate
     * @param context the translation context
     */
    @Override
    public void translate(
        External.MultiOutputExpandableTransform transform, TranslationContext context) {
        // Optimized by LLM: Added null checks for transform and context parameters
        if (transform == null) {
            throw new IllegalArgumentException("Transform cannot be null");
        }
        if (context == null) {
            throw new IllegalArgumentException("Context cannot be null");
        }
        
        StepTranslationContext stepContext = context.addStep(transform, "ExternalTransform");
        Map<TupleTag<?>, PCollection<?>> outputs = context.getOutputs(transform);
        // Optimized by LLM: Extracted the logic for adding outputs to a separate method
        addOutputsToStepContext(outputs, stepContext);
    }

    // Optimized by LLM: Renamed taggedOutput to outputEntry for clarity
    private void addOutputsToStepContext(Map<TupleTag<?>, PCollection<?>> outputs, StepTranslationContext stepContext) {
        for (Map.Entry<TupleTag<?>, PCollection<?>> outputEntry : outputs.entrySet()) {
            TupleTag<?> tag = outputEntry.getKey();
            stepContext.addOutput(tag.getId(), outputEntry.getValue());
        }
    }
}