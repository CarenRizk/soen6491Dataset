package org.apache.beam.runners.dataflow;

/** 
 * Translates SingleOutputExpandableTransform to a Dataflow step.
 */
import org.apache.beam.runners.dataflow.TransformTranslator.StepTranslationContext;
import org.apache.beam.runners.dataflow.TransformTranslator.TranslationContext;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.sdk.util.construction.External;
import org.apache.beam.sdk.values.PCollection;

class SingleOutputExpandableTransformTranslator
      implements TransformTranslator<External.SingleOutputExpandableTransform> {
    
    private static final String EXTERNAL_TRANSFORM_NAME = "ExternalTransform"; // Optimized by LLM: Extracted magic string into a constant

    /** 
     * Translates the given transform into a Dataflow step.
     * 
     * @param transform the transform to translate
     * @param context the translation context
     */
    @Override
    public void translate(
        External.SingleOutputExpandableTransform transform, TranslationContext context) {
      StepTranslationContext stepContext = context.addStep(transform, EXTERNAL_TRANSFORM_NAME); // Optimized by LLM: Used constant for step name
      PCollection<?> output = (PCollection<?>) context.getOutput(transform); // Optimized by LLM: Consider using a more specific type if applicable
      stepContext.addOutput(PropertyNames.OUTPUT, output); // Optimized by LLM: Check if addOutput can be chained
    }
}