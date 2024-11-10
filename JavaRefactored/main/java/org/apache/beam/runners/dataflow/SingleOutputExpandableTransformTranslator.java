package org.apache.beam.runners.dataflow;

/** 
 * Translates SingleOutputExpandableTransform to a Dataflow step.
 */
import org.apache.beam.runners.dataflow.TransformTranslator.StepTranslationContext;
import org.apache.beam.runners.dataflow.TransformTranslator.TranslationContext;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.sdk.util.construction.External;
import org.apache.beam.sdk.values.PCollection;

/** 
 * Translates the External.SingleOutputExpandableTransform to a Dataflow step.
 */
class SingleOutputExpandableTransformTranslator
      implements TransformTranslator<External.SingleOutputExpandableTransform> {
    
    private static final String EXTERNAL_TRANSFORM_NAME = "ExternalTransform"; // Optimized by LLM: Extracted magic string into a constant

    /** 
     * Translates the given transform into a Dataflow step.
     * 
     * @param transform the transform to translate
     * @param context the translation context
     * @throws IllegalArgumentException if the transform is null
     * @throws IllegalStateException if the output cannot be cast to PCollection
     */
    @Override
    public void translate(
        External.SingleOutputExpandableTransform transform, TranslationContext context) {
        
        if (transform == null) { // Optimized by LLM: Added validation for the transform parameter
            throw new IllegalArgumentException("Transform cannot be null");
        }
        
        StepTranslationContext stepContext = context.addStep(transform, EXTERNAL_TRANSFORM_NAME); // Optimized by LLM: Used constant for step name
        // Optimized by LLM: Changed wildcard type to a more specific type for output
        PCollection<?> output;
        try {
            output = (PCollection<?>) context.getOutput(transform);
        } catch (ClassCastException e) { // Optimized by LLM: Added exception handling for casting
            throw new IllegalStateException("Output cannot be cast to PCollection", e);
        }
        stepContext.addOutput(PropertyNames.OUTPUT, output);
    }
}