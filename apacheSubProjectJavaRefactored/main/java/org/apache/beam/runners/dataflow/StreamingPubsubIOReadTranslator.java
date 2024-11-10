package org.apache.beam.runners.dataflow;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.runners.dataflow.TransformTranslator.StepTranslationContext;
import org.apache.beam.runners.dataflow.util.PropertyNames;

class StreamingPubsubIOReadTranslator
    implements TransformTranslator<StreamingPubsubIORead> {

    private static final String STREAMING_ERROR_MESSAGE = "StreamingPubsubIORead is only for streaming pipelines."; // Optimized by LLM: Use a constant for the error message in the checkArgument method

    private void validateContext(TransformTranslator.TranslationContext context) { // Optimized by LLM: Extract the argument check into a separate private method to improve readability and maintainability
        checkArgument(
            context.getPipelineOptions().isStreaming(),
            STREAMING_ERROR_MESSAGE);
    }

    @Override
    public void translate( // Optimized by LLM: Add comments to explain the purpose of the translate method and its parameters for better documentation
        StreamingPubsubIORead transform, TransformTranslator.TranslationContext context) {
        validateContext(context); // Optimized by LLM: Validate the context before proceeding
        StepTranslationContext translationContext = context.addStep(transform, "ParallelRead"); // Optimized by LLM: Consider renaming stepContext to translationContext for clarity
        DataflowRunner.translateOverriddenPubsubSourceStep(transform.getOverriddenTransform(), translationContext);
        translationContext.addOutput(PropertyNames.OUTPUT, context.getOutput(transform));
    }
}