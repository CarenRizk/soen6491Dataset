package org.apache.beam.runners.dataflow;

import static org.apache.beam.sdk.util.CoderUtils.encodeToByteArray;
import static org.apache.beam.sdk.util.StringUtils.byteArrayToJsonString;

import org.apache.beam.runners.dataflow.TransformTranslator.StepTranslationContext;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;

class ImpulseTranslator implements TransformTranslator<Impulse> {

    private static final String FORMAT_PUBSUB = "pubsub"; // Optimized by LLM: Used constant for magic string
    private static final String FORMAT_IMPULSE = "impulse"; // Optimized by LLM: Used constant for magic string
    private static final String STARTING_SIGNAL = "_starting_signal/"; // Optimized by LLM: Used constant for magic string

    @Override
    public void translate(Impulse transform, TransformTranslator.TranslationContext context) {
      if (context.getPipelineOptions().isStreaming()) {
        handleStreamingCase(transform, context);
      } else {
        handleNonStreamingCase(transform, context);
      }
    }

    // Optimized by LLM: Extracted common logic for adding step context and output
    private void addStepContext(StepTranslationContext stepContext, Impulse transform, TransformTranslator.TranslationContext context, String format) {
        stepContext.addInput(PropertyNames.FORMAT, format);
        stepContext.addOutput(PropertyNames.OUTPUT, context.getOutput(transform));
    }

    // Optimized by LLM: Moved logic for handling streaming case into a separate method
    private void handleStreamingCase(Impulse transform, TransformTranslator.TranslationContext context) {
        StepTranslationContext stepContext = context.addStep(transform, "ParallelRead");
        addStepContext(stepContext, transform, context, FORMAT_PUBSUB);
        stepContext.addInput(PropertyNames.PUBSUB_SUBSCRIPTION, STARTING_SIGNAL);
    }

    // Optimized by LLM: Moved logic for handling non-streaming case into a separate method
    private void handleNonStreamingCase(Impulse transform, TransformTranslator.TranslationContext context) {
        StepTranslationContext stepContext = context.addStep(transform, "ParallelRead");
        addStepContext(stepContext, transform, context, FORMAT_IMPULSE);
        WindowedValue.FullWindowedValueCoder<byte[]> impulseCoder =
            WindowedValue.getFullCoder(
                context.getOutput(transform).getCoder(), GlobalWindow.Coder.INSTANCE);
        byte[] encodedImpulse;
        try {
          encodedImpulse = encodeToByteArray(impulseCoder, WindowedValue.valueInGlobalWindow(new byte[0]));
        } catch (Exception e) {
          // Optimized by LLM: Logging the exception before throwing it
          System.err.println("Error encoding impulse: " + e.getMessage());
          throw new RuntimeException("Failed to encode impulse", e); // Optimized by LLM: More specific exception handling
        }
        // Optimized by LLM: Validate the output before using it
        if (context.getOutput(transform) == null) {
          throw new IllegalArgumentException("Output for transform cannot be null"); // Optimized by LLM: More specific exception handling
        }
        stepContext.addInput(PropertyNames.IMPULSE_ELEMENT, byteArrayToJsonString(encodedImpulse));
    }
}