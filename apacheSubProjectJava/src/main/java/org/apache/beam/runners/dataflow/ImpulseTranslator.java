package org.apache.beam.runners.dataflow;

import static org.apache.beam.sdk.util.CoderUtils.encodeToByteArray;
import static org.apache.beam.sdk.util.StringUtils.byteArrayToJsonString;

import org.apache.beam.runners.dataflow.TransformTranslator.StepTranslationContext;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;

class ImpulseTranslator implements TransformTranslator<Impulse> {

    @Override
    public void translate(Impulse transform, TransformTranslator.TranslationContext context) {
      if (context.getPipelineOptions().isStreaming()) {
        StepTranslationContext stepContext = context.addStep(transform, "ParallelRead");
        stepContext.addInput(PropertyNames.FORMAT, "pubsub");
        stepContext.addInput(PropertyNames.PUBSUB_SUBSCRIPTION, "_starting_signal/");
        stepContext.addOutput(PropertyNames.OUTPUT, context.getOutput(transform));
      } else {
        StepTranslationContext stepContext = context.addStep(transform, "ParallelRead");
        stepContext.addInput(PropertyNames.FORMAT, "impulse");
        WindowedValue.FullWindowedValueCoder<byte[]> coder =
            WindowedValue.getFullCoder(
                context.getOutput(transform).getCoder(), GlobalWindow.Coder.INSTANCE);
        byte[] encodedImpulse;
        try {
          encodedImpulse = encodeToByteArray(coder, WindowedValue.valueInGlobalWindow(new byte[0]));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        stepContext.addInput(PropertyNames.IMPULSE_ELEMENT, byteArrayToJsonString(encodedImpulse));
        stepContext.addOutput(PropertyNames.OUTPUT, context.getOutput(transform));
      }
    }
  }