package org.apache.beam.runners.dataflow;

import static org.apache.beam.sdk.util.SerializableUtils.serializeToByteArray;
import static org.apache.beam.sdk.util.StringUtils.byteArrayToJsonString;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.runners.dataflow.TransformTranslator.StepTranslationContext;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSink;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;

class StreamingPubsubSinkTranslators {
    static class StreamingPubsubIOWriteTranslator
        implements TransformTranslator<StreamingPubsubIOWrite> {

      @Override
      public void translate(
          StreamingPubsubIOWrite transform, TransformTranslator.TranslationContext context) {
        checkArgument(
            context.getPipelineOptions().isStreaming(),
            "StreamingPubsubIOWrite is only for streaming pipelines.");
        StepTranslationContext stepContext = context.addStep(transform, "ParallelWrite");
        StreamingPubsubSinkTranslators.translate(
            transform.getOverriddenTransform(), stepContext, context.getInput(transform));
      }
    }

    private static void translate(
        PubsubUnboundedSink overriddenTransform,
        StepTranslationContext stepContext,
        PCollection input) {
      stepContext.addInput(PropertyNames.FORMAT, "pubsub");
      addTopicInput(overriddenTransform, stepContext);
      if (overriddenTransform.getTimestampAttribute() != null) {
        stepContext.addInput(
            PropertyNames.PUBSUB_TIMESTAMP_ATTRIBUTE, overriddenTransform.getTimestampAttribute());
      }
      if (overriddenTransform.getIdAttribute() != null) {
        stepContext.addInput(
            PropertyNames.PUBSUB_ID_ATTRIBUTE, overriddenTransform.getIdAttribute());
      }
      stepContext.addInput(
          PropertyNames.PUBSUB_SERIALIZED_ATTRIBUTES_FN,
          byteArrayToJsonString(serializeToByteArray(new IdentityMessageFn())));

      stepContext.addEncodingInput(
          WindowedValue.getFullCoder(VoidCoder.of(), GlobalWindow.Coder.INSTANCE));
      stepContext.addInput(PropertyNames.PARALLEL_INPUT, input);
    }

    // Optimized by LLM: Extracted logic for adding inputs to stepContext into a separate private method
    private static void addTopicInput(PubsubUnboundedSink overriddenTransform, StepTranslationContext stepContext) {
      if (overriddenTransform.getTopicProvider() == null) { // Optimized by LLM: Used guard clause for null check
        stepContext.addInput(PropertyNames.PUBSUB_DYNAMIC_DESTINATIONS, true);
        return;
      }
      if (overriddenTransform.getTopicProvider().isAccessible()) {
        stepContext.addInput(
            PropertyNames.PUBSUB_TOPIC, overriddenTransform.getTopic().getFullPath());
      } else {
        stepContext.addInput(
            PropertyNames.PUBSUB_TOPIC_OVERRIDE,
            ((NestedValueProvider) overriddenTransform.getTopicProvider()).propertyName());
      }
    }
}