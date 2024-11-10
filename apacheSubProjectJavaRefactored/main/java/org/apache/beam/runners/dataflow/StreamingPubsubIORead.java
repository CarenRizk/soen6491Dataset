package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSource;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection.IsBounded;

  class StreamingPubsubIORead
      extends PTransform<PBegin, PCollection<PubsubMessage>> {

    private final PubsubUnboundedSource transform; // Optimized by LLM: Made the transform field final

    public StreamingPubsubIORead(PubsubUnboundedSource transform) {
      this.transform = transform;
    }

    public PubsubUnboundedSource getOverriddenTransform() {
      return transform;
    }

    // Optimized by LLM: Extracted the logic for creating the coder into a separate private method
    private Coder createCoder() {
      boolean needsMessageId = transform.getNeedsMessageId(); // Optimized by LLM: Cached the result of transform.getNeedsMessageId()
      return needsMessageId
          ? new PubsubMessageWithAttributesAndMessageIdCoder()
          : new PubsubMessageWithAttributesCoder();
    }

    @Override
    public PCollection<PubsubMessage> expand(PBegin input) {
      Coder pubsubCoder = createCoder(); // Optimized by LLM: Changed variable name from coder to pubsubCoder
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED, pubsubCoder);
    }

    @Override
    protected String getKindString() {
      return "StreamingPubsubIORead";
    }

    static {
      DataflowPipelineTranslator.registerTransformTranslator(
          StreamingPubsubIORead.class, new StreamingPubsubIOReadTranslator());
    }
  }