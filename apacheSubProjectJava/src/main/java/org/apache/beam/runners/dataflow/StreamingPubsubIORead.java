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

/**
   * Suppress application of {@link PubsubUnboundedSource#expand} in streaming mode so that we can
   * instead defer to Windmill's implementation.
   */
  class StreamingPubsubIORead
      extends PTransform<PBegin, PCollection<PubsubMessage>> {

    private final PubsubUnboundedSource transform;

    public StreamingPubsubIORead(PubsubUnboundedSource transform) {
      this.transform = transform;
    }

    public PubsubUnboundedSource getOverriddenTransform() {
      return transform;
    }

    @Override
    public PCollection<PubsubMessage> expand(PBegin input) {
      Coder coder =
          transform.getNeedsMessageId()
              ? new PubsubMessageWithAttributesAndMessageIdCoder()
              : new PubsubMessageWithAttributesCoder();
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED, coder);
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