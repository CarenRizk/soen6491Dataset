package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSink;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

  class StreamingPubsubIOWrite extends PTransform<PCollection<PubsubMessage>, PDone> {

    private final PubsubUnboundedSink transform;

    public StreamingPubsubIOWrite(DataflowRunner runner, PubsubUnboundedSink transform) {
      this.transform = transform;
    }

    PubsubUnboundedSink getOverriddenTransform() {
      return transform;
    }

    @Override
    public PDone expand(PCollection<PubsubMessage> input) {
      return PDone.in(input.getPipeline());
    }

    @Override
    protected String getKindString() {
      return "StreamingPubsubIOWrite";
    }

    static {
      DataflowPipelineTranslator.registerTransformTranslator(
          StreamingPubsubIOWrite.class,
          new StreamingPubsubSinkTranslators.StreamingPubsubIOWriteTranslator());
    }
  }