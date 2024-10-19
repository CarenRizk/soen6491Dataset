package org.apache.beam.runners.dataflow;

import java.util.Map;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSource;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

class StreamingPubsubIOReadOverrideFactory
      implements PTransformOverrideFactory<
          PBegin, PCollection<PubsubMessage>, PubsubUnboundedSource> {

    @Override
    public PTransformReplacement<PBegin, PCollection<PubsubMessage>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<PubsubMessage>, PubsubUnboundedSource> transform) {
      return PTransformReplacement.of(
          transform.getPipeline().begin(), new StreamingPubsubIORead(transform.getTransform()));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<PubsubMessage> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }