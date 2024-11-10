package org.apache.beam.runners.dataflow;

/**
 * StreamingPubsubIOReadOverrideFactory is a factory class that provides a mechanism to override
 * the PTransform for reading from Pub/Sub in a streaming fashion.
 */
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

    /**
     * Returns a replacement transform for reading from Pub/Sub.
     *
     * @param transform The applied transform to be replaced.
     * @return A PTransformReplacement containing the new transform.
     */
    @Override
    public PTransformReplacement<PBegin, PCollection<PubsubMessage>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<PubsubMessage>, PubsubUnboundedSource> transform) {
      return PTransformReplacement.of(
          transform.getPipeline().begin(), new StreamingPubsubIORead(transform.getTransform()));
    }

    /**
     * Maps the outputs of the transform to a new output collection.
     *
     * @param outputCollections A map of output collections keyed by TupleTag.
     * @param newOutput The new output collection to be added.
     * @return A map of the updated outputs.
     */
    // Optimized by LLM: Suggestion 2 applied
    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputCollections, PCollection<PubsubMessage> newOutput) {
      return ReplacementOutputs.singleton(outputCollections, newOutput);
    }
}