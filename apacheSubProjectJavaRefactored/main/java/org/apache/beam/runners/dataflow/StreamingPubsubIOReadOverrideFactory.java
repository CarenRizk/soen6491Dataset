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

// Optimized by LLM: Added JavaDoc comments to the class
/**
 * A factory for overriding the streaming Pub/Sub I/O read transform.
 */
class StreamingPubsubIOReadOverrideFactory
      implements PTransformOverrideFactory<
          PBegin, PCollection<PubsubMessage>, PubsubUnboundedSource> {

    // Optimized by LLM: Added JavaDoc comments to the method
    /**
     * Returns a replacement transform for the given applied transform.
     *
     * @param transform the applied transform to replace
     * @return a replacement transform
     */
    @Override
    public PTransformReplacement<PBegin, PCollection<PubsubMessage>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<PubsubMessage>, PubsubUnboundedSource> transform) {
      return PTransformReplacement.of(
          transform.getPipeline().begin(), new StreamingPubsubIORead(transform.getTransform()));
    }

    // Optimized by LLM: Renamed variable for clarity
    /**
     * Maps the outputs of the transform to a new output collection.
     *
     * @param outputCollections the original output collections
     * @param newOutput the new output collection
     * @return a map of output collections
     */
    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputCollections, PCollection<PubsubMessage> newOutput) {
      return ReplacementOutputs.singleton(outputCollections, newOutput);
    }
}