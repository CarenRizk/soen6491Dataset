package org.apache.beam.runners.dataflow;

import java.util.Collections;
import java.util.Map;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSink;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;

/** 
 * StreamingPubsubIOWriteOverrideFactory is a factory for creating replacement transforms 
 * for streaming Pub/Sub I/O writes in the Dataflow runner.
 */
class StreamingPubsubIOWriteOverrideFactory
      implements PTransformOverrideFactory<PCollection<PubsubMessage>, PDone, PubsubUnboundedSink> {

    private final DataflowRunner runner; // Optimized by LLM: Marked as final to indicate immutability

    /** 
     * Constructs a StreamingPubsubIOWriteOverrideFactory with the specified DataflowRunner.
     * 
     * @param runner the DataflowRunner to be used for the replacement transforms
     */
    StreamingPubsubIOWriteOverrideFactory(DataflowRunner runner) {
      this.runner = runner;
    }

    /** 
     * Returns a replacement transform for the specified applied transform.
     * 
     * @param transform the applied transform to be replaced
     * @return a PTransformReplacement containing the new transform
     */
    @Override
    public PTransformReplacement<PCollection<PubsubMessage>, PDone> getReplacementTransform(
        AppliedPTransform<PCollection<PubsubMessage>, PDone, PubsubUnboundedSink> transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new StreamingPubsubIOWrite(runner, transform.getTransform()));
    }

    /** 
     * Maps the outputs of the specified transform to the new output.
     * 
     * @param outputs the outputs of the original transform
     * @param newOutput the new output for the replacement transform
     * @return a map of the outputs for the replacement transform
     */
    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PDone newOutput) {
      return Collections.emptyMap();
    }
}