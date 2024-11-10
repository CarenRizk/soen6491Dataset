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
 * StreamingPubsubIORead is a PTransform that reads messages from a Pub/Sub source.
 */
class StreamingPubsubIORead
    extends PTransform<PBegin, PCollection<PubsubMessage>> {

    private final PubsubUnboundedSource transform; // Optimized by LLM: Consider making the `transform` field final to ensure it is immutable after construction.

    public StreamingPubsubIORead(PubsubUnboundedSource transform) {
        if (transform == null) { // Optimized by LLM: Consider adding validation for the `transform` parameter in the constructor to ensure it is not null.
            throw new IllegalArgumentException("Transform cannot be null");
        }
        this.transform = transform;
    }

    private Coder getPubsubCoder() { // Optimized by LLM: Extract the logic for determining the coder into a private method to enhance code clarity and maintainability.
        return transform.getNeedsMessageId()
            ? new PubsubMessageWithAttributesAndMessageIdCoder()
            : new PubsubMessageWithAttributesCoder();
    }

    PubsubUnboundedSource getOverriddenTransform() { // Optimized by LLM: If the `getOverriddenTransform` method is not used outside of this class, consider making it private to encapsulate the implementation details.
        return transform;
    }

    @Override
    public PCollection<PubsubMessage> expand(PBegin input) {
        Coder pubsubCoder = getPubsubCoder(); // Optimized by LLM: Consider using a more descriptive variable name than `coder` to clarify its purpose, such as `pubsubCoder`.
        return PCollection.createPrimitiveOutputInternal(
            input.getPipeline(), WindowingStrategy.globalDefault(), IsBounded.UNBOUNDED, pubsubCoder); // Optimized by LLM: Use a ternary operator directly in the `PCollection.createPrimitiveOutputInternal` method call to reduce the number of lines and improve readability.
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