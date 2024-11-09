package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.SimpleFunction;

// Optimized by LLM: Removed IdentityMessageFn class and used a lambda expression
SimpleFunction<PubsubMessage, PubsubMessage> identityMessageFn = input -> input;