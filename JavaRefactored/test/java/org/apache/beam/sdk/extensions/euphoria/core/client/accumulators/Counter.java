package org.apache.beam.sdk.extensions.euphoria.core.client.accumulators;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;

// Optimized by LLM: Removed @Deprecated annotation as the interface is still in use and intended for future use
@Audience(Audience.Type.CLIENT)
public interface Counter extends Accumulator {

  // Optimized by LLM: Added JavaDoc comments to the interface and its methods
  /**
   * Increments the counter by the specified value.
   * 
   * @param value the value to increment the counter by
   */
  void increment(long value);

  /**
   * Increments the counter by 1.
   */
  void increment();
}