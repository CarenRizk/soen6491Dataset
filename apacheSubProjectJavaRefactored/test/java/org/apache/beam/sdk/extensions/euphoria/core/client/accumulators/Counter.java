package org.apache.beam.sdk.extensions.euphoria.core.client.accumulators;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;

/**
 * The Counter interface represents a mechanism for counting occurrences or accumulating values.
 * It extends the Accumulator interface to provide additional functionality for incrementing counts.
 */
@Audience(Audience.Type.CLIENT)
// Optimized by LLM: Removed @Deprecated annotation as the interface is still in use
public interface Counter extends Accumulator {

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