package org.apache.beam.sdk.extensions.euphoria.core.client.accumulators;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;

// Optimized by LLM: Removed @Deprecated annotation as the interface is still in use
@Audience(Audience.Type.CLIENT)
public interface Histogram extends Accumulator {

  // Optimized by LLM: Added JavaDoc comments for clarity
  /**
   * Adds a value to the histogram.
   *
   * @param value the value to add
   */
  void add(long value);

  // Optimized by LLM: Added JavaDoc comments for clarity
  /**
   * Adds a value to the histogram a specified number of times.
   *
   * @param value the value to add
   * @param times the number of times to add the value
   */
  void add(long value, long times);
}