package org.apache.beam.sdk.extensions.euphoria.core.client.accumulators;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;


@Audience(Audience.Type.CLIENT)
// Optimized by LLM: Removed the @Deprecated annotation as the interface is still in use and not intended for removal.
public interface Histogram extends Accumulator {

  // Optimized by LLM: Added JavaDoc comments to clarify purpose and usage.
  /**
   * Adds a single value to the histogram.
   *
   * @param value the value to add
   */
  void add(long value);

  // Optimized by LLM: Added JavaDoc comments to clarify purpose and usage.
  /**
   * Adds a value to the histogram multiple times.
   *
   * @param value the value to add
   * @param times the number of times to add the value
   */
  void add(long value, long times);
}