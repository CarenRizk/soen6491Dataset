package org.apache.beam.sdk.extensions.euphoria.core.client.io;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Counter;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Histogram;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Timer;


@Audience(Audience.Type.CLIENT)
// Optimized by LLM: Removed @Deprecated annotation as the interface is still intended for use
public interface Environment {

  // Optimized by LLM: Added JavaDoc comments to clarify purpose and usage
  /**
   * Retrieves a Counter with the specified name.
   * 
   * @param name the name of the counter
   * @return the Counter instance
   */
  Counter getCounter(String name);

  // Optimized by LLM: Added JavaDoc comments to clarify purpose and usage
  /**
   * Retrieves a Histogram with the specified name.
   * 
   * @param name the name of the histogram
   * @return the Histogram instance
   */
  Histogram getHistogram(String name);

  // Optimized by LLM: Added JavaDoc comments to clarify purpose and usage
  /**
   * Retrieves a Timer with the specified name.
   * 
   * @param name the name of the timer
   * @return the Timer instance
   */
  Timer getTimer(String name);
}