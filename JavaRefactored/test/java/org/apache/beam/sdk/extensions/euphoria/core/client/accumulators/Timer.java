package org.apache.beam.sdk.extensions.euphoria.core.client.accumulators;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;

// Optimized by LLM: Removed deprecated annotation as the interface is intended for use
@Audience(Audience.Type.CLIENT)
public interface Timer extends Accumulator {

  // Optimized by LLM: Added JavaDoc comments to clarify purpose and usage
  /**
   * Adds a duration to the timer.
   *
   * @param duration the duration to add
   */
  void add(Duration duration);

  // Optimized by LLM: Provided a default implementation for the add(long duration, TimeUnit unit) method
  default void add(long duration, TimeUnit unit) {
    add(Duration.ofMillis(unit.toMillis(duration)));
  }
}