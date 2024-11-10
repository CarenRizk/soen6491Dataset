package org.apache.beam.sdk.extensions.euphoria.core.client.accumulators;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;

// Optimized by LLM: Removed @Deprecated annotation as the interface is still in use and relevant
@Audience(Audience.Type.CLIENT)
public interface Timer extends Accumulator {

  // Optimized by LLM: Added JavaDoc comment to clarify usage
  /**
   * Adds the specified duration to the timer.
   *
   * @param duration the duration to add
   */
  void add(Duration duration);

  // Optimized by LLM: Evaluated necessity of add(long duration, TimeUnit unit) method
  // This method is retained for backward compatibility
  default void add(long duration, TimeUnit unit) {
    add(Duration.ofMillis(unit.toMillis(duration)));
  }
}