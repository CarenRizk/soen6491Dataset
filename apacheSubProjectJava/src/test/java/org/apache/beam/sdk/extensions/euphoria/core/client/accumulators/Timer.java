package org.apache.beam.sdk.extensions.euphoria.core.client.accumulators;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;


@Audience(Audience.Type.CLIENT)
@Deprecated
public interface Timer extends Accumulator {

  
  void add(Duration duration);

  
  default void add(long duration, TimeUnit unit) {
    add(Duration.ofMillis(unit.toMillis(duration)));
  }
}
