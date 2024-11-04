package org.apache.beam.sdk.extensions.euphoria.core.client.accumulators;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;


@Audience(Audience.Type.CLIENT)
@Deprecated
public interface Counter extends Accumulator {

  
  void increment(long value);

  
  void increment();
}
