package org.apache.beam.sdk.extensions.euphoria.core.client.accumulators;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;


@Audience(Audience.Type.CLIENT)
@Deprecated
public interface Histogram extends Accumulator {

  
  void add(long value);

  
  void add(long value, long times);
}
