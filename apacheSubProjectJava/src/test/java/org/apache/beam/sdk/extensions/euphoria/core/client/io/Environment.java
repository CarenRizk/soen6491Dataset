package org.apache.beam.sdk.extensions.euphoria.core.client.io;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Counter;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Histogram;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Timer;


@Audience(Audience.Type.CLIENT)
@Deprecated
public interface Environment {

  Counter getCounter(String name);

  
  Histogram getHistogram(String name);

  
  Timer getTimer(String name);
}
