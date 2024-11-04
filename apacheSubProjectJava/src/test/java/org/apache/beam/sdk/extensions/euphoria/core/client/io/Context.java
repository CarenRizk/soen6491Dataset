package org.apache.beam.sdk.extensions.euphoria.core.client.io;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;


@Audience(Audience.Type.CLIENT)
@Deprecated
public interface Context extends Environment {}
