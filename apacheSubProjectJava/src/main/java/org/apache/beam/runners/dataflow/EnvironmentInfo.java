package org.apache.beam.runners.dataflow;

import java.util.List;

import com.google.auto.value.AutoValue;

@AutoValue
  abstract class EnvironmentInfo {
    static AutoValue_DataflowRunner_EnvironmentInfo create(
        String environmentId, String containerUrl, List<String> capabilities) {
      return new AutoValue_DataflowRunner_EnvironmentInfo(
          environmentId, containerUrl, capabilities);
    }

    abstract String environmentId();

    abstract String containerUrl();

    abstract List<String> capabilities();
  }