package org.apache.beam.runners.dataflow;

import java.util.List;
import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DataflowRunner_EnvironmentInfo extends DataflowRunner.EnvironmentInfo {

  private final String environmentId;

  private final String containerUrl;

  private final List<String> capabilities;

  AutoValue_DataflowRunner_EnvironmentInfo(
      String environmentId,
      String containerUrl,
      List<String> capabilities) {
    if (environmentId == null) {
      throw new NullPointerException("Null environmentId");
    }
    this.environmentId = environmentId;
    if (containerUrl == null) {
      throw new NullPointerException("Null containerUrl");
    }
    this.containerUrl = containerUrl;
    if (capabilities == null) {
      throw new NullPointerException("Null capabilities");
    }
    this.capabilities = capabilities;
  }

  @Override
  String environmentId() {
    return environmentId;
  }

  @Override
  String containerUrl() {
    return containerUrl;
  }

  @Override
  List<String> capabilities() {
    return capabilities;
  }

  @Override
  public String toString() {
    return "EnvironmentInfo{"
        + "environmentId=" + environmentId + ", "
        + "containerUrl=" + containerUrl + ", "
        + "capabilities=" + capabilities
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DataflowRunner.EnvironmentInfo) {
      DataflowRunner.EnvironmentInfo that = (DataflowRunner.EnvironmentInfo) o;
      return this.environmentId.equals(that.environmentId())
          && this.containerUrl.equals(that.containerUrl())
          && this.capabilities.equals(that.capabilities());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= environmentId.hashCode();
    h$ *= 1000003;
    h$ ^= containerUrl.hashCode();
    h$ *= 1000003;
    h$ ^= capabilities.hashCode();
    return h$;
  }

}
