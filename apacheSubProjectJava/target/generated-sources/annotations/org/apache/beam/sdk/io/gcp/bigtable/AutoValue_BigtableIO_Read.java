package org.apache.beam.sdk.io.gcp.bigtable;

import javax.annotation.Generated;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_BigtableIO_Read extends BigtableIO.Read {

  private final BigtableConfig bigtableConfig;

  private final BigtableReadOptions bigtableReadOptions;

  private final BigtableServiceFactory serviceFactory;

  private AutoValue_BigtableIO_Read(
      BigtableConfig bigtableConfig,
      BigtableReadOptions bigtableReadOptions,
      BigtableServiceFactory serviceFactory) {
    this.bigtableConfig = bigtableConfig;
    this.bigtableReadOptions = bigtableReadOptions;
    this.serviceFactory = serviceFactory;
  }

  @Override
  BigtableConfig getBigtableConfig() {
    return bigtableConfig;
  }

  @Override
  BigtableReadOptions getBigtableReadOptions() {
    return bigtableReadOptions;
  }

  @VisibleForTesting
  @Override
  BigtableServiceFactory getServiceFactory() {
    return serviceFactory;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof BigtableIO.Read) {
      BigtableIO.Read that = (BigtableIO.Read) o;
      return this.bigtableConfig.equals(that.getBigtableConfig())
          && this.bigtableReadOptions.equals(that.getBigtableReadOptions())
          && this.serviceFactory.equals(that.getServiceFactory());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= bigtableConfig.hashCode();
    h$ *= 1000003;
    h$ ^= bigtableReadOptions.hashCode();
    h$ *= 1000003;
    h$ ^= serviceFactory.hashCode();
    return h$;
  }

  @Override
  BigtableIO.Read.Builder toBuilder() {
    return new AutoValue_BigtableIO_Read.Builder(this);
  }

  static final class Builder extends BigtableIO.Read.Builder {
    private @Nullable BigtableConfig bigtableConfig;
    private @Nullable BigtableReadOptions bigtableReadOptions;
    private @Nullable BigtableServiceFactory serviceFactory;
    Builder() {
    }
    Builder(BigtableIO.Read source) {
      this.bigtableConfig = source.getBigtableConfig();
      this.bigtableReadOptions = source.getBigtableReadOptions();
      this.serviceFactory = source.getServiceFactory();
    }
    @Override
    BigtableIO.Read.Builder setBigtableConfig(BigtableConfig bigtableConfig) {
      if (bigtableConfig == null) {
        throw new NullPointerException("Null bigtableConfig");
      }
      this.bigtableConfig = bigtableConfig;
      return this;
    }
    @Override
    BigtableIO.Read.Builder setBigtableReadOptions(BigtableReadOptions bigtableReadOptions) {
      if (bigtableReadOptions == null) {
        throw new NullPointerException("Null bigtableReadOptions");
      }
      this.bigtableReadOptions = bigtableReadOptions;
      return this;
    }
    @Override
    BigtableIO.Read.Builder setServiceFactory(BigtableServiceFactory serviceFactory) {
      if (serviceFactory == null) {
        throw new NullPointerException("Null serviceFactory");
      }
      this.serviceFactory = serviceFactory;
      return this;
    }
    @Override
    BigtableIO.Read build() {
      if (this.bigtableConfig == null
          || this.bigtableReadOptions == null
          || this.serviceFactory == null) {
        StringBuilder missing = new StringBuilder();
        if (this.bigtableConfig == null) {
          missing.append(" bigtableConfig");
        }
        if (this.bigtableReadOptions == null) {
          missing.append(" bigtableReadOptions");
        }
        if (this.serviceFactory == null) {
          missing.append(" serviceFactory");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_BigtableIO_Read(
          this.bigtableConfig,
          this.bigtableReadOptions,
          this.serviceFactory);
    }
  }

}
