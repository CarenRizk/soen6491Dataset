package org.apache.beam.sdk.io.gcp.bigtable;

import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_BigtableIO_Write extends BigtableIO.Write {

  private final BigtableConfig bigtableConfig;

  private final BigtableWriteOptions bigtableWriteOptions;

  private final BigtableServiceFactory serviceFactory;

  private final ErrorHandler<BadRecord, ?> badRecordErrorHandler;

  private final BadRecordRouter badRecordRouter;

  private AutoValue_BigtableIO_Write(
      BigtableConfig bigtableConfig,
      BigtableWriteOptions bigtableWriteOptions,
      BigtableServiceFactory serviceFactory,
      ErrorHandler<BadRecord, ?> badRecordErrorHandler,
      BadRecordRouter badRecordRouter) {
    this.bigtableConfig = bigtableConfig;
    this.bigtableWriteOptions = bigtableWriteOptions;
    this.serviceFactory = serviceFactory;
    this.badRecordErrorHandler = badRecordErrorHandler;
    this.badRecordRouter = badRecordRouter;
  }

  @Override
  BigtableConfig getBigtableConfig() {
    return bigtableConfig;
  }

  @Override
  BigtableWriteOptions getBigtableWriteOptions() {
    return bigtableWriteOptions;
  }

  @VisibleForTesting
  @Override
  BigtableServiceFactory getServiceFactory() {
    return serviceFactory;
  }

  @Override
  ErrorHandler<BadRecord, ?> getBadRecordErrorHandler() {
    return badRecordErrorHandler;
  }

  @Override
  BadRecordRouter getBadRecordRouter() {
    return badRecordRouter;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof BigtableIO.Write) {
      BigtableIO.Write that = (BigtableIO.Write) o;
      return this.bigtableConfig.equals(that.getBigtableConfig())
          && this.bigtableWriteOptions.equals(that.getBigtableWriteOptions())
          && this.serviceFactory.equals(that.getServiceFactory())
          && this.badRecordErrorHandler.equals(that.getBadRecordErrorHandler())
          && this.badRecordRouter.equals(that.getBadRecordRouter());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= bigtableConfig.hashCode();
    h$ *= 1000003;
    h$ ^= bigtableWriteOptions.hashCode();
    h$ *= 1000003;
    h$ ^= serviceFactory.hashCode();
    h$ *= 1000003;
    h$ ^= badRecordErrorHandler.hashCode();
    h$ *= 1000003;
    h$ ^= badRecordRouter.hashCode();
    return h$;
  }

  @Override
  BigtableIO.Write.Builder toBuilder() {
    return new AutoValue_BigtableIO_Write.Builder(this);
  }

  static final class Builder extends BigtableIO.Write.Builder {
    private @Nullable BigtableConfig bigtableConfig;
    private @Nullable BigtableWriteOptions bigtableWriteOptions;
    private @Nullable BigtableServiceFactory serviceFactory;
    private @Nullable ErrorHandler<BadRecord, ?> badRecordErrorHandler;
    private @Nullable BadRecordRouter badRecordRouter;
    Builder() {
    }
    Builder(BigtableIO.Write source) {
      this.bigtableConfig = source.getBigtableConfig();
      this.bigtableWriteOptions = source.getBigtableWriteOptions();
      this.serviceFactory = source.getServiceFactory();
      this.badRecordErrorHandler = source.getBadRecordErrorHandler();
      this.badRecordRouter = source.getBadRecordRouter();
    }
    @Override
    BigtableIO.Write.Builder setBigtableConfig(BigtableConfig bigtableConfig) {
      if (bigtableConfig == null) {
        throw new NullPointerException("Null bigtableConfig");
      }
      this.bigtableConfig = bigtableConfig;
      return this;
    }
    @Override
    BigtableIO.Write.Builder setBigtableWriteOptions(BigtableWriteOptions bigtableWriteOptions) {
      if (bigtableWriteOptions == null) {
        throw new NullPointerException("Null bigtableWriteOptions");
      }
      this.bigtableWriteOptions = bigtableWriteOptions;
      return this;
    }
    @Override
    BigtableIO.Write.Builder setServiceFactory(BigtableServiceFactory serviceFactory) {
      if (serviceFactory == null) {
        throw new NullPointerException("Null serviceFactory");
      }
      this.serviceFactory = serviceFactory;
      return this;
    }
    @Override
    BigtableIO.Write.Builder setBadRecordErrorHandler(ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
      if (badRecordErrorHandler == null) {
        throw new NullPointerException("Null badRecordErrorHandler");
      }
      this.badRecordErrorHandler = badRecordErrorHandler;
      return this;
    }
    @Override
    BigtableIO.Write.Builder setBadRecordRouter(BadRecordRouter badRecordRouter) {
      if (badRecordRouter == null) {
        throw new NullPointerException("Null badRecordRouter");
      }
      this.badRecordRouter = badRecordRouter;
      return this;
    }
    @Override
    BigtableIO.Write build() {
      if (this.bigtableConfig == null
          || this.bigtableWriteOptions == null
          || this.serviceFactory == null
          || this.badRecordErrorHandler == null
          || this.badRecordRouter == null) {
        StringBuilder missing = new StringBuilder();
        if (this.bigtableConfig == null) {
          missing.append(" bigtableConfig");
        }
        if (this.bigtableWriteOptions == null) {
          missing.append(" bigtableWriteOptions");
        }
        if (this.serviceFactory == null) {
          missing.append(" serviceFactory");
        }
        if (this.badRecordErrorHandler == null) {
          missing.append(" badRecordErrorHandler");
        }
        if (this.badRecordRouter == null) {
          missing.append(" badRecordRouter");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_BigtableIO_Write(
          this.bigtableConfig,
          this.bigtableWriteOptions,
          this.serviceFactory,
          this.badRecordErrorHandler,
          this.badRecordRouter);
    }
  }

}
