package org.apache.beam.sdk.io.gcp.pubsub;

import javax.annotation.Generated;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_PubsubIO_Write<T> extends PubsubIO.Write<T> {

  private final @Nullable ValueProvider<PubsubIO.PubsubTopic> topicProvider;

  private final @Nullable SerializableFunction<ValueInSingleWindow<T>, PubsubIO.PubsubTopic> topicFunction;

  private final boolean dynamicDestinations;

  private final PubsubClient.PubsubClientFactory pubsubClientFactory;

  private final @Nullable Integer maxBatchSize;

  private final @Nullable Integer maxBatchBytesSize;

  private final @Nullable String timestampAttribute;

  private final @Nullable String idAttribute;

  private final SerializableFunction<ValueInSingleWindow<T>, PubsubMessage> formatFn;

  private final @Nullable String pubsubRootUrl;

  private final BadRecordRouter badRecordRouter;

  private final ErrorHandler<BadRecord, ?> badRecordErrorHandler;

  private AutoValue_PubsubIO_Write(
      @Nullable ValueProvider<PubsubIO.PubsubTopic> topicProvider,
      @Nullable SerializableFunction<ValueInSingleWindow<T>, PubsubIO.PubsubTopic> topicFunction,
      boolean dynamicDestinations,
      PubsubClient.PubsubClientFactory pubsubClientFactory,
      @Nullable Integer maxBatchSize,
      @Nullable Integer maxBatchBytesSize,
      @Nullable String timestampAttribute,
      @Nullable String idAttribute,
      SerializableFunction<ValueInSingleWindow<T>, PubsubMessage> formatFn,
      @Nullable String pubsubRootUrl,
      BadRecordRouter badRecordRouter,
      ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
    this.topicProvider = topicProvider;
    this.topicFunction = topicFunction;
    this.dynamicDestinations = dynamicDestinations;
    this.pubsubClientFactory = pubsubClientFactory;
    this.maxBatchSize = maxBatchSize;
    this.maxBatchBytesSize = maxBatchBytesSize;
    this.timestampAttribute = timestampAttribute;
    this.idAttribute = idAttribute;
    this.formatFn = formatFn;
    this.pubsubRootUrl = pubsubRootUrl;
    this.badRecordRouter = badRecordRouter;
    this.badRecordErrorHandler = badRecordErrorHandler;
  }

  @Override
  @Nullable ValueProvider<PubsubIO.PubsubTopic> getTopicProvider() {
    return topicProvider;
  }

  @Override
  @Nullable SerializableFunction<ValueInSingleWindow<T>, PubsubIO.PubsubTopic> getTopicFunction() {
    return topicFunction;
  }

  @Override
  boolean getDynamicDestinations() {
    return dynamicDestinations;
  }

  @Override
  PubsubClient.PubsubClientFactory getPubsubClientFactory() {
    return pubsubClientFactory;
  }

  @Override
  @Nullable Integer getMaxBatchSize() {
    return maxBatchSize;
  }

  @Override
  @Nullable Integer getMaxBatchBytesSize() {
    return maxBatchBytesSize;
  }

  @Override
  @Nullable String getTimestampAttribute() {
    return timestampAttribute;
  }

  @Override
  @Nullable String getIdAttribute() {
    return idAttribute;
  }

  @Override
  SerializableFunction<ValueInSingleWindow<T>, PubsubMessage> getFormatFn() {
    return formatFn;
  }

  @Override
  @Nullable String getPubsubRootUrl() {
    return pubsubRootUrl;
  }

  @Override
  BadRecordRouter getBadRecordRouter() {
    return badRecordRouter;
  }

  @Override
  ErrorHandler<BadRecord, ?> getBadRecordErrorHandler() {
    return badRecordErrorHandler;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof PubsubIO.Write) {
      PubsubIO.Write<?> that = (PubsubIO.Write<?>) o;
      return (this.topicProvider == null ? that.getTopicProvider() == null : this.topicProvider.equals(that.getTopicProvider()))
          && (this.topicFunction == null ? that.getTopicFunction() == null : this.topicFunction.equals(that.getTopicFunction()))
          && this.dynamicDestinations == that.getDynamicDestinations()
          && this.pubsubClientFactory.equals(that.getPubsubClientFactory())
          && (this.maxBatchSize == null ? that.getMaxBatchSize() == null : this.maxBatchSize.equals(that.getMaxBatchSize()))
          && (this.maxBatchBytesSize == null ? that.getMaxBatchBytesSize() == null : this.maxBatchBytesSize.equals(that.getMaxBatchBytesSize()))
          && (this.timestampAttribute == null ? that.getTimestampAttribute() == null : this.timestampAttribute.equals(that.getTimestampAttribute()))
          && (this.idAttribute == null ? that.getIdAttribute() == null : this.idAttribute.equals(that.getIdAttribute()))
          && this.formatFn.equals(that.getFormatFn())
          && (this.pubsubRootUrl == null ? that.getPubsubRootUrl() == null : this.pubsubRootUrl.equals(that.getPubsubRootUrl()))
          && this.badRecordRouter.equals(that.getBadRecordRouter())
          && this.badRecordErrorHandler.equals(that.getBadRecordErrorHandler());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (topicProvider == null) ? 0 : topicProvider.hashCode();
    h$ *= 1000003;
    h$ ^= (topicFunction == null) ? 0 : topicFunction.hashCode();
    h$ *= 1000003;
    h$ ^= dynamicDestinations ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= pubsubClientFactory.hashCode();
    h$ *= 1000003;
    h$ ^= (maxBatchSize == null) ? 0 : maxBatchSize.hashCode();
    h$ *= 1000003;
    h$ ^= (maxBatchBytesSize == null) ? 0 : maxBatchBytesSize.hashCode();
    h$ *= 1000003;
    h$ ^= (timestampAttribute == null) ? 0 : timestampAttribute.hashCode();
    h$ *= 1000003;
    h$ ^= (idAttribute == null) ? 0 : idAttribute.hashCode();
    h$ *= 1000003;
    h$ ^= formatFn.hashCode();
    h$ *= 1000003;
    h$ ^= (pubsubRootUrl == null) ? 0 : pubsubRootUrl.hashCode();
    h$ *= 1000003;
    h$ ^= badRecordRouter.hashCode();
    h$ *= 1000003;
    h$ ^= badRecordErrorHandler.hashCode();
    return h$;
  }

  @Override
  PubsubIO.Write.Builder<T> toBuilder() {
    return new AutoValue_PubsubIO_Write.Builder<T>(this);
  }

  static final class Builder<T> extends PubsubIO.Write.Builder<T> {
    private @Nullable ValueProvider<PubsubIO.PubsubTopic> topicProvider;
    private @Nullable SerializableFunction<ValueInSingleWindow<T>, PubsubIO.PubsubTopic> topicFunction;
    private boolean dynamicDestinations;
    private PubsubClient.@Nullable PubsubClientFactory pubsubClientFactory;
    private @Nullable Integer maxBatchSize;
    private @Nullable Integer maxBatchBytesSize;
    private @Nullable String timestampAttribute;
    private @Nullable String idAttribute;
    private @Nullable SerializableFunction<ValueInSingleWindow<T>, PubsubMessage> formatFn;
    private @Nullable String pubsubRootUrl;
    private @Nullable BadRecordRouter badRecordRouter;
    private @Nullable ErrorHandler<BadRecord, ?> badRecordErrorHandler;
    private byte set$0;
    Builder() {
    }
    Builder(PubsubIO.Write<T> source) {
      this.topicProvider = source.getTopicProvider();
      this.topicFunction = source.getTopicFunction();
      this.dynamicDestinations = source.getDynamicDestinations();
      this.pubsubClientFactory = source.getPubsubClientFactory();
      this.maxBatchSize = source.getMaxBatchSize();
      this.maxBatchBytesSize = source.getMaxBatchBytesSize();
      this.timestampAttribute = source.getTimestampAttribute();
      this.idAttribute = source.getIdAttribute();
      this.formatFn = source.getFormatFn();
      this.pubsubRootUrl = source.getPubsubRootUrl();
      this.badRecordRouter = source.getBadRecordRouter();
      this.badRecordErrorHandler = source.getBadRecordErrorHandler();
      set$0 = (byte) 1;
    }
    @Override
    PubsubIO.Write.Builder<T> setTopicProvider(ValueProvider<PubsubIO.PubsubTopic> topicProvider) {
      this.topicProvider = topicProvider;
      return this;
    }
    @Override
    PubsubIO.Write.Builder<T> setTopicFunction(SerializableFunction<ValueInSingleWindow<T>, PubsubIO.PubsubTopic> topicFunction) {
      this.topicFunction = topicFunction;
      return this;
    }
    @Override
    PubsubIO.Write.Builder<T> setDynamicDestinations(boolean dynamicDestinations) {
      this.dynamicDestinations = dynamicDestinations;
      set$0 |= (byte) 1;
      return this;
    }
    @Override
    PubsubIO.Write.Builder<T> setPubsubClientFactory(PubsubClient.PubsubClientFactory pubsubClientFactory) {
      if (pubsubClientFactory == null) {
        throw new NullPointerException("Null pubsubClientFactory");
      }
      this.pubsubClientFactory = pubsubClientFactory;
      return this;
    }
    @Override
    PubsubIO.Write.Builder<T> setMaxBatchSize(Integer maxBatchSize) {
      this.maxBatchSize = maxBatchSize;
      return this;
    }
    @Override
    PubsubIO.Write.Builder<T> setMaxBatchBytesSize(Integer maxBatchBytesSize) {
      this.maxBatchBytesSize = maxBatchBytesSize;
      return this;
    }
    @Override
    PubsubIO.Write.Builder<T> setTimestampAttribute(String timestampAttribute) {
      this.timestampAttribute = timestampAttribute;
      return this;
    }
    @Override
    PubsubIO.Write.Builder<T> setIdAttribute(String idAttribute) {
      this.idAttribute = idAttribute;
      return this;
    }
    @Override
    PubsubIO.Write.Builder<T> setFormatFn(SerializableFunction<ValueInSingleWindow<T>, PubsubMessage> formatFn) {
      if (formatFn == null) {
        throw new NullPointerException("Null formatFn");
      }
      this.formatFn = formatFn;
      return this;
    }
    @Override
    PubsubIO.Write.Builder<T> setPubsubRootUrl(String pubsubRootUrl) {
      this.pubsubRootUrl = pubsubRootUrl;
      return this;
    }
    @Override
    PubsubIO.Write.Builder<T> setBadRecordRouter(BadRecordRouter badRecordRouter) {
      if (badRecordRouter == null) {
        throw new NullPointerException("Null badRecordRouter");
      }
      this.badRecordRouter = badRecordRouter;
      return this;
    }
    @Override
    PubsubIO.Write.Builder<T> setBadRecordErrorHandler(ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
      if (badRecordErrorHandler == null) {
        throw new NullPointerException("Null badRecordErrorHandler");
      }
      this.badRecordErrorHandler = badRecordErrorHandler;
      return this;
    }
    @Override
    PubsubIO.Write<T> build() {
      if (set$0 != 1
          || this.pubsubClientFactory == null
          || this.formatFn == null
          || this.badRecordRouter == null
          || this.badRecordErrorHandler == null) {
        StringBuilder missing = new StringBuilder();
        if ((set$0 & 1) == 0) {
          missing.append(" dynamicDestinations");
        }
        if (this.pubsubClientFactory == null) {
          missing.append(" pubsubClientFactory");
        }
        if (this.formatFn == null) {
          missing.append(" formatFn");
        }
        if (this.badRecordRouter == null) {
          missing.append(" badRecordRouter");
        }
        if (this.badRecordErrorHandler == null) {
          missing.append(" badRecordErrorHandler");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_PubsubIO_Write<T>(
          this.topicProvider,
          this.topicFunction,
          this.dynamicDestinations,
          this.pubsubClientFactory,
          this.maxBatchSize,
          this.maxBatchBytesSize,
          this.timestampAttribute,
          this.idAttribute,
          this.formatFn,
          this.pubsubRootUrl,
          this.badRecordRouter,
          this.badRecordErrorHandler);
    }
  }

}
