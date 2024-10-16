package org.apache.beam.sdk.io.gcp.pubsub;

import com.google.api.client.util.Clock;
import javax.annotation.Generated;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_PubsubIO_Read<T> extends PubsubIO.Read<T> {

  private final @Nullable ValueProvider<PubsubIO.PubsubTopic> topicProvider;

  private final @Nullable ValueProvider<PubsubIO.PubsubTopic> deadLetterTopicProvider;

  private final PubsubClient.PubsubClientFactory pubsubClientFactory;

  private final @Nullable ValueProvider<PubsubIO.PubsubSubscription> subscriptionProvider;

  private final @Nullable String timestampAttribute;

  private final @Nullable String idAttribute;

  private final Coder<T> coder;

  private final @Nullable SerializableFunction<PubsubMessage, T> parseFn;

  private final @Nullable Schema beamSchema;

  private final @Nullable TypeDescriptor<T> typeDescriptor;

  private final @Nullable SerializableFunction<T, Row> toRowFn;

  private final @Nullable SerializableFunction<Row, T> fromRowFn;

  private final @Nullable Clock clock;

  private final boolean needsAttributes;

  private final boolean needsMessageId;

  private final boolean needsOrderingKey;

  private final BadRecordRouter badRecordRouter;

  private final ErrorHandler<BadRecord, ?> badRecordErrorHandler;

  private AutoValue_PubsubIO_Read(
      @Nullable ValueProvider<PubsubIO.PubsubTopic> topicProvider,
      @Nullable ValueProvider<PubsubIO.PubsubTopic> deadLetterTopicProvider,
      PubsubClient.PubsubClientFactory pubsubClientFactory,
      @Nullable ValueProvider<PubsubIO.PubsubSubscription> subscriptionProvider,
      @Nullable String timestampAttribute,
      @Nullable String idAttribute,
      Coder<T> coder,
      @Nullable SerializableFunction<PubsubMessage, T> parseFn,
      @Nullable Schema beamSchema,
      @Nullable TypeDescriptor<T> typeDescriptor,
      @Nullable SerializableFunction<T, Row> toRowFn,
      @Nullable SerializableFunction<Row, T> fromRowFn,
      @Nullable Clock clock,
      boolean needsAttributes,
      boolean needsMessageId,
      boolean needsOrderingKey,
      BadRecordRouter badRecordRouter,
      ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
    this.topicProvider = topicProvider;
    this.deadLetterTopicProvider = deadLetterTopicProvider;
    this.pubsubClientFactory = pubsubClientFactory;
    this.subscriptionProvider = subscriptionProvider;
    this.timestampAttribute = timestampAttribute;
    this.idAttribute = idAttribute;
    this.coder = coder;
    this.parseFn = parseFn;
    this.beamSchema = beamSchema;
    this.typeDescriptor = typeDescriptor;
    this.toRowFn = toRowFn;
    this.fromRowFn = fromRowFn;
    this.clock = clock;
    this.needsAttributes = needsAttributes;
    this.needsMessageId = needsMessageId;
    this.needsOrderingKey = needsOrderingKey;
    this.badRecordRouter = badRecordRouter;
    this.badRecordErrorHandler = badRecordErrorHandler;
  }

  @Override
  @Nullable ValueProvider<PubsubIO.PubsubTopic> getTopicProvider() {
    return topicProvider;
  }

  @Override
  @Nullable ValueProvider<PubsubIO.PubsubTopic> getDeadLetterTopicProvider() {
    return deadLetterTopicProvider;
  }

  @Override
  PubsubClient.PubsubClientFactory getPubsubClientFactory() {
    return pubsubClientFactory;
  }

  @Override
  @Nullable ValueProvider<PubsubIO.PubsubSubscription> getSubscriptionProvider() {
    return subscriptionProvider;
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
  Coder<T> getCoder() {
    return coder;
  }

  @Override
  @Nullable SerializableFunction<PubsubMessage, T> getParseFn() {
    return parseFn;
  }

  @Override
  @Nullable Schema getBeamSchema() {
    return beamSchema;
  }

  @Override
  @Nullable TypeDescriptor<T> getTypeDescriptor() {
    return typeDescriptor;
  }

  @Override
  @Nullable SerializableFunction<T, Row> getToRowFn() {
    return toRowFn;
  }

  @Override
  @Nullable SerializableFunction<Row, T> getFromRowFn() {
    return fromRowFn;
  }

  @Override
  @Nullable Clock getClock() {
    return clock;
  }

  @Override
  boolean getNeedsAttributes() {
    return needsAttributes;
  }

  @Override
  boolean getNeedsMessageId() {
    return needsMessageId;
  }

  @Override
  boolean getNeedsOrderingKey() {
    return needsOrderingKey;
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
    if (o instanceof PubsubIO.Read) {
      PubsubIO.Read<?> that = (PubsubIO.Read<?>) o;
      return (this.topicProvider == null ? that.getTopicProvider() == null : this.topicProvider.equals(that.getTopicProvider()))
          && (this.deadLetterTopicProvider == null ? that.getDeadLetterTopicProvider() == null : this.deadLetterTopicProvider.equals(that.getDeadLetterTopicProvider()))
          && this.pubsubClientFactory.equals(that.getPubsubClientFactory())
          && (this.subscriptionProvider == null ? that.getSubscriptionProvider() == null : this.subscriptionProvider.equals(that.getSubscriptionProvider()))
          && (this.timestampAttribute == null ? that.getTimestampAttribute() == null : this.timestampAttribute.equals(that.getTimestampAttribute()))
          && (this.idAttribute == null ? that.getIdAttribute() == null : this.idAttribute.equals(that.getIdAttribute()))
          && this.coder.equals(that.getCoder())
          && (this.parseFn == null ? that.getParseFn() == null : this.parseFn.equals(that.getParseFn()))
          && (this.beamSchema == null ? that.getBeamSchema() == null : this.beamSchema.equals(that.getBeamSchema()))
          && (this.typeDescriptor == null ? that.getTypeDescriptor() == null : this.typeDescriptor.equals(that.getTypeDescriptor()))
          && (this.toRowFn == null ? that.getToRowFn() == null : this.toRowFn.equals(that.getToRowFn()))
          && (this.fromRowFn == null ? that.getFromRowFn() == null : this.fromRowFn.equals(that.getFromRowFn()))
          && (this.clock == null ? that.getClock() == null : this.clock.equals(that.getClock()))
          && this.needsAttributes == that.getNeedsAttributes()
          && this.needsMessageId == that.getNeedsMessageId()
          && this.needsOrderingKey == that.getNeedsOrderingKey()
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
    h$ ^= (deadLetterTopicProvider == null) ? 0 : deadLetterTopicProvider.hashCode();
    h$ *= 1000003;
    h$ ^= pubsubClientFactory.hashCode();
    h$ *= 1000003;
    h$ ^= (subscriptionProvider == null) ? 0 : subscriptionProvider.hashCode();
    h$ *= 1000003;
    h$ ^= (timestampAttribute == null) ? 0 : timestampAttribute.hashCode();
    h$ *= 1000003;
    h$ ^= (idAttribute == null) ? 0 : idAttribute.hashCode();
    h$ *= 1000003;
    h$ ^= coder.hashCode();
    h$ *= 1000003;
    h$ ^= (parseFn == null) ? 0 : parseFn.hashCode();
    h$ *= 1000003;
    h$ ^= (beamSchema == null) ? 0 : beamSchema.hashCode();
    h$ *= 1000003;
    h$ ^= (typeDescriptor == null) ? 0 : typeDescriptor.hashCode();
    h$ *= 1000003;
    h$ ^= (toRowFn == null) ? 0 : toRowFn.hashCode();
    h$ *= 1000003;
    h$ ^= (fromRowFn == null) ? 0 : fromRowFn.hashCode();
    h$ *= 1000003;
    h$ ^= (clock == null) ? 0 : clock.hashCode();
    h$ *= 1000003;
    h$ ^= needsAttributes ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= needsMessageId ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= needsOrderingKey ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= badRecordRouter.hashCode();
    h$ *= 1000003;
    h$ ^= badRecordErrorHandler.hashCode();
    return h$;
  }

  @Override
  PubsubIO.Read.Builder<T> toBuilder() {
    return new AutoValue_PubsubIO_Read.Builder<T>(this);
  }

  static final class Builder<T> extends PubsubIO.Read.Builder<T> {
    private @Nullable ValueProvider<PubsubIO.PubsubTopic> topicProvider;
    private @Nullable ValueProvider<PubsubIO.PubsubTopic> deadLetterTopicProvider;
    private PubsubClient.@Nullable PubsubClientFactory pubsubClientFactory;
    private @Nullable ValueProvider<PubsubIO.PubsubSubscription> subscriptionProvider;
    private @Nullable String timestampAttribute;
    private @Nullable String idAttribute;
    private @Nullable Coder<T> coder;
    private @Nullable SerializableFunction<PubsubMessage, T> parseFn;
    private @Nullable Schema beamSchema;
    private @Nullable TypeDescriptor<T> typeDescriptor;
    private @Nullable SerializableFunction<T, Row> toRowFn;
    private @Nullable SerializableFunction<Row, T> fromRowFn;
    private @Nullable Clock clock;
    private boolean needsAttributes;
    private boolean needsMessageId;
    private boolean needsOrderingKey;
    private @Nullable BadRecordRouter badRecordRouter;
    private @Nullable ErrorHandler<BadRecord, ?> badRecordErrorHandler;
    private byte set$0;
    Builder() {
    }
    Builder(PubsubIO.Read<T> source) {
      this.topicProvider = source.getTopicProvider();
      this.deadLetterTopicProvider = source.getDeadLetterTopicProvider();
      this.pubsubClientFactory = source.getPubsubClientFactory();
      this.subscriptionProvider = source.getSubscriptionProvider();
      this.timestampAttribute = source.getTimestampAttribute();
      this.idAttribute = source.getIdAttribute();
      this.coder = source.getCoder();
      this.parseFn = source.getParseFn();
      this.beamSchema = source.getBeamSchema();
      this.typeDescriptor = source.getTypeDescriptor();
      this.toRowFn = source.getToRowFn();
      this.fromRowFn = source.getFromRowFn();
      this.clock = source.getClock();
      this.needsAttributes = source.getNeedsAttributes();
      this.needsMessageId = source.getNeedsMessageId();
      this.needsOrderingKey = source.getNeedsOrderingKey();
      this.badRecordRouter = source.getBadRecordRouter();
      this.badRecordErrorHandler = source.getBadRecordErrorHandler();
      set$0 = (byte) 7;
    }
    @Override
    PubsubIO.Read.Builder<T> setTopicProvider(ValueProvider<PubsubIO.PubsubTopic> topicProvider) {
      this.topicProvider = topicProvider;
      return this;
    }
    @Override
    PubsubIO.Read.Builder<T> setDeadLetterTopicProvider(ValueProvider<PubsubIO.PubsubTopic> deadLetterTopicProvider) {
      this.deadLetterTopicProvider = deadLetterTopicProvider;
      return this;
    }
    @Override
    PubsubIO.Read.Builder<T> setPubsubClientFactory(PubsubClient.PubsubClientFactory pubsubClientFactory) {
      if (pubsubClientFactory == null) {
        throw new NullPointerException("Null pubsubClientFactory");
      }
      this.pubsubClientFactory = pubsubClientFactory;
      return this;
    }
    @Override
    PubsubIO.Read.Builder<T> setSubscriptionProvider(ValueProvider<PubsubIO.PubsubSubscription> subscriptionProvider) {
      this.subscriptionProvider = subscriptionProvider;
      return this;
    }
    @Override
    PubsubIO.Read.Builder<T> setTimestampAttribute(String timestampAttribute) {
      this.timestampAttribute = timestampAttribute;
      return this;
    }
    @Override
    PubsubIO.Read.Builder<T> setIdAttribute(String idAttribute) {
      this.idAttribute = idAttribute;
      return this;
    }
    @Override
    PubsubIO.Read.Builder<T> setCoder(Coder<T> coder) {
      if (coder == null) {
        throw new NullPointerException("Null coder");
      }
      this.coder = coder;
      return this;
    }
    @Override
    PubsubIO.Read.Builder<T> setParseFn(SerializableFunction<PubsubMessage, T> parseFn) {
      this.parseFn = parseFn;
      return this;
    }
    @Override
    PubsubIO.Read.Builder<T> setBeamSchema(@Nullable Schema beamSchema) {
      this.beamSchema = beamSchema;
      return this;
    }
    @Override
    PubsubIO.Read.Builder<T> setTypeDescriptor(@Nullable TypeDescriptor<T> typeDescriptor) {
      this.typeDescriptor = typeDescriptor;
      return this;
    }
    @Override
    PubsubIO.Read.Builder<T> setToRowFn(@Nullable SerializableFunction<T, Row> toRowFn) {
      this.toRowFn = toRowFn;
      return this;
    }
    @Override
    PubsubIO.Read.Builder<T> setFromRowFn(@Nullable SerializableFunction<Row, T> fromRowFn) {
      this.fromRowFn = fromRowFn;
      return this;
    }
    @Override
    PubsubIO.Read.Builder<T> setClock(Clock clock) {
      this.clock = clock;
      return this;
    }
    @Override
    PubsubIO.Read.Builder<T> setNeedsAttributes(boolean needsAttributes) {
      this.needsAttributes = needsAttributes;
      set$0 |= (byte) 1;
      return this;
    }
    @Override
    PubsubIO.Read.Builder<T> setNeedsMessageId(boolean needsMessageId) {
      this.needsMessageId = needsMessageId;
      set$0 |= (byte) 2;
      return this;
    }
    @Override
    PubsubIO.Read.Builder<T> setNeedsOrderingKey(boolean needsOrderingKey) {
      this.needsOrderingKey = needsOrderingKey;
      set$0 |= (byte) 4;
      return this;
    }
    @Override
    PubsubIO.Read.Builder<T> setBadRecordRouter(BadRecordRouter badRecordRouter) {
      if (badRecordRouter == null) {
        throw new NullPointerException("Null badRecordRouter");
      }
      this.badRecordRouter = badRecordRouter;
      return this;
    }
    @Override
    PubsubIO.Read.Builder<T> setBadRecordErrorHandler(ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
      if (badRecordErrorHandler == null) {
        throw new NullPointerException("Null badRecordErrorHandler");
      }
      this.badRecordErrorHandler = badRecordErrorHandler;
      return this;
    }
    @Override
    PubsubIO.Read<T> build() {
      if (set$0 != 7
          || this.pubsubClientFactory == null
          || this.coder == null
          || this.badRecordRouter == null
          || this.badRecordErrorHandler == null) {
        StringBuilder missing = new StringBuilder();
        if (this.pubsubClientFactory == null) {
          missing.append(" pubsubClientFactory");
        }
        if (this.coder == null) {
          missing.append(" coder");
        }
        if ((set$0 & 1) == 0) {
          missing.append(" needsAttributes");
        }
        if ((set$0 & 2) == 0) {
          missing.append(" needsMessageId");
        }
        if ((set$0 & 4) == 0) {
          missing.append(" needsOrderingKey");
        }
        if (this.badRecordRouter == null) {
          missing.append(" badRecordRouter");
        }
        if (this.badRecordErrorHandler == null) {
          missing.append(" badRecordErrorHandler");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_PubsubIO_Read<T>(
          this.topicProvider,
          this.deadLetterTopicProvider,
          this.pubsubClientFactory,
          this.subscriptionProvider,
          this.timestampAttribute,
          this.idAttribute,
          this.coder,
          this.parseFn,
          this.beamSchema,
          this.typeDescriptor,
          this.toRowFn,
          this.fromRowFn,
          this.clock,
          this.needsAttributes,
          this.needsMessageId,
          this.needsOrderingKey,
          this.badRecordRouter,
          this.badRecordErrorHandler);
    }
  }

}
