package org.apache.beam.sdk.io.gcp.pubsub;

import static org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter.BAD_RECORD_TAG;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.client.util.Clock;
import com.google.auto.value.AutoValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.naming.SizeLimitExceededException;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoDomain;
import org.apache.beam.sdk.extensions.protobuf.ProtoDynamicMessageSchema;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter.ThrowingBadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler.DefaultErrorHandler;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.EncodableThrowable;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({
  "nullness" 
})
public class PubsubIO {

  private static final Logger LOG = LoggerFactory.getLogger(PubsubIO.class);

  private static final PubsubClient.PubsubClientFactory FACTORY = PubsubJsonClient.FACTORY;

  private static final Pattern PROJECT_ID_REGEXP =
      Pattern.compile("[a-z][-a-z0-9:.]{4,61}[a-z0-9]");

  private static final Pattern SUBSCRIPTION_REGEXP =
      Pattern.compile("projects/([^/]+)/subscriptions/(.+)");

  private static final Pattern TOPIC_REGEXP = Pattern.compile("projects/([^/]+)/topics/(.+)");

  private static final Pattern V1BETA1_SUBSCRIPTION_REGEXP =
      Pattern.compile("/subscriptions/([^/]+)/(.+)");

  private static final Pattern V1BETA1_TOPIC_REGEXP = Pattern.compile("/topics/([^/]+)/(.+)");

  private static final Pattern PUBSUB_NAME_REGEXP = Pattern.compile("[a-zA-Z][-._~%+a-zA-Z0-9]+");

  static final int PUBSUB_MESSAGE_MAX_TOTAL_SIZE = 10_000_000;

  private static final int PUBSUB_NAME_MIN_LENGTH = 3;
  private static final int PUBSUB_NAME_MAX_LENGTH = 255;

  private static final String SUBSCRIPTION_RANDOM_TEST_PREFIX = "_random/";
  private static final String SUBSCRIPTION_STARTING_SIGNAL = "_starting_signal/";
  private static final String TOPIC_DEV_NULL_TEST_NAME = "/topics/dev/null";

  public static final String ENABLE_CUSTOM_PUBSUB_SINK = "enable_custom_pubsub_sink";
  public static final String ENABLE_CUSTOM_PUBSUB_SOURCE = "enable_custom_pubsub_source";

  private static void validateProjectName(String project) {
    Matcher match = PROJECT_ID_REGEXP.matcher(project);
    if (!match.matches()) {
      throw new IllegalArgumentException(
          "Illegal project name specified in Pubsub subscription: " + project);
    }
  }

  // Optimized by LLM: Extracted common validation logic into a utility method
  private static void validatePubsubName(String name) {
    if (name.length() < PUBSUB_NAME_MIN_LENGTH || name.length() > PUBSUB_NAME_MAX_LENGTH || name.startsWith("goog") || !PUBSUB_NAME_REGEXP.matcher(name).matches()) {
      throw new IllegalArgumentException("Illegal Pubsub object name specified: " + name);
    }
  }

  private static void populateCommonDisplayData(
      DisplayData.Builder builder,
      String timestampAttribute,
      String idAttribute,
      ValueProvider<PubsubTopic> topic) {
    builder
        .addIfNotNull(
            DisplayData.item("timestampAttribute", timestampAttribute)
                .withLabel("Timestamp Attribute"))
        .addIfNotNull(DisplayData.item("idAttribute", idAttribute).withLabel("ID Attribute"))
        .addIfNotNull(DisplayData.item("topic", topic).withLabel("Pubsub Topic"));
  }

  // Optimized by LLM: Consolidated path generation logic
  private static String generatePath(String project, String name, boolean isSubscription) {
    return isSubscription ? "projects/" + project + "/subscriptions/" + name : "projects/" + project + "/topics/" + name;
  }

  public static class PubsubSubscription implements Serializable {

    private enum Type {
      NORMAL,
      FAKE
    }

    private final PubsubSubscription.Type type;
    private final String project;
    private final String subscription;

    private PubsubSubscription(PubsubSubscription.Type type, String project, String subscription) {
      this.type = type;
      this.project = project;
      this.subscription = subscription;
    }

    public static PubsubSubscription fromPath(String path) {
      if (path.startsWith(SUBSCRIPTION_RANDOM_TEST_PREFIX)
          || path.startsWith(SUBSCRIPTION_STARTING_SIGNAL)) {
        return new PubsubSubscription(PubsubSubscription.Type.FAKE, "", path);
      }

      String projectName, subscriptionName;

      Matcher v1beta1Match = V1BETA1_SUBSCRIPTION_REGEXP.matcher(path);
      if (v1beta1Match.matches()) {
        LOG.warn(
            "Saw subscription in v1beta1 format. Subscriptions should be in the format "
                + "projects/<project_id>/subscriptions/<subscription_name>");
        projectName = v1beta1Match.group(1);
        subscriptionName = v1beta1Match.group(2);
      } else {
        Matcher match = SUBSCRIPTION_REGEXP.matcher(path);
        if (!match.matches()) {
          throw new IllegalArgumentException(
              "Pubsub subscription is not in "
                  + "projects/<project_id>/subscriptions/<subscription_name> format: "
                  + path);
        }
        projectName = match.group(1);
        subscriptionName = match.group(2);
      }

      validateProjectName(projectName);
      validatePubsubName(subscriptionName);
      return new PubsubSubscription(PubsubSubscription.Type.NORMAL, projectName, subscriptionName);
    }

    @Deprecated
    public String asV1Beta1Path() {
      return type == PubsubSubscription.Type.NORMAL ? "/subscriptions/" + project + "/" + subscription : subscription;
    }

    @Deprecated
    public String asV1Beta2Path() {
      return type == PubsubSubscription.Type.NORMAL ? "projects/" + project + "/subscriptions/" + subscription : subscription;
    }

    public String asPath() {
      return generatePath(project, subscription, true); // Optimized by LLM: Used consolidated path generation
    }

    @Override
    public String toString() {
      return asPath();
    }
  }

  private static class SubscriptionPathTranslator
      implements SerializableFunction<PubsubSubscription, SubscriptionPath> {

    @Override
    public SubscriptionPath apply(PubsubSubscription from) {
      return PubsubClient.subscriptionPathFromName(from.project, from.subscription);
    }
  }

  private static class TopicPathTranslator implements SerializableFunction<PubsubTopic, TopicPath> {

    @Override
    public TopicPath apply(PubsubTopic from) {
      return PubsubClient.topicPathFromName(from.project, from.topic);
    }
  }

  public static class PubsubTopic implements Serializable {
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PubsubTopic)) {
        return false;
      }
      PubsubTopic that = (PubsubTopic) o;
      return type == that.type && project.equals(that.project) && topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, project, topic);
    }

    private enum Type {
      NORMAL,
      FAKE
    }

    private final PubsubTopic.Type type;
    private final String project;
    private final String topic;

    private PubsubTopic(PubsubTopic.Type type, String project, String topic) {
      this.type = type;
      this.project = project;
      this.topic = topic;
    }

    public static PubsubTopic fromPath(String path) {
      if (path.equals(TOPIC_DEV_NULL_TEST_NAME)) {
        return new PubsubTopic(PubsubTopic.Type.FAKE, "", path);
      }

      String projectName, topicName;

      Matcher v1beta1Match = V1BETA1_TOPIC_REGEXP.matcher(path);
      if (v1beta1Match.matches()) {
        LOG.warn(
            "Saw topic in v1beta1 format.  Topics should be in the format "
                + "projects/<project_id>/topics/<topic_name>");
        projectName = v1beta1Match.group(1);
        topicName = v1beta1Match.group(2);
      } else {
        Matcher match = TOPIC_REGEXP.matcher(path);
        if (!match.matches()) {
          throw new IllegalArgumentException(
              "Pubsub topic is not in projects/<project_id>/topics/<topic_name> format: " + path);
        }
        projectName = match.group(1);
        topicName = match.group(2);
      }

      validateProjectName(projectName);
      validatePubsubName(topicName);
      return new PubsubTopic(PubsubTopic.Type.NORMAL, projectName, topicName);
    }

    @Deprecated
    public String asV1Beta1Path() {
      return type == PubsubTopic.Type.NORMAL ? "/topics/" + project + "/" + topic : topic;
    }

    @Deprecated
    public String asV1Beta2Path() {
      return type == PubsubTopic.Type.NORMAL ? "projects/" + project + "/topics/" + topic : topic;
    }

    public String asPath() {
      return generatePath(project, topic, false); // Optimized by LLM: Used consolidated path generation
    }

    public List<String> dataCatalogSegments() {
      return ImmutableList.of(project, topic);
    }

    @Override
    public String toString() {
      return asPath();
    }
  }

  public static Read<PubsubMessage> readMessages() {
    return Read.newBuilder().setCoder(PubsubMessagePayloadOnlyCoder.of()).build();
  }

  public static Read<PubsubMessage> readMessagesWithMessageId() {
    return Read.newBuilder()
        .setCoder(PubsubMessageWithMessageIdCoder.of())
        .setNeedsMessageId(true)
        .build();
  }

  public static Read<PubsubMessage> readMessagesWithAttributes() {
    return Read.newBuilder()
        .setCoder(PubsubMessageWithAttributesCoder.of())
        .setNeedsAttributes(true)
        .build();
  }

  public static Read<PubsubMessage> readMessagesWithAttributesAndMessageId() {
    return Read.newBuilder()
        .setCoder(PubsubMessageWithAttributesAndMessageIdCoder.of())
        .setNeedsAttributes(true)
        .setNeedsMessageId(true)
        .build();
  }

  public static Read<PubsubMessage> readMessagesWithAttributesAndMessageIdAndOrderingKey() {
    return Read.newBuilder()
        .setCoder(PubsubMessageWithAttributesAndMessageIdAndOrderingKeyCoder.of())
        .setNeedsAttributes(true)
        .setNeedsMessageId(true)
        .setNeedsOrderingKey(true)
        .build();
  }

  public static Read<String> readStrings() {
    return Read.newBuilder(
            (PubsubMessage message) -> new String(message.getPayload(), StandardCharsets.UTF_8))
        .setCoder(StringUtf8Coder.of())
        .build();
  }

  public static <T extends Message> Read<T> readProtos(Class<T> messageClass) {
    ProtoCoder<T> coder = ProtoCoder.of(messageClass);
    return Read.newBuilder(parsePayloadUsingCoder(coder)).setCoder(coder).build();
  }

  public static Read<DynamicMessage> readProtoDynamicMessages(
      ProtoDomain domain, String fullMessageName) {
    SerializableFunction<PubsubMessage, DynamicMessage> parser =
        message -> {
          try {
            return DynamicMessage.parseFrom(
                domain.getDescriptor(fullMessageName), message.getPayload());
          } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Could not parse Pub/Sub message", e);
          }
        };

    ProtoDynamicMessageSchema<DynamicMessage> schema =
        ProtoDynamicMessageSchema.forDescriptor(domain, domain.getDescriptor(fullMessageName));
    return Read.newBuilder(parser)
        .setCoder(
            SchemaCoder.of(
                schema.getSchema(),
                TypeDescriptor.of(DynamicMessage.class),
                schema.getToRowFunction(),
                schema.getFromRowFunction()))
        .build();
  }

  public static Read<DynamicMessage> readProtoDynamicMessages(Descriptor descriptor) {
    return readProtoDynamicMessages(ProtoDomain.buildFrom(descriptor), descriptor.getFullName());
  }

  public static <T> Read<T> readAvros(Class<T> clazz) {
    AvroCoder<T> coder = AvroCoder.of(clazz);
    return Read.newBuilder(parsePayloadUsingCoder(coder)).setCoder(coder).build();
  }

  public static <T> Read<T> readMessagesWithCoderAndParseFn(
      Coder<T> coder, SimpleFunction<PubsubMessage, T> parseFn) {
    return Read.newBuilder(parseFn).setCoder(coder).build();
  }

  public static <T> Read<T> readMessagesWithAttributesWithCoderAndParseFn(
      Coder<T> coder, SimpleFunction<PubsubMessage, T> parseFn) {
    return Read.newBuilder(parseFn).setCoder(coder).setNeedsAttributes(true).build();
  }

  public static Read<GenericRecord> readAvroGenericRecords(org.apache.avro.Schema avroSchema) {
    AvroCoder<GenericRecord> coder = AvroCoder.of(avroSchema);
    Schema schema = AvroUtils.getSchema(GenericRecord.class, avroSchema);
    return Read.newBuilder(parsePayloadUsingCoder(coder))
        .setCoder(
            SchemaCoder.of(
                schema,
                TypeDescriptor.of(GenericRecord.class),
                AvroUtils.getToRowFunction(GenericRecord.class, avroSchema),
                AvroUtils.getFromRowFunction(GenericRecord.class)))
        .build();
  }

  public static <T> Read<T> readAvrosWithBeamSchema(Class<T> clazz) {
    if (clazz.equals(GenericRecord.class)) {
      throw new IllegalArgumentException("For GenericRecord, please call readAvroGenericRecords");
    }
    AvroCoder<T> coder = AvroCoder.of(clazz);
    org.apache.avro.Schema avroSchema = coder.getSchema();
    Schema schema = AvroUtils.getSchema(clazz, avroSchema);
    return Read.newBuilder(parsePayloadUsingCoder(coder))
        .setCoder(
            SchemaCoder.of(
                schema,
                TypeDescriptor.of(clazz),
                AvroUtils.getToRowFunction(clazz, avroSchema),
                AvroUtils.getFromRowFunction(clazz)))
        .build();
  }

  public static Write<PubsubMessage> writeMessages() {
    return Write.newBuilder()
        .setTopicProvider(null)
        .setTopicFunction(null)
        .setDynamicDestinations(false)
        .build();
  }

  public static Write<PubsubMessage> writeMessagesDynamic() {
    return Write.newBuilder()
        .setTopicProvider(null)
        .setTopicFunction(null)
        .setDynamicDestinations(true)
        .build();
  }

  public static Write<String> writeStrings() {
    return Write.newBuilder(
            (ValueInSingleWindow<String> stringAndWindow) ->
                new PubsubMessage(
                    stringAndWindow.getValue().getBytes(StandardCharsets.UTF_8), ImmutableMap.of()))
        .setDynamicDestinations(false)
        .build();
  }

  public static <T extends Message> Write<T> writeProtos(Class<T> messageClass) {
    return Write.newBuilder(formatPayloadUsingCoder(ProtoCoder.of(messageClass)))
        .setDynamicDestinations(false)
        .build();
  }

  public static <T extends Message> Write<T> writeProtos(
      Class<T> messageClass,
      SerializableFunction<ValueInSingleWindow<T>, Map<String, String>> attributeFn) {
    return Write.newBuilder(formatPayloadUsingCoder(ProtoCoder.of(messageClass), attributeFn))
        .setDynamicDestinations(false)
        .build();
  }

  public static <T> Write<T> writeAvros(Class<T> clazz) {
    return Write.newBuilder(formatPayloadUsingCoder(AvroCoder.of(clazz)))
        .setDynamicDestinations(false)
        .build();
  }

  public static <T> Write<T> writeAvros(
      Class<T> clazz,
      SerializableFunction<ValueInSingleWindow<T>, Map<String, String>> attributeFn) {
    return Write.newBuilder(formatPayloadUsingCoder(AvroCoder.of(clazz), attributeFn))
        .setDynamicDestinations(false)
        .build();
  }

  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    abstract @Nullable ValueProvider<PubsubTopic> getTopicProvider();

    abstract @Nullable ValueProvider<PubsubTopic> getDeadLetterTopicProvider();

    abstract PubsubClient.PubsubClientFactory getPubsubClientFactory();

    abstract @Nullable ValueProvider<PubsubSubscription> getSubscriptionProvider();

    abstract @Nullable String getTimestampAttribute();

    abstract @Nullable String getIdAttribute();

    abstract Coder<T> getCoder();

    abstract @Nullable SerializableFunction<PubsubMessage, T> getParseFn();

    abstract @Nullable Schema getBeamSchema();

    abstract @Nullable TypeDescriptor<T> getTypeDescriptor();

    abstract @Nullable SerializableFunction<T, Row> getToRowFn();

    abstract @Nullable SerializableFunction<Row, T> getFromRowFn();

    abstract @Nullable Clock getClock();

    abstract boolean getNeedsAttributes();

    abstract boolean getNeedsMessageId();

    abstract boolean getNeedsOrderingKey();

    abstract BadRecordRouter getBadRecordRouter();

    abstract ErrorHandler<BadRecord, ?> getBadRecordErrorHandler();

    abstract Builder<T> toBuilder();

    static <T> Builder<T> newBuilder(SerializableFunction<PubsubMessage, T> parseFn) {
      Builder<T> builder = new AutoValue_PubsubIO_Read.Builder<T>();
      builder.setParseFn(parseFn);
      builder.setPubsubClientFactory(FACTORY);
      builder.setNeedsAttributes(false);
      builder.setNeedsMessageId(false);
      builder.setNeedsOrderingKey(false);
      builder.setBadRecordRouter(BadRecordRouter.THROWING_ROUTER);
      builder.setBadRecordErrorHandler(new DefaultErrorHandler<>());
      return builder;
    }

    static Builder<PubsubMessage> newBuilder() {
      return newBuilder(x -> x);
    }

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setTopicProvider(ValueProvider<PubsubTopic> topic);

      abstract Builder<T> setDeadLetterTopicProvider(ValueProvider<PubsubTopic> deadLetterTopic);

      abstract Builder<T> setPubsubClientFactory(PubsubClient.PubsubClientFactory clientFactory);

      abstract Builder<T> setSubscriptionProvider(ValueProvider<PubsubSubscription> subscription);

      abstract Builder<T> setTimestampAttribute(String timestampAttribute);

      abstract Builder<T> setIdAttribute(String idAttribute);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setParseFn(SerializableFunction<PubsubMessage, T> parseFn);

      abstract Builder<T> setBeamSchema(@Nullable Schema beamSchema);

      abstract Builder<T> setTypeDescriptor(@Nullable TypeDescriptor<T> typeDescriptor);

      abstract Builder<T> setToRowFn(@Nullable SerializableFunction<T, Row> toRowFn);

      abstract Builder<T> setFromRowFn(@Nullable SerializableFunction<Row, T> fromRowFn);

      abstract Builder<T> setNeedsAttributes(boolean needsAttributes);

      abstract Builder<T> setNeedsMessageId(boolean needsMessageId);

      abstract Builder<T> setNeedsOrderingKey(boolean needsOrderingKey);

      abstract Builder<T> setClock(Clock clock);

      abstract Builder<T> setBadRecordRouter(BadRecordRouter badRecordRouter);

      abstract Builder<T> setBadRecordErrorHandler(
          ErrorHandler<BadRecord, ?> badRecordErrorHandler);

      abstract Read<T> build();
    }

    public Read<T> fromSubscription(String subscription) {
      return fromSubscription(StaticValueProvider.of(subscription));
    }

    public Read<T> fromSubscription(ValueProvider<String> subscription) {
      if (subscription.isAccessible()) {
        PubsubSubscription.fromPath(subscription.get());
      }
      return toBuilder()
          .setSubscriptionProvider(
              NestedValueProvider.of(subscription, PubsubSubscription::fromPath))
          .build();
    }

    public Read<T> fromTopic(String topic) {
      return fromTopic(StaticValueProvider.of(topic));
    }

    public Read<T> fromTopic(ValueProvider<String> topic) {
      validateTopic(topic);
      return toBuilder()
          .setTopicProvider(NestedValueProvider.of(topic, PubsubTopic::fromPath))
          .build();
    }

    public Read<T> withDeadLetterTopic(String deadLetterTopic) {
      return withDeadLetterTopic(StaticValueProvider.of(deadLetterTopic));
    }

    public Read<T> withDeadLetterTopic(ValueProvider<String> deadLetterTopic) {
      validateTopic(deadLetterTopic);
      return toBuilder()
          .setDeadLetterTopicProvider(
              NestedValueProvider.of(deadLetterTopic, PubsubTopic::fromPath))
          .build();
    }

    private static void validateTopic(ValueProvider<String> topic) {
      if (topic.isAccessible()) {
        PubsubTopic.fromPath(topic.get());
      }
    }

    public Read<T> withClientFactory(PubsubClient.PubsubClientFactory factory) {
      return toBuilder().setPubsubClientFactory(factory).build();
    }

    public Read<T> withTimestampAttribute(String timestampAttribute) {
      return toBuilder().setTimestampAttribute(timestampAttribute).build();
    }

    public Read<T> withIdAttribute(String idAttribute) {
      return toBuilder().setIdAttribute(idAttribute).build();
    }

    public Read<T> withCoderAndParseFn(Coder<T> coder, SimpleFunction<PubsubMessage, T> parseFn) {
      return toBuilder().setCoder(coder).setParseFn(parseFn).build();
    }

    public Read<T> withErrorHandler(ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
      return toBuilder()
          .setBadRecordErrorHandler(badRecordErrorHandler)
          .setBadRecordRouter(BadRecordRouter.RECORDING_ROUTER)
          .build();
    }

    @VisibleForTesting
    Read<T> withClock(Clock clock) {
      return toBuilder().setClock(clock).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      if (getTopicProvider() == null && getSubscriptionProvider() == null) {
        throw new IllegalStateException(
            "Need to set either the topic or the subscription for " + "a PubsubIO.Read transform");
      }
      if (getTopicProvider() != null && getSubscriptionProvider() != null) {
        throw new IllegalStateException(
            "Can't set both the topic and the subscription for " + "a PubsubIO.Read transform");
      }

      if (getDeadLetterTopicProvider() != null
          && !(getBadRecordRouter() instanceof ThrowingBadRecordRouter)) {
        throw new IllegalArgumentException(
            "PubSubIO cannot be configured with both a dead letter topic and a bad record router");
      }

      @Nullable
      ValueProvider<TopicPath> topicPath =
          getTopicProvider() == null
              ? null
              : NestedValueProvider.of(getTopicProvider(), new TopicPathTranslator());
      @Nullable
      ValueProvider<SubscriptionPath> subscriptionPath =
          getSubscriptionProvider() == null
              ? null
              : NestedValueProvider.of(getSubscriptionProvider(), new SubscriptionPathTranslator());
      PubsubUnboundedSource source =
          new PubsubUnboundedSource(
              getClock(),
              getPubsubClientFactory(),
              null ,
              topicPath,
              subscriptionPath,
              getTimestampAttribute(),
              getIdAttribute(),
              getNeedsAttributes(),
              getNeedsMessageId(),
              getNeedsOrderingKey());

      PCollection<PubsubMessage> preParse = input.apply(source);
      return expandReadContinued(preParse, topicPath, subscriptionPath);
    }

    // Optimized by LLM: Separated handling of bad records into a new method
    private PCollection<T> expandReadContinued(
        PCollection<PubsubMessage> preParse,
        @Nullable ValueProvider<TopicPath> topicPath,
        @Nullable ValueProvider<SubscriptionPath> subscriptionPath) {

      TypeDescriptor<T> typeDescriptor = new TypeDescriptor<T>() {};
      SerializableFunction<PubsubMessage, T> parseFnWrapped =
          new SerializableFunction<PubsubMessage, T>() {
            private final SerializableFunction<PubsubMessage, T> underlying =
                Objects.requireNonNull(getParseFn());
            private transient boolean reportedMetrics = false;

            @Override
            public T apply(PubsubMessage input) {
              if (!reportedMetrics) {
                if (topicPath != null) {
                  TopicPath topic = topicPath.get();
                  if (topic != null) {
                    Lineage.getSources().add("pubsub", "topic", topic.getDataCatalogSegments());
                  }
                }
                if (subscriptionPath != null) {
                  SubscriptionPath sub = subscriptionPath.get();
                  if (sub != null) {
                    Lineage.getSources()
                        .add("pubsub", "subscription", sub.getDataCatalogSegments());
                  }
                }
                reportedMetrics = true;
              }
              return underlying.apply(input);
            }
          };
      PCollection<T> read;
      if (getDeadLetterTopicProvider() == null
          && (getBadRecordRouter() instanceof ThrowingBadRecordRouter)) {
        read = preParse.apply(MapElements.into(typeDescriptor).via(parseFnWrapped));
      } else {
        Result<PCollection<T>, KV<PubsubMessage, EncodableThrowable>> result =
            preParse.apply(
                "PubsubIO.Read/Map/Parse-Incoming-Messages",
                MapElements.into(typeDescriptor)
                    .via(parseFnWrapped)
                    .exceptionsVia(new WithFailures.ThrowableHandler<PubsubMessage>() {}));

        read = result.output();

        if (!(getBadRecordRouter() instanceof ThrowingBadRecordRouter)) {
          PCollection<BadRecord> badRecords =
              result
                  .failures()
                  .apply(
                      "Map Failures To BadRecords",
                      ParDo.of(new ParseReadFailuresToBadRecords(preParse.getCoder())));
          getBadRecordErrorHandler()
              .addErrorCollection(badRecords.setCoder(BadRecord.getCoder(preParse.getPipeline())));
        } else {
          handleBadRecords(result, preParse);
        }
      }
      return read.setCoder(getCoder());
    }

    // Optimized by LLM: Extracted bad record handling logic
    private void handleBadRecords(Result<PCollection<T>, KV<PubsubMessage, EncodableThrowable>> result, PCollection<PubsubMessage> preParse) {
      result
          .failures()
          .apply(
              "PubsubIO.Read/Map/Remove-Stack-Trace-Attribute",
              MapElements.into(new TypeDescriptor<KV<PubsubMessage, Map<String, String>>>() {})
                  .via(
                      kv -> {
                        PubsubMessage message = kv.getKey();
                        String messageId =
                            message.getMessageId() == null ? "<null>" : message.getMessageId();
                        Throwable throwable = kv.getValue().throwable();

                        LOG.error(
                            "Error parsing Pub/Sub message with id '{}'", messageId, throwable);

                        ImmutableMap<String, String> attributes =
                            ImmutableMap.<String, String>builder()
                                .put("exceptionClassName", throwable.getClass().getName())
                                .put("exceptionMessage", throwable.getMessage())
                                .put("pubsubMessageId", messageId)
                                .build();

                        return KV.of(kv.getKey(), attributes);
                      }))
          .apply(
              "PubsubIO.Read/Map/Create-Dead-Letter-Payload",
              MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                  .via(kv -> new PubsubMessage(kv.getKey().getPayload(), kv.getValue())))
          .apply(
              writeMessages()
                  .to(getDeadLetterTopicProvider().get().asPath())
                  .withClientFactory(getPubsubClientFactory()));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      populateCommonDisplayData(
          builder, getTimestampAttribute(), getIdAttribute(), getTopicProvider());
      builder.addIfNotNull(
          DisplayData.item("subscription", getSubscriptionProvider())
              .withLabel("Pubsub Subscription"));
    }
  }

  private static class ParseReadFailuresToBadRecords
      extends DoFn<KV<PubsubMessage, EncodableThrowable>, BadRecord> {
    private final Coder<PubsubMessage> coder;

    public ParseReadFailuresToBadRecords(Coder<PubsubMessage> coder) {
      this.coder = coder;
    }

    @ProcessElement
    public void processElement(
        OutputReceiver<BadRecord> outputReceiver,
        @Element KV<PubsubMessage, EncodableThrowable> element)
        throws Exception {
      outputReceiver.output(
          BadRecord.fromExceptionInformation(
              element.getKey(),
              coder,
              (Exception) element.getValue().throwable(),
              "Failed to parse message read from PubSub"));
    }
  }

  private PubsubIO() {}

  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {

    private static final int MAX_PUBLISH_BATCH_BYTE_SIZE_DEFAULT = ((10 * 1000 * 1000) / 4) * 3;

    private static final int MAX_PUBLISH_BATCH_SIZE = 100;

    abstract @Nullable ValueProvider<PubsubTopic> getTopicProvider();

    abstract @Nullable SerializableFunction<ValueInSingleWindow<T>, PubsubTopic> getTopicFunction();

    abstract boolean getDynamicDestinations();

    abstract PubsubClient.PubsubClientFactory getPubsubClientFactory();

    abstract @Nullable Integer getMaxBatchSize();

    abstract @Nullable Integer getMaxBatchBytesSize();

    abstract @Nullable String getTimestampAttribute();

    abstract @Nullable String getIdAttribute();

    abstract SerializableFunction<ValueInSingleWindow<T>, PubsubMessage> getFormatFn();

    abstract @Nullable String getPubsubRootUrl();

    abstract BadRecordRouter getBadRecordRouter();

    abstract ErrorHandler<BadRecord, ?> getBadRecordErrorHandler();

    abstract Builder<T> toBuilder();

    static <T> Builder<T> newBuilder(
        SerializableFunction<ValueInSingleWindow<T>, PubsubMessage> formatFn) {
      Builder<T> builder = new AutoValue_PubsubIO_Write.Builder<T>();
      builder.setPubsubClientFactory(FACTORY);
      builder.setFormatFn(formatFn);
      builder.setBadRecordRouter(BadRecordRouter.THROWING_ROUTER);
      builder.setBadRecordErrorHandler(new DefaultErrorHandler<>());
      return builder;
    }

    static Builder<PubsubMessage> newBuilder() {
      return newBuilder(x -> x.getValue());
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setTopicProvider(ValueProvider<PubsubTopic> topicProvider);

      abstract Builder<T> setTopicFunction(
          SerializableFunction<ValueInSingleWindow<T>, PubsubTopic> topicFunction);

      abstract Builder<T> setDynamicDestinations(boolean dynamicDestinations);

      abstract Builder<T> setPubsubClientFactory(PubsubClient.PubsubClientFactory factory);

      abstract Builder<T> setMaxBatchSize(Integer batchSize);

      abstract Builder<T> setMaxBatchBytesSize(Integer maxBatchBytesSize);

      abstract Builder<T> setTimestampAttribute(String timestampAttribute);

      abstract Builder<T> setIdAttribute(String idAttribute);

      abstract Builder<T> setFormatFn(
          SerializableFunction<ValueInSingleWindow<T>, PubsubMessage> formatFn);

      abstract Builder<T> setPubsubRootUrl(String pubsubRootUrl);

      abstract Builder<T> setBadRecordRouter(BadRecordRouter badRecordRouter);

      abstract Builder<T> setBadRecordErrorHandler(
          ErrorHandler<BadRecord, ?> badRecordErrorHandler);

      abstract Write<T> build();
    }

    public Write<T> to(String topic) {
      return to(StaticValueProvider.of(topic));
    }

    public Write<T> to(ValueProvider<String> topic) {
      return toBuilder()
          .setTopicProvider(NestedValueProvider.of(topic, PubsubTopic::fromPath))
          .setTopicFunction(null)
          .setDynamicDestinations(false)
          .build();
    }

    public Write<T> to(SerializableFunction<ValueInSingleWindow<T>, String> topicFunction) {
      return toBuilder()
          .setTopicProvider(null)
          .setTopicFunction(v -> PubsubTopic.fromPath(topicFunction.apply(v)))
          .setDynamicDestinations(true)
          .build();
    }

    public Write<T> withClientFactory(PubsubClient.PubsubClientFactory factory) {
      return toBuilder().setPubsubClientFactory(factory).build();
    }

    public Write<T> withMaxBatchSize(int batchSize) {
      return toBuilder().setMaxBatchSize(batchSize).build();
    }

    public Write<T> withMaxBatchBytesSize(int maxBatchBytesSize) {
      return toBuilder().setMaxBatchBytesSize(maxBatchBytesSize).build();
    }

    public Write<T> withTimestampAttribute(String timestampAttribute) {
      return toBuilder().setTimestampAttribute(timestampAttribute).build();
    }

    public Write<T> withIdAttribute(String idAttribute) {
      return toBuilder().setIdAttribute(idAttribute).build();
    }

    public Write<T> withPubsubRootUrl(String pubsubRootUrl) {
      return toBuilder().setPubsubRootUrl(pubsubRootUrl).build();
    }

    public Write<T> withErrorHandler(ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
      return toBuilder()
          .setBadRecordErrorHandler(badRecordErrorHandler)
          .setBadRecordRouter(BadRecordRouter.RECORDING_ROUTER)
          .build();
    }

    @Override
    public PDone expand(PCollection<T> input) {
      if (getTopicProvider() == null && !getDynamicDestinations()) {
        throw new IllegalStateException(
            "need to set the topic of a PubsubIO.Write transform if not using "
                + "dynamic topic destinations.");
      }

      SerializableFunction<ValueInSingleWindow<T>, PubsubIO.PubsubTopic> topicFunction =
          getTopicFunction();
      if (topicFunction == null && getTopicProvider() != null) {
        topicFunction = v -> getTopicProvider().get();
      }
      int maxMessageSize = PUBSUB_MESSAGE_MAX_TOTAL_SIZE;
      if (input.isBounded() == PCollection.IsBounded.BOUNDED) {
        maxMessageSize =
            Math.min(
                maxMessageSize,
                MoreObjects.firstNonNull(
                    getMaxBatchBytesSize(), MAX_PUBLISH_BATCH_BYTE_SIZE_DEFAULT));
      }
      TupleTag<PubsubMessage> pubsubMessageTupleTag = new TupleTag<>();
      PCollectionTuple pubsubMessageTuple =
          input.apply(
              ParDo.of(
                      new PreparePubsubWriteDoFn<>(
                          getFormatFn(),
                          topicFunction,
                          maxMessageSize,
                          getBadRecordRouter(),
                          input.getCoder(),
                          pubsubMessageTupleTag))
                  .withOutputTags(pubsubMessageTupleTag, TupleTagList.of(BAD_RECORD_TAG)));

      getBadRecordErrorHandler()
          .addErrorCollection(
              pubsubMessageTuple
                  .get(BAD_RECORD_TAG)
                  .setCoder(BadRecord.getCoder(input.getPipeline())));
      PCollection<PubsubMessage> pubsubMessages =
          pubsubMessageTuple.get(pubsubMessageTupleTag).setCoder(PubsubMessageWithTopicCoder.of());
      switch (input.isBounded()) {
        case BOUNDED:
          pubsubMessages.apply(
              ParDo.of(
                  new PubsubBoundedWriter(
                      MoreObjects.firstNonNull(getMaxBatchSize(), MAX_PUBLISH_BATCH_SIZE),
                      MoreObjects.firstNonNull(
                          getMaxBatchBytesSize(), MAX_PUBLISH_BATCH_BYTE_SIZE_DEFAULT))));
          return PDone.in(input.getPipeline());
        case UNBOUNDED:
          return pubsubMessages.apply(
              new PubsubUnboundedSink(
                  getPubsubClientFactory(),
                  getTopicProvider() != null
                      ? NestedValueProvider.of(getTopicProvider(), new TopicPathTranslator())
                      : null,
                  getTimestampAttribute(),
                  getIdAttribute(),
                  100 ,
                  MoreObjects.firstNonNull(
                      getMaxBatchSize(), PubsubUnboundedSink.DEFAULT_PUBLISH_BATCH_SIZE),
                  MoreObjects.firstNonNull(
                      getMaxBatchBytesSize(), PubsubUnboundedSink.DEFAULT_PUBLISH_BATCH_BYTES),
                  getPubsubRootUrl()));
      }
      throw new RuntimeException(); 
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      populateCommonDisplayData(
          builder, getTimestampAttribute(), getIdAttribute(), getTopicProvider());
    }

    public class PubsubBoundedWriter extends DoFn<PubsubMessage, Void> {
      private class OutgoingData {
        final List<OutgoingMessage> messages;
        long bytes;

        OutgoingData() {
          this.messages = Lists.newArrayList();
          this.bytes = 0;
        }
      }

      private transient Map<PubsubTopic, OutgoingData> output;

      private transient PubsubClient pubsubClient;

      private final int maxPublishBatchByteSize;
      private final int maxPublishBatchSize;

      PubsubBoundedWriter(int maxPublishBatchSize, int maxPublishBatchByteSize) {
        this.maxPublishBatchSize = maxPublishBatchSize;
        this.maxPublishBatchByteSize = maxPublishBatchByteSize;
      }

      PubsubBoundedWriter() {
        this(MAX_PUBLISH_BATCH_SIZE, MAX_PUBLISH_BATCH_BYTE_SIZE_DEFAULT);
      }

      @StartBundle
      public void startBundle(StartBundleContext c) throws IOException {
        this.output = Maps.newHashMap();

        this.pubsubClient =
            getPubsubClientFactory()
                .newClient(
                    getTimestampAttribute(), null, c.getPipelineOptions().as(PubsubOptions.class));
      }

      @ProcessElement
      public void processElement(@Element PubsubMessage message, @Timestamp Instant timestamp)
          throws IOException, SizeLimitExceededException {
        PreparePubsubWriteDoFn.validatePubsubMessageSize(message, maxPublishBatchByteSize);
        byte[] payload = message.getPayload();
        int messageSize = payload.length;

        PubsubTopic pubsubTopic;
        if (getTopicProvider() != null) {
          pubsubTopic = getTopicProvider().get();
        } else {
          pubsubTopic =
              PubsubTopic.fromPath(Preconditions.checkArgumentNotNull(message.getTopic()));
        }

        OutgoingData currentTopicOutput =
            output.computeIfAbsent(pubsubTopic, t -> new OutgoingData());
        if (currentTopicOutput.messages.size() >= maxPublishBatchSize
            || (!currentTopicOutput.messages.isEmpty()
                && (currentTopicOutput.bytes + messageSize) >= maxPublishBatchByteSize)) {
          publish(pubsubTopic, currentTopicOutput.messages);
          currentTopicOutput.messages.clear();
          currentTopicOutput.bytes = 0;
        }

        Map<String, String> attributes = message.getAttributeMap();
        String orderingKey = message.getOrderingKey();

        com.google.pubsub.v1.PubsubMessage.Builder msgBuilder =
            com.google.pubsub.v1.PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom(payload))
                .putAllAttributes(attributes);

        if (orderingKey != null) {
          msgBuilder.setOrderingKey(orderingKey);
        }

        currentTopicOutput.messages.add(
            OutgoingMessage.of(
                msgBuilder.build(), timestamp.getMillis(), null, message.getTopic()));
        currentTopicOutput.bytes += messageSize;
      }

      @FinishBundle
      public void finishBundle() throws IOException {
        for (Map.Entry<PubsubTopic, OutgoingData> entry : output.entrySet()) {
          publish(entry.getKey(), entry.getValue().messages);
        }
        output = null;
        pubsubClient.close();
        pubsubClient = null;
      }

      private void publish(PubsubTopic topic, List<OutgoingMessage> messages) throws IOException {
        int n =
            pubsubClient.publish(
                PubsubClient.topicPathFromName(topic.project, topic.topic), messages);
        checkState(n == messages.size());
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder.delegate(Write.this);
      }
    }
  }

  private static <T> SerializableFunction<PubsubMessage, T> parsePayloadUsingCoder(Coder<T> coder) {
    return message -> {
      try {
        return CoderUtils.decodeFromByteArray(coder, message.getPayload());
      } catch (CoderException e) {
        throw new RuntimeException("Could not decode Pubsub message", e);
      }
    };
  }

  private static <T>
      SerializableFunction<ValueInSingleWindow<T>, PubsubMessage> formatPayloadUsingCoder(
          Coder<T> coder) {
    return input -> {
      try {
        return new PubsubMessage(
            CoderUtils.encodeToByteArray(coder, input.getValue()), ImmutableMap.of());
      } catch (CoderException e) {
        throw new RuntimeException("Could not encode Pubsub message", e);
      }
    };
  }

  private static <T>
      SerializableFunction<ValueInSingleWindow<T>, PubsubMessage> formatPayloadUsingCoder(
          Coder<T> coder,
          SerializableFunction<ValueInSingleWindow<T>, Map<String, String>> attributesFn) {
    return input -> {
      try {
        return new PubsubMessage(
            CoderUtils.encodeToByteArray(coder, input.getValue()), attributesFn.apply(input));
      } catch (CoderException e) {
        throw new RuntimeException("Could not encode Pubsub message", e);
      }
    };
  }
}