package org.apache.beam.sdk.io.gcp.pubsub;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.api.client.util.Clock;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.AvroSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoDomain;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.IncomingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.Read;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubTestClient.PubsubTestClientFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandlingTestUtils.ErrorSinkTransform;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.Statement;


@RunWith(JUnit4.class)
public class PubsubIOTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(60);
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testPubsubIOGetName() {
    assertEquals(
        "PubsubIO.Read",
        PubsubIO.readStrings().fromTopic("projects/myproject/topics/mytopic").getName());
    assertEquals(
        "PubsubIO.Write",
        PubsubIO.writeStrings().to("projects/myproject/topics/mytopic").getName());
  }

  @Test
  public void testTopicValidationSuccess() throws Exception {
    PubsubIO.readStrings().fromTopic("projects/my-project/topics/abc");
    PubsubIO.readStrings().fromTopic("projects/my-project/topics/ABC");
    PubsubIO.readStrings().fromTopic("projects/my-project/topics/AbC-DeF");
    PubsubIO.readStrings().fromTopic("projects/my-project/topics/AbC-1234");
    PubsubIO.readStrings().fromTopic("projects/my-project/topics/AbC-1234-_.~%+-_.~%+-_.~%+-abc");
    PubsubIO.readStrings()
        .fromTopic(
            new StringBuilder()
                .append("projects/my-project/topics/A-really-long-one-")
                .append(
                    "111111111111111111111111111111111111111111111111111111111111111111111111111111111")
                .append(
                    "111111111111111111111111111111111111111111111111111111111111111111111111111111111")
                .append(
                    "11111111111111111111111111111111111111111111111111111111111111111111111111")
                .toString());
  }

  @Test
  public void testTopicValidationBadCharacter() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    PubsubIO.readStrings().fromTopic("projects/my-project/topics/abc-*-abc");
  }

  @Test
  public void testTopicValidationTooLong() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    PubsubIO.readStrings()
        .fromTopic(
            new StringBuilder()
                .append("projects/my-project/topics/A-really-long-one-")
                .append(
                    "111111111111111111111111111111111111111111111111111111111111111111111111111111111")
                .append(
                    "111111111111111111111111111111111111111111111111111111111111111111111111111111111")
                .append(
                    "1111111111111111111111111111111111111111111111111111111111111111111111111111")
                .toString());
  }

  @Test
  public void testNullTopic() {
    String subscription = "projects/project/subscriptions/subscription";
    PubsubIO.Read<String> read =
        PubsubIO.readStrings().fromSubscription(StaticValueProvider.of(subscription));
    assertNull(read.getTopicProvider());
    assertNotNull(read.getSubscriptionProvider());
    assertNotNull(DisplayData.from(read));
  }

  @Test
  public void testNullSubscription() {
    String topic = "projects/project/topics/topic";
    PubsubIO.Read<String> read = PubsubIO.readStrings().fromTopic(StaticValueProvider.of(topic));
    assertNotNull(read.getTopicProvider());
    assertNull(read.getSubscriptionProvider());
    assertNotNull(DisplayData.from(read));
  }

  @Test
  public void testValueProviderSubscription() {
    StaticValueProvider<String> provider =
        StaticValueProvider.of("projects/project/subscriptions/subscription");
    Read<String> pubsubRead = PubsubIO.readStrings().fromSubscription(provider);
    Pipeline.create().apply(pubsubRead);
    assertThat(pubsubRead.getSubscriptionProvider(), not(nullValue()));
    assertThat(pubsubRead.getSubscriptionProvider().isAccessible(), is(true));
    assertThat(pubsubRead.getSubscriptionProvider().get().asPath(), equalTo(provider.get()));
  }

  @Test
  public void testRuntimeValueProviderSubscription() {
    TestPipeline pipeline = TestPipeline.create();
    ValueProvider<String> subscription =
        pipeline.newProvider("projects/project/subscriptions/subscription");
    Read<String> pubsubRead = PubsubIO.readStrings().fromSubscription(subscription);
    pipeline.apply(pubsubRead);
    assertThat(pubsubRead.getSubscriptionProvider(), not(nullValue()));
    assertThat(pubsubRead.getSubscriptionProvider().isAccessible(), is(false));
  }

  @Test
  public void testValueProviderTopic() {
    StaticValueProvider<String> provider = StaticValueProvider.of("projects/project/topics/topic");
    Read<String> pubsubRead = PubsubIO.readStrings().fromTopic(provider);
    Pipeline.create().apply(pubsubRead);
    assertThat(pubsubRead.getTopicProvider(), not(nullValue()));
    assertThat(pubsubRead.getTopicProvider().isAccessible(), is(true));
    assertThat(pubsubRead.getTopicProvider().get().asPath(), equalTo(provider.get()));
    assertThat(
        pubsubRead.getTopicProvider().get().dataCatalogSegments(),
        equalTo(ImmutableList.of("project", "topic")));
  }

  @Test
  public void testRuntimeValueProviderTopic() {
    TestPipeline pipeline = TestPipeline.create();
    ValueProvider<String> topic = pipeline.newProvider("projects/project/topics/topic");
    Read<String> pubsubRead = PubsubIO.readStrings().fromTopic(topic);
    pipeline.apply(pubsubRead);
    assertThat(pubsubRead.getTopicProvider(), not(nullValue()));
    assertThat(pubsubRead.getTopicProvider().isAccessible(), is(false));
  }

  static class GenericClass {
    int intField;
    String stringField;

    @AvroSchema("{\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}")
    public DateTime timestamp;

    public GenericClass() {}

    public GenericClass(int intField, String stringField, DateTime timestamp) {
      this.intField = intField;
      this.stringField = stringField;
      this.timestamp = timestamp;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("intField", intField)
          .add("stringField", stringField)
          .add("timestamp", timestamp)
          .toString();
    }

    @Override
    public int hashCode() {
      return Objects.hash(intField, stringField, timestamp);
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (other == null || !(other instanceof GenericClass)) {
        return false;
      }
      GenericClass o = (GenericClass) other;
      return intField == o.intField
          && Objects.equals(stringField, o.stringField)
          && Objects.equals(timestamp, o.timestamp);
    }
  }

  private transient PipelineOptions options;
  private static final SubscriptionPath SUBSCRIPTION =
      PubsubClient.subscriptionPathFromName("test-project", "testSubscription");
  private static final TopicPath TOPIC =
      PubsubClient.topicPathFromName("test-project", "testTopic");
  private static final Clock CLOCK = (Clock & Serializable) () -> 673L;
  transient TestPipeline pipeline;

  private static final String SCHEMA_STRING =
      "{\"namespace\": \"example.avro\",\n"
          + " \"type\": \"record\",\n"
          + " \"name\": \"AvroGeneratedUser\",\n"
          + " \"fields\": [\n"
          + "     {\"name\": \"name\", \"type\": \"string\"},\n"
          + "     {\"name\": \"favorite_number\", \"type\": [\"int\", \"null\"]},\n"
          + "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n"
          + " ]\n"
          + "}";

  private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STRING);

  @Rule
  public final transient TestRule setupPipeline =
      new TestRule() {
        @Override
        public Statement apply(final Statement base, final Description description) {
          
          
          
          
          Statement withPipeline =
              new Statement() {
                @Override
                public void evaluate() throws Throwable {
                  options = TestPipeline.testingPipelineOptions();
                  options.as(PubsubOptions.class).setProject("test-project");
                  pipeline = TestPipeline.fromOptions(options);
                  pipeline.apply(base, description).evaluate();
                }
              };
          return withPipeline;
        }
      };

  private <T> void setupTestClient(List<T> inputs, Coder<T> coder) {
    List<IncomingMessage> messages =
        inputs.stream()
            .map(
                t -> {
                  try {
                    return CoderUtils.encodeToByteArray(coder, t);
                  } catch (CoderException e) {
                    throw new RuntimeException(e);
                  }
                })
            .map(
                ba ->
                    IncomingMessage.of(
                        com.google.pubsub.v1.PubsubMessage.newBuilder()
                            .setData(ByteString.copyFrom(ba))
                            .build(),
                        1234L,
                        0,
                        UUID.randomUUID().toString(),
                        UUID.randomUUID().toString()))
            .collect(Collectors.toList());

    clientFactory = PubsubTestClient.createFactoryForPull(CLOCK, SUBSCRIPTION, 60, messages);
  }

  private PubsubTestClientFactory clientFactory;

  @After
  public void after() throws IOException {
    if (clientFactory != null) {
      clientFactory.close();
      clientFactory = null;
    }
  }

  @Test
  public void testFailedParseWithDeadLetterConfigured() {
    ByteString data = ByteString.copyFrom("Hello, World!".getBytes(StandardCharsets.UTF_8));
    RuntimeException exception = new RuntimeException("Some error message");
    ImmutableList<IncomingMessage> expectedReads =
        ImmutableList.of(
            IncomingMessage.of(
                com.google.pubsub.v1.PubsubMessage.newBuilder().setData(data).build(),
                1234L,
                0,
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()));
    ImmutableList<OutgoingMessage> expectedWrites =
        ImmutableList.of(
            OutgoingMessage.of(
                com.google.pubsub.v1.PubsubMessage.newBuilder()
                    .setData(data)
                    .putAttributes("exceptionClassName", exception.getClass().getName())
                    .putAttributes("exceptionMessage", exception.getMessage())
                    .putAttributes("pubsubMessageId", "<null>")
                    .build(),
                1234L,
                null,
                null));
    clientFactory =
        PubsubTestClient.createFactoryForPullAndPublish(
            SUBSCRIPTION, TOPIC, CLOCK, 60, expectedReads, expectedWrites, ImmutableList.of());

    PCollection<String> read =
        pipeline.apply(
            PubsubIO.readStrings()
                .fromSubscription(SUBSCRIPTION.getPath())
                .withDeadLetterTopic(TOPIC.getPath())
                .withClock(CLOCK)
                .withClientFactory(clientFactory)
                .withCoderAndParseFn(
                    StringUtf8Coder.of(),
                    SimpleFunction.fromSerializableFunctionWithOutputType(
                        message -> {
                          throw exception;
                        },
                        TypeDescriptors.strings())));

    PAssert.that(read).empty();
    pipeline.run();
  }

  @Test
  public void testFailedParseWithErrorHandlerConfigured() throws Exception {
    ByteString data = ByteString.copyFrom("Hello, World!".getBytes(StandardCharsets.UTF_8));
    RuntimeException exception = new RuntimeException("Some error message");
    ImmutableList<IncomingMessage> expectedReads =
        ImmutableList.of(
            IncomingMessage.of(
                com.google.pubsub.v1.PubsubMessage.newBuilder().setData(data).build(),
                1234L,
                0,
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()));
    ImmutableList<OutgoingMessage> expectedWrites = ImmutableList.of();
    clientFactory =
        PubsubTestClient.createFactoryForPullAndPublish(
            SUBSCRIPTION, TOPIC, CLOCK, 60, expectedReads, expectedWrites, ImmutableList.of());

    ErrorHandler<BadRecord, PCollection<Long>> errorHandler =
        pipeline.registerBadRecordErrorHandler(new ErrorSinkTransform());
    PCollection<String> read =
        pipeline.apply(
            PubsubIO.readStrings()
                .fromSubscription(SUBSCRIPTION.getPath())
                .withErrorHandler(errorHandler)
                .withClock(CLOCK)
                .withClientFactory(clientFactory)
                .withCoderAndParseFn(
                    StringUtf8Coder.of(),
                    SimpleFunction.fromSerializableFunctionWithOutputType(
                        message -> {
                          throw exception;
                        },
                        TypeDescriptors.strings())));
    errorHandler.close();

    PAssert.thatSingleton(errorHandler.getOutput()).isEqualTo(1L);
    PAssert.that(read).empty();
    pipeline.run();
  }

  @Test
  public void testAvroPojo() {
    AvroCoder<GenericClass> coder = AvroCoder.of(GenericClass.class);
    List<GenericClass> inputs =
        Lists.newArrayList(
            new GenericClass(
                1, "foo", new DateTime().withDate(2019, 10, 1).withZone(DateTimeZone.UTC)),
            new GenericClass(
                2, "bar", new DateTime().withDate(1986, 10, 1).withZone(DateTimeZone.UTC)));
    setupTestClient(inputs, coder);
    PCollection<GenericClass> read =
        pipeline.apply(
            PubsubIO.readAvrosWithBeamSchema(GenericClass.class)
                .fromSubscription(SUBSCRIPTION.getPath())
                .withClock(CLOCK)
                .withClientFactory(clientFactory));
    PAssert.that(read).containsInAnyOrder(inputs);
    pipeline.run();
  }

  static class StringPayloadParseFn extends SimpleFunction<PubsubMessage, String> {
    @Override
    public String apply(PubsubMessage input) {
      return new String(input.getPayload(), StandardCharsets.UTF_8);
    }
  }

  @Test
  public void testWriteMalformedMessagesWithErrorHandler() throws Exception {
    OutgoingMessage msg =
        OutgoingMessage.of(
            com.google.pubsub.v1.PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8("foo"))
                .build(),
            0,
            null,
            "projects/project/topics/topic1");

    try (PubsubTestClientFactory factory =
        PubsubTestClient.createFactoryForPublish(null, ImmutableList.of(msg), ImmutableList.of())) {
      TimestampedValue<PubsubMessage> pubsubMsg =
          TimestampedValue.of(
              new PubsubMessage(
                      msg.getMessage().getData().toByteArray(),
                      Collections.emptyMap(),
                      msg.recordId())
                  .withTopic(msg.topic()),
              Instant.ofEpochMilli(msg.getTimestampMsSinceEpoch()));

      TimestampedValue<PubsubMessage> failingPubsubMsg =
          TimestampedValue.of(
              new PubsubMessage(
                      "foo".getBytes(StandardCharsets.UTF_8),
                      Collections.emptyMap(),
                      msg.recordId())
                  .withTopic("badTopic"),
              Instant.ofEpochMilli(msg.getTimestampMsSinceEpoch()));

      PCollection<PubsubMessage> messages =
          pipeline.apply(
              Create.timestamped(ImmutableList.of(pubsubMsg, failingPubsubMsg))
                  .withCoder(PubsubMessageWithTopicCoder.of()));
      messages.setIsBoundedInternal(PCollection.IsBounded.BOUNDED);
      ErrorHandler<BadRecord, PCollection<Long>> badRecordErrorHandler =
          pipeline.registerBadRecordErrorHandler(new ErrorSinkTransform());
      
      
      messages.apply(
          PubsubIO.writeMessages()
              .toBuilder()
              .setFormatFn(
                  (ValueInSingleWindow<PubsubMessage> messageAndWindow) -> {
                    if (messageAndWindow.getValue().getTopic().equals("badTopic")) {
                      throw new RuntimeException("expected exception");
                    }
                    return messageAndWindow.getValue();
                  })
              .build()
              .to("projects/project/topics/topic1")
              .withClientFactory(factory)
              .withErrorHandler(badRecordErrorHandler));
      badRecordErrorHandler.close();
      PAssert.thatSingleton(badRecordErrorHandler.getOutput()).isEqualTo(1L);
      pipeline.run();
    }
  }

  @Test
  public void testReadMessagesWithCoderAndParseFn() {
    Coder<PubsubMessage> coder = PubsubMessagePayloadOnlyCoder.of();
    List<PubsubMessage> inputs =
        ImmutableList.of(
            new PubsubMessage("foo".getBytes(StandardCharsets.UTF_8), new HashMap<>()),
            new PubsubMessage("bar".getBytes(StandardCharsets.UTF_8), new HashMap<>()));
    setupTestClient(inputs, coder);

    PCollection<String> read =
        pipeline.apply(
            PubsubIO.readMessagesWithCoderAndParseFn(
                    StringUtf8Coder.of(), new StringPayloadParseFn())
                .fromSubscription(SUBSCRIPTION.getPath())
                .withClock(CLOCK)
                .withClientFactory(clientFactory));

    List<String> outputs = ImmutableList.of("foo", "bar");
    PAssert.that(read).containsInAnyOrder(outputs);
    pipeline.run();
  }

  static class AppendSuffixAttributeToStringPayloadParseFn
      extends SimpleFunction<PubsubMessage, String> {
    @Override
    public String apply(PubsubMessage input) {
      String payload = new String(input.getPayload(), StandardCharsets.UTF_8);
      String suffixAttribute = input.getAttributeMap().get("suffix");
      return payload + suffixAttribute;
    }
  }

  private IncomingMessage messageWithSuffixAttribute(String payload, String suffix) {
    return IncomingMessage.of(
        com.google.pubsub.v1.PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(payload))
            .putAttributes("suffix", suffix)
            .build(),
        1234L,
        0,
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
  }

  @Test
  public void testReadMessagesWithAttributesWithCoderAndParseFn() {
    ImmutableList<IncomingMessage> inputs =
        ImmutableList.of(
            messageWithSuffixAttribute("foo", "-some-suffix"),
            messageWithSuffixAttribute("bar", "-some-other-suffix"));
    clientFactory = PubsubTestClient.createFactoryForPull(CLOCK, SUBSCRIPTION, 60, inputs);

    PCollection<String> read =
        pipeline.apply(
            PubsubIO.readMessagesWithAttributesWithCoderAndParseFn(
                    StringUtf8Coder.of(), new AppendSuffixAttributeToStringPayloadParseFn())
                .fromSubscription(SUBSCRIPTION.getPath())
                .withClock(CLOCK)
                .withClientFactory(clientFactory));

    List<String> outputs = ImmutableList.of("foo-some-suffix", "bar-some-other-suffix");
    PAssert.that(read).containsInAnyOrder(outputs);
    pipeline.run();
  }

  @Test
  public void testDynamicTopicsBounded() throws IOException {
    testDynamicTopics(true);
  }

  @Test
  public void testDynamicTopicsUnbounded() throws IOException {
    testDynamicTopics(false);
  }

  public void testDynamicTopics(boolean isBounded) throws IOException {
    List<OutgoingMessage> expectedOutgoing =
        ImmutableList.of(
            OutgoingMessage.of(
                com.google.pubsub.v1.PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8("0"))
                    .build(),
                0,
                null,
                "projects/project/topics/topic1"),
            OutgoingMessage.of(
                com.google.pubsub.v1.PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8("1"))
                    .build(),
                1,
                null,
                "projects/project/topics/topic1"),
            OutgoingMessage.of(
                com.google.pubsub.v1.PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8("2"))
                    .build(),
                2,
                null,
                "projects/project/topics/topic2"),
            OutgoingMessage.of(
                com.google.pubsub.v1.PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8("3"))
                    .build(),
                3,
                null,
                "projects/project/topics/topic2"));

    try (PubsubTestClientFactory factory =
        PubsubTestClient.createFactoryForPublish(null, expectedOutgoing, ImmutableList.of())) {
      List<TimestampedValue<PubsubMessage>> pubsubMessages =
          expectedOutgoing.stream()
              .map(
                  o ->
                      TimestampedValue.of(
                          new PubsubMessage(
                                  o.getMessage().getData().toByteArray(),
                                  Collections.emptyMap(),
                                  o.recordId())
                              .withTopic(o.topic()),
                          Instant.ofEpochMilli(o.getTimestampMsSinceEpoch())))
              .collect(Collectors.toList());

      PCollection<PubsubMessage> messages =
          pipeline.apply(
              Create.timestamped(pubsubMessages).withCoder(PubsubMessageWithTopicCoder.of()));
      if (!isBounded) {
        messages = messages.setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED);
      }
      messages.apply(PubsubIO.writeMessagesDynamic().withClientFactory(factory));
      pipeline.run();
    }
  }

  @Test
  public void testBigMessageBounded() throws IOException {
    String bigMsg =
        IntStream.range(0, 100_000).mapToObj(_unused -> "x").collect(Collectors.joining(""));

    OutgoingMessage msg =
        OutgoingMessage.of(
            com.google.pubsub.v1.PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(bigMsg))
                .build(),
            0,
            null,
            "projects/project/topics/topic1");

    try (PubsubTestClientFactory factory =
        PubsubTestClient.createFactoryForPublish(null, ImmutableList.of(msg), ImmutableList.of())) {
      TimestampedValue<PubsubMessage> pubsubMsg =
          TimestampedValue.of(
              new PubsubMessage(
                      msg.getMessage().getData().toByteArray(),
                      Collections.emptyMap(),
                      msg.recordId())
                  .withTopic(msg.topic()),
              Instant.ofEpochMilli(msg.getTimestampMsSinceEpoch()));

      PCollection<PubsubMessage> messages =
          pipeline.apply(
              Create.timestamped(ImmutableList.of(pubsubMsg))
                  .withCoder(PubsubMessageWithTopicCoder.of()));
      messages.setIsBoundedInternal(PCollection.IsBounded.BOUNDED);
      messages.apply(PubsubIO.writeMessagesDynamic().withClientFactory(factory));
      pipeline.run();
    }
  }
}
