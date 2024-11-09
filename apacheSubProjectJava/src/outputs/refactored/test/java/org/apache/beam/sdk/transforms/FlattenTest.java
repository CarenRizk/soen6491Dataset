package org.apache.beam.sdk.transforms;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.testing.FlattenWithHeterogeneousCoders;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSideInputs;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FlattenTest implements Serializable {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Rule public transient Timeout globalTimeout = Timeout.seconds(1200);

  private static class ClassWithoutCoder {}

  @Test
  @Category(ValidatesRunner.class)
  public void testFlattenPCollectionsEmpty() {
    PCollection<String> output =
        PCollectionList.<String>empty(p)
            .apply(Flatten.pCollections())
            .setCoder(StringUtf8Coder.of());

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testFlattenInputMultipleCopies() {
    int count = 5;
    PCollection<Long> longs = p.apply("mkLines", GenerateSequence.from(0).to(count));
    PCollection<Long> biggerLongs =
        p.apply("mkOtherLines", GenerateSequence.from(0).to(count))
            .apply(
                MapElements.via(
                    (Long input) -> input + 10L // Optimized by LLM: Replaced anonymous inner class with lambda expression
                ));

    PCollection<Long> flattened =
        PCollectionList.of(longs).and(longs).and(biggerLongs).apply(Flatten.pCollections());

    List<Long> expectedLongs = List.of(); // Optimized by LLM: Replaced ArrayList instantiation with List.of()
    for (int i = 0; i < count; i++) {
      expectedLongs.add((long) i);
      expectedLongs.add((long) i);
      expectedLongs.add(i + 10L);
    }
    PAssert.that(flattened).containsInAnyOrder(expectedLongs);

    p.run();
  }

  @Test
  @Category({ValidatesRunner.class, FlattenWithHeterogeneousCoders.class})
  public void testFlattenMultipleCoders() throws CannotProvideCoderException {
    PCollection<Long> bigEndianLongs =
        p.apply(
            "BigEndianLongs",
            Create.of(0L, 1L, 2L, 3L, null, 4L, 5L, null, 6L, 7L, 8L, null, 9L)
                .withCoder(NullableCoder.of(BigEndianLongCoder.of())));
    PCollection<Long> varLongs =
        p.apply("VarLengthLongs", GenerateSequence.from(0).to(5)).setCoder(VarLongCoder.of());

    PCollection<Long> flattened =
        PCollectionList.of(bigEndianLongs)
            .and(varLongs)
            .apply(Flatten.pCollections())
            .setCoder(NullableCoder.of(VarLongCoder.of()));
    PAssert.that(flattened)
        .containsInAnyOrder(
            0L, 0L, 1L, 1L, 2L, 3L, 2L, 4L, 5L, 3L, 6L, 7L, 4L, 8L, 9L, null, null, null);
    p.run();
  }

  @Test
  @Category({ValidatesRunner.class, UsesSideInputs.class})
  public void testEmptyFlattenAsSideInput() {
    final PCollectionView<Iterable<String>> view =
        PCollectionList.<String>empty(p)
            .apply(Flatten.pCollections())
            .setCoder(StringUtf8Coder.of())
            .apply(View.asIterable());

    PCollection<String> output =
        p.apply(Create.of((Void) null).withCoder(VoidCoder.of()))
            .apply(
                ParDo.of(
                        new DoFn<Void, String>() {
                          @ProcessElement
                          public void processElement(ProcessContext c) {
                            for (String side : c.sideInput(view)) {
                              c.output(side);
                            }
                          }
                        })
                    .withSideInputs(view));

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testFlattenPCollectionsEmptyThenParDo() {
    PCollection<String> output =
        PCollectionList.<String>empty(p)
            .apply(Flatten.pCollections())
            .setCoder(StringUtf8Coder.of())
            .apply(ParDo.of(new IdentityFn<>()));

    PAssert.that(output).empty();
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testFlattenNoListsNoCoder() {
    
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Unable to return a default Coder");

    PCollectionList.<ClassWithoutCoder>empty(p).apply(Flatten.pCollections());

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testFlattenMultiplePCollectionsHavingMultipleConsumers() {
    PCollection<String> input = p.apply(Create.of("AA", "BBB", "CC"));
    final TupleTag<String> outputEvenLengthTag = new TupleTag<String>() {};
    final TupleTag<String> outputOddLengthTag = new TupleTag<String>() {};

    PCollectionTuple tuple =
        input.apply(
            ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        if (c.element().length() % 2 == 0) {
                          c.output(c.element());
                        } else {
                          c.output(outputOddLengthTag, c.element());
                        }
                      }
                    })
                .withOutputTags(outputEvenLengthTag, TupleTagList.of(outputOddLengthTag)));

    PCollection<String> outputEvenLength = tuple.get(outputEvenLengthTag);
    PCollection<String> outputOddLength = tuple.get(outputOddLengthTag);

    PCollection<String> outputMerged =
        PCollectionList.of(outputEvenLength).and(outputOddLength).apply(Flatten.pCollections());

    PAssert.that(outputMerged).containsInAnyOrder("AA", "BBB", "CC");
    PAssert.that(outputEvenLength).containsInAnyOrder("AA", "CC");
    PAssert.that(outputOddLength).containsInAnyOrder("BBB");

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testEqualWindowFnPropagation() {
    PCollection<String> input1 = createAndWindowInput1();
    PCollection<String> input2 =
        p.apply("CreateInput2", Create.of("Input2"))
            .apply("Window2", Window.into(FixedWindows.of(Duration.standardMinutes(1))));

    PCollection<String> output =
        PCollectionList.of(input1).and(input2).apply(Flatten.pCollections());

    p.run();

    Assert.assertTrue(
        output
            .getWindowingStrategy()
            .getWindowFn()
            .isCompatible(FixedWindows.of(Duration.standardMinutes(1))));
  }

private PCollection<String> createAndWindowInput1() {
	PCollection<String> input1 =
        p.apply("CreateInput1", Create.of("Input1"))
            .apply("Window1", Window.into(FixedWindows.of(Duration.standardMinutes(1))));
	return input1;
}

  @Test
  @Category(NeedsRunner.class)
  public void testCompatibleWindowFnPropagation() {
    PCollection<String> input1 =
        p.apply("CreateInput1", Create.of("Input1"))
            .apply("Window1", Window.into(Sessions.withGapDuration(Duration.standardMinutes(1))));
    PCollection<String> input2 =
        p.apply("CreateInput2", Create.of("Input2"))
            .apply("Window2", Window.into(Sessions.withGapDuration(Duration.standardMinutes(2))));

    PCollection<String> output =
        PCollectionList.of(input1).and(input2).apply(Flatten.pCollections());

    p.run();

    Assert.assertTrue(
        output
            .getWindowingStrategy()
            .getWindowFn()
            .isCompatible(Sessions.withGapDuration(Duration.standardMinutes(2))));
  }

  @Test
  public void testIncompatibleWindowFnPropagationFailure() {
    p.enableAbandonedNodeEnforcement(false);

    PCollection<String> input1 = createAndWindowInput1();

    PCollection<String> input2 =
        p.apply("CreateInput2", Create.of("Input2"))
            .apply("Window2", Window.into(FixedWindows.of(Duration.standardMinutes(2))));

    Assert.assertThrows(IllegalStateException.class, () -> { // Optimized by LLM: Used Assert.assertThrows() instead of try-catch block
      PCollectionList.of(input1).and(input2).apply(Flatten.pCollections());
    });
  }

  @Test
  public void testFlattenGetName() {
    Assert.assertEquals("Flatten.Iterables", Flatten.<String>iterables().getName());
    Assert.assertEquals("Flatten.PCollections", Flatten.<String>pCollections().getName());
  }

  private static class IdentityFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }

  private PCollectionList<String> makePCollectionListOfStrings(
      Pipeline p, List<List<String>> lists) {
    return makePCollectionList(p, StringUtf8Coder.of(), lists);
  }

  private <T> PCollectionList<T> makePCollectionList(
      Pipeline p, Coder<T> coder, List<List<T>> lists) {
    List<PCollection<T>> pcs = Arrays.stream(lists) // Optimized by LLM: Used Stream to simplify creation of PCollection instances
        .map(list -> p.apply("Create" + index++, Create.of(list).withCoder(coder)))
        .collect(Collectors.toList());
    return PCollectionList.of(pcs);
  }

  private <T> List<T> flattenLists(List<List<T>> lists) {
    return lists.stream() // Optimized by LLM: Used Stream and flatMap to simplify flattening of lists
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }
}