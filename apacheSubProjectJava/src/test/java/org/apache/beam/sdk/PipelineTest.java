package org.apache.beam.sdk;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.testing.CrashingRunner;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", 
})
public class PipelineTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public transient Timeout globalTimeout = Timeout.seconds(1200);

  
  
  static class TestPipelineRunnerThrowingUserException extends PipelineRunner<PipelineResult> {

    public static TestPipelineRunnerThrowingUserException fromOptions(PipelineOptions options) {
      return new TestPipelineRunnerThrowingUserException();
    }

    @Override
    public PipelineResult run(Pipeline pipeline) {
      Throwable t = new IllegalStateException("user code exception");
      throw UserCodeException.wrap(t);
    }
  }

  
  
  static class TestPipelineRunnerThrowingSdkException extends PipelineRunner<PipelineResult> {

    public static TestPipelineRunnerThrowingSdkException fromOptions(PipelineOptions options) {
      return new TestPipelineRunnerThrowingSdkException();
    }

    @Override
    public PipelineResult run(Pipeline pipeline) {
      throw new IllegalStateException("SDK exception");
    }
  }

  @Test
  public void testConflictingNames() {
    final PipelineOptions options = TestPipeline.testingPipelineOptions();
    final Pipeline p = Pipeline.create(options);

    
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        new BaseMatcher<String>() { 
          @Override
          public void describeTo(final Description description) {
            description.appendText(
                "validates the conflicting instances are " + "listed into the exception message");
          }

          @Override
          public boolean matches(final Object o) {
            
            final String sanitized =
                String.class.cast(o).replaceAll("\\$[\\p{Alnum}]+@[\\p{Alnum}]+", "\\$x@y");
            return sanitized.contains(
                "Conflicting instances:\n"
                    + "- name=ParDo(Anonymous):\n"
                    + "    - org.apache.beam.sdk.PipelineTest$x@y\n"
                    + "    - org.apache.beam.sdk.PipelineTest$x@y\n\n"
                    + "You can fix it adding a name when you call apply(): "
                    + "pipeline.apply(<name>, <transform>).");
          }
        });
    p.apply(Create.of("a"))
        
        .apply(
            ParDo.of(
                new DoFn<String, String>() {
                  @ProcessElement
                  public void onElement(final ProcessContext ctx) {
                    ctx.output(ctx.element());
                  }
                }))
        .apply(
            ParDo.of(
                new DoFn<String, String>() {
                  @ProcessElement
                  public void onElement(final ProcessContext ctx) {
                    
                  }
                }));
    p.run();
  }

  @Test
  public void testPipelineUserExceptionHandling() {
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    options.setRunner(TestPipelineRunnerThrowingUserException.class);
    Pipeline p = Pipeline.create(options);

    
    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(isA(IllegalStateException.class));
    thrown.expectMessage("user code exception");
    p.run();
  }

  @Test
  public void testPipelineSDKExceptionHandling() {
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    options.setRunner(TestPipelineRunnerThrowingSdkException.class);
    Pipeline p = Pipeline.create(options);

    
    try {
      p.run();
      fail("Should have thrown an exception.");
    } catch (RuntimeException exn) {
      
      assertThat(exn, not(instanceOf(UserCodeException.class)));
      
      assertThat(exn.getMessage(), containsString("SDK exception"));
      
      assertThat(exn, instanceOf(IllegalStateException.class));
    }
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testMultipleApply() {
    PTransform<PCollection<? extends String>, PCollection<String>> myTransform = addSuffix("+");

    PCollection<String> input = pipeline.apply(Create.of(ImmutableList.of("a", "b")));

    PCollection<String> left = input.apply("Left1", myTransform).apply("Left2", myTransform);
    PCollection<String> right = input.apply("Right", myTransform);

    PCollection<String> both = PCollectionList.of(left).and(right).apply(Flatten.pCollections());

    PAssert.that(both).containsInAnyOrder("a++", "b++", "a+", "b+");

    pipeline.run();
  }

  private static PTransform<PCollection<? extends String>, PCollection<String>> addSuffix(
      final String suffix) {
    return MapElements.via(
        new SimpleFunction<String, String>() {
          @Override
          public String apply(String input) {
            return input + suffix;
          }
        });
  }

  @Test
  public void testToString() {
    PipelineOptions options = PipelineOptionsFactory.as(PipelineOptions.class);
    options.setRunner(CrashingRunner.class);
    Pipeline pipeline = Pipeline.create(options);
    assertEquals("Pipeline#" + pipeline.hashCode(), pipeline.toString());
  }

  @Test
  public void testStableUniqueNameOff() {
    testStableUniqueNameOffextracted(CheckEnabled.OFF);
  }

  @Test
  public void testStableUniqueNameWarning() {
    testStableUniqueNameOffextracted(CheckEnabled.WARNING);
  }

  @Test
  public void testStableUniqueNameError() {
    pipeline.getOptions().setStableUniqueNames(CheckEnabled.ERROR);

    pipeline.apply(Create.of(5, 6, 7));

    thrown.expectMessage("do not have stable unique names");
    pipeline.apply(Create.of(5, 6, 7));
    ((Pipeline) pipeline).validate(pipeline.getOptions());
  }

  
  @Test
  @Category(ValidatesRunner.class)
  public void testIdentityTransform() {

    PCollection<Integer> output =
        pipeline.apply(Create.of(1, 2, 3, 4)).apply("IdentityTransform", new IdentityTransform<>());

    PAssert.that(output).containsInAnyOrder(1, 2, 3, 4);
    pipeline.run();
  }

  private static class IdentityTransform<T extends PInput & POutput> extends PTransform<T, T> {
    @Override
    public T expand(T input) {
      return input;
    }
  }

  
  @Test
  @Category(ValidatesRunner.class)
  public void testTupleProjectionTransform() throws Exception {
    PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4));

    TupleTag<Integer> tag = new TupleTag<>();
    PCollectionTuple tuple = PCollectionTuple.of(tag, input);

    PCollection<Integer> output = tuple.apply("ProjectTag", new TupleProjectionTransform<>(tag));

    PAssert.that(output).containsInAnyOrder(1, 2, 3, 4);
    pipeline.run();
  }

  private static class TupleProjectionTransform<T>
      extends PTransform<PCollectionTuple, PCollection<T>> {
    private TupleTag<T> tag;

    public TupleProjectionTransform(TupleTag<T> tag) {
      this.tag = tag;
    }

    @Override
    public PCollection<T> expand(PCollectionTuple input) {
      return input.get(tag);
    }
  }

  
  @Test
  @Category(ValidatesRunner.class)
  public void testTupleInjectionTransform() throws Exception {
    PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3, 4));

    TupleTag<Integer> tag = new TupleTag<>();

    PCollectionTuple output = input.apply("ProjectTag", new TupleInjectionTransform<>(tag));

    PAssert.that(output.get(tag)).containsInAnyOrder(1, 2, 3, 4);
    pipeline.run();
  }

  private static class TupleInjectionTransform<T>
      extends PTransform<PCollection<T>, PCollectionTuple> {
    private TupleTag<T> tag;

    public TupleInjectionTransform(TupleTag<T> tag) {
      this.tag = tag;
    }

    @Override
    public PCollectionTuple expand(PCollection<T> input) {
      return PCollectionTuple.of(tag, input);
    }
  }

  
  @Test
  @Category(NeedsRunner.class)
  public void testEmptyPipeline() throws Exception {
    pipeline.run();
  }

  @Test
  public void testReplaceAll() {
    pipeline.enableAbandonedNodeEnforcement(false);
    pipeline.apply("unbounded", GenerateSequence.from(0));
    pipeline.apply("bounded", GenerateSequence.from(0).to(100));

    pipeline.replaceAll(
        ImmutableList.of(
            PTransformOverride.of(
                application -> application.getTransform() instanceof GenerateSequence,
                new GenerateSequenceToCreateOverride()),
            PTransformOverride.of(
                application -> application.getTransform() instanceof Create.Values,
                new CreateValuesToEmptyFlattenOverride())));
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(Node node) {
            String fullName = node.getFullName();
            if (fullName.equals("unbounded") || fullName.equals("bounded")) {
              assertThat(node.getTransform(), Matchers.instanceOf(EmptyFlatten.class));
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }
        });
  }

  
  @Test
  public void testReplaceAllIncomplete() {
    pipeline.enableAbandonedNodeEnforcement(false);
    pipeline.apply(GenerateSequence.from(0));

    
    pipeline.replaceAll(
        ImmutableList.of(
            PTransformOverride.of(
                application -> application.getTransform() instanceof Create.Values,
                new CreateValuesToEmptyFlattenOverride()),
            PTransformOverride.of(
                application -> application.getTransform() instanceof GenerateSequence,
                new GenerateSequenceToCreateOverride())));
  }

  @Test
  public void testReplaceWithExistingName() {
    pipeline.enableAbandonedNodeEnforcement(false);
    final PCollection<Integer> originalInput = pipeline.apply(Create.of(1, 2, 3));
    class OriginalTransform extends PTransform<PCollection<Integer>, PCollection<Integer>> {
      @Override
      public PCollection<Integer> expand(PCollection<Integer> input) {
        return input.apply("custom_name", Sum.integersGlobally());
      }
    }

    class ReplacementTransform extends PTransform<PCollection<Integer>, PCollection<Integer>> {
      @Override
      public PCollection<Integer> expand(PCollection<Integer> input) {
        return input.apply("custom_name", Max.integersGlobally());
      }
    }

    class ReplacementOverrideFactory
        implements PTransformOverrideFactory<
            PCollection<Integer>, PCollection<Integer>, OriginalTransform> {

      @Override
      public PTransformReplacement<PCollection<Integer>, PCollection<Integer>>
          getReplacementTransform(
              AppliedPTransform<PCollection<Integer>, PCollection<Integer>, OriginalTransform>
                  transform) {
        return PTransformReplacement.of(originalInput, new ReplacementTransform());
      }

      @Override
      public Map<PCollection<?>, ReplacementOutput> mapOutputs(
          Map<TupleTag<?>, PCollection<?>> outputs, PCollection<Integer> newOutput) {
        return Collections.singletonMap(
            newOutput,
            ReplacementOutput.of(
                TaggedPValue.ofExpandedValue(Iterables.getOnlyElement(outputs.values())),
                TaggedPValue.ofExpandedValue(newOutput)));
      }
    }

    class OriginalMatcher implements PTransformMatcher {
      @Override
      public boolean matches(AppliedPTransform<?, ?, ?> application) {
        return application.getTransform() instanceof OriginalTransform;
      }
    }

    originalInput.apply("original_application", new OriginalTransform());
    pipeline.replaceAll(
        Collections.singletonList(
            PTransformOverride.of(new OriginalMatcher(), new ReplacementOverrideFactory())));
    final Map<String, Class<?>> nameToTransformClass = new HashMap<>();
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public void leaveCompositeTransform(Node node) {
            if (!node.isRootNode()) {
              nameToTransformClass.put(node.getFullName(), node.getTransform().getClass());
            }
          }

          @Override
          public void visitPrimitiveTransform(Node node) {
            nameToTransformClass.put(node.getFullName(), node.getTransform().getClass());
          }
        });

    assertThat(nameToTransformClass.keySet(), hasItem("original_application/custom_name"));
    assertThat(nameToTransformClass.keySet(), not(hasItem("original_application/custom_name2")));
    assertEquals(
        nameToTransformClass.get("original_application/custom_name"),
        Max.integersGlobally().getClass());
  }

  static class GenerateSequenceToCreateOverride
      implements PTransformOverrideFactory<PBegin, PCollection<Long>, GenerateSequence> {
    @Override
    public PTransformReplacement<PBegin, PCollection<Long>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<Long>, GenerateSequence> transform) {
      return PTransformReplacement.of(transform.getPipeline().begin(), Create.of(0L));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<Long> newOutput) {
      Map.Entry<TupleTag<?>, PCollection<?>> original =
          Iterables.getOnlyElement(outputs.entrySet());
      Map.Entry<TupleTag<?>, PCollection<?>> replacement =
          (Map.Entry) Iterables.getOnlyElement(newOutput.expand().entrySet());
      return Collections.singletonMap(
          newOutput,
          ReplacementOutput.of(
              TaggedPValue.of(original.getKey(), original.getValue()),
              TaggedPValue.of(replacement.getKey(), replacement.getValue())));
    }
  }

  private static class EmptyFlatten<T> extends PTransform<PBegin, PCollection<T>> {
    @Override
    public PCollection<T> expand(PBegin input) {
      PCollectionList<T> empty = PCollectionList.empty(input.getPipeline());
      return empty.apply(Flatten.pCollections());
    }
  }

  static class CreateValuesToEmptyFlattenOverride<T>
      implements PTransformOverrideFactory<PBegin, PCollection<T>, Create.Values<T>> {

    @Override
    public PTransformReplacement<PBegin, PCollection<T>> getReplacementTransform(
        AppliedPTransform<PBegin, PCollection<T>, Create.Values<T>> transform) {
      return PTransformReplacement.of(transform.getPipeline().begin(), new EmptyFlatten<T>());
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<T> newOutput) {
      Map.Entry<TupleTag<?>, PCollection<?>> original =
          Iterables.getOnlyElement(outputs.entrySet());
      Map.Entry<TupleTag<?>, PCollection<?>> replacement =
          (Map.Entry) Iterables.getOnlyElement(newOutput.expand().entrySet());
      return Collections.singletonMap(
          newOutput,
          ReplacementOutput.of(
              TaggedPValue.of(original.getKey(), original.getValue()),
              TaggedPValue.of(replacement.getKey(), replacement.getValue())));
    }
  }

private void testStableUniqueNameOffextracted(PipelineOptions.CheckEnabled arg0) {
	pipeline.enableAbandonedNodeEnforcement(false);
	pipeline.getOptions().setStableUniqueNames(arg0);
	pipeline.apply(Create.of(5, 6, 7));
	pipeline.apply(Create.of(5, 6, 7));
	((Pipeline) pipeline).validate(pipeline.getOptions());
}
}
