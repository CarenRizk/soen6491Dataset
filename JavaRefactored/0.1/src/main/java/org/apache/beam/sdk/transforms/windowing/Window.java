package org.apache.beam.sdk.transforms.windowing;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Ordering;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

@AutoValue
@SuppressWarnings({
  "nullness", 
  "rawtypes"
})
public abstract class Window<T> extends PTransform<PCollection<T>, PCollection<T>> {

  
  public enum ClosingBehavior {
    
    FIRE_ALWAYS,
    
    FIRE_IF_NON_EMPTY
  }

  
  public enum OnTimeBehavior {
    
    FIRE_ALWAYS,
    
    FIRE_IF_NON_EMPTY
  }

  
  public static <T> Window<T> into(WindowFn<? super T, ?> fn) {
    try {
      fn.windowCoder().verifyDeterministic();
    } catch (NonDeterministicException e) {
      throw new IllegalArgumentException("Window coders must be deterministic.", e);
    }
    return Window.<T>configure().withWindowFn(fn);
  }

  
  public static <T> Window<T> configure() {
    return new AutoValue_Window.Builder<T>().build();
  }

  public abstract @Nullable WindowFn<? super T, ?> getWindowFn();

  abstract @Nullable Trigger getTrigger();

  abstract @Nullable AccumulationMode getAccumulationMode();

  abstract @Nullable Duration getAllowedLateness();

  abstract @Nullable ClosingBehavior getClosingBehavior();

  abstract @Nullable OnTimeBehavior getOnTimeBehavior();

  abstract @Nullable TimestampCombiner getTimestampCombiner();

  abstract Builder<T> toBuilder();

  @AutoValue.Builder
  abstract static class Builder<T> {
    abstract Builder<T> setWindowFn(WindowFn<? super T, ?> windowFn);

    abstract Builder<T> setTrigger(Trigger trigger);

    abstract Builder<T> setAccumulationMode(AccumulationMode mode);

    abstract Builder<T> setAllowedLateness(Duration allowedLateness);

    abstract Builder<T> setClosingBehavior(ClosingBehavior closingBehavior);

    abstract Builder<T> setOnTimeBehavior(OnTimeBehavior onTimeBehavior);

    abstract Builder<T> setTimestampCombiner(TimestampCombiner timestampCombiner);

    abstract Window<T> build();
  }

  private Window<T> withWindowFn(WindowFn<? super T, ?> windowFn) {
    return toBuilder().setWindowFn(windowFn).build();
  }

  
  public Window<T> triggering(Trigger trigger) {
    return toBuilder().setTrigger(trigger).build();
  }

  
  public Window<T> discardingFiredPanes() {
    return toBuilder().setAccumulationMode(AccumulationMode.DISCARDING_FIRED_PANES).build();
  }

  
  public Window<T> accumulatingFiredPanes() {
    return toBuilder().setAccumulationMode(AccumulationMode.ACCUMULATING_FIRED_PANES).build();
  }

  
  public Window<T> withAllowedLateness(Duration allowedLateness) {
    return toBuilder().setAllowedLateness(allowedLateness).build();
  }

  
  public Window<T> withTimestampCombiner(TimestampCombiner timestampCombiner) {
    return toBuilder().setTimestampCombiner(timestampCombiner).build();
  }

  
  public Window<T> withAllowedLateness(Duration allowedLateness, ClosingBehavior behavior) {
    return toBuilder().setAllowedLateness(allowedLateness).setClosingBehavior(behavior).build();
  }

  
  public Window<T> withOnTimeBehavior(OnTimeBehavior behavior) {
    return toBuilder().setOnTimeBehavior(behavior).build();
  }

  
  // Optimized by LLM: Extracted repeated logic into a separate private method
  private WindowingStrategy<?, ?> applyOutputStrategy(WindowingStrategy<?, ?> result) {
    if (getWindowFn() != null) {
      result = result.withAlreadyMerged(false).withWindowFn(getWindowFn());
    }
    if (getTrigger() != null) {
      result = result.withTrigger(getTrigger());
    }
    if (getAccumulationMode() != null) {
      result = result.withMode(getAccumulationMode());
    }
    if (getAllowedLateness() != null) {
      result =
          result.withAllowedLateness(
              Ordering.natural().max(getAllowedLateness(), result.getAllowedLateness()));
    }
    if (getClosingBehavior() != null) {
      result = result.withClosingBehavior(getClosingBehavior());
    }
    if (getOnTimeBehavior() != null) {
      result = result.withOnTimeBehavior(getOnTimeBehavior());
    }
    if (getTimestampCombiner() != null) {
      result = result.withTimestampCombiner(getTimestampCombiner());
    }
    return result;
  }

  // Optimized by LLM: Refactored getOutputStrategyInternal to use applyOutputStrategy
  public WindowingStrategy<?, ?> getOutputStrategyInternal(WindowingStrategy<?, ?> inputStrategy) {
    return applyOutputStrategy(inputStrategy);
  }

  private void applicableTo(PCollection<?> input) {
    WindowingStrategy<?, ?> outputStrategy =
        getOutputStrategyInternal(input.getWindowingStrategy());

    
    if (outputStrategy.isTriggerSpecified()
        && !(outputStrategy.getTrigger() instanceof DefaultTrigger)
        && !(outputStrategy.getWindowFn() instanceof GlobalWindows)
        && !outputStrategy.isAllowedLatenessSpecified()) {
      throw new IllegalArgumentException(
          "Except when using GlobalWindows,"
              + " calling .triggering() to specify a trigger requires that the allowed lateness"
              + " be specified using .withAllowedLateness() to set the upper bound on how late"
              + " data can arrive before being dropped. See Javadoc for more details.");
    }

    if (!outputStrategy.isModeSpecified() && canProduceMultiplePanes(outputStrategy)) {
      throw new IllegalArgumentException(
          "Calling .triggering() to specify a trigger or calling .withAllowedLateness() to"
              + " specify an allowed lateness greater than zero requires that the accumulation"
              + " mode be specified using .discardingFiredPanes() or .accumulatingFiredPanes()."
              + " See Javadoc for more details.");
    }
  }

  // Optimized by LLM: Simplified canProduceMultiplePanes with early returns
  private boolean canProduceMultiplePanes(WindowingStrategy<?, ?> strategy) {
    if (strategy.getWindowFn() instanceof GlobalWindows) {
      return false;
    }
    if (strategy.getAllowedLateness().getMillis() <= 0) {
      return false;
    }
    return !(strategy.getTrigger() instanceof DefaultTrigger);
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    applicableTo(input);

    WindowingStrategy<?, ?> outputStrategy =
        getOutputStrategyInternal(input.getWindowingStrategy());

    if (getWindowFn() == null) {
      
      
      return PCollectionList.of(input)
          .apply(Flatten.pCollections())
          .setWindowingStrategyInternal(outputStrategy);
    } else {
      
      return input.apply(new Assign<>(this, outputStrategy));
    }
  }

  // Optimized by LLM: Extracted conditional blocks into private helper methods
  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    addWindowFnDisplayData(builder);
    addAllowedLatenessDisplayData(builder);
    addTriggerDisplayData(builder);
    addAccumulationModeDisplayData(builder);
    addClosingBehaviorDisplayData(builder);
    addTimestampCombinerDisplayData(builder);
  }

  private void addWindowFnDisplayData(DisplayData.Builder builder) {
    if (getWindowFn() != null) {
      builder
          .add(
              DisplayData.item("windowFn", getWindowFn().getClass())
                  .withLabel("Windowing Function"))
          .include("windowFn", getWindowFn());
    }
  }

  private void addAllowedLatenessDisplayData(DisplayData.Builder builder) {
    if (getAllowedLateness() != null) {
      builder.addIfNotDefault(
          DisplayData.item("allowedLateness", getAllowedLateness()).withLabel("Allowed Lateness"),
          Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));
    }
  }

  private void addTriggerDisplayData(DisplayData.Builder builder) {
    if (getTrigger() != null && !(getTrigger() instanceof DefaultTrigger)) {
      builder.add(DisplayData.item("trigger", getTrigger().toString()).withLabel("Trigger"));
    }
  }

  private void addAccumulationModeDisplayData(DisplayData.Builder builder) {
    if (getAccumulationMode() != null) {
      builder.add(
          DisplayData.item("accumulationMode", getAccumulationMode().toString())
              .withLabel("Accumulation Mode"));
    }
  }

  private void addClosingBehaviorDisplayData(DisplayData.Builder builder) {
    if (getClosingBehavior() != null) {
      builder.add(
          DisplayData.item("closingBehavior", getClosingBehavior().toString())
              .withLabel("Window Closing Behavior"));
    }
  }

  private void addTimestampCombinerDisplayData(DisplayData.Builder builder) {
    if (getTimestampCombiner() != null) {
      builder.add(
          DisplayData.item("timestampCombiner", getTimestampCombiner().toString())
              .withLabel("Timestamp Combiner"));
    }
  }

  @Override
  protected String getKindString() {
    return "Window.Into()";
  }

  
  public static class Assign<T> extends PTransform<PCollection<T>, PCollection<T>> {
    private final @Nullable Window<T> original;
    private final WindowingStrategy<T, ?> updatedStrategy;

    
    @VisibleForTesting
    Assign(@Nullable Window<T> original, WindowingStrategy updatedStrategy) {
      this.original = original;
      this.updatedStrategy = updatedStrategy;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(), updatedStrategy, input.isBounded(), input.getCoder());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      if (original != null) {
        original.populateDisplayData(builder);
      }
    }

    public @Nullable WindowFn<T, ?> getWindowFn() {
      return updatedStrategy.getWindowFn();
    }

    public static <T> Assign<T> createInternal(WindowingStrategy finalStrategy) {
      return new Assign<T>(null, finalStrategy);
    }
  }

  
  public static <T> Remerge<T> remerge() {
    return new Remerge<>();
  }

  
  private static class Remerge<T> extends PTransform<PCollection<T>, PCollection<T>> {
    private static final String IDENTITY_LABEL = "Identity"; // Optimized by LLM: Used constant for string literal

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      return input
          
          
          
          .apply(
              IDENTITY_LABEL,
              MapElements.via(
                  new SimpleFunction<T, T>() {
                    @Override
                    public T apply(T element) {
                      return element;
                    }
                  }))
          
          .setWindowingStrategyInternal(input.getWindowingStrategy().withAlreadyMerged(false));
    }
  }
}