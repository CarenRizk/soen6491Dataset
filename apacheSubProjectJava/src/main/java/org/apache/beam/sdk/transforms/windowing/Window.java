/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
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

  
  public WindowingStrategy<?, ?> getOutputStrategyInternal(WindowingStrategy<?, ?> inputStrategy) {
    WindowingStrategy<?, ?> result = inputStrategy;
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
              Ordering.natural().max(getAllowedLateness(), inputStrategy.getAllowedLateness()));
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

  private void applicableTo(PCollection<?> input) {
    WindowingStrategy<?, ?> outputStrategy =
        getOutputStrategyInternal(input.getWindowingStrategy());

    // Make sure that the windowing strategy is complete & valid.
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

  private boolean canProduceMultiplePanes(WindowingStrategy<?, ?> strategy) {
    // The default trigger is Repeatedly.forever(AfterWatermark.pastEndOfWindow()); This fires
    // for every late-arriving element if allowed lateness is nonzero, and thus we must have
    // an accumulating mode specified
    boolean dataCanArriveLate =
        !(strategy.getWindowFn() instanceof GlobalWindows)
            && strategy.getAllowedLateness().getMillis() > 0;
    boolean hasCustomTrigger = !(strategy.getTrigger() instanceof DefaultTrigger);
    return dataCanArriveLate || hasCustomTrigger;
  }

  @Override
  public PCollection<T> expand(PCollection<T> input) {
    applicableTo(input);

    WindowingStrategy<?, ?> outputStrategy =
        getOutputStrategyInternal(input.getWindowingStrategy());

    if (getWindowFn() == null) {
      // A new PCollection must be created in case input is reused in a different location as the
      // two PCollections will, in general, have a different windowing strategy.
      return PCollectionList.of(input)
          .apply(Flatten.pCollections())
          .setWindowingStrategyInternal(outputStrategy);
    } else {
      // This is the AssignWindows primitive
      return input.apply(new Assign<>(this, outputStrategy));
    }
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);

    if (getWindowFn() != null) {
      builder
          .add(
              DisplayData.item("windowFn", getWindowFn().getClass())
                  .withLabel("Windowing Function"))
          .include("windowFn", getWindowFn());
    }

    if (getAllowedLateness() != null) {
      builder.addIfNotDefault(
          DisplayData.item("allowedLateness", getAllowedLateness()).withLabel("Allowed Lateness"),
          Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()));
    }

    if (getTrigger() != null && !(getTrigger() instanceof DefaultTrigger)) {
      builder.add(DisplayData.item("trigger", getTrigger().toString()).withLabel("Trigger"));
    }

    if (getAccumulationMode() != null) {
      builder.add(
          DisplayData.item("accumulationMode", getAccumulationMode().toString())
              .withLabel("Accumulation Mode"));
    }

    if (getClosingBehavior() != null) {
      builder.add(
          DisplayData.item("closingBehavior", getClosingBehavior().toString())
              .withLabel("Window Closing Behavior"));
    }

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
    @Override
    public PCollection<T> expand(PCollection<T> input) {
      return input
          // We first apply a (trivial) transform to the input PCollection to produce a new
          // PCollection. This ensures that we don't modify the windowing strategy of the input
          // which may be used elsewhere.
          .apply(
              "Identity",
              MapElements.via(
                  new SimpleFunction<T, T>() {
                    @Override
                    public T apply(T element) {
                      return element;
                    }
                  }))
          // Then we modify the windowing strategy.
          .setWindowingStrategyInternal(input.getWindowingStrategy().withAlreadyMerged(false));
    }
  }
}
