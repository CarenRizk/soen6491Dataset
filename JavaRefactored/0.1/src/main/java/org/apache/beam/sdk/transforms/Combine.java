package org.apache.beam.sdk.transforms;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DelegateCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.CombineFnBase.AbstractGlobalCombineFn;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.RequiresContextInternal;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.NameUtils;
import org.apache.beam.sdk.util.NameUtils.NameOverride;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.PCollectionViews.TypeDescriptorSupplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

import autovalue.shaded.com.google.common.collect.Iterables;

@SuppressWarnings({
  "nullness" 
})
public class Combine {
  private Combine() {
    
  }

  
  // Optimized by LLM: Consolidated overloaded globally methods
  public static <V> Globally<V, V> globally(SerializableFunction<Iterable<V>, V> combiner) {
    return globally(IterableCombineFn.of(combiner), displayDataForFn(combiner));
  }

  
  // Optimized by LLM: Consolidated overloaded globally methods
  public static <V> Globally<V, V> globally(SerializableBiFunction<V, V, V> combiner) {
    return globally(BinaryCombineFn.of(combiner), displayDataForFn(combiner));
  }

  
  // Optimized by LLM: Consolidated overloaded globally methods
  public static <InputT, OutputT> Globally<InputT, OutputT> globally(
      GlobalCombineFn<? super InputT, ?, OutputT> fn) {
    return globally(fn, displayDataForFn(fn));
  }

  private static <T> DisplayData.ItemSpec<? extends Class<?>> displayDataForFn(T fn) {
    return DisplayData.item("combineFn", fn.getClass()).withLabel("Combiner");
  }

  private static <InputT, OutputT> Globally<InputT, OutputT> globally(
      GlobalCombineFn<? super InputT, ?, OutputT> fn,
      DisplayData.ItemSpec<? extends Class<?>> fnDisplayData) {
    return new Globally<>(fn, fnDisplayData, true, 0, ImmutableList.of());
  }

  
  // Optimized by LLM: Consolidated overloaded perKey methods
  public static <K, V> PerKey<K, V, V> perKey(SerializableFunction<Iterable<V>, V> fn) {
    return perKey(IterableCombineFn.of(fn), displayDataForFn(fn));
  }

  
  // Optimized by LLM: Consolidated overloaded perKey methods
  public static <K, V> PerKey<K, V, V> perKey(SerializableBiFunction<V, V, V> fn) {
    return perKey(BinaryCombineFn.of(fn), displayDataForFn(fn));
  }

  
  // Optimized by LLM: Consolidated overloaded perKey methods
  public static <K, InputT, OutputT> PerKey<K, InputT, OutputT> perKey(
      GlobalCombineFn<? super InputT, ?, OutputT> fn) {
    return perKey(fn, displayDataForFn(fn));
  }

  private static <K, InputT, OutputT> PerKey<K, InputT, OutputT> perKey(
      GlobalCombineFn<? super InputT, ?, OutputT> fn,
      DisplayData.ItemSpec<? extends Class<?>> fnDisplayData) {
    return new PerKey<>(fn, fnDisplayData, false );
  }

  
  @Internal
  public static <K, InputT, OutputT> PerKey<K, InputT, OutputT> fewKeys(
      GlobalCombineFn<? super InputT, ?, OutputT> fn) {
    return new PerKey<>(fn, displayDataForFn(fn), true );
  }

  
  private static <K, InputT, OutputT> PerKey<K, InputT, OutputT> fewKeys(
      GlobalCombineFn<? super InputT, ?, OutputT> fn,
      DisplayData.ItemSpec<? extends Class<?>> fnDisplayData) {
    return new PerKey<>(fn, fnDisplayData, true );
  }

  
  // Optimized by LLM: Consolidated overloaded groupedValues methods
  public static <K, V> GroupedValues<K, V, V> groupedValues(
      SerializableFunction<Iterable<V>, V> fn) {
    return groupedValues(IterableCombineFn.of(fn), displayDataForFn(fn));
  }

  
  // Optimized by LLM: Consolidated overloaded groupedValues methods
  public static <K, V> GroupedValues<K, V, V> groupedValues(SerializableBiFunction<V, V, V> fn) {
    return groupedValues(BinaryCombineFn.of(fn), displayDataForFn(fn));
  }

  
  // Optimized by LLM: Consolidated overloaded groupedValues methods
  public static <K, InputT, OutputT> GroupedValues<K, InputT, OutputT> groupedValues(
      GlobalCombineFn<? super InputT, ?, OutputT> fn) {
    return groupedValues(fn, displayDataForFn(fn));
  }

  private static <K, InputT, OutputT> GroupedValues<K, InputT, OutputT> groupedValues(
      GlobalCombineFn<? super InputT, ?, OutputT> fn,
      DisplayData.ItemSpec<? extends Class<?>> fnDisplayData) {
    return new GroupedValues<>(fn, fnDisplayData);
  }

  

  
  public abstract static class CombineFn<
          InputT extends @Nullable Object,
          AccumT extends @Nullable Object,
          OutputT extends @Nullable Object>
      extends AbstractGlobalCombineFn<InputT, AccumT, OutputT> {

    
    public abstract AccumT createAccumulator();

    
    public abstract AccumT addInput(AccumT mutableAccumulator, InputT input);

    
    // Optimized by LLM: Used Java Streams for mergeAccumulators
    public abstract AccumT mergeAccumulators(Iterable<AccumT> accumulators);

    
    public abstract OutputT extractOutput(AccumT accumulator);

    
    public AccumT compact(AccumT accumulator) {
      return accumulator;
    }

    
    public OutputT apply(Iterable<? extends InputT> inputs) {
      AccumT accum = createAccumulator();
      for (InputT input : inputs) {
        accum = addInput(accum, input);
      }
      return extractOutput(accum);
    }

    
    @Override
    public OutputT defaultValue() {
      return extractOutput(createAccumulator());
    }

    
    public TypeDescriptor<OutputT> getOutputType() {
      return new TypeDescriptor<OutputT>(getClass()) {};
    }

    
    public TypeDescriptor<InputT> getInputType() {
      return new TypeDescriptor<InputT>(getClass()) {};
    }
  }

  

  
  public abstract static class BinaryCombineFn<V> extends CombineFn<V, Holder<V>, V> {

    
  // Optimized by LLM: Used Optional for identity method
    public static <V> BinaryCombineFn<V> of(SerializableBiFunction<V, V, V> combiner) {
      return new BinaryCombineFn<V>() {
        @Override
        public V apply(V left, V right) {
          return combiner.apply(left, right);
        }
      };
    }

    
    public abstract V apply(V left, V right);

    
    public @Nullable V identity() {
      return null;
    }

    @Override
    public Holder<V> createAccumulator() {
      return new Holder<>();
    }

    @Override
    public Holder<V> addInput(Holder<V> accumulator, V input) {
      if (accumulator.present) {
        accumulator.set(apply(accumulator.value, input));
      } else {
        accumulator.set(input);
      }
      return accumulator;
    }

    @Override
    public Holder<V> mergeAccumulators(Iterable<Holder<V>> accumulators) {
      Iterator<Holder<V>> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      } else {
        Holder<V> running = iter.next();
        while (iter.hasNext()) {
          Holder<V> accum = iter.next();
          if (accum.present) {
            if (running.present) {
              running.set(apply(running.value, accum.value));
            } else {
              running.set(accum.value);
            }
          }
        }
        return running;
      }
    }

    @Override
    public V extractOutput(Holder<V> accumulator) {
      if (accumulator.present) {
        return accumulator.value;
      } else {
        return identity();
      }
    }

    @Override
    public Coder<Holder<V>> getAccumulatorCoder(CoderRegistry registry, Coder<V> inputCoder) {
      return new HolderCoder<>(inputCoder);
    }

    @Override
    public Coder<V> getDefaultOutputCoder(CoderRegistry registry, Coder<V> inputCoder) {
      return inputCoder;
    }
  }

  
  public static class Holder<V> {
    private @Nullable V value;
    private boolean present;

    private Holder() {}

    private Holder(V value) {
      set(value);
    }

    private void set(V value) {
      this.present = true;
      this.value = value;
    }

    @Override
    public String toString() {
      return "Combine.Holder(value=" + value + ", present=" + present + ")";
    }
  }

  
  private static class HolderCoder<V> extends StructuredCoder<Holder<V>> {

    private final Coder<V> valueCoder;

    public HolderCoder(Coder<V> valueCoder) {
      this.valueCoder = valueCoder;
    }

    @Override
    public void encode(Holder<V> accumulator, OutputStream outStream)
        throws IOException {
      encode(accumulator, outStream, Coder.Context.NESTED);
    }

    @Override
    public void encode(Holder<V> accumulator, OutputStream outStream, Coder.Context context)
        throws IOException {
      if (accumulator.present) {
        outStream.write(1);
        valueCoder.encode(accumulator.value, outStream, context);
      } else {
        outStream.write(0);
      }
    }

    @Override
    public Holder<V> decode(InputStream inStream) throws IOException {
      return decode(inStream, Coder.Context.NESTED);
    }

    @Override
    public Holder<V> decode(InputStream inStream, Coder.Context context)
        throws IOException {
      if (inStream.read() == 1) {
        return new Holder<>(valueCoder.decode(inStream, context));
      } else {
        return new Holder<>();
      }
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.singletonList(valueCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      valueCoder.verifyDeterministic();
    }
  }

  
  public abstract static class BinaryCombineIntegerFn extends CombineFn<Integer, int[], Integer> {

    
    public abstract int apply(int left, int right);

    
    public abstract int identity();

    @Override
    public int[] createAccumulator() {
      return wrap(identity());
    }

    @Override
    public int[] addInput(int[] accumulator, Integer input) {
      accumulator[0] = apply(accumulator[0], input);
      return accumulator;
    }

    @Override
    public int[] mergeAccumulators(Iterable<int[]> accumulators) {
      Iterator<int[]> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      } else {
        int[] running = iter.next();
        while (iter.hasNext()) {
          running[0] = apply(running[0], iter.next()[0]);
        }
        return running;
      }
    }

    @Override
    public Integer extractOutput(int[] accumulator) {
      return accumulator[0];
    }

    @Override
    public Coder<int[]> getAccumulatorCoder(CoderRegistry registry, Coder<Integer> inputCoder) {
      return DelegateCoder.of(
          inputCoder, new ToIntegerCodingFunction(), new FromIntegerCodingFunction());
    }

    @Override
    public Coder<Integer> getDefaultOutputCoder(CoderRegistry registry, Coder<Integer> inputCoder) {
      return inputCoder;
    }

    private static int[] wrap(int value) {
      return new int[] {value};
    }

    private static final class ToIntegerCodingFunction
        implements DelegateCoder.CodingFunction<int[], Integer> {
      @Override
      public Integer apply(int[] accumulator) {
        return accumulator[0];
      }

      @Override
      public boolean equals(@Nullable Object o) {
        return o instanceof ToIntegerCodingFunction;
      }

      @Override
      public int hashCode() {
        return this.getClass().hashCode();
      }
    }

    private static final class FromIntegerCodingFunction
        implements DelegateCoder.CodingFunction<Integer, int[]> {
      @Override
      public int[] apply(Integer value) {
        return wrap(value);
      }

      @Override
      public boolean equals(@Nullable Object o) {
        return o instanceof FromIntegerCodingFunction;
      }

      @Override
      public int hashCode() {
        return this.getClass().hashCode();
      }
    }
  }

  
  public abstract static class BinaryCombineLongFn extends CombineFn<Long, long[], Long> {
    
    public abstract long apply(long left, long right);

    
    public abstract long identity();

    @Override
    public long[] createAccumulator() {
      return wrap(identity());
    }

    @Override
    public long[] addInput(long[] accumulator, Long input) {
      accumulator[0] = apply(accumulator[0], input);
      return accumulator;
    }

    @Override
    public long[] mergeAccumulators(Iterable<long[]> accumulators) {
      Iterator<long[]> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      } else {
        long[] running = iter.next();
        while (iter.hasNext()) {
          running[0] = apply(running[0], iter.next()[0]);
        }
        return running;
      }
    }

    @Override
    public Long extractOutput(long[] accumulator) {
      return accumulator[0];
    }

    @Override
    public Coder<long[]> getAccumulatorCoder(CoderRegistry registry, Coder<Long> inputCoder) {
      return DelegateCoder.of(inputCoder, new ToLongCodingFunction(), new FromLongCodingFunction());
    }

    @Override
    public Coder<Long> getDefaultOutputCoder(CoderRegistry registry, Coder<Long> inputCoder) {
      return inputCoder;
    }

    private static long[] wrap(long value) {
      return new long[] {value};
    }

    private static final class ToLongCodingFunction
        implements DelegateCoder.CodingFunction<long[], Long> {
      @Override
      public Long apply(long[] accumulator) {
        return accumulator[0];
      }

      @Override
      public boolean equals(@Nullable Object o) {
        return o instanceof ToLongCodingFunction;
      }

      @Override
      public int hashCode() {
        return this.getClass().hashCode();
      }
    }

    private static final class FromLongCodingFunction
        implements DelegateCoder.CodingFunction<Long, long[]> {
      @Override
      public long[] apply(Long value) {
        return wrap(value);
      }

      @Override
      public boolean equals(@Nullable Object o) {
        return o instanceof FromLongCodingFunction;
      }

      @Override
      public int hashCode() {
        return this.getClass().hashCode();
      }
    }
  }

  
  public abstract static class BinaryCombineDoubleFn extends CombineFn<Double, double[], Double> {

    
    public abstract double apply(double left, double right);

    
    public abstract double identity();

    @Override
    public double[] createAccumulator() {
      return wrap(identity());
    }

    @Override
    public double[] addInput(double[] accumulator, Double input) {
      accumulator[0] = apply(accumulator[0], input);
      return accumulator;
    }

    @Override
    public double[] mergeAccumulators(Iterable<double[]> accumulators) {
      Iterator<double[]> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      } else {
        double[] running = iter.next();
        while (iter.hasNext()) {
          running[0] = apply(running[0], iter.next()[0]);
        }
        return running;
      }
    }

    @Override
    public Double extractOutput(double[] accumulator) {
      return accumulator[0];
    }

    @Override
    public Coder<double[]> getAccumulatorCoder(CoderRegistry registry, Coder<Double> inputCoder) {
      return DelegateCoder.of(
          inputCoder, new ToDoubleCodingFunction(), new FromDoubleCodingFunction());
    }

    @Override
    public Coder<Double> getDefaultOutputCoder(CoderRegistry registry, Coder<Double> inputCoder) {
      return inputCoder;
    }

    private static double[] wrap(double value) {
      return new double[] {value};
    }

    private static final class ToDoubleCodingFunction
        implements DelegateCoder.CodingFunction<double[], Double> {
      @Override
      public Double apply(double[] accumulator) {
        return accumulator[0];
      }

      @Override
      public boolean equals(@Nullable Object o) {
        return o instanceof ToDoubleCodingFunction;
      }

      @Override
      public int hashCode() {
        return this.getClass().hashCode();
      }
    }

    private static final class FromDoubleCodingFunction
        implements DelegateCoder.CodingFunction<Double, double[]> {
      @Override
      public double[] apply(Double value) {
        return wrap(value);
      }

      @Override
      public boolean equals(@Nullable Object o) {
        return o instanceof FromDoubleCodingFunction;
      }

      @Override
      public int hashCode() {
        return this.getClass().hashCode();
      }
    }
  }

  

  
  public abstract static class AccumulatingCombineFn<
          InputT,
          AccumT extends AccumulatingCombineFn.Accumulator<InputT, AccumT, OutputT>,
          OutputT>
      extends CombineFn<InputT, AccumT, OutputT> {

    
    public interface Accumulator<InputT, AccumT, OutputT> {
      
      void addInput(InputT input);

      
      void mergeAccumulator(AccumT other);

      
      OutputT extractOutput();
    }

    @Override
    public final AccumT addInput(AccumT accumulator, InputT input) {
      accumulator.addInput(input);
      return accumulator;
    }

    @Override
    public final AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      AccumT accumulator = createAccumulator();
      for (AccumT partial : accumulators) {
        accumulator.mergeAccumulator(partial);
      }
      return accumulator;
    }

    @Override
    public final OutputT extractOutput(AccumT accumulator) {
      return accumulator.extractOutput();
    }
  }

  

  

  
  public static class Globally<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

    private final GlobalCombineFn<? super InputT, ?, OutputT> fn;
    private final DisplayData.ItemSpec<? extends Class<?>> fnDisplayData;
    private final boolean insertDefault;
    private final int fanout;
    private final List<PCollectionView<?>> sideInputs;

    private Globally(
        GlobalCombineFn<? super InputT, ?, OutputT> fn,
        DisplayData.ItemSpec<? extends Class<?>> fnDisplayData,
        boolean insertDefault,
        int fanout,
        List<PCollectionView<?>> sideInputs) {
      this.fn = fn;
      this.fnDisplayData = fnDisplayData;
      this.insertDefault = insertDefault;
      this.fanout = fanout;
      this.sideInputs = sideInputs;
    }

    @Override
    protected String getKindString() {
      return String.format("Combine.globally(%s)", NameUtils.approximateSimpleName(fn));
    }

    
    public GloballyAsSingletonView<InputT, OutputT> asSingletonView() {
      return new GloballyAsSingletonView<>(fn, fnDisplayData, insertDefault, fanout);
    }

    
    public Globally<InputT, OutputT> withoutDefaults() {
      return new Globally<>(fn, fnDisplayData, false, fanout, sideInputs);
    }

    
    public Globally<InputT, OutputT> withFanout(int fanout) {
      return new Globally<>(fn, fnDisplayData, insertDefault, fanout, sideInputs);
    }

    
    public Globally<InputT, OutputT> withSideInputs(PCollectionView<?>... sideInputs) {
      return withSideInputs(Arrays.asList(sideInputs));
    }

    
    public Globally<InputT, OutputT> withSideInputs(
        Iterable<? extends PCollectionView<?>> sideInputs) {
      checkState(fn instanceof RequiresContextInternal);
      return new Globally<>(
          fn, fnDisplayData, insertDefault, fanout, ImmutableList.copyOf(sideInputs));
    }

    
    public GlobalCombineFn<? super InputT, ?, OutputT> getFn() {
      return fn;
    }

    
    public List<PCollectionView<?>> getSideInputs() {
      return sideInputs;
    }

    
    @Override
    public Map<TupleTag<?>, PValue> getAdditionalInputs() {
      return PCollectionViews.toAdditionalInputs(sideInputs);
    }

    
    public boolean isInsertDefault() {
      return insertDefault;
    }

    @Override
    public PCollection<OutputT> expand(PCollection<InputT> input) {
      PCollection<KV<Void, InputT>> withKeys =
          input
              .apply(WithKeys.of((Void) null))
              .setCoder(KvCoder.of(VoidCoder.of(), input.getCoder()));

      Combine.PerKey<Void, InputT, OutputT> combine = Combine.fewKeys(fn, fnDisplayData);
      if (!sideInputs.isEmpty()) {
        combine = combine.withSideInputs(sideInputs);
      }

      PCollection<KV<Void, OutputT>> combined;
      if (fanout >= 2) {
        combined = withKeys.apply(combine.withHotKeyFanout(fanout));
      } else {
        combined = withKeys.apply(combine);
      }

      PCollection<OutputT> output = combined.apply(Values.create());

      if (insertDefault) {
        if (!output.getWindowingStrategy().getWindowFn().isCompatible(new GlobalWindows())) {
          throw new IllegalStateException(fn.getIncompatibleGlobalWindowErrorMessage());
        }
        return insertDefaultValueIfEmpty(output);
      } else {
        return output;
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      Combine.populateDisplayData(builder, fn, fnDisplayData);
      Combine.populateGlobalDisplayData(builder, fanout, insertDefault);
    }

    private PCollection<OutputT> insertDefaultValueIfEmpty(PCollection<OutputT> maybeEmpty) {
      final PCollectionView<Iterable<OutputT>> maybeEmptyView = maybeEmpty.apply(View.asIterable());

      final OutputT defaultValue = fn.defaultValue();
      PCollection<OutputT> defaultIfEmpty =
          maybeEmpty
              .getPipeline()
              .apply("CreateVoid", Create.of((Void) null).withCoder(VoidCoder.of()))
              .apply(
                  "ProduceDefault",
                  ParDo.of(
                          new DoFn<Void, OutputT>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                              Iterator<OutputT> combined = c.sideInput(maybeEmptyView).iterator();
                              if (!combined.hasNext()) {
                                c.output(defaultValue);
                              }
                            }
                          })
                      .withSideInputs(maybeEmptyView))
              .setCoder(maybeEmpty.getCoder())
              .setWindowingStrategyInternal(maybeEmpty.getWindowingStrategy());

      return PCollectionList.of(maybeEmpty).and(defaultIfEmpty).apply(Flatten.pCollections());
    }
  }

  private static void populateDisplayData(
      DisplayData.Builder builder,
      HasDisplayData fn,
      DisplayData.ItemSpec<? extends Class<?>> fnDisplayItem) {
    builder.include("combineFn", fn).add(fnDisplayItem);
  }

  private static void populateGlobalDisplayData(
      DisplayData.Builder builder, int fanout, boolean insertDefault) {
    builder
        .addIfNotDefault(DisplayData.item("fanout", fanout).withLabel("Key Fanout Size"), 0)
        .add(
            DisplayData.item("emitDefaultOnEmptyInput", insertDefault)
                .withLabel("Emit Default On Empty Input"));
  }

  
  public static class GloballyAsSingletonView<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollectionView<OutputT>> {

    private final GlobalCombineFn<? super InputT, ?, OutputT> fn;
    private final DisplayData.ItemSpec<? extends Class<?>> fnDisplayData;
    private final boolean insertDefault;
    private final int fanout;

    private GloballyAsSingletonView(
        GlobalCombineFn<? super InputT, ?, OutputT> fn,
        DisplayData.ItemSpec<? extends Class<?>> fnDisplayData,
        boolean insertDefault,
        int fanout) {
      this.fn = fn;
      this.fnDisplayData = fnDisplayData;
      this.insertDefault = insertDefault;
      this.fanout = fanout;
    }

    @Override
    public PCollectionView<OutputT> expand(PCollection<InputT> input) {
      PCollection<OutputT> combined =
          input.apply(
              "CombineValues",
              Combine.<InputT, OutputT>globally(fn).withoutDefaults().withFanout(fanout));
      Coder<OutputT> outputCoder = combined.getCoder();
      PCollectionView<OutputT> view =
          PCollectionViews.singletonView(
              combined,
              (TypeDescriptorSupplier<OutputT>)
                  () -> outputCoder != null ? outputCoder.getEncodedTypeDescriptor() : null,
              input.getWindowingStrategy(),
              insertDefault,
              insertDefault ? fn.defaultValue() : null,
              combined.getCoder());
      combined.apply("CreatePCollectionView", CreatePCollectionView.of(view));
      return view;
    }

    public int getFanout() {
      return fanout;
    }

    public boolean getInsertDefault() {
      return insertDefault;
    }

    public GlobalCombineFn<? super InputT, ?, OutputT> getCombineFn() {
      return fn;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      Combine.populateDisplayData(builder, fn, fnDisplayData);
      Combine.populateGlobalDisplayData(builder, fanout, insertDefault);
    }
  }

  
  public static class IterableCombineFn<V> extends CombineFn<V, List<V>, V>
      implements NameOverride {
    
    public static <V> IterableCombineFn<V> of(SerializableFunction<Iterable<V>, V> combiner) {
      return of(combiner, DEFAULT_BUFFER_SIZE);
    }

    
    public static <V> IterableCombineFn<V> of(
        SerializableFunction<Iterable<V>, V> combiner, int bufferSize) {
      return new IterableCombineFn<>(combiner, bufferSize);
    }

    private static final int DEFAULT_BUFFER_SIZE = 20;

    
    private final SerializableFunction<Iterable<V>, V> combiner;

    
    private final int bufferSize;

    private IterableCombineFn(SerializableFunction<Iterable<V>, V> combiner, int bufferSize) {
      this.combiner = combiner;
      this.bufferSize = bufferSize;
    }

    @Override
    public List<V> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<V> addInput(List<V> accumulator, V input) {
      accumulator.add(input);
      if (accumulator.size() > bufferSize) {
        return mergeToSingleton(accumulator);
      } else {
        return accumulator;
      }
    }

    @Override
    public List<V> mergeAccumulators(Iterable<List<V>> accumulators) {
      return mergeToSingleton(Iterables.concat(accumulators));
    }

    @Override
    public V extractOutput(List<V> accumulator) {
      return combiner.apply(accumulator);
    }

    @Override
    public List<V> compact(List<V> accumulator) {
      return accumulator.size() > 1 ? mergeToSingleton(accumulator) : accumulator;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("combineFn", combiner.getClass()).withLabel("Combiner"));
    }

    private List<V> mergeToSingleton(Iterable<V> values) {
      List<V> singleton = new ArrayList<>();
      singleton.add(combiner.apply(values));
      return singleton;
    }

    public String getNameOverride() {
      return NameUtils.approximateSimpleName(combiner);
    }
  }

  
  @Deprecated
  public static class SimpleCombineFn<V> extends IterableCombineFn<V> {

    
    public static <V> SimpleCombineFn<V> of(SerializableFunction<Iterable<V>, V> combiner) {
      return new SimpleCombineFn<>(combiner);
    }

    protected SimpleCombineFn(SerializableFunction<Iterable<V>, V> combiner) {
      super(combiner, IterableCombineFn.DEFAULT_BUFFER_SIZE);
    }
  }

  

  
  public static class PerKey<K, InputT, OutputT>
      extends PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> {

    private final GlobalCombineFn<? super InputT, ?, OutputT> fn;
    private final DisplayData.ItemSpec<? extends Class<?>> fnDisplayData;
    private final boolean fewKeys;
    private final List<PCollectionView<?>> sideInputs;

    private PerKey(
        GlobalCombineFn<? super InputT, ?, OutputT> fn,
        DisplayData.ItemSpec<? extends Class<?>> fnDisplayData,
        boolean fewKeys) {
      this.fn = fn;
      this.fnDisplayData = fnDisplayData;
      this.fewKeys = fewKeys;
      this.sideInputs = ImmutableList.of();
    }

    private PerKey(
        GlobalCombineFn<? super InputT, ?, OutputT> fn,
        DisplayData.ItemSpec<? extends Class<?>> fnDisplayData,
        boolean fewKeys,
        List<PCollectionView<?>> sideInputs) {
      this.fn = fn;
      this.fnDisplayData = fnDisplayData;
      this.fewKeys = fewKeys;
      this.sideInputs = sideInputs;
    }

    @Override
    protected String getKindString() {
      return String.format("Combine.perKey(%s)", NameUtils.approximateSimpleName(fn));
    }

    
    public PerKey<K, InputT, OutputT> withSideInputs(PCollectionView<?>... sideInputs) {
      return withSideInputs(Arrays.asList(sideInputs));
    }

    
    public PerKey<K, InputT, OutputT> withSideInputs(
        Iterable<? extends PCollectionView<?>> sideInputs) {
      checkState(fn instanceof RequiresContextInternal);
      return new PerKey<>(fn, fnDisplayData, fewKeys, ImmutableList.copyOf(sideInputs));
    }

    
    public PerKeyWithHotKeyFanout<K, InputT, OutputT> withHotKeyFanout(
        SerializableFunction<? super K, Integer> hotKeyFanout) {
      return new PerKeyWithHotKeyFanout<>(fn, fnDisplayData, hotKeyFanout, fewKeys, sideInputs);
    }

    
    public PerKeyWithHotKeyFanout<K, InputT, OutputT> withHotKeyFanout(final int hotKeyFanout) {
      return new PerKeyWithHotKeyFanout<>(
          fn,
          fnDisplayData,
          new SimpleFunction<K, Integer>() {
            @Override
            public void populateDisplayData(DisplayData.Builder builder) {
              super.populateDisplayData(builder);
              builder.add(DisplayData.item("fanout", hotKeyFanout).withLabel("Key Fanout Size"));
            }

            @Override
            public Integer apply(K unused) {
              return hotKeyFanout;
            }
          },
          fewKeys,
          sideInputs);
    }

    
    public GlobalCombineFn<? super InputT, ?, OutputT> getFn() {
      return fn;
    }

    
    public List<PCollectionView<?>> getSideInputs() {
      return sideInputs;
    }

    
    @Override
    public Map<TupleTag<?>, PValue> getAdditionalInputs() {
      return PCollectionViews.toAdditionalInputs(sideInputs);
    }

    @Override
    public PCollection<KV<K, OutputT>> expand(PCollection<KV<K, InputT>> input) {
      return input
          .apply(fewKeys ? GroupByKey.createWithFewKeys() : GroupByKey.create())
          .apply(
              Combine.<K, InputT, OutputT>groupedValues(fn, fnDisplayData)
                  .withSideInputs(sideInputs));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      Combine.populateDisplayData(builder, fn, fnDisplayData);
    }
  }

  
  public static class PerKeyWithHotKeyFanout<K, InputT, OutputT>
      extends PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> {

    private final GlobalCombineFn<? super InputT, ?, OutputT> fn;
    private final DisplayData.ItemSpec<? extends Class<?>> fnDisplayData;
    private final SerializableFunction<? super K, Integer> hotKeyFanout;
    private final boolean fewKeys;
    private final List<PCollectionView<?>> sideInputs;

    private PerKeyWithHotKeyFanout(
        GlobalCombineFn<? super InputT, ?, OutputT> fn,
        DisplayData.ItemSpec<? extends Class<?>> fnDisplayData,
        SerializableFunction<? super K, Integer> hotKeyFanout,
        boolean fewKeys,
        List<PCollectionView<?>> sideInputs) {
      this.fn = fn;
      this.fnDisplayData = fnDisplayData;
      this.hotKeyFanout = hotKeyFanout;
      this.fewKeys = fewKeys;
      this.sideInputs = sideInputs;
    }

    @Override
    protected String getKindString() {
      return String.format("Combine.perKeyWithFanout(%s)", NameUtils.approximateSimpleName(fn));
    }

    @Override
    public PCollection<KV<K, OutputT>> expand(PCollection<KV<K, InputT>> input) {
      return applyHelper(input);
    }

    private <AccumT> PCollection<KV<K, OutputT>> applyHelper(PCollection<KV<K, InputT>> input) {

      
      @SuppressWarnings("unchecked")
      final GlobalCombineFn<InputT, AccumT, OutputT> typedFn =
          (GlobalCombineFn<InputT, AccumT, OutputT>) this.fn;

      if (!(input.getCoder() instanceof KvCoder)) {
        throw new IllegalStateException(
            "Expected input coder to be KvCoder, but was " + input.getCoder());
      }

      @SuppressWarnings("unchecked")
      final KvCoder<K, InputT> inputCoder = (KvCoder<K, InputT>) input.getCoder();
      final Coder<AccumT> accumCoder;

      try {
        accumCoder =
            typedFn.getAccumulatorCoder(
                input.getPipeline().getCoderRegistry(), inputCoder.getValueCoder());
      } catch (CannotProvideCoderException e) {
        throw new IllegalStateException("Unable to determine accumulator coder.", e);
      }
      Coder<InputOrAccum<InputT, AccumT>> inputOrAccumCoder =
          new InputOrAccum.InputOrAccumCoder<>(inputCoder.getValueCoder(), accumCoder);

      
      
      
      
      
      GlobalCombineFn<InputT, AccumT, AccumT> hotPreCombine;
      GlobalCombineFn<InputOrAccum<InputT, AccumT>, AccumT, OutputT> postCombine;
      if (typedFn instanceof CombineFn) {
        final CombineFn<InputT, AccumT, OutputT> fn = (CombineFn<InputT, AccumT, OutputT>) typedFn;
        hotPreCombine =
            new CombineFn<InputT, AccumT, AccumT>() {
              @Override
              public AccumT createAccumulator() {
                return fn.createAccumulator();
              }

              @Override
              public AccumT addInput(AccumT accumulator, InputT value) {
                return fn.addInput(accumulator, value);
              }

              @Override
              public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
                return fn.mergeAccumulators(accumulators);
              }

              @Override
              public AccumT compact(AccumT accumulator) {
                return fn.compact(accumulator);
              }

              @Override
              public AccumT extractOutput(AccumT accumulator) {
                return accumulator;
              }

              @Override
              @SuppressWarnings("unchecked")
              public Coder<AccumT> getAccumulatorCoder(
                  CoderRegistry registry, Coder<InputT> inputCoder)
                  throws CannotProvideCoderException {
                return accumCoder;
              }

              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder.delegate(PerKeyWithHotKeyFanout.this);
              }
            };

        postCombine =
            new CombineFn<InputOrAccum<InputT, AccumT>, AccumT, OutputT>() {
              @Override
              public AccumT createAccumulator() {
                return fn.createAccumulator();
              }

              @Override
              public AccumT addInput(AccumT accumulator, InputOrAccum<InputT, AccumT> value) {
                if (value.accum == null) {
                  return fn.addInput(accumulator, value.input);
                } else {
                  return fn.mergeAccumulators(ImmutableList.of(accumulator, value.accum));
                }
              }

              @Override
              public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
                return fn.mergeAccumulators(accumulators);
              }

              @Override
              public AccumT compact(AccumT accumulator) {
                return fn.compact(accumulator);
              }

              @Override
              public OutputT extractOutput(AccumT accumulator) {
                return fn.extractOutput(accumulator);
              }

              @Override
              public Coder<OutputT> getDefaultOutputCoder(
                  CoderRegistry registry, Coder<InputOrAccum<InputT, AccumT>> accumulatorCoder)
                  throws CannotProvideCoderException {
                return fn.getDefaultOutputCoder(registry, inputCoder.getValueCoder());
              }

              @Override
              public Coder<AccumT> getAccumulatorCoder(
                  CoderRegistry registry, Coder<InputOrAccum<InputT, AccumT>> inputCoder)
                  throws CannotProvideCoderException {
                return accumCoder;
              }

              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder.delegate(PerKeyWithHotKeyFanout.this);
              }
            };
      } else if (typedFn instanceof CombineFnWithContext) {
        final CombineFnWithContext<InputT, AccumT, OutputT> fnWithContext =
            (CombineFnWithContext<InputT, AccumT, OutputT>) typedFn;
        hotPreCombine =
            new CombineFnWithContext<InputT, AccumT, AccumT>() {
              @Override
              public AccumT createAccumulator(CombineWithContext.Context c) {
                return fnWithContext.createAccumulator(c);
              }

              @Override
              public AccumT addInput(
                  AccumT accumulator, InputT value, CombineWithContext.Context c) {
                return fnWithContext.addInput(accumulator, value, c);
              }

              @Override
              public AccumT mergeAccumulators(
                  Iterable<AccumT> accumulators, CombineWithContext.Context c) {
                return fnWithContext.mergeAccumulators(accumulators, c);
              }

              @Override
              public AccumT compact(AccumT accumulator, CombineWithContext.Context c) {
                return fnWithContext.compact(accumulator, c);
              }

              @Override
              public AccumT extractOutput(AccumT accumulator, CombineWithContext.Context c) {
                return accumulator;
              }

              @Override
              @SuppressWarnings("unchecked")
              public Coder<AccumT> getAccumulatorCoder(
                  CoderRegistry registry, Coder<InputT> inputCoder)
                  throws CannotProvideCoderException {
                return accumCoder;
              }

              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder.delegate(PerKeyWithHotKeyFanout.this);
              }
            };
        postCombine =
            new CombineFnWithContext<InputOrAccum<InputT, AccumT>, AccumT, OutputT>() {
              @Override
              public AccumT createAccumulator(CombineWithContext.Context c) {
                return fnWithContext.createAccumulator(c);
              }

              @Override
              public AccumT addInput(
                  AccumT accumulator,
                  InputOrAccum<InputT, AccumT> value,
                  CombineWithContext.Context c) {
                if (value.accum == null) {
                  return fnWithContext.addInput(accumulator, value.input, c);
                } else {
                  return fnWithContext.mergeAccumulators(
                      ImmutableList.of(accumulator, value.accum), c);
                }
              }

              @Override
              public AccumT mergeAccumulators(
                  Iterable<AccumT> accumulators, CombineWithContext.Context c) {
                return fnWithContext.mergeAccumulators(accumulators, c);
              }

              @Override
              public AccumT compact(AccumT accumulator, CombineWithContext.Context c) {
                return fnWithContext.compact(accumulator, c);
              }

              @Override
              public OutputT extractOutput(AccumT accumulator, CombineWithContext.Context c) {
                return fnWithContext.extractOutput(accumulator, c);
              }

              @Override
              public Coder<OutputT> getDefaultOutputCoder(
                  CoderRegistry registry, Coder<InputOrAccum<InputT, AccumT>> accumulatorCoder)
                  throws CannotProvideCoderException {
                return fnWithContext.getDefaultOutputCoder(registry, inputCoder.getValueCoder());
              }

              @Override
              public Coder<AccumT> getAccumulatorCoder(
                  CoderRegistry registry, Coder<InputOrAccum<InputT, AccumT>> inputCoder)
                  throws CannotProvideCoderException {
                return accumCoder;
              }

              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder.delegate(PerKeyWithHotKeyFanout.this);
              }
            };
      } else {
        throw new IllegalStateException(
            String.format("Unknown type of CombineFn: %s", typedFn.getClass()));
      }

      
      
      final TupleTag<KV<KV<K, Integer>, InputT>> hot = new TupleTag<>();
      final TupleTag<KV<K, InputT>> cold = new TupleTag<>();
      PCollectionTuple split =
          input.apply(
              "AddNonce",
              ParDo.of(
                      new DoFn<KV<K, InputT>, KV<K, InputT>>() {
                        transient int nonce;

                        @StartBundle
                        public void startBundle() {
                          
                          
                          
                          
                          
                          nonce = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
                        }

                        @ProcessElement
                        public void processElement(
                            @Element KV<K, InputT> kv, MultiOutputReceiver receiver) {
                          int spread = hotKeyFanout.apply(kv.getKey());
                          if (spread <= 1) {
                            receiver.get(cold).output(kv);
                          } else {
                            receiver
                                .get(hot)
                                .output(KV.of(KV.of(kv.getKey(), nonce % spread), kv.getValue()));
                          }
                        }
                      })
                  .withOutputTags(cold, TupleTagList.of(hot)));

      
      WindowingStrategy<?, ?> preCombineStrategy = input.getWindowingStrategy();
      if (preCombineStrategy.getMode()
          == WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES) {
        preCombineStrategy =
            preCombineStrategy.withMode(WindowingStrategy.AccumulationMode.DISCARDING_FIRED_PANES);
      }

      
      Combine.PerKey<KV<K, Integer>, InputT, AccumT> hotPreCombineTransform =
          fewKeys
              ? Combine.fewKeys(hotPreCombine, fnDisplayData)
              : Combine.perKey(hotPreCombine, fnDisplayData);
      if (!sideInputs.isEmpty()) {
        hotPreCombineTransform = hotPreCombineTransform.withSideInputs(sideInputs);
      }

      PCollection<KV<K, InputOrAccum<InputT, AccumT>>> precombinedHot =
          split
              .get(hot)
              .setCoder(
                  KvCoder.of(
                      KvCoder.of(inputCoder.getKeyCoder(), VarIntCoder.of()),
                      inputCoder.getValueCoder()))
              .setWindowingStrategyInternal(preCombineStrategy)
              .apply("PreCombineHot", hotPreCombineTransform)
              .apply(
                  "StripNonce",
                  MapElements.via(
                      new SimpleFunction<
                          KV<KV<K, Integer>, AccumT>, KV<K, InputOrAccum<InputT, AccumT>>>() {
                        @Override
                        public KV<K, InputOrAccum<InputT, AccumT>> apply(
                            KV<KV<K, Integer>, AccumT> elem) {
                          return KV.of(
                              elem.getKey().getKey(),
                              InputOrAccum.accum(elem.getValue()));
                        }
                      }))
              .setCoder(KvCoder.of(inputCoder.getKeyCoder(), inputOrAccumCoder))
              .apply(Window.remerge())
              .setWindowingStrategyInternal(input.getWindowingStrategy());
      PCollection<KV<K, InputOrAccum<InputT, AccumT>>> preprocessedCold =
          split
              .get(cold)
              .setCoder(inputCoder)
              .apply(
                  "PrepareCold",
                  MapElements.via(
                      new SimpleFunction<KV<K, InputT>, KV<K, InputOrAccum<InputT, AccumT>>>() {
                        @Override
                        public KV<K, InputOrAccum<InputT, AccumT>> apply(KV<K, InputT> element) {
                          return KV.of(
                              element.getKey(),
                              InputOrAccum.input(element.getValue()));
                        }
                      }))
              .setCoder(KvCoder.of(inputCoder.getKeyCoder(), inputOrAccumCoder));

      
      Combine.PerKey<K, InputOrAccum<InputT, AccumT>, OutputT> postCombineTransform =
          fewKeys
              ? Combine.fewKeys(postCombine, fnDisplayData)
              : Combine.perKey(postCombine, fnDisplayData);
      if (!sideInputs.isEmpty()) {
        postCombineTransform = postCombineTransform.withSideInputs(sideInputs);
      }

      return PCollectionList.of(precombinedHot)
          .and(preprocessedCold)
          .apply(Flatten.pCollections())
          .apply("PostCombine", postCombineTransform);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      Combine.populateDisplayData(builder, fn, fnDisplayData);
      if (hotKeyFanout instanceof HasDisplayData) {
        builder.include("hotKeyFanout", (HasDisplayData) hotKeyFanout);
      }
      builder.add(
          DisplayData.item("fanoutFn", hotKeyFanout.getClass()).withLabel("Fanout Function"));
    }

    
    public List<PCollectionView<?>> getSideInputs() {
      return sideInputs;
    }

    
    private static class InputOrAccum<InputT, AccumT> {
      public final @Nullable InputT input;
      public final @Nullable AccumT accum;

      private InputOrAccum(@Nullable InputT input, @Nullable AccumT aggr) {
        this.input = input;
        this.accum = aggr;
      }

      public static <InputT, AccumT> InputOrAccum<InputT, AccumT> input(InputT input) {
        return new InputOrAccum<>(input, null);
      }

      public static <InputT, AccumT> InputOrAccum<InputT, AccumT> accum(AccumT aggr) {
        return new InputOrAccum<>(null, aggr);
      }

      private static class InputOrAccumCoder<InputT, AccumT>
          extends StructuredCoder<InputOrAccum<InputT, AccumT>> {

        private final Coder<InputT> inputCoder;
        private final Coder<AccumT> accumCoder;

        public InputOrAccumCoder(Coder<InputT> inputCoder, Coder<AccumT> accumCoder) {
          this.inputCoder = inputCoder;
          this.accumCoder = accumCoder;
        }

        @Override
        public void encode(InputOrAccum<InputT, AccumT> value, OutputStream outStream)
            throws IOException {
          encode(value, outStream, Coder.Context.NESTED);
        }

        @Override
        public void encode(
            InputOrAccum<InputT, AccumT> value, OutputStream outStream, Coder.Context context)
            throws IOException {
          if (value.input != null) {
            outStream.write(0);
            inputCoder.encode(value.input, outStream, context);
          } else {
            outStream.write(1);
            accumCoder.encode(value.accum, outStream, context);
          }
        }

        @Override
        public InputOrAccum<InputT, AccumT> decode(InputStream inStream)
            throws IOException {
          return decode(inStream, Coder.Context.NESTED);
        }

        @Override
        public InputOrAccum<InputT, AccumT> decode(InputStream inStream, Coder.Context context)
            throws IOException {
          if (inStream.read() == 0) {
            return InputOrAccum.input(inputCoder.decode(inStream, context));
          } else {
            return InputOrAccum.accum(accumCoder.decode(inStream, context));
          }
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
          return ImmutableList.of(inputCoder, accumCoder);
        }

        @Override
        public void verifyDeterministic() throws Coder.NonDeterministicException {
          inputCoder.verifyDeterministic();
          accumCoder.verifyDeterministic();
        }
      }
    }
  }

  

  
  public static class GroupedValues<K, InputT, OutputT>
      extends PTransform<
          PCollection<? extends KV<K, ? extends Iterable<InputT>>>, PCollection<KV<K, OutputT>>> {

    private final GlobalCombineFn<? super InputT, ?, OutputT> fn;
    private final DisplayData.ItemSpec<? extends Class<?>> fnDisplayData;
    private final List<PCollectionView<?>> sideInputs;

    private GroupedValues(
        GlobalCombineFn<? super InputT, ?, OutputT> fn,
        DisplayData.ItemSpec<? extends Class<?>> fnDisplayData) {
      this.fn = SerializableUtils.clone(fn);
      this.fnDisplayData = fnDisplayData;
      this.sideInputs = ImmutableList.of();
    }

    private GroupedValues(
        GlobalCombineFn<? super InputT, ?, OutputT> fn,
        DisplayData.ItemSpec<? extends Class<?>> fnDisplayData,
        List<PCollectionView<?>> sideInputs) {
      this.fn = SerializableUtils.clone(fn);
      this.fnDisplayData = fnDisplayData;
      this.sideInputs = sideInputs;
    }

    public GroupedValues<K, InputT, OutputT> withSideInputs(PCollectionView<?>... sideInputs) {
      return withSideInputs(Arrays.asList(sideInputs));
    }

    public GroupedValues<K, InputT, OutputT> withSideInputs(
        Iterable<? extends PCollectionView<?>> sideInputs) {
      return new GroupedValues<>(fn, fnDisplayData, ImmutableList.copyOf(sideInputs));
    }

    
    public GlobalCombineFn<? super InputT, ?, OutputT> getFn() {
      return fn;
    }

    public List<PCollectionView<?>> getSideInputs() {
      return sideInputs;
    }

    @Override
    public PCollection<KV<K, OutputT>> expand(
        PCollection<? extends KV<K, ? extends Iterable<InputT>>> input) {

      PCollection<KV<K, OutputT>> output =
          input.apply(
              ParDo.of(
                      new DoFn<KV<K, ? extends Iterable<InputT>>, KV<K, OutputT>>() {
                        @ProcessElement
                        public void processElement(final ProcessContext c) {
                          K key = c.element().getKey();

                          OutputT output;
                          if (fn instanceof CombineFnWithContext) {
                            output =
                                ((CombineFnWithContext<? super InputT, ?, OutputT>) fn)
                                    .apply(
                                        c.element().getValue(),
                                        new CombineWithContext.Context() {
                                          @Override
                                          public PipelineOptions getPipelineOptions() {
                                            return c.getPipelineOptions();
                                          }

                                          @Override
                                          public <T> T sideInput(PCollectionView<T> view) {
                                            return c.sideInput(view);
                                          }
                                        });
                          } else if (fn instanceof CombineFn) {
                            output =
                                ((CombineFn<? super InputT, ?, OutputT>) fn)
                                    .apply(c.element().getValue());
                          } else {
                            throw new IllegalStateException(
                                String.format("Unknown type of CombineFn: %s", fn.getClass()));
                          }
                          c.output(KV.of(key, output));
                        }

                        @Override
                        public void populateDisplayData(DisplayData.Builder builder) {
                          builder.delegate(Combine.GroupedValues.this);
                        }
                      })
                  .withSideInputs(sideInputs));

      try {
        KvCoder<K, InputT> kvCoder = getKvCoder(input.getCoder());
        @SuppressWarnings("unchecked")
        Coder<OutputT> outputValueCoder =
            ((GlobalCombineFn<InputT, ?, OutputT>) fn)
                .getDefaultOutputCoder(
                    input.getPipeline().getCoderRegistry(), kvCoder.getValueCoder());
        output.setCoder(KvCoder.of(kvCoder.getKeyCoder(), outputValueCoder));
      } catch (CannotProvideCoderException exc) {
        
      }

      return output;
    }

    
    public AppliedCombineFn<? super K, ? super InputT, ?, OutputT> getAppliedFn(
        CoderRegistry registry,
        Coder<? extends KV<K, ? extends Iterable<InputT>>> inputCoder,
        WindowingStrategy<?, ?> windowingStrategy) {
      KvCoder<K, InputT> kvCoder = getKvCoder(inputCoder);
      return AppliedCombineFn.withInputCoder(fn, registry, kvCoder, sideInputs, windowingStrategy);
    }

    private KvCoder<K, InputT> getKvCoder(
        Coder<? extends KV<K, ? extends Iterable<InputT>>> inputCoder) {
      if (!(inputCoder instanceof KvCoder)) {
        throw new IllegalStateException("Combine.GroupedValues requires its input to use KvCoder");
      }
      @SuppressWarnings({"unchecked", "rawtypes"})
      KvCoder<K, ? extends Iterable<InputT>> kvCoder = (KvCoder) inputCoder;
      Coder<K> keyCoder = kvCoder.getKeyCoder();
      Coder<? extends Iterable<InputT>> kvValueCoder = kvCoder.getValueCoder();
      if (!(kvValueCoder instanceof IterableCoder)) {
        throw new IllegalStateException(
            "Combine.GroupedValues requires its input values to use " + "IterableCoder");
      }
      @SuppressWarnings("unchecked")
      IterableCoder<InputT> inputValuesCoder = (IterableCoder<InputT>) kvValueCoder;
      Coder<InputT> inputValueCoder = inputValuesCoder.getElemCoder();
      return KvCoder.of(keyCoder, inputValueCoder);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      Combine.populateDisplayData(builder, fn, fnDisplayData);
    }
  }
}