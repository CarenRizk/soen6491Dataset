package org.apache.beam.sdk.transforms;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.transforms.ApproximateQuantiles.ApproximateQuantilesCombineFn;
import org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn;
import org.apache.beam.sdk.transforms.Combine.AccumulatingCombineFn.Accumulator;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.WeightedValue;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.UnmodifiableIterator;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings({
  "nullness" 
})
public class ApproximateQuantiles {
  private ApproximateQuantiles() {
    
  }

  
  public static <T, ComparatorT extends Comparator<T> & Serializable>
      PTransform<PCollection<T>, PCollection<List<T>>> globally(
          int numQuantiles, ComparatorT compareFn) {
    return Combine.globally(ApproximateQuantilesCombineFn.create(numQuantiles, compareFn));
  }

  
  public static <T extends Comparable<T>> PTransform<PCollection<T>, PCollection<List<T>>> globally(
	      int numQuantiles) {
	    return Combine.globally(ApproximateQuantilesCombineFn.create(numQuantiles));
  }

  
  public static <K, V, ComparatorT extends Comparator<V> & Serializable>
      PTransform<PCollection<KV<K, V>>, PCollection<KV<K, List<V>>>> perKey(
          int numQuantiles, ComparatorT compareFn) {
    return Combine.perKey(ApproximateQuantilesCombineFn.create(numQuantiles, compareFn));
  }

  
  public static <K, V extends Comparable<V>>
      PTransform<PCollection<KV<K, V>>, PCollection<KV<K, List<V>>>> perKey(int numQuantiles) {
    return Combine.perKey(ApproximateQuantilesCombineFn.create(numQuantiles));
  }

  

  
  public static class ApproximateQuantilesCombineFn<
          T, ComparatorT extends Comparator<T> & Serializable>
      extends AccumulatingCombineFn<T, QuantileState<T, ComparatorT>, List<T>> {

    
    public static final long DEFAULT_MAX_NUM_ELEMENTS = (long) 1e9;

    
    private final ComparatorT compareFn;

    
    private final int numQuantiles;

    
    private final int bufferSize;

    
    private final int numBuffers;

    private final long maxNumElements;

    private ApproximateQuantilesCombineFn(
        int numQuantiles,
        ComparatorT compareFn,
        int bufferSize,
        int numBuffers,
        long maxNumElements) {
      checkArgument(numQuantiles >= 2);
      checkArgument(bufferSize >= 2);
      checkArgument(numBuffers >= 2);
      this.numQuantiles = numQuantiles;
      this.compareFn = compareFn;
      this.bufferSize = bufferSize;
      this.numBuffers = numBuffers;
      this.maxNumElements = maxNumElements;
    }

    
    public static <T, ComparatorT extends Comparator<T> & Serializable>
        ApproximateQuantilesCombineFn<T, ComparatorT> create(
            int numQuantiles, ComparatorT compareFn) {
      return create(numQuantiles, compareFn, DEFAULT_MAX_NUM_ELEMENTS, 1.0 / numQuantiles);
    }

    
    public static <T extends Comparable<T>> ApproximateQuantilesCombineFn<T, Top.Natural<T>> create(
        int numQuantiles) {
      return create(numQuantiles, new Top.Natural<T>());
    }

    
    public ApproximateQuantilesCombineFn<T, ComparatorT> withEpsilon(double epsilon) {
      return create(numQuantiles, compareFn, maxNumElements, epsilon);
    }

    
    public static <T, ComparatorT extends Comparator<T> & Serializable>
        ApproximateQuantilesCombineFn<T, ComparatorT> create(
            int numQuantiles, ComparatorT compareFn, long maxNumElements, double epsilon) {
      
      int b = 2;
      while ((b - 2) * (1 << (b - 2)) < epsilon * maxNumElements) {
        b++;
      }
      b--;
      int k = Math.max(2, (int) Math.ceil(maxNumElements / (float) (1 << (b - 1))));
      return new ApproximateQuantilesCombineFn<>(numQuantiles, compareFn, k, b, maxNumElements);
    }

    @Override
    public QuantileState<T, ComparatorT> createAccumulator() {
      return QuantileState.empty(compareFn, numQuantiles, numBuffers, bufferSize);
    }

    @Override
    public Coder<QuantileState<T, ComparatorT>> getAccumulatorCoder(
        CoderRegistry registry, Coder<T> elementCoder) {
      return new QuantileStateCoder<>(compareFn, elementCoder);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(DisplayData.item("numQuantiles", numQuantiles).withLabel("Quantile Count"))
          .add(DisplayData.item("comparer", compareFn.getClass()).withLabel("Record Comparer"));
    }

    int getNumBuffers() {
      return numBuffers;
    }

    int getBufferSize() {
      return bufferSize;
    }
  }

  
  static class QuantileState<T, ComparatorT extends Comparator<T> & Serializable>
      implements Accumulator<T, QuantileState<T, ComparatorT>, List<T>> {

    private final ComparatorT compareFn;
    private final int numQuantiles;
    private final int numBuffers;
    private final int bufferSize;

    private @Nullable T min;

    private @Nullable T max;

    
    private final PriorityQueue<QuantileBuffer<T>> buffers;

    
    private List<T> unbufferedElements = Lists.newArrayList();

    private QuantileState(
        ComparatorT compareFn,
        int numQuantiles,
        @Nullable T min,
        @Nullable T max,
        int numBuffers,
        int bufferSize,
        Collection<T> unbufferedElements,
        Collection<QuantileBuffer<T>> buffers) {
      this.compareFn = compareFn;
      this.numQuantiles = numQuantiles;
      this.numBuffers = numBuffers;
      this.bufferSize = bufferSize;
      this.buffers =
          new PriorityQueue<>(numBuffers + 1, (q1, q2) -> Integer.compare(q1.level, q2.level));
      this.min = min;
      this.max = max;
      this.unbufferedElements.addAll(unbufferedElements);
      this.buffers.addAll(buffers);
    }

    // Optimized by LLM: Use Collections.emptyList() directly instead of creating a new list
    public static <T, ComparatorT extends Comparator<T> & Serializable>
        QuantileState<T, ComparatorT> empty(
            ComparatorT compareFn, int numQuantiles, int numBuffers, int bufferSize) {
      return new QuantileState<>(
          compareFn,
          numQuantiles,
          null, 
          null, 
          numBuffers,
          bufferSize,
          Collections.emptyList(),
          Collections.emptyList());
    }

    
    @Override
    public void addInput(T elem) {
      if (isEmpty()) {
        min = max = elem;
      } else if (compareFn.compare(elem, min) < 0) {
        min = elem;
      } else if (compareFn.compare(elem, max) > 0) {
        max = elem;
      }
      addUnbuffered(elem);
    }

    
    private void addUnbuffered(T elem) {
      unbufferedElements.add(elem);
      if (unbufferedElements.size() == bufferSize) {
        unbufferedElements.sort(compareFn);
        buffers.add(new QuantileBuffer<>(unbufferedElements));
        unbufferedElements = Lists.newArrayListWithCapacity(bufferSize);
        collapseIfNeeded();
      }
    }

    
    @Override
    public void mergeAccumulator(QuantileState<T, ComparatorT> other) {
      if (other.isEmpty()) {
        return;
      }
      if (min == null || compareFn.compare(other.min, min) < 0) {
        min = other.min;
      }
      if (max == null || compareFn.compare(other.max, max) > 0) {
        max = other.max;
      }
      for (T elem : other.unbufferedElements) {
        addUnbuffered(elem);
      }
      buffers.addAll(other.buffers);
      collapseIfNeeded();
    }

    public boolean isEmpty() {
      return unbufferedElements.isEmpty() && buffers.isEmpty();
    }

    // Optimized by LLM: Refactor collapseIfNeeded() to reduce complexity
    private void collapseIfNeeded() {
      while (buffers.size() > numBuffers) {
        List<QuantileBuffer<T>> toCollapse = Lists.newArrayList();
        toCollapse.add(buffers.poll());
        toCollapse.add(buffers.poll());
        int minLevel = toCollapse.get(1).level;
        while (!buffers.isEmpty() && buffers.peek().level == minLevel) {
          toCollapse.add(buffers.poll());
        }
        buffers.add(collapse(toCollapse));
      }
    }

    private QuantileBuffer<T> collapse(Iterable<QuantileBuffer<T>> buffers) {
      int newLevel = 0;
      long newWeight = 0;
      for (QuantileBuffer<T> buffer : buffers) {
        newLevel = Math.max(newLevel, buffer.level + 1);
        newWeight += buffer.weight;
      }
      List<T> newElements = interpolate(buffers, bufferSize, newWeight, offset(newWeight));
      return new QuantileBuffer<>(newLevel, newWeight, newElements);
    }

    
    private long offset(long newWeight) {
      if (newWeight % 2 == 1) {
        return (newWeight + 1) / 2;
      } else {
        offsetJitter = 2 - offsetJitter;
        return (newWeight + offsetJitter) / 2;
      }
    }

    
    private int offsetJitter = 0;

    
    private List<T> interpolate(
        Iterable<QuantileBuffer<T>> buffers, int count, double step, double offset) {
      List<Iterator<WeightedValue<T>>> iterators = Lists.newArrayList();
      for (QuantileBuffer<T> buffer : buffers) {
        iterators.add(buffer.sizedIterator());
      }
      
      Iterator<WeightedValue<T>> sorted =
          Iterators.mergeSorted(iterators, (a, b) -> compareFn.compare(a.getValue(), b.getValue()));

      List<T> newElements = Lists.newArrayListWithCapacity(count);
      WeightedValue<T> weightedElement = sorted.next();
      double current = weightedElement.getWeight();
      for (int j = 0; j < count; j++) {
        double target = j * step + offset;
        while (current <= target && sorted.hasNext()) {
          weightedElement = sorted.next();
          current += weightedElement.getWeight();
        }
        newElements.add(weightedElement.getValue());
      }
      return newElements;
    }

    
    @Override
    public List<T> extractOutput() {
      if (isEmpty()) {
        return Lists.newArrayList();
      }
      long totalCount = unbufferedElements.size();
      for (QuantileBuffer<T> buffer : buffers) {
        totalCount += bufferSize * buffer.weight;
      }
      List<QuantileBuffer<T>> all = Lists.newArrayList(buffers);
      if (!unbufferedElements.isEmpty()) {
        unbufferedElements.sort(compareFn);
        all.add(new QuantileBuffer<>(unbufferedElements));
      }
      double step = 1.0 * totalCount / (numQuantiles - 1);
      double offset = (1.0 * totalCount - 1) / (numQuantiles - 1);
      List<T> quantiles = interpolate(all, numQuantiles - 2, step, offset);
      quantiles.add(0, min);
      quantiles.add(max);
      return quantiles;
    }
  }

  
  private static class QuantileBuffer<T> {
    private final int level;
    private final long weight;
    private final List<T> elements;

    public QuantileBuffer(List<T> elements) {
      this(0, 1, elements);
    }

    public QuantileBuffer(int level, long weight, List<T> elements) {
      this.level = level;
      this.weight = weight;
      this.elements = elements;
    }

    @Override
    public String toString() {
      return "QuantileBuffer["
          + "level="
          + level
          + ", weight="
          + weight
          + ", elements="
          + elements
          + "]";
    }

    public Iterator<WeightedValue<T>> sizedIterator() {
      return new UnmodifiableIterator<WeightedValue<T>>() {
        final Iterator<T> iter = elements.iterator();

        @Override
        public boolean hasNext() {
          return iter.hasNext();
        }

        @Override
        public WeightedValue<T> next() {
          return WeightedValue.of(iter.next(), weight);
        }
      };
    }
  }

  
  private static class QuantileStateCoder<T, ComparatorT extends Comparator<T> & Serializable>
      extends CustomCoder<QuantileState<T, ComparatorT>> {
    private final ComparatorT compareFn;
    private final Coder<T> elementCoder;
    private final Coder<List<T>> elementListCoder;
    private final Coder<Integer> intCoder = BigEndianIntegerCoder.of();

    public QuantileStateCoder(ComparatorT compareFn, Coder<T> elementCoder) {
      this.compareFn = compareFn;
      this.elementCoder = elementCoder;
      this.elementListCoder = ListCoder.of(elementCoder);
    }

    @Override
    public void encode(QuantileState<T, ComparatorT> state, OutputStream outStream)
        throws IOException {
      intCoder.encode(state.numQuantiles, outStream);
      intCoder.encode(state.bufferSize, outStream);
      elementCoder.encode(state.min, outStream);
      elementCoder.encode(state.max, outStream);
      elementListCoder.encode(state.unbufferedElements, outStream);
      BigEndianIntegerCoder.of().encode(state.buffers.size(), outStream);
      for (QuantileBuffer<T> buffer : state.buffers) {
        encodeBuffer(buffer, outStream);
      }
    }

    @Override
    public QuantileState<T, ComparatorT> decode(InputStream inStream)
        throws IOException {
      int numQuantiles = intCoder.decode(inStream);
      int bufferSize = intCoder.decode(inStream);
      T min = elementCoder.decode(inStream);
      T max = elementCoder.decode(inStream);
      List<T> unbufferedElements = elementListCoder.decode(inStream);
      int numBuffers = BigEndianIntegerCoder.of().decode(inStream);
      List<QuantileBuffer<T>> buffers = new ArrayList<>(numBuffers);
      for (int i = 0; i < numBuffers; i++) {
        buffers.add(decodeBuffer(inStream));
      }
      return new QuantileState<>(
          compareFn, numQuantiles, min, max, numBuffers, bufferSize, unbufferedElements, buffers);
    }

    private void encodeBuffer(QuantileBuffer<T> buffer, OutputStream outStream)
        throws IOException {
      DataOutputStream outData = new DataOutputStream(outStream);
      outData.writeInt(buffer.level);
      outData.writeLong(buffer.weight);
      elementListCoder.encode(buffer.elements, outStream);
    }

    private QuantileBuffer<T> decodeBuffer(InputStream inStream)
        throws IOException {
      DataInputStream inData = new DataInputStream(inStream);
      return new QuantileBuffer<>(
          inData.readInt(), inData.readLong(), elementListCoder.decode(inStream));
    }

    
    @Override
    public void registerByteSizeObserver(
        QuantileState<T, ComparatorT> state, ElementByteSizeObserver observer) throws Exception {
      elementCoder.registerByteSizeObserver(state.min, observer);
      elementCoder.registerByteSizeObserver(state.max, observer);
      elementListCoder.registerByteSizeObserver(state.unbufferedElements, observer);

      BigEndianIntegerCoder.of().registerByteSizeObserver(state.buffers.size(), observer);
      for (QuantileBuffer<T> buffer : state.buffers) {
        observer.update(4L + 8);
        elementListCoder.registerByteSizeObserver(buffer.elements, observer);
      }
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (other == this) {
        return true;
      }
      if (!(other instanceof QuantileStateCoder)) {
        return false;
      }
      QuantileStateCoder<?, ?> that = (QuantileStateCoder<?, ?>) other;
      return Objects.equals(this.elementCoder, that.elementCoder)
          && Objects.equals(this.compareFn, that.compareFn);
    }

    @Override
    public int hashCode() {
      return Objects.hash(elementCoder, compareFn);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(this, "QuantileState.ElementCoder must be deterministic", elementCoder);
      verifyDeterministic(
          this, "QuantileState.ElementListCoder must be deterministic", elementListCoder);
    }
  }
}