package org.apache.beam.sdk.transforms;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.TreeSet;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.HashingOutputStream;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.checkerframework.checker.nullness.qual.Nullable;


@Deprecated
public class ApproximateUnique {

  
  public static <T> Globally<T> globally(int sampleSize) {
    return new Globally<>(sampleSize);
  }

  
  public static <T> Globally<T> globally(double maximumEstimationError) {
    return new Globally<>(maximumEstimationError);
  }

  
  public static <K, V> PerKey<K, V> perKey(int sampleSize) {
    return new PerKey<>(sampleSize);
  }

  
  public static <K, V> PerKey<K, V> perKey(double maximumEstimationError) {
    return new PerKey<>(maximumEstimationError);
  }

  

  
  public static final class Globally<T> extends PTransform<PCollection<T>, PCollection<Long>> {

    
    private final long sampleSize;

    
    private final @Nullable Double maximumEstimationError;

    
    public Globally(int sampleSize) {
      if (sampleSize < 16) {
        throw new IllegalArgumentException(
            "ApproximateUnique needs a sampleSize "
                + ">= 16 for an estimation error <= 50%.  "
                + "In general, the estimation "
                + "error is about 2 / sqrt(sampleSize).");
      }

      this.sampleSize = sampleSize;
      this.maximumEstimationError = null;
    }

    
    public Globally(double maximumEstimationError) {
      if (maximumEstimationError < 0.01 || maximumEstimationError > 0.5) {
        throw new IllegalArgumentException(
            "ApproximateUnique needs an " + "estimation error between 1% (0.01) and 50% (0.5).");
      }

      this.sampleSize = sampleSizeFromEstimationError(maximumEstimationError);
      this.maximumEstimationError = maximumEstimationError;
    }

    @Override
    public PCollection<Long> expand(PCollection<T> input) {
      Coder<T> coder = input.getCoder();
      return input.apply(Combine.globally(new ApproximateUniqueCombineFn<>(sampleSize, coder)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      ApproximateUnique.populateDisplayData(builder, sampleSize, maximumEstimationError);
    }
  }

  
  public static final class PerKey<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Long>>> {

    
    private final long sampleSize;

    
    private final @Nullable Double maximumEstimationError;

    
    public PerKey(int sampleSize) {
      if (sampleSize < 16) {
        throw new IllegalArgumentException(
            "ApproximateUnique needs a "
                + "sampleSize >= 16 for an estimation error <= 50%.  In general, "
                + "the estimation error is about 2 / sqrt(sampleSize).");
      }

      this.sampleSize = sampleSize;
      this.maximumEstimationError = null;
    }

    
    public PerKey(double estimationError) {
      if (estimationError < 0.01 || estimationError > 0.5) {
        throw new IllegalArgumentException(
            "ApproximateUnique.PerKey needs an "
                + "estimation error between 1% (0.01) and 50% (0.5).");
      }

      this.sampleSize = sampleSizeFromEstimationError(estimationError);
      this.maximumEstimationError = estimationError;
    }

    @Override
    public PCollection<KV<K, Long>> expand(PCollection<KV<K, V>> input) {
      Coder<KV<K, V>> inputCoder = input.getCoder();
      if (!(inputCoder instanceof KvCoder)) {
        throw new IllegalStateException(
            "ApproximateUnique.PerKey requires its input to use KvCoder");
      }
      @SuppressWarnings("unchecked")
      final Coder<V> coder = ((KvCoder<K, V>) inputCoder).getValueCoder();

      return input.apply(Combine.perKey(new ApproximateUniqueCombineFn<>(sampleSize, coder)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      ApproximateUnique.populateDisplayData(builder, sampleSize, maximumEstimationError);
    }
  }

  

  
  public static class ApproximateUniqueCombineFn<T>
      extends CombineFn<T, ApproximateUniqueCombineFn.LargestUnique, Long> {

    
    static final double HASH_SPACE_SIZE = Long.MAX_VALUE - (double) Long.MIN_VALUE;

    
    public static class LargestUnique implements Serializable {
      private final TreeSet<Long> heap = new TreeSet<>();
      private long minHash = Long.MAX_VALUE;
      private final long sampleSize;

      
      public LargestUnique(long sampleSize) {
        this.sampleSize = sampleSize;
      }

      
      public void add(long value) {
        if (heap.size() >= sampleSize && value < minHash) {
          return; 
        }
        if (heap.add(value)) {
          if (heap.size() > sampleSize) {
            heap.remove(minHash);
            minHash = heap.first();
          } else if (value < minHash) {
            minHash = value;
          }
        }
      }

      long getEstimate() {
        if (heap.size() < sampleSize) {
          return heap.size();
        } else {
          double sampleSpaceSize = Long.MAX_VALUE - (double) minHash;
          
          
          
          
          
          
          double estimate =
              Math.log1p(-sampleSize / sampleSpaceSize)
                  / Math.log1p(-1 / sampleSpaceSize)
                  * HASH_SPACE_SIZE
                  / sampleSpaceSize;
          return Math.round(estimate);
        }
      }

      @Override
      public boolean equals(@Nullable Object o) {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }
        LargestUnique that = (LargestUnique) o;

        return sampleSize == that.sampleSize && Iterables.elementsEqual(heap, that.heap);
      }

      @Override
      public int hashCode() {
        return Objects.hashCode(Lists.newArrayList(heap), sampleSize);
      }
    }

    private final long sampleSize;
    private final Coder<T> coder;

    public ApproximateUniqueCombineFn(long sampleSize, Coder<T> coder) {
      this.sampleSize = sampleSize;
      this.coder = coder;
    }

    @Override
    public LargestUnique createAccumulator() {
      return new LargestUnique(sampleSize);
    }

    @Override
    public LargestUnique addInput(LargestUnique heap, T input) {
      try {
        heap.add(hash(input, coder));
        return heap;
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public LargestUnique mergeAccumulators(Iterable<LargestUnique> heaps) {
      Iterator<LargestUnique> iterator = heaps.iterator();
      LargestUnique accumulator = iterator.next();
      while (iterator.hasNext()) {
        iterator.next().heap.forEach(h -> accumulator.add(h));
      }
      return accumulator;
    }

    @Override
    public Long extractOutput(LargestUnique heap) {
      return heap.getEstimate();
    }

    @Override
    public Coder<LargestUnique> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return SerializableCoder.of(LargestUnique.class);
    }

    
    static <T> long hash(T element, Coder<T> coder) throws IOException {
      try (HashingOutputStream stream =
          new HashingOutputStream(Hashing.murmur3_128(), ByteStreams.nullOutputStream())) {
        coder.encode(element, stream, Context.OUTER);
        return stream.hash().asLong();
      }
    }
  }

  
  static long sampleSizeFromEstimationError(double estimationError) {
    return Math.round(Math.ceil(4.0 / Math.pow(estimationError, 2.0)));
  }

  private static void populateDisplayData(
      DisplayData.Builder builder, long sampleSize, @Nullable Double maxEstimationError) {
    builder
        .add(DisplayData.item("sampleSize", sampleSize).withLabel("Sample Size"))
        .addIfNotNull(
            DisplayData.item("maximumEstimationError", maxEstimationError)
                .withLabel("Maximum Estimation Error"));
  }
}
