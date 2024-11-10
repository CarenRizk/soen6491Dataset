package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableLikeCoder;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.WindowingStrategy;

public class Flatten {

  
  public static <T> PCollections<T> pCollections() {
    return new PCollections<>();
  }

  
  public static <T> Iterables<T> iterables() {
    return new Iterables<>();
  }

  
  public static class PCollections<T> extends PTransform<PCollectionList<T>, PCollection<T>> {

    private PCollections() {}

    private boolean areWindowFnsCompatible(WindowingStrategy<?, ?> windowingStrategy, PCollection<?> input) {
      return windowingStrategy.getWindowFn().isCompatible(input.getWindowingStrategy().getWindowFn());
    } // Optimized by LLM: Extracted window function compatibility check into a separate method

    private boolean areTriggersCompatible(WindowingStrategy<?, ?> windowingStrategy, PCollection<?> input) {
      return windowingStrategy.getTrigger().isCompatible(input.getWindowingStrategy().getTrigger());
    } // Optimized by LLM: Extracted trigger compatibility check into a separate method

    @Override
    public PCollection<T> expand(PCollectionList<T> inputs) {
      WindowingStrategy<?, ?> windowingStrategy;
      IsBounded isBounded = IsBounded.BOUNDED;
      if (!inputs.getAll().isEmpty()) {
        windowingStrategy = inputs.get(0).getWindowingStrategy();
        for (PCollection<?> input : inputs.getAll()) {
          WindowingStrategy<?, ?> other = input.getWindowingStrategy();
          if (!windowingStrategy.getWindowFn().isCompatible(other.getWindowFn())) {
            throw new IllegalStateException(
                "Inputs to Flatten had incompatible window windowFns: "
                    + windowingStrategy.getWindowFn()
                    + ", "
                    + other.getWindowFn());
          }

          if (!windowingStrategy.getTrigger().isCompatible(other.getTrigger())) {
            throw new IllegalStateException(
                "Inputs to Flatten had incompatible triggers: "
                    + windowingStrategy.getTrigger()
                    + ", "
                    + other.getTrigger());
          }
          isBounded = isBounded.and(input.isBounded());
        }
      } else {
        windowingStrategy = WindowingStrategy.globalDefault();
      }

      return PCollection.createPrimitiveOutputInternal(
          inputs.getPipeline(),
          windowingStrategy,
          isBounded,
          
          inputs.getAll().isEmpty() ? null : inputs.get(0).getCoder());
    }
  }

  
  public static class Iterables<T>
      extends PTransform<PCollection<? extends Iterable<T>>, PCollection<T>> {
    private Iterables() {}

    private void validateInputCoder(Coder<? extends Iterable<T>> inCoder) { // Optimized by LLM: Extracted coder validation into a separate method
      if (!(inCoder instanceof IterableLikeCoder)) {
        throw new IllegalArgumentException(
            "expecting the input Coder<Iterable> to be an IterableLikeCoder");
      }
    }

    @Override
    public PCollection<T> expand(PCollection<? extends Iterable<T>> in) {
      Coder<? extends Iterable<T>> inCoder = in.getCoder();
      validateInputCoder(inCoder); // Optimized by LLM: Used extracted method for coder validation
      @SuppressWarnings("unchecked")
      Coder<T> elemCoder = ((IterableLikeCoder<T, ?>) inCoder).getElemCoder();

      return in.apply(
              "FlattenIterables",
              FlatMapElements.via(
                  new SimpleFunction<Iterable<T>, Iterable<T>>() {
                    @Override
                    public Iterable<T> apply(Iterable<T> element) {
                      return element;
                    }
                  }))
          .setCoder(elemCoder);
    }
  }
}