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
          // Take coder from first collection. If there are none, will be left unspecified.
          inputs.getAll().isEmpty() ? null : inputs.get(0).getCoder());
    }
  }

  
  public static class Iterables<T>
      extends PTransform<PCollection<? extends Iterable<T>>, PCollection<T>> {
    private Iterables() {}

    @Override
    public PCollection<T> expand(PCollection<? extends Iterable<T>> in) {
      Coder<? extends Iterable<T>> inCoder = in.getCoder();
      if (!(inCoder instanceof IterableLikeCoder)) {
        throw new IllegalArgumentException(
            "expecting the input Coder<Iterable> to be an IterableLikeCoder");
      }
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
