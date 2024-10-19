package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Combine.GroupedValues;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

class CombineGroupedValues<K, InputT, OutputT>
      extends PTransform<PCollection<KV<K, Iterable<InputT>>>, PCollection<KV<K, OutputT>>> {

    private final Combine.GroupedValues<K, InputT, OutputT> original;
    private final Coder<KV<K, OutputT>> outputCoder;

    CombineGroupedValues(
        GroupedValues<K, InputT, OutputT> original, Coder<KV<K, OutputT>> outputCoder) {
      this.original = original;
      this.outputCoder = outputCoder;
    }

    @Override
    public PCollection<KV<K, OutputT>> expand(PCollection<KV<K, Iterable<InputT>>> input) {
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(), input.getWindowingStrategy(), input.isBounded(), outputCoder);
    }

    public Combine.GroupedValues<K, InputT, OutputT> getOriginalCombine() {
      return original;
    }
  }