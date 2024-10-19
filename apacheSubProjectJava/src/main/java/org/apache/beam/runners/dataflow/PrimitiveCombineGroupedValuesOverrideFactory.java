package org.apache.beam.runners.dataflow;

import java.util.Map;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.GroupedValues;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

class PrimitiveCombineGroupedValuesOverrideFactory<K, InputT, OutputT>
      implements PTransformOverrideFactory<
          PCollection<KV<K, Iterable<InputT>>>,
          PCollection<KV<K, OutputT>>,
          Combine.GroupedValues<K, InputT, OutputT>> {

    @Override
    public PTransformReplacement<PCollection<KV<K, Iterable<InputT>>>, PCollection<KV<K, OutputT>>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<KV<K, Iterable<InputT>>>,
                    PCollection<KV<K, OutputT>>,
                    GroupedValues<K, InputT, OutputT>>
                transform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          new CombineGroupedValues<>(
              transform.getTransform(),
              PTransformReplacements.getSingletonMainOutput(transform).getCoder()));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<KV<K, OutputT>> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }