package org.apache.beam.runners.dataflow;

import java.util.Map;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.GroupedValues;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A factory for overriding the Combine.GroupedValues transform in Apache Beam.
 * This factory provides a replacement transform that combines grouped values.
 *
 * @param <K> The type of the key.
 * @param <InputT> The type of the input values.
 * @param <OutputT> The type of the output values.
 */
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
                appliedTransform) { // Optimized by LLM: Renamed 'transform' to 'appliedTransform' for clarity
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(appliedTransform),
          createCombineGroupedValues(appliedTransform)); // Optimized by LLM: Extracted CombineGroupedValues creation to a separate method
    }

    // Optimized by LLM: Created a private method to handle CombineGroupedValues instantiation
    private CombineGroupedValues<K, InputT, OutputT> createCombineGroupedValues(
            AppliedPTransform<
                    PCollection<KV<K, Iterable<InputT>>>,
                    PCollection<KV<K, OutputT>>,
                    GroupedValues<K, InputT, OutputT>> appliedTransform) {
        // Optimized by LLM: Added input validation for null values
        if (appliedTransform == null || appliedTransform.getTransform() == null) {
            throw new IllegalArgumentException("Applied transform or its transform cannot be null");
        }
        return new CombineGroupedValues<>(
            appliedTransform.getTransform(),
            PTransformReplacements.getSingletonMainOutput(appliedTransform).getCoder());
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<KV<K, OutputT>> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
}