package org.apache.beam.runners.dataflow;

import java.util.Map;

import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Create.Values;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

class AlwaysCreateViaRead<T>
      implements PTransformOverrideFactory<PBegin, PCollection<T>, Create.Values<T>> {
    @Override
    public PTransformOverrideFactory.PTransformReplacement<PBegin, PCollection<T>>
        getReplacementTransform(
            AppliedPTransform<PBegin, PCollection<T>, Create.Values<T>> appliedTransform) {
      return PTransformOverrideFactory.PTransformReplacement.of(
          appliedTransform.getPipeline().begin(), appliedTransform.getTransform().alwaysUseRead());
    }

    @Override
    public final Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<T> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }