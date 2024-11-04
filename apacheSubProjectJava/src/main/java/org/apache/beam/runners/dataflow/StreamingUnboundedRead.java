package org.apache.beam.runners.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.NameUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection.IsBounded;

  class StreamingUnboundedRead<T> extends PTransform<PBegin, PCollection<T>> {

    private final UnboundedSource<T, ?> source;

    public StreamingUnboundedRead(Read.Unbounded<T> transform) {
      this.source = transform.getSource();
    }

    @Override
    public final PCollection<T> expand(PBegin input) {
      source.validate();

      if (source.requiresDeduping()) {
        return Pipeline.applyTransform(input, new StreamingUnboundedRead.ReadWithIds<>(source)).apply(new Deduplicate<>());
      } else {
        return Pipeline.applyTransform(input, new StreamingUnboundedRead.ReadWithIds<>(source))
            .apply("StripIds", ParDo.of(new ValueWithRecordId.StripIdsDoFn<>()));
      }
    }

    /**
     * {@link PTransform} that reads {@code (record,recordId)} pairs from an {@link
     * UnboundedSource}.
     */
    private static class ReadWithIds<T>
        extends PTransform<PInput, PCollection<ValueWithRecordId<T>>> {

      private final UnboundedSource<T, ?> source;

      private ReadWithIds(UnboundedSource<T, ?> source) {
        this.source = source;
      }

      @Override
      public final PCollection<ValueWithRecordId<T>> expand(PInput input) {
        return PCollection.createPrimitiveOutputInternal(
            input.getPipeline(),
            WindowingStrategy.globalDefault(),
            IsBounded.UNBOUNDED,
            ValueWithRecordId.ValueWithRecordIdCoder.of(source.getOutputCoder()));
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.delegate(source);
      }

      public UnboundedSource<T, ?> getSource() {
        return source;
      }
    }

    @Override
    public String getKindString() {
      return String.format("Read(%s)", NameUtils.approximateSimpleName(source));
    }

    static {
      DataflowPipelineTranslator.registerTransformTranslator(
          StreamingUnboundedRead.ReadWithIds.class, new ReadWithIdsTranslator());
    }

    private static class ReadWithIdsTranslator implements TransformTranslator<StreamingUnboundedRead.ReadWithIds<?>> {

      @Override
      public void translate(
          StreamingUnboundedRead.ReadWithIds<?> transform, TransformTranslator.TranslationContext context) {
        ReadTranslator.translateReadHelper(transform.getSource(), transform, context);
      }
    }
  }