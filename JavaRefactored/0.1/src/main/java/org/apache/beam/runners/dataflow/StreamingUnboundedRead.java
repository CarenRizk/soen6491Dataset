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
    private static final String STRIP_IDS_TRANSFORM_NAME = "StripIds"; // Optimized by LLM: Used constant for magic string

    public StreamingUnboundedRead(Read.Unbounded<T> transform) {
        this.source = transform.getSource();
    }

    @Override
    public final PCollection<T> expand(PBegin input) {
        validateSource(); // Optimized by LLM: Moved validate call to a separate method

        if (source.requiresDeduping()) {
            return Pipeline.applyTransform(input, createReadWithIdsTransform()).apply(new Deduplicate<>());
        } else {
            return Pipeline.applyTransform(input, createReadWithIdsTransform())
                .apply(STRIP_IDS_TRANSFORM_NAME, ParDo.of(new ValueWithRecordId.StripIdsDoFn<>()));
        }
    }

    // Optimized by LLM: Extracted logic for creating ReadWithIds transform into a separate method
    private ReadWithIds<T> createReadWithIdsTransform() {
        return new ReadWithIds<>(source);
    }

    // Optimized by LLM: Extracted validation logic into a separate method
    private void validateSource() {
        try {
            source.validate();
        } catch (Exception e) {
            throw new IllegalArgumentException("Source validation failed: " + e.getMessage(), e); // Optimized by LLM: Implemented error handling for validate method
        }
    }

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