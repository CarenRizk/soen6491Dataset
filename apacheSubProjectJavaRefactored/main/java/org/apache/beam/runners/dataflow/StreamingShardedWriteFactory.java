package org.apache.beam.runners.dataflow;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.util.construction.WriteFilesTranslation;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@VisibleForTesting class StreamingShardedWriteFactory<UserT, DestinationT, OutputT>
      implements PTransformOverrideFactory<
          PCollection<UserT>,
          WriteFilesResult<DestinationT>,
          WriteFiles<UserT, DestinationT, OutputT>> {

    static final int DEFAULT_NUM_SHARDS = 10;
    static final int MULTIPLIER = 2; // Optimized by LLM: Suggestion 4
    final DataflowPipelineWorkerPoolOptions options;

    StreamingShardedWriteFactory(PipelineOptions options) {
      if (options == null) { // Optimized by LLM: Suggestion 7
        throw new IllegalArgumentException("PipelineOptions must not be null");
      }
      this.options = options.as(DataflowPipelineWorkerPoolOptions.class);
    }

    private int calculateNumShards() { // Optimized by LLM: Suggestion 1
      if (options.getMaxNumWorkers() > 0) {
        return options.getMaxNumWorkers() * MULTIPLIER;
      } else if (options.getNumWorkers() > 0) {
        return options.getNumWorkers() * MULTIPLIER;
      } else {
        return DEFAULT_NUM_SHARDS;
      }
    }

    @Override
    public PTransformReplacement<PCollection<UserT>, WriteFilesResult<DestinationT>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<UserT>,
                    WriteFilesResult<DestinationT>,
                    WriteFiles<UserT, DestinationT, OutputT>>
                transform) {
      
      int numShards = calculateNumShards(); // Optimized by LLM: Suggestion 1

      try {
        List<PCollectionView<?>> sideInputs =
            WriteFilesTranslation.getDynamicDestinationSideInputs(transform);
        FileBasedSink sink = WriteFilesTranslation.getSink(transform);
        WriteFiles<UserT, DestinationT, OutputT> replacement =
            WriteFiles.to(sink).withSideInputs(sideInputs);
        if (WriteFilesTranslation.isWindowedWrites(transform)) {
          replacement = replacement.withWindowedWrites();
        }

        if (WriteFilesTranslation.isAutoSharded(transform)) {
          replacement = replacement.withAutoSharding();
          return PTransformReplacement.of(
              PTransformReplacements.getSingletonMainInput(transform), replacement);
        }

        return PTransformReplacement.of(
                PTransformReplacements.getSingletonMainInput(transform),
                replacement.withNumShards(numShards));
      } catch (RuntimeException e) { // Optimized by LLM: Suggestion 2
        // Optimized by LLM: Suggestion 6
        System.err.println("An error occurred: " + e.getMessage());
        throw new RuntimeException(e);
      } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	return null;
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, WriteFilesResult<DestinationT> newOutput) {
      return ReplacementOutputs.tagged(outputs, newOutput);
    }
  }