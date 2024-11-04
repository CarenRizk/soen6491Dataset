package org.apache.beam.runners.dataflow;

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

@VisibleForTesting class StreamingShardedWriteFactory<UserT, DestinationT, OutputT>
      implements PTransformOverrideFactory<
          PCollection<UserT>,
          WriteFilesResult<DestinationT>,
          WriteFiles<UserT, DestinationT, OutputT>> {

    static final int DEFAULT_NUM_SHARDS = 10;
    final DataflowPipelineWorkerPoolOptions options;

    StreamingShardedWriteFactory(PipelineOptions options) {
      this.options = options.as(DataflowPipelineWorkerPoolOptions.class);
    }

    @Override
    public PTransformReplacement<PCollection<UserT>, WriteFilesResult<DestinationT>>
        getReplacementTransform(
            AppliedPTransform<
                    PCollection<UserT>,
                    WriteFilesResult<DestinationT>,
                    WriteFiles<UserT, DestinationT, OutputT>>
                transform) {
      
      
      
      
      
      
      int numShards;
      if (options.getMaxNumWorkers() > 0) {
        numShards = options.getMaxNumWorkers() * 2;
      } else if (options.getNumWorkers() > 0) {
        numShards = options.getNumWorkers() * 2;
      } else {
        numShards = DEFAULT_NUM_SHARDS;
      }

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
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, WriteFilesResult<DestinationT> newOutput) {
      return ReplacementOutputs.tagged(outputs, newOutput);
    }
  }