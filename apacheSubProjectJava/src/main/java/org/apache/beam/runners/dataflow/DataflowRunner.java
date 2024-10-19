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
package org.apache.beam.runners.dataflow;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.ENABLE_CUSTOM_PUBSUB_SINK;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.ENABLE_CUSTOM_PUBSUB_SOURCE;
import static org.apache.beam.sdk.util.SerializableUtils.serializeToByteArray;
import static org.apache.beam.sdk.util.StringUtils.byteArrayToJsonString;
import static org.apache.beam.sdk.util.construction.resources.PipelineResources.detectClassPathResourcesToStage;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings.isNullOrEmpty;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.ListJobsResponse;
import com.google.api.services.dataflow.model.SdkHarnessContainerImage;
import com.google.api.services.dataflow.model.WorkerPool;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.dataflow.DataflowPipelineTranslator.JobSpecification;
import org.apache.beam.runners.dataflow.StreamingViewOverrides.StreamingCreatePCollectionViewFactory;
import org.apache.beam.runners.dataflow.TransformTranslator.StepTranslationContext;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.dataflow.options.DataflowStreamingPipelineOptions;
import org.apache.beam.runners.dataflow.util.DataflowTemplateJob;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.runners.dataflow.util.PackageUtil.StagedFile;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.gcp.storage.PathValidator;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.StorageApiLoads;
import org.apache.beam.sdk.io.gcp.bigquery.StreamingWriteTables;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSink;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSource;
import org.apache.beam.sdk.io.gcp.pubsublite.internal.SubscribeTransform;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.MultimapState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.util.construction.BeamUrns;
import org.apache.beam.sdk.util.construction.DeduplicatedFlattenFactory;
import org.apache.beam.sdk.util.construction.EmptyFlattenAsCreateFactory;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.External;
import org.apache.beam.sdk.util.construction.PTransformMatchers;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.util.construction.SplittableParDoNaiveBounded;
import org.apache.beam.sdk.util.construction.UnconsumedReads;
import org.apache.beam.sdk.util.construction.graph.ProjectionPushdownOptimizer;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.TextFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.HashCode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PipelineRunner} that executes the operations in the pipeline by first translating them
 * to the Dataflow representation using the {@link DataflowPipelineTranslator} and then submitting
 * them to a Dataflow service for execution.
 *
 * <h3>Permissions</h3>
 *
 * <p>When reading from a Dataflow source or writing to a Dataflow sink using {@code
 * DataflowRunner}, the Google cloudservices account and the Google compute engine service account
 * of the GCP project running the Dataflow Job will need access to the corresponding source/sink.
 *
 * <p>Please see <a href="https://cloud.google.com/dataflow/security-and-permissions">Google Cloud
 * Dataflow Security and Permissions</a> for more details.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class DataflowRunner extends PipelineRunner<DataflowPipelineJob> {

  /** Experiment to "unsafely attempt to process unbounded data in batch mode". */
  public static final String UNSAFELY_ATTEMPT_TO_PROCESS_UNBOUNDED_DATA_IN_BATCH_MODE =
      "unsafely_attempt_to_process_unbounded_data_in_batch_mode";

  private static final Logger LOG = LoggerFactory.getLogger(DataflowRunner.class);
  /** Provided configuration options. */
  private final DataflowPipelineOptions options;

  /** Client for the Dataflow service. This is used to actually submit jobs. */
  private final DataflowClient dataflowClient;

  /** Translator for this DataflowRunner, based on options. */
  private final DataflowPipelineTranslator translator;

  /** A set of user defined functions to invoke at different points in execution. */
  private DataflowRunnerHooks hooks;

  // The limit of CreateJob request size.
  private static final int CREATE_JOB_REQUEST_LIMIT_BYTES = 10 * 1024 * 1024;

  @VisibleForTesting static final int GCS_UPLOAD_BUFFER_SIZE_BYTES_DEFAULT = 1024 * 1024;

  @VisibleForTesting static final String PIPELINE_FILE_NAME = "pipeline.pb";
  @VisibleForTesting static final String DATAFLOW_GRAPH_FILE_NAME = "dataflow_graph.json";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Use an {@link ObjectMapper} configured with any {@link Module}s in the class path allowing for
   * user specified configuration injection into the ObjectMapper. This supports user custom types
   * on {@link PipelineOptions}.
   */
  private static final ObjectMapper MAPPER_WITH_MODULES =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

  private final Set<PCollection<?>> pcollectionsRequiringIndexedFormat;

  private final Set<PCollection<?>> pCollectionsPreservedKeys;
  private final Set<PCollection<?>> pcollectionsRequiringAutoSharding;

  /**
   * Project IDs must contain lowercase letters, digits, or dashes. IDs must start with a letter and
   * may not end with a dash. This regex isn't exact - this allows for patterns that would be
   * rejected by the service, but this is sufficient for basic validation of project IDs.
   */
  public static final String PROJECT_ID_REGEXP = "[a-z][-a-z0-9:.]+[a-z0-9]";

  /** Dataflow service endpoints are expected to match this pattern. */
  static final String ENDPOINT_REGEXP = "https://[\\S]*googleapis\\.com[/]?";

  /**
   * Construct a runner from the provided options.
   *
   * @param options Properties that configure the runner.
   * @return The newly created runner.
   */
  public static DataflowRunner fromOptions(PipelineOptions options) {
    DataflowPipelineOptions dataflowOptions =
        PipelineOptionsValidator.validate(DataflowPipelineOptions.class, options);
    ArrayList<String> missing = new ArrayList<>();

    if (dataflowOptions.getAppName() == null) {
      missing.add("appName");
    }

    if (Strings.isNullOrEmpty(dataflowOptions.getRegion())
        && isServiceEndpoint(dataflowOptions.getDataflowEndpoint())) {
      missing.add("region");
    }
    if (missing.size() > 0) {
      throw new IllegalArgumentException(
          "Missing required pipeline options: " + Joiner.on(',').join(missing));
    }

    validateWorkerSettings(
        PipelineOptionsValidator.validate(DataflowPipelineWorkerPoolOptions.class, options));

    PathValidator validator = dataflowOptions.getPathValidator();
    String gcpTempLocation;
    try {
      gcpTempLocation = dataflowOptions.getGcpTempLocation();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "DataflowRunner requires gcpTempLocation, "
              + "but failed to retrieve a value from PipelineOptions",
          e);
    }
    validator.validateOutputFilePrefixSupported(gcpTempLocation);

    String stagingLocation;
    try {
      stagingLocation = dataflowOptions.getStagingLocation();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "DataflowRunner requires stagingLocation, "
              + "but failed to retrieve a value from PipelineOptions",
          e);
    }
    validator.validateOutputFilePrefixSupported(stagingLocation);

    if (!isNullOrEmpty(dataflowOptions.getSaveProfilesToGcs())) {
      validator.validateOutputFilePrefixSupported(dataflowOptions.getSaveProfilesToGcs());
    }

    if (dataflowOptions.getFilesToStage() != null) {
      // The user specifically requested these files, so fail now if they do not exist.
      // (automatically detected classpath elements are permitted to not exist, so later
      // staging will not fail on nonexistent files)
      dataflowOptions.getFilesToStage().stream()
          .forEach(
              stagedFileSpec -> {
                File localFile;
                if (stagedFileSpec.contains("=")) {
                  String[] components = stagedFileSpec.split("=", 2);
                  localFile = new File(components[1]);
                } else {
                  localFile = new File(stagedFileSpec);
                }
                if (!localFile.exists()) {
                  // should be FileNotFoundException, but for build-time backwards compatibility
                  // cannot add checked exception
                  throw new RuntimeException(
                      String.format("Non-existent files specified in filesToStage: %s", localFile));
                }
              });
    } else {
      dataflowOptions.setFilesToStage(
          detectClassPathResourcesToStage(DataflowRunner.class.getClassLoader(), options));
      if (dataflowOptions.getFilesToStage().isEmpty()) {
        throw new IllegalArgumentException("No files to stage has been found.");
      } else {
        LOG.info(
            "PipelineOptions.filesToStage was not specified. "
                + "Defaulting to files from the classpath: will stage {} files. "
                + "Enable logging at DEBUG level to see which files will be staged.",
            dataflowOptions.getFilesToStage().size());
        LOG.debug("Classpath elements: {}", dataflowOptions.getFilesToStage());
      }
    }

    // Verify jobName according to service requirements, truncating converting to lowercase if
    // necessary.
    String jobName = dataflowOptions.getJobName().toLowerCase();
    checkArgument(
        jobName.matches("[a-z]([-a-z0-9]*[a-z0-9])?"),
        "JobName invalid; the name must consist of only the characters "
            + "[-a-z0-9], starting with a letter and ending with a letter "
            + "or number");
    if (!jobName.equals(dataflowOptions.getJobName())) {
      LOG.info(
          "PipelineOptions.jobName did not match the service requirements. "
              + "Using {} instead of {}.",
          jobName,
          dataflowOptions.getJobName());
    }
    dataflowOptions.setJobName(jobName);

    // Verify project
    String project = dataflowOptions.getProject();
    if (project.matches("[0-9]*")) {
      throw new IllegalArgumentException(
          "Project ID '"
              + project
              + "' invalid. Please make sure you specified the Project ID, not project number.");
    } else if (!project.matches(PROJECT_ID_REGEXP)) {
      throw new IllegalArgumentException(
          "Project ID '"
              + project
              + "' invalid. Please make sure you specified the Project ID, not project"
              + " description.");
    }

    DataflowPipelineDebugOptions debugOptions =
        dataflowOptions.as(DataflowPipelineDebugOptions.class);
    // Verify the number of worker threads is a valid value
    if (debugOptions.getNumberOfWorkerHarnessThreads() < 0) {
      throw new IllegalArgumentException(
          "Number of worker harness threads '"
              + debugOptions.getNumberOfWorkerHarnessThreads()
              + "' invalid. Please make sure the value is non-negative.");
    }

    // Verify that if recordJfrOnGcThrashing is set, the pipeline is at least on java 11
    if (dataflowOptions.getRecordJfrOnGcThrashing()
        && Environments.getJavaVersion() == Environments.JavaVersion.java8) {
      throw new IllegalArgumentException(
          "recordJfrOnGcThrashing is only supported on java 9 and up.");
    }

    if (dataflowOptions.isStreaming() && dataflowOptions.getGcsUploadBufferSizeBytes() == null) {
      dataflowOptions.setGcsUploadBufferSizeBytes(GCS_UPLOAD_BUFFER_SIZE_BYTES_DEFAULT);
    }

    // Adding the Java version to the SDK name for user's and support convenience.
    String agentJavaVer = "(JRE 8 environment)";
    if (Environments.getJavaVersion() != Environments.JavaVersion.java8) {
      agentJavaVer =
          String.format("(JRE %s environment)", Environments.getJavaVersion().specification());
    }

    DataflowRunnerInfo dataflowRunnerInfo = DataflowRunnerInfo.getDataflowRunnerInfo();
    String userAgentName = dataflowRunnerInfo.getName();
    Preconditions.checkArgument(
        !userAgentName.equals(""), "Dataflow runner's `name` property cannot be empty.");
    String userAgentVersion = dataflowRunnerInfo.getVersion();
    Preconditions.checkArgument(
        !userAgentVersion.equals(""), "Dataflow runner's `version` property cannot be empty.");
    String userAgent =
        String.format("%s/%s%s", userAgentName, userAgentVersion, agentJavaVer).replace(" ", "_");
    dataflowOptions.setUserAgent(userAgent);

    return new DataflowRunner(dataflowOptions);
  }

  static boolean isServiceEndpoint(String endpoint) {
    return Strings.isNullOrEmpty(endpoint) || Pattern.matches(ENDPOINT_REGEXP, endpoint);
  }

  static void validateSdkContainerImageOptions(DataflowPipelineWorkerPoolOptions workerOptions) {
    // Check against null - empty string value for workerHarnessContainerImage
    // must be preserved for legacy dataflowWorkerJar to work.
    String sdkContainerOption = workerOptions.getSdkContainerImage();
    String workerHarnessOption = workerOptions.getWorkerHarnessContainerImage();
    Preconditions.checkArgument(
        sdkContainerOption == null
            || workerHarnessOption == null
            || sdkContainerOption.equals(workerHarnessOption),
        "Cannot use legacy option workerHarnessContainerImage with sdkContainerImage. Prefer sdkContainerImage.");

    // Default to new option, which may be null.
    String containerImage = workerOptions.getSdkContainerImage();
    if (workerOptions.getWorkerHarnessContainerImage() != null
        && workerOptions.getSdkContainerImage() == null) {
      // Set image to old option if old option was set but new option is not set.
      LOG.warn(
          "Prefer --sdkContainerImage over deprecated legacy option --workerHarnessContainerImage.");
      containerImage = workerOptions.getWorkerHarnessContainerImage();
    }

    // Make sure both options have same value.
    workerOptions.setSdkContainerImage(containerImage);
    workerOptions.setWorkerHarnessContainerImage(containerImage);
  }

  @VisibleForTesting
  static void validateWorkerSettings(DataflowPipelineWorkerPoolOptions workerOptions) {
    DataflowPipelineOptions dataflowOptions = workerOptions.as(DataflowPipelineOptions.class);

    validateSdkContainerImageOptions(workerOptions);

    GcpOptions gcpOptions = workerOptions.as(GcpOptions.class);
    Preconditions.checkArgument(
        gcpOptions.getZone() == null || gcpOptions.getWorkerRegion() == null,
        "Cannot use option zone with workerRegion. Prefer either workerZone or workerRegion.");
    Preconditions.checkArgument(
        gcpOptions.getZone() == null || gcpOptions.getWorkerZone() == null,
        "Cannot use option zone with workerZone. Prefer workerZone.");
    Preconditions.checkArgument(
        gcpOptions.getWorkerRegion() == null || gcpOptions.getWorkerZone() == null,
        "workerRegion and workerZone options are mutually exclusive.");

    boolean hasExperimentWorkerRegion = false;
    if (dataflowOptions.getExperiments() != null) {
      for (String experiment : dataflowOptions.getExperiments()) {
        if (experiment.startsWith("worker_region")) {
          hasExperimentWorkerRegion = true;
          break;
        }
      }
    }
    Preconditions.checkArgument(
        !hasExperimentWorkerRegion || gcpOptions.getWorkerRegion() == null,
        "Experiment worker_region and option workerRegion are mutually exclusive.");
    Preconditions.checkArgument(
        !hasExperimentWorkerRegion || gcpOptions.getWorkerZone() == null,
        "Experiment worker_region and option workerZone are mutually exclusive.");

    if (gcpOptions.getZone() != null) {
      LOG.warn("Option --zone is deprecated. Please use --workerZone instead.");
      gcpOptions.setWorkerZone(gcpOptions.getZone());
      gcpOptions.setZone(null);
    }
  }

  @VisibleForTesting
  protected DataflowRunner(DataflowPipelineOptions options) {
    this.options = options;
    this.dataflowClient = DataflowClient.create(options);
    this.translator = DataflowPipelineTranslator.fromOptions(options);
    this.pcollectionsRequiringIndexedFormat = new HashSet<>();
    this.pCollectionsPreservedKeys = new HashSet<>();
    this.pcollectionsRequiringAutoSharding = new HashSet<>();
    this.ptransformViewsWithNonDeterministicKeyCoders = new HashSet<>();
  }

  static void configureSdkHarnessContainerImages(
      DataflowPipelineOptions options, RunnerApi.Pipeline pipelineProto, Job newJob) {
    List<SdkHarnessContainerImage> sdkContainerList =
        getAllEnvironmentInfo(pipelineProto).stream()
            .map(
                environmentInfo -> {
                  SdkHarnessContainerImage image = new SdkHarnessContainerImage();
                  image.setEnvironmentId(environmentInfo.environmentId());
                  image.setContainerImage(environmentInfo.containerUrl());
                  if (!environmentInfo
                      .capabilities()
                      .contains(
                          BeamUrns.getUrn(
                              RunnerApi.StandardProtocols.Enum.MULTI_CORE_BUNDLE_PROCESSING))) {
                    image.setUseSingleCorePerContainer(true);
                  }
                  image.setCapabilities(environmentInfo.capabilities());
                  return image;
                })
            .collect(Collectors.toList());
    for (WorkerPool workerPool : newJob.getEnvironment().getWorkerPools()) {
      workerPool.setSdkHarnessContainerImages(sdkContainerList);
    }
  }

  private List<PTransformOverride> getOverrides(boolean streaming) {
	    ImmutableList.Builder<PTransformOverride> overridesBuilder = ImmutableList.builder();

	    addCommonOverrides(overridesBuilder);
	    
	    if (streaming) {
	        addStreamingOverrides(overridesBuilder);
	    } else {
	        addBatchOverrides(overridesBuilder);
	    }

	    return overridesBuilder.build();
	}

	private void addCommonOverrides(ImmutableList.Builder<PTransformOverride> overridesBuilder) {
	    // Add common overrides that apply to both streaming and batch
	    overridesBuilder
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.flattenWithDuplicateInputs(),
	                DeduplicatedFlattenFactory.create()))
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.emptyFlatten(), EmptyFlattenAsCreateFactory.instance()));
	}

	private void addStreamingOverrides(ImmutableList.Builder<PTransformOverride> overridesBuilder) {
	    // Overrides specific to streaming mode
	    if (!hasExperiment(options, ENABLE_CUSTOM_PUBSUB_SOURCE)) {
	        overridesBuilder.add(
	            PTransformOverride.of(
	                PTransformMatchers.classEqualTo(PubsubUnboundedSource.class),
	                new StreamingPubsubIOReadOverrideFactory()));
	    }
	    if (!hasExperiment(options, ENABLE_CUSTOM_PUBSUB_SINK)) {
	        overridesBuilder.add(
	            PTransformOverride.of(
	                PTransformMatchers.classEqualTo(PubsubUnboundedSink.class),
	                new StreamingPubsubIOWriteOverrideFactory(this)));
	    }

	    addOptionalKafkaRead(overridesBuilder);

	    overridesBuilder
	        .add(SubscribeTransform.V1_READ_OVERRIDE)
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.writeWithRunnerDeterminedSharding(),
	                new StreamingShardedWriteFactory(options)))
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.groupIntoBatches(),
	                new GroupIntoBatchesOverride.StreamingGroupIntoBatchesOverrideFactory(this)))
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.groupWithShardableStates(),
	                new GroupIntoBatchesOverride.StreamingGroupIntoBatchesWithShardedKeyOverrideFactory(this)))
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.classEqualTo(Read.Bounded.class),
	                new StreamingBoundedReadOverrideFactory()))
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.classEqualTo(Read.Unbounded.class),
	                new StreamingUnboundedReadOverrideFactory()))
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.classEqualTo(View.CreatePCollectionView.class),
	                new StreamingCreatePCollectionViewFactory()));
	}

	private void addBatchOverrides(ImmutableList.Builder<PTransformOverride> overridesBuilder) {
	    // Overrides specific to batch mode
	    overridesBuilder.add(SplittableParDo.PRIMITIVE_BOUNDED_READ_OVERRIDE)
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.classEqualTo(GroupIntoBatches.class),
	                new GroupIntoBatchesOverride.BatchGroupIntoBatchesOverrideFactory<>(this)))
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.classEqualTo(GroupIntoBatches.WithShardedKey.class),
	                new GroupIntoBatchesOverride.BatchGroupIntoBatchesWithShardedKeyOverrideFactory<>(this)))
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.stateOrTimerParDoMulti(),
	                BatchStatefulParDoOverrides.multiOutputOverrideFactory(options)))
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.stateOrTimerParDoSingle(),
	                BatchStatefulParDoOverrides.singleOutputOverrideFactory()))
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.splittableProcessKeyedBounded(),
	                new SplittableParDoNaiveBounded.OverrideFactory()))
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.classEqualTo(View.AsMap.class),
	                new ReflectiveViewOverrideFactory(BatchViewOverrides.BatchViewAsMap.class, this)))
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.classEqualTo(View.AsMultimap.class),
	                new ReflectiveViewOverrideFactory(BatchViewOverrides.BatchViewAsMultimap.class, this)))
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.classEqualTo(Combine.GloballyAsSingletonView.class),
	                new CombineGloballyAsSingletonViewOverrideFactory(this)))
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.classEqualTo(View.AsList.class),
	                new ReflectiveViewOverrideFactory(BatchViewOverrides.BatchViewAsList.class, this)))
	        .add(
	            PTransformOverride.of(
	                PTransformMatchers.classEqualTo(View.AsIterable.class),
	                new ReflectiveViewOverrideFactory(BatchViewOverrides.BatchViewAsIterable.class, this)));
	}

	private void addOptionalKafkaRead(ImmutableList.Builder<PTransformOverride> overridesBuilder) {
	    try {
	        overridesBuilder.add(KafkaIO.Read.KAFKA_READ_OVERRIDE);
	    } catch (NoClassDefFoundError e) {
	        // KafkaIO is optional, ignore if not present
	    }
	}

  private RunnerApi.Pipeline resolveAnyOfEnvironments(RunnerApi.Pipeline pipeline) {
    RunnerApi.Pipeline.Builder pipelineBuilder = pipeline.toBuilder();
    RunnerApi.Components.Builder componentsBuilder = pipelineBuilder.getComponentsBuilder();
    componentsBuilder.clearEnvironments();
    for (Map.Entry<String, RunnerApi.Environment> entry :
        pipeline.getComponents().getEnvironmentsMap().entrySet()) {
      componentsBuilder.putEnvironments(
          entry.getKey(),
          Environments.resolveAnyOfEnvironment(
              entry.getValue(),
              BeamUrns.getUrn(RunnerApi.StandardEnvironments.Environments.DOCKER)));
    }
    return pipelineBuilder.build();
  }

  protected RunnerApi.Pipeline applySdkEnvironmentOverrides(
      RunnerApi.Pipeline pipeline, DataflowPipelineOptions options) {
    String sdkHarnessContainerImageOverrides = options.getSdkHarnessContainerImageOverrides();
    String[] overrides =
        Strings.isNullOrEmpty(sdkHarnessContainerImageOverrides)
            ? new String[0]
            : sdkHarnessContainerImageOverrides.split(",", -1);
    if (overrides.length % 2 != 0) {
      throw new RuntimeException(
          "invalid syntax for SdkHarnessContainerImageOverrides: "
              + options.getSdkHarnessContainerImageOverrides());
    }
    RunnerApi.Pipeline.Builder pipelineBuilder = pipeline.toBuilder();
    RunnerApi.Components.Builder componentsBuilder = pipelineBuilder.getComponentsBuilder();
    componentsBuilder.clearEnvironments();
    for (Map.Entry<String, RunnerApi.Environment> entry :
        pipeline.getComponents().getEnvironmentsMap().entrySet()) {
      RunnerApi.Environment.Builder environmentBuilder = entry.getValue().toBuilder();
      if (BeamUrns.getUrn(RunnerApi.StandardEnvironments.Environments.DOCKER)
          .equals(environmentBuilder.getUrn())) {
        RunnerApi.DockerPayload dockerPayload;
        try {
          dockerPayload = RunnerApi.DockerPayload.parseFrom(environmentBuilder.getPayload());
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException("Error parsing environment docker payload.", e);
        }
        String containerImage = dockerPayload.getContainerImage();
        boolean updated = false;
        for (int i = 0; i < overrides.length; i += 2) {
          containerImage = containerImage.replaceAll(overrides[i], overrides[i + 1]);
          if (!containerImage.equals(dockerPayload.getContainerImage())) {
            updated = true;
          }
        }
        if (containerImage.startsWith("apache/beam")
            && !updated
            // don't update if the container image is already configured by DataflowRunner
            && !containerImage.equals(getContainerImageForJob(options))) {
          containerImage =
              DataflowRunnerInfo.getDataflowRunnerInfo().getContainerImageBaseRepository()
                  + containerImage.substring(containerImage.lastIndexOf("/"));
        }
        environmentBuilder.setPayload(
            RunnerApi.DockerPayload.newBuilder()
                .setContainerImage(containerImage)
                .build()
                .toByteString());
      }
      componentsBuilder.putEnvironments(entry.getKey(), environmentBuilder.build());
    }
    return pipelineBuilder.build();
  }

  @VisibleForTesting
  protected RunnerApi.Pipeline resolveArtifacts(RunnerApi.Pipeline pipeline) {
    RunnerApi.Pipeline.Builder pipelineBuilder = pipeline.toBuilder();
    RunnerApi.Components.Builder componentsBuilder = pipelineBuilder.getComponentsBuilder();
    componentsBuilder.clearEnvironments();
    for (Map.Entry<String, RunnerApi.Environment> entry :
        pipeline.getComponents().getEnvironmentsMap().entrySet()) {
      RunnerApi.Environment.Builder environmentBuilder = entry.getValue().toBuilder();
      environmentBuilder.clearDependencies();
      for (RunnerApi.ArtifactInformation info : entry.getValue().getDependenciesList()) {
        if (!BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.FILE).equals(info.getTypeUrn())) {
          throw new RuntimeException(
              String.format("unsupported artifact type %s", info.getTypeUrn()));
        }
        RunnerApi.ArtifactFilePayload filePayload;
        try {
          filePayload = RunnerApi.ArtifactFilePayload.parseFrom(info.getTypePayload());
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException("Error parsing artifact file payload.", e);
        }
        String stagedName;
        if (BeamUrns.getUrn(RunnerApi.StandardArtifacts.Roles.STAGING_TO)
            .equals(info.getRoleUrn())) {
          try {
            RunnerApi.ArtifactStagingToRolePayload stagingPayload =
                RunnerApi.ArtifactStagingToRolePayload.parseFrom(info.getRolePayload());
            stagedName = stagingPayload.getStagedName();
          } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Error parsing artifact staging_to role payload.", e);
          }
        } else {
          try {
            File source = new File(filePayload.getPath());
            HashCode hashCode = Files.asByteSource(source).hash(Hashing.sha256());
            stagedName = Environments.createStagingFileName(source, hashCode);
          } catch (IOException e) {
            throw new RuntimeException(
                String.format("Error creating staged name for artifact %s", filePayload.getPath()),
                e);
          }
        }
        environmentBuilder.addDependencies(
            info.toBuilder()
                .setTypeUrn(BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.URL))
                .setTypePayload(
                    RunnerApi.ArtifactUrlPayload.newBuilder()
                        .setUrl(
                            FileSystems.matchNewResource(options.getStagingLocation(), true)
                                .resolve(
                                    stagedName, ResolveOptions.StandardResolveOptions.RESOLVE_FILE)
                                .toString())
                        .setSha256(filePayload.getSha256())
                        .build()
                        .toByteString()));
      }
      componentsBuilder.putEnvironments(entry.getKey(), environmentBuilder.build());
    }
    return pipelineBuilder.build();
  }

  protected List<DataflowPackage> stageArtifacts(RunnerApi.Pipeline pipeline) {
    ImmutableList.Builder<StagedFile> filesToStageBuilder = ImmutableList.builder();
    Set<String> stagedNames = new HashSet<>();
    for (Map.Entry<String, RunnerApi.Environment> entry :
        pipeline.getComponents().getEnvironmentsMap().entrySet()) {
      for (RunnerApi.ArtifactInformation info : entry.getValue().getDependenciesList()) {
        if (!BeamUrns.getUrn(RunnerApi.StandardArtifacts.Types.FILE).equals(info.getTypeUrn())) {
          throw new RuntimeException(
              String.format("unsupported artifact type %s", info.getTypeUrn()));
        }
        RunnerApi.ArtifactFilePayload filePayload;
        try {
          filePayload = RunnerApi.ArtifactFilePayload.parseFrom(info.getTypePayload());
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException("Error parsing artifact file payload.", e);
        }
        String stagedName;
        if (BeamUrns.getUrn(RunnerApi.StandardArtifacts.Roles.STAGING_TO)
            .equals(info.getRoleUrn())) {
          try {
            RunnerApi.ArtifactStagingToRolePayload stagingPayload =
                RunnerApi.ArtifactStagingToRolePayload.parseFrom(info.getRolePayload());
            stagedName = stagingPayload.getStagedName();
          } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Error parsing artifact staging_to role payload.", e);
          }
        } else {
          try {
            File source = new File(filePayload.getPath());
            HashCode hashCode = Files.asByteSource(source).hash(Hashing.sha256());
            stagedName = Environments.createStagingFileName(source, hashCode);
          } catch (IOException e) {
            throw new RuntimeException(
                String.format("Error creating staged name for artifact %s", filePayload.getPath()),
                e);
          }
        }
        if (stagedNames.contains(stagedName)) {
          continue;
        } else {
          stagedNames.add(stagedName);
        }
        filesToStageBuilder.add(
            StagedFile.of(filePayload.getPath(), filePayload.getSha256(), stagedName));
      }
    }
    return options.getStager().stageFiles(filesToStageBuilder.build());
  }

  private List<RunnerApi.ArtifactInformation> getDefaultArtifacts() {
    ImmutableList.Builder<String> pathsToStageBuilder = ImmutableList.builder();
    String windmillBinary =
        options.as(DataflowStreamingPipelineOptions.class).getOverrideWindmillBinary();
    String dataflowWorkerJar = options.getDataflowWorkerJar();
    if (dataflowWorkerJar != null && !dataflowWorkerJar.isEmpty() && !useUnifiedWorker(options)) {
      // Put the user specified worker jar at the start of the classpath, to be consistent with the
      // built in worker order.
      pathsToStageBuilder.add("dataflow-worker.jar=" + dataflowWorkerJar);
    }
    pathsToStageBuilder.addAll(options.getFilesToStage());
    if (windmillBinary != null) {
      pathsToStageBuilder.add("windmill_main=" + windmillBinary);
    }
    return Environments.getArtifacts(pathsToStageBuilder.build());
  }

  @VisibleForTesting
  static boolean isMultiLanguagePipeline(Pipeline pipeline) {
    class IsMultiLanguageVisitor extends PipelineVisitor.Defaults {
      private boolean isMultiLanguage = false;

      private void performMultiLanguageTest(Node node) {
        if (node.getTransform() instanceof External.ExpandableTransform) {
          isMultiLanguage = true;
        }
      }

      @Override
      public CompositeBehavior enterCompositeTransform(Node node) {
        performMultiLanguageTest(node);
        return super.enterCompositeTransform(node);
      }

      @Override
      public void visitPrimitiveTransform(Node node) {
        performMultiLanguageTest(node);
        super.visitPrimitiveTransform(node);
      }
    }

    IsMultiLanguageVisitor visitor = new IsMultiLanguageVisitor();
    pipeline.traverseTopologically(visitor);

    return visitor.isMultiLanguage;
  }

  @Override
  public DataflowPipelineJob run(Pipeline pipeline) {
    // Multi-language pipelines and pipelines that include upgrades should automatically be upgraded
    // to Runner v2.
    if (DataflowRunner.isMultiLanguagePipeline(pipeline) || Schema.Options.includesTransformUpgrades(pipeline)) {
      List<String> experiments = firstNonNull(options.getExperiments(), Collections.emptyList());
      if (!experiments.contains("use_runner_v2")) {
        LOG.info(
            "Automatically enabling Dataflow Runner v2 since the pipeline used cross-language"
                + " transforms or pipeline needed a transform upgrade.");
        options.setExperiments(
            ImmutableList.<String>builder().addAll(experiments).add("use_runner_v2").build());
      }
    }
    if (useUnifiedWorker(options)) {
      if (hasExperiment(options, "disable_runner_v2")
          || hasExperiment(options, "disable_runner_v2_until_2023")
          || hasExperiment(options, "disable_prime_runner_v2")) {
        throw new IllegalArgumentException(
            "Runner V2 both disabled and enabled: at least one of ['beam_fn_api', 'use_unified_worker', 'use_runner_v2', 'use_portable_job_submission'] is set and also one of ['disable_runner_v2', 'disable_runner_v2_until_2023', 'disable_prime_runner_v2'] is set.");
      }
      List<String> experiments =
          new ArrayList<>(options.getExperiments()); // non-null if useUnifiedWorker is true
      if (!experiments.contains("use_runner_v2")) {
        experiments.add("use_runner_v2");
      }
      if (!experiments.contains("use_unified_worker")) {
        experiments.add("use_unified_worker");
      }
      if (!experiments.contains("beam_fn_api")) {
        experiments.add("beam_fn_api");
      }
      if (!experiments.contains("use_portable_job_submission")) {
        experiments.add("use_portable_job_submission");
      }
      options.setExperiments(ImmutableList.copyOf(experiments));
    }

    logWarningIfPCollectionViewHasNonDeterministicKeyCoder(pipeline);
    logWarningIfBigqueryDLQUnused(pipeline);
    if (shouldActAsStreaming(pipeline)) {
      options.setStreaming(true);

      if (useUnifiedWorker(options)) {
        options.setEnableStreamingEngine(true);
        List<String> experiments =
            new ArrayList<>(options.getExperiments()); // non-null if useUnifiedWorker is true
        if (!experiments.contains("enable_streaming_engine")) {
          experiments.add("enable_streaming_engine");
        }
        if (!experiments.contains("enable_windmill_service")) {
          experiments.add("enable_windmill_service");
        }
      }
    }

    if (!ExperimentalOptions.hasExperiment(options, "disable_projection_pushdown")) {
      ProjectionPushdownOptimizer.optimize(pipeline);
    }

    LOG.info(
        "Executing pipeline on the Dataflow Service, which will have billing implications "
            + "related to Google Compute Engine usage and other Google Cloud Services.");

    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    String workerHarnessContainerImageURL = DataflowRunner.getContainerImageForJob(dataflowOptions);

    // This incorrectly puns the worker harness container image (which implements v1beta3 API)
    // with the SDK harness image (which implements Fn API).
    //
    // The same Environment is used in different and contradictory ways, depending on whether
    // it is a v1 or v2 job submission.
    RunnerApi.Environment defaultEnvironmentForDataflow =
        Environments.createDockerEnvironment(workerHarnessContainerImageURL);

    // The SdkComponents for portable an non-portable job submission must be kept distinct. Both
    // need the default environment.
    SdkComponents portableComponents = SdkComponents.create();
    portableComponents.registerEnvironment(
        defaultEnvironmentForDataflow
            .toBuilder()
            .addAllDependencies(getDefaultArtifacts())
            .addAllCapabilities(Environments.getJavaCapabilities())
            .build());

    RunnerApi.Pipeline portablePipelineProto =
        PipelineTranslation.toProto(pipeline, portableComponents, false);
    // Note that `stageArtifacts` has to be called before `resolveArtifact` because
    // `resolveArtifact` updates local paths to staged paths in pipeline proto.
    portablePipelineProto = resolveAnyOfEnvironments(portablePipelineProto);
    List<DataflowPackage> packages = stageArtifacts(portablePipelineProto);
    portablePipelineProto = resolveArtifacts(portablePipelineProto);
    portablePipelineProto = applySdkEnvironmentOverrides(portablePipelineProto, options);
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Portable pipeline proto:\n{}",
          TextFormat.printer().printToString(portablePipelineProto));
    }
    // Stage the portable pipeline proto, retrieving the staged pipeline path, then update
    // the options on the new job
    // TODO: add an explicit `pipeline` parameter to the submission instead of pipeline options
    LOG.info("Staging portable pipeline proto to {}", options.getStagingLocation());
    byte[] serializedProtoPipeline = portablePipelineProto.toByteArray();

    DataflowPackage stagedPipeline =
        options.getStager().stageToFile(serializedProtoPipeline, PIPELINE_FILE_NAME);
    dataflowOptions.setPipelineUrl(stagedPipeline.getLocation());

    if (useUnifiedWorker(options)) {
      LOG.info("Skipping v1 transform replacements since job will run on v2.");
    } else {
      // Now rewrite things to be as needed for v1 (mutates the pipeline)
      // This way the job submitted is valid for v1 and v2, simultaneously
      replaceV1Transforms(pipeline);
    }
    // Capture the SdkComponents for look up during step translations
    SdkComponents dataflowV1Components = SdkComponents.create();
    dataflowV1Components.registerEnvironment(
        defaultEnvironmentForDataflow
            .toBuilder()
            .addAllDependencies(getDefaultArtifacts())
            .addAllCapabilities(Environments.getJavaCapabilities())
            .build());
    // No need to perform transform upgrading for the Runner v1 proto.
    RunnerApi.Pipeline dataflowV1PipelineProto =
        PipelineTranslation.toProto(pipeline, dataflowV1Components, true, false);

    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Dataflow v1 pipeline proto:\n{}",
          TextFormat.printer().printToString(dataflowV1PipelineProto));
    }

    // Set a unique client_request_id in the CreateJob request.
    // This is used to ensure idempotence of job creation across retried
    // attempts to create a job. Specifically, if the service returns a job with
    // a different client_request_id, it means the returned one is a different
    // job previously created with the same job name, and that the job creation
    // has been effectively rejected. The SDK should return
    // Error::Already_Exists to user in that case.
    int randomNum = new Random().nextInt(9000) + 1000;
    String requestId =
        DateTimeFormat.forPattern("YYYYMMddHHmmssmmm")
                .withZone(DateTimeZone.UTC)
                .print(DateTimeUtils.currentTimeMillis())
            + "_"
            + randomNum;

    JobSpecification jobSpecification =
        translator.translate(
            pipeline, dataflowV1PipelineProto, dataflowV1Components, this, packages);

    if (!isNullOrEmpty(dataflowOptions.getDataflowWorkerJar()) && !useUnifiedWorker(options)) {
      List<String> experiments =
          firstNonNull(dataflowOptions.getExperiments(), Collections.emptyList());
      if (!experiments.contains("use_staged_dataflow_worker_jar")) {
        dataflowOptions.setExperiments(
            ImmutableList.<String>builder()
                .addAll(experiments)
                .add("use_staged_dataflow_worker_jar")
                .build());
      }
    }

    Job newJob = jobSpecification.getJob();
    try {
      newJob
          .getEnvironment()
          .setSdkPipelineOptions(
              MAPPER.readValue(MAPPER_WITH_MODULES.writeValueAsBytes(options), Map.class));
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "PipelineOptions specified failed to serialize to JSON.", e);
    }
    newJob.setClientRequestId(requestId);

    DataflowRunnerInfo dataflowRunnerInfo = DataflowRunnerInfo.getDataflowRunnerInfo();
    String version = dataflowRunnerInfo.getVersion();
    checkState(
        !"${pom.version}".equals(version),
        "Unable to submit a job to the Dataflow service with unset version ${pom.version}");
    LOG.info("Dataflow SDK version: {}", version);

    newJob.getEnvironment().setUserAgent((Map) dataflowRunnerInfo.getProperties());
    // The Dataflow Service may write to the temporary directory directly, so
    // must be verified.
    if (!isNullOrEmpty(options.getGcpTempLocation())) {
      newJob
          .getEnvironment()
          .setTempStoragePrefix(
              dataflowOptions.getPathValidator().verifyPath(options.getGcpTempLocation()));
    }
    newJob.getEnvironment().setDataset(options.getTempDatasetId());

    if (options.getWorkerRegion() != null) {
      newJob.getEnvironment().setWorkerRegion(options.getWorkerRegion());
    }
    if (options.getWorkerZone() != null) {
      newJob.getEnvironment().setWorkerZone(options.getWorkerZone());
    }

    if (options.getFlexRSGoal()
        == DataflowPipelineOptions.FlexResourceSchedulingGoal.COST_OPTIMIZED) {
      newJob.getEnvironment().setFlexResourceSchedulingGoal("FLEXRS_COST_OPTIMIZED");
    } else if (options.getFlexRSGoal()
        == DataflowPipelineOptions.FlexResourceSchedulingGoal.SPEED_OPTIMIZED) {
      newJob.getEnvironment().setFlexResourceSchedulingGoal("FLEXRS_SPEED_OPTIMIZED");
    }

    // Represent the minCpuPlatform pipeline option as an experiment, if not already present.
    if (!isNullOrEmpty(dataflowOptions.getMinCpuPlatform())) {
      List<String> experiments =
          firstNonNull(dataflowOptions.getExperiments(), Collections.emptyList());

      List<String> minCpuFlags =
          experiments.stream()
              .filter(p -> p.startsWith("min_cpu_platform"))
              .collect(Collectors.toList());

      if (minCpuFlags.isEmpty()) {
        dataflowOptions.setExperiments(
            ImmutableList.<String>builder()
                .addAll(experiments)
                .add("min_cpu_platform=" + dataflowOptions.getMinCpuPlatform())
                .build());
      } else {
        LOG.warn(
            "Flag min_cpu_platform is defined in both top level PipelineOption, "
                + "as well as under experiments. Proceed using {}.",
            minCpuFlags.get(0));
      }
    }

    newJob
        .getEnvironment()
        .setExperiments(
            ImmutableList.copyOf(
                firstNonNull(dataflowOptions.getExperiments(), Collections.emptyList())));

    // Set the Docker container image that executes Dataflow worker harness, residing in Google
    // Container Registry. Translator is guaranteed to create a worker pool prior to this point.
    // For runner_v1, only worker_harness_container is set.
    // For runner_v2, both worker_harness_container and sdk_harness_container are set to the same
    // value.
    String containerImage = getContainerImageForJob(options);
    for (WorkerPool workerPool : newJob.getEnvironment().getWorkerPools()) {
      workerPool.setWorkerHarnessContainerImage(containerImage);
    }

    configureSdkHarnessContainerImages(options, portablePipelineProto, newJob);

    newJob.getEnvironment().setVersion(getEnvironmentVersion(options));

    if (hooks != null) {
      hooks.modifyEnvironmentBeforeSubmission(newJob.getEnvironment());
    }

    // enable upload_graph when the graph is too large
    byte[] jobGraphBytes = DataflowPipelineTranslator.jobToString(newJob).getBytes(UTF_8);
    int jobGraphByteSize = jobGraphBytes.length;
    if (jobGraphByteSize >= CREATE_JOB_REQUEST_LIMIT_BYTES
        && !hasExperiment(options, "upload_graph")
        && !useUnifiedWorker(options)) {
      List<String> experiments = firstNonNull(options.getExperiments(), Collections.emptyList());
      options.setExperiments(
          ImmutableList.<String>builder().addAll(experiments).add("upload_graph").build());
      LOG.info(
          "The job graph size ({} in bytes) is larger than {}. Automatically add "
              + "the upload_graph option to experiments.",
          jobGraphByteSize,
          CREATE_JOB_REQUEST_LIMIT_BYTES);
    }

    if (hasExperiment(options, "upload_graph") && useUnifiedWorker(options)) {
      ArrayList<String> experiments = new ArrayList<>(options.getExperiments());
      while (experiments.remove("upload_graph")) {}
      options.setExperiments(experiments);
      LOG.warn(
          "The upload_graph experiment was specified, but it does not apply "
              + "to runner v2 jobs. Option has been automatically removed.");
    }

    // Upload the job to GCS and remove the graph object from the API call.  The graph
    // will be downloaded from GCS by the service.
    if (hasExperiment(options, "upload_graph")) {
      DataflowPackage stagedGraph =
          options.getStager().stageToFile(jobGraphBytes, DATAFLOW_GRAPH_FILE_NAME);
      newJob.getSteps().clear();
      newJob.setStepsLocation(stagedGraph.getLocation());
    }

    if (!isNullOrEmpty(options.getDataflowJobFile())
        || !isNullOrEmpty(options.getTemplateLocation())) {
      boolean isTemplate = !isNullOrEmpty(options.getTemplateLocation());
      if (isTemplate) {
        checkArgument(
            isNullOrEmpty(options.getDataflowJobFile()),
            "--dataflowJobFile and --templateLocation are mutually exclusive.");
      }
      String fileLocation =
          firstNonNull(options.getTemplateLocation(), options.getDataflowJobFile());
      checkArgument(
          fileLocation.startsWith("/") || fileLocation.startsWith("gs://"),
          "Location must be local or on Cloud Storage, got %s.",
          fileLocation);
      ResourceId fileResource = FileSystems.matchNewResource(fileLocation, false /* isDirectory */);
      String workSpecJson = DataflowPipelineTranslator.jobToString(newJob);
      try (PrintWriter printWriter =
          new PrintWriter(
              new BufferedWriter(
                  new OutputStreamWriter(
                      Channels.newOutputStream(FileSystems.create(fileResource, MimeTypes.TEXT)),
                      UTF_8)))) {
        printWriter.print(workSpecJson);
        LOG.info("Printed job specification to {}", fileLocation);
      } catch (IOException ex) {
        String error = String.format("Cannot create output file at %s", fileLocation);
        if (isTemplate) {
          throw new RuntimeException(error, ex);
        } else {
          LOG.warn(error, ex);
        }
      }
      if (isTemplate) {
        LOG.info("Template successfully created.");
        return new DataflowTemplateJob();
      }
    }

    String jobIdToUpdate = null;
    if (options.isUpdate()) {
      jobIdToUpdate = getJobIdFromName(options.getJobName());
      newJob.setTransformNameMapping(options.getTransformNameMapping());
      newJob.setReplaceJobId(jobIdToUpdate);
    }
    if (options.getCreateFromSnapshot() != null && !options.getCreateFromSnapshot().isEmpty()) {
      newJob.setTransformNameMapping(options.getTransformNameMapping());
      newJob.setCreatedFromSnapshotId(options.getCreateFromSnapshot());
    }

    Job jobResult;
    try {
      jobResult = dataflowClient.createJob(newJob);
    } catch (GoogleJsonResponseException e) {
      String errorMessages = "Unexpected errors";
      if (e.getDetails() != null) {
        if (jobGraphByteSize >= CREATE_JOB_REQUEST_LIMIT_BYTES) {
          errorMessages =
              "The size of the serialized JSON representation of the pipeline "
                  + "exceeds the allowable limit. "
                  + "For more information, please see the documentation on job submission:\n"
                  + "https://cloud.google.com/dataflow/docs/guides/deploying-a-pipeline#jobs";
        } else {
          errorMessages = e.getDetails().getMessage();
        }
      }
      throw new RuntimeException("Failed to create a workflow job: " + errorMessages, e);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create a workflow job", e);
    }

    // Use a raw client for post-launch monitoring, as status calls may fail
    // regularly and need not be retried automatically.
    DataflowPipelineJob dataflowPipelineJob =
        new DataflowPipelineJob(
            DataflowClient.create(options),
            jobResult.getId(),
            options,
            jobSpecification != null ? jobSpecification.getStepNames() : Collections.emptyMap(),
            portablePipelineProto);

    // If the service returned client request id, the SDK needs to compare it
    // with the original id generated in the request, if they are not the same
    // (i.e., the returned job is not created by this request), throw
    // DataflowJobAlreadyExistsException or DataflowJobAlreadyUpdatedException
    // depending on whether this is a reload or not.
    if (jobResult.getClientRequestId() != null
        && !jobResult.getClientRequestId().isEmpty()
        && !jobResult.getClientRequestId().equals(requestId)) {
      // If updating a job.
      if (options.isUpdate()) {
        throw new DataflowJobAlreadyUpdatedException(
            dataflowPipelineJob,
            String.format(
                "The job named %s with id: %s has already been updated into job id: %s "
                    + "and cannot be updated again.",
                newJob.getName(), jobIdToUpdate, jobResult.getId()));
      } else {
        throw new DataflowJobAlreadyExistsException(
            dataflowPipelineJob,
            String.format(
                "There is already an active job named %s with id: %s. If you want to submit a"
                    + " second job, try again by setting a different name using --jobName.",
                newJob.getName(), jobResult.getId()));
      }
    }

    LOG.info(
        "To access the Dataflow monitoring console, please navigate to {}",
        MonitoringUtil.getJobMonitoringPageURL(
            options.getProject(), options.getRegion(), jobResult.getId()));
    LOG.info("Submitted job: {}", jobResult.getId());

    LOG.info(
        "To cancel the job using the 'gcloud' tool, run:\n> {}",
        MonitoringUtil.getGcloudCancelCommand(options, jobResult.getId()));

    return dataflowPipelineJob;
  }

  private static EnvironmentInfo getEnvironmentInfoFromEnvironmentId(
      String environmentId, RunnerApi.Pipeline pipelineProto) {
    RunnerApi.Environment environment =
        pipelineProto.getComponents().getEnvironmentsMap().get(environmentId);
    if (!BeamUrns.getUrn(RunnerApi.StandardEnvironments.Environments.DOCKER)
        .equals(environment.getUrn())) {
      throw new RuntimeException(
          "Dataflow can only execute pipeline steps in Docker environments: "
              + environment.getUrn());
    }
    RunnerApi.DockerPayload dockerPayload;
    try {
      dockerPayload = RunnerApi.DockerPayload.parseFrom(environment.getPayload());
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Error parsing docker payload.", e);
    }
    return EnvironmentInfo.create(
        environmentId, dockerPayload.getContainerImage(), environment.getCapabilitiesList());
  }

  private static List<EnvironmentInfo> getAllEnvironmentInfo(RunnerApi.Pipeline pipelineProto) {
    return pipelineProto.getComponents().getTransformsMap().values().stream()
        .map(transform -> transform.getEnvironmentId())
        .filter(environmentId -> !environmentId.isEmpty())
        .distinct()
        .map(environmentId -> getEnvironmentInfoFromEnvironmentId(environmentId, pipelineProto))
        .collect(Collectors.toList());
  }

  /** Returns true if the specified experiment is enabled, handling null experiments. */
  public static boolean hasExperiment(DataflowPipelineDebugOptions options, String experiment) {
    List<String> experiments =
        firstNonNull(options.getExperiments(), Collections.emptyList());
    return experiments.contains(experiment);
  }

  /** Helper to configure the Dataflow Job Environment based on the user's job options. */
  private static Map<String, Object> getEnvironmentVersion(DataflowPipelineOptions options) {
    DataflowRunnerInfo runnerInfo = DataflowRunnerInfo.getDataflowRunnerInfo();
    String majorVersion;
    String jobType;
    if (useUnifiedWorker(options)) {
      majorVersion = runnerInfo.getFnApiEnvironmentMajorVersion();
      jobType = options.isStreaming() ? "FNAPI_STREAMING" : "FNAPI_BATCH";
    } else {
      majorVersion = runnerInfo.getLegacyEnvironmentMajorVersion();
      jobType = options.isStreaming() ? "STREAMING" : "JAVA_BATCH_AUTOSCALING";
    }
    return ImmutableMap.of(
        PropertyNames.ENVIRONMENT_VERSION_MAJOR_KEY, majorVersion,
        PropertyNames.ENVIRONMENT_VERSION_JOB_TYPE_KEY, jobType);
  }

  // This method is protected to allow a Google internal subclass to properly
  // setup overrides.
  @VisibleForTesting
  protected void replaceV1Transforms(Pipeline pipeline) {
    boolean streaming = shouldActAsStreaming(pipeline);
    // Ensure all outputs of all reads are consumed before potentially replacing any
    // Read PTransforms
    UnconsumedReads.ensureAllReadsConsumed(pipeline);
    pipeline.replaceAll(getOverrides(streaming));
  }

  private boolean shouldActAsStreaming(Pipeline p) {
    class BoundednessVisitor extends PipelineVisitor.Defaults {

      final List<PCollection> unboundedPCollections = new ArrayList<>();

      @Override
      public void visitValue(PValue value, Node producer) {
        if (value instanceof PCollection) {
          PCollection pc = (PCollection) value;
          if (pc.isBounded() == IsBounded.UNBOUNDED) {
            unboundedPCollections.add(pc);
          }
        }
      }
    }

    BoundednessVisitor visitor = new BoundednessVisitor();
    p.traverseTopologically(visitor);
    if (visitor.unboundedPCollections.isEmpty()) {
      if (options.isStreaming()) {
        LOG.warn(
            "No unbounded PCollection(s) found in a streaming pipeline! "
                + "You might consider using 'streaming=false'!");
        return true;
      } else {
        return false;
      }
    } else {
      if (options.isStreaming()) {
        return true;
      } else if (hasExperiment(options, UNSAFELY_ATTEMPT_TO_PROCESS_UNBOUNDED_DATA_IN_BATCH_MODE)) {
        LOG.info(
            "Turning a batch pipeline into streaming due to unbounded PCollection(s) has been avoided! "
                + "Unbounded PCollection(s): {}",
            visitor.unboundedPCollections);
        return false;
      } else {
        LOG.warn(
            "Unbounded PCollection(s) found in a batch pipeline! "
                + "You might consider using 'streaming=true'! "
                + "Unbounded PCollection(s): {}",
            visitor.unboundedPCollections);
        return true;
      }
    }
  }

    /** Returns the DataflowPipelineTranslator associated with this object. */
  public DataflowPipelineTranslator getTranslator() {
    return translator;
  }

  /** Sets callbacks to invoke during execution see {@code DataflowRunnerHooks}. */
  public void setHooks(DataflowRunnerHooks hooks) {
    this.hooks = hooks;
  }

  /////////////////////////////////////////////////////////////////////////////

  private void logWarningIfBigqueryDLQUnused(Pipeline pipeline) {
    Map<PCollection<?>, String> unconsumedDLQ = Maps.newHashMap();
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(Node node) {
            PTransform<?, ?> transform = node.getTransform();
            if (transform != null) {
              TupleTag<?> failedTag = null;
              String rootBigQueryTransform = "";
              if (transform.getClass().equals(StorageApiLoads.class)) {
                StorageApiLoads<?, ?> storageLoads = (StorageApiLoads<?, ?>) transform;
                // If the storage load is directing exceptions to an error handler, we don't need to
                // warn for unconsumed rows
                if (!storageLoads.usesErrorHandler()) {
                  failedTag = storageLoads.getFailedRowsTag();
                }
                // For storage API the transform that outputs failed rows is nested one layer below
                // BigQueryIO.
                rootBigQueryTransform = node.getEnclosingNode().getFullName();
              } else if (transform.getClass().equals(StreamingWriteTables.class)) {
                StreamingWriteTables<?> streamingInserts = (StreamingWriteTables<?>) transform;
                failedTag = streamingInserts.getFailedRowsTupleTag();
                // For streaming inserts the transform that outputs failed rows is nested two layers
                // below BigQueryIO.
                rootBigQueryTransform = node.getEnclosingNode().getEnclosingNode().getFullName();
              }
              if (failedTag != null) {
                PCollection<?> dlq = node.getOutputs().get(failedTag);
                if (dlq != null) {
                  unconsumedDLQ.put(dlq, rootBigQueryTransform);
                }
              }
            }

            for (PCollection<?> input : node.getInputs().values()) {
              unconsumedDLQ.remove(input);
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void visitPrimitiveTransform(Node node) {
            for (PCollection<?> input : node.getInputs().values()) {
              unconsumedDLQ.remove(input);
            }
          }
        });
    for (String unconsumed : unconsumedDLQ.values()) {
      LOG.warn(
          "No transform processes the failed-inserts output from BigQuery sink: "
              + unconsumed
              + "! Not processing failed inserts means that those rows will be lost.");
    }
  }

  /** Outputs a warning about PCollection views without deterministic key coders. */
  private void logWarningIfPCollectionViewHasNonDeterministicKeyCoder(Pipeline pipeline) {
    // We need to wait till this point to determine the names of the transforms since only
    // at this time do we know the hierarchy of the transforms otherwise we could
    // have just recorded the full names during apply time.
    if (!ptransformViewsWithNonDeterministicKeyCoders.isEmpty()) {
      final SortedSet<String> ptransformViewNamesWithNonDeterministicKeyCoders = new TreeSet<>();
      pipeline.traverseTopologically(
          new PipelineVisitor.Defaults() {
            @Override
            public void visitValue(PValue value, TransformHierarchy.Node producer) {}

            @Override
            public void visitPrimitiveTransform(TransformHierarchy.Node node) {
              if (ptransformViewsWithNonDeterministicKeyCoders.contains(node.getTransform())) {
                ptransformViewNamesWithNonDeterministicKeyCoders.add(node.getFullName());
              }
            }

            @Override
            public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
              if (node.getTransform() instanceof View.AsMap
                  || node.getTransform() instanceof View.AsMultimap) {
                PCollection<KV<?, ?>> input =
                    (PCollection<KV<?, ?>>) Iterables.getOnlyElement(node.getInputs().values());
                KvCoder<?, ?> inputCoder = (KvCoder) input.getCoder();
                try {
                  inputCoder.getKeyCoder().verifyDeterministic();
                } catch (NonDeterministicException e) {
                  ptransformViewNamesWithNonDeterministicKeyCoders.add(node.getFullName());
                }
              }
              if (ptransformViewsWithNonDeterministicKeyCoders.contains(node.getTransform())) {
                ptransformViewNamesWithNonDeterministicKeyCoders.add(node.getFullName());
              }
              return CompositeBehavior.ENTER_TRANSFORM;
            }

            @Override
            public void leaveCompositeTransform(TransformHierarchy.Node node) {}
          });

      LOG.warn(
          "Unable to use indexed implementation for View.AsMap and View.AsMultimap for {} because"
              + " the key coder is not deterministic. Falling back to singleton implementation"
              + " which may cause memory and/or performance problems. Future major versions of"
              + " Dataflow will require deterministic key coders.",
          ptransformViewNamesWithNonDeterministicKeyCoders);
    }
  }

  /**
   * Returns true if the passed in {@link PCollection} needs to be materialized using an indexed
   * format.
   */
  boolean doesPCollectionRequireIndexedFormat(PCollection<?> pcol) {
    return pcollectionsRequiringIndexedFormat.contains(pcol);
  }

  /**
   * Marks the passed in {@link PCollection} as requiring to be materialized using an indexed
   * format.
   */
  void addPCollectionRequiringIndexedFormat(PCollection<?> pcol) {
    pcollectionsRequiringIndexedFormat.add(pcol);
  }

  void maybeRecordPCollectionPreservedKeys(PCollection<?> pcol) {
    pCollectionsPreservedKeys.add(pcol);
  }

  void maybeRecordPCollectionWithAutoSharding(PCollection<?> pcol) {
    // Auto-sharding is only supported in Streaming Engine.
    checkArgument(
        options.isEnableStreamingEngine(),
        "Runner determined sharding not available in Dataflow for GroupIntoBatches for"
            + " non-Streaming-Engine jobs. In order to use runner determined sharding, please use"
            + " --streaming --experiments=enable_streaming_engine");
    pCollectionsPreservedKeys.add(pcol);
    pcollectionsRequiringAutoSharding.add(pcol);
  }

  boolean doesPCollectionPreserveKeys(PCollection<?> pcol) {
    return pCollectionsPreservedKeys.contains(pcol);
  }

  boolean doesPCollectionRequireAutoSharding(PCollection<?> pcol) {
    return pcollectionsRequiringAutoSharding.contains(pcol);
  }

  /** A set of {@link View}s with non-deterministic key coders. */
  private final Set<PTransform<?, ?>> ptransformViewsWithNonDeterministicKeyCoders;

  /** Records that the {@link PTransform} requires a deterministic key coder. */
  void recordViewUsesNonDeterministicKeyCoder(PTransform<?, ?> ptransform) {
    ptransformViewsWithNonDeterministicKeyCoders.add(ptransform);
  }

  // ================================================================================
  // PubsubIO translations
  // ================================================================================

  static void translateOverriddenPubsubSourceStep(
      PubsubUnboundedSource overriddenTransform, StepTranslationContext stepTranslationContext) {
    stepTranslationContext.addInput(PropertyNames.FORMAT, "pubsub");
    if (overriddenTransform.getTopicProvider() != null) {
      if (overriddenTransform.getTopicProvider().isAccessible()) {
        stepTranslationContext.addInput(
            PropertyNames.PUBSUB_TOPIC, overriddenTransform.getTopic().getFullPath());
      } else {
        stepTranslationContext.addInput(
            PropertyNames.PUBSUB_TOPIC_OVERRIDE,
            ((NestedValueProvider) overriddenTransform.getTopicProvider()).propertyName());
      }
    }
    if (overriddenTransform.getSubscriptionProvider() != null) {
      if (overriddenTransform.getSubscriptionProvider().isAccessible()) {
        stepTranslationContext.addInput(
            PropertyNames.PUBSUB_SUBSCRIPTION, overriddenTransform.getSubscription().getFullPath());
      } else {
        stepTranslationContext.addInput(
            PropertyNames.PUBSUB_SUBSCRIPTION_OVERRIDE,
            ((NestedValueProvider) overriddenTransform.getSubscriptionProvider()).propertyName());
      }
    }
    if (overriddenTransform.getTimestampAttribute() != null) {
      stepTranslationContext.addInput(
          PropertyNames.PUBSUB_TIMESTAMP_ATTRIBUTE, overriddenTransform.getTimestampAttribute());
    }
    if (overriddenTransform.getIdAttribute() != null) {
      stepTranslationContext.addInput(
          PropertyNames.PUBSUB_ID_ATTRIBUTE, overriddenTransform.getIdAttribute());
    }
    // In both cases, the transform needs to read PubsubMessage. However, in case it needs
    // the attributes or messageId, we supply an identity "parse fn" so the worker will
    // read PubsubMessage's from Windmill and simply pass them around; and in case it
    // doesn't need attributes, we're already implicitly using a "Coder" that interprets
    // the data as a PubsubMessage's payload.
    if (overriddenTransform.getNeedsAttributes() || overriddenTransform.getNeedsMessageId()) {
      stepTranslationContext.addInput(
          PropertyNames.PUBSUB_SERIALIZED_ATTRIBUTES_FN,
          byteArrayToJsonString(serializeToByteArray(new IdentityMessageFn())));
    }
  }

  

  // ================================================================================

  static {
    DataflowPipelineTranslator.registerTransformTranslator(
        External.SingleOutputExpandableTransform.class,
        new SingleOutputExpandableTransformTranslator());
  }

  static {
    DataflowPipelineTranslator.registerTransformTranslator(
        External.MultiOutputExpandableTransform.class,
        new MultiOutputExpandableTransformTranslator());
  }

  static {
    DataflowPipelineTranslator.registerTransformTranslator(Impulse.class, new ImpulseTranslator());
  }

  @Override
  public String toString() {
    return "DataflowRunner#" + options.getJobName();
  }

  /** Finds the id for the running job of the given name. */
  private String getJobIdFromName(String jobName) {
    try {
      ListJobsResponse listResult;
      String token = null;
      do {
        listResult = dataflowClient.listJobs(token);
        token = listResult.getNextPageToken();
        for (Job job : listResult.getJobs()) {
          if (job.getName().equals(jobName)
              && MonitoringUtil.toState(job.getCurrentState()).equals(State.RUNNING)) {
            return job.getId();
          }
        }
      } while (token != null);
    } catch (GoogleJsonResponseException e) {
      throw new RuntimeException(
          "Got error while looking up jobs: "
              + (e.getDetails() != null ? e.getDetails().getMessage() : e),
          e);
    } catch (IOException e) {
      throw new RuntimeException("Got error while looking up jobs: ", e);
    }

    throw new IllegalArgumentException("Could not find running job named " + jobName);
  }

  @VisibleForTesting
  static String getContainerImageForJob(DataflowPipelineOptions options) {
    String containerImage = options.getSdkContainerImage();

    if (containerImage == null) {
      // If not set, construct and return default image URL.
      return getDefaultContainerImageUrl(options);
    } else if (containerImage.contains("IMAGE")) {
      // Replace placeholder with default image name
      return containerImage.replace("IMAGE", getDefaultContainerImageNameForJob(options));
    } else {
      return containerImage;
    }
  }

  /** Construct the default Dataflow container full image URL. */
  static String getDefaultContainerImageUrl(DataflowPipelineOptions options) {
    DataflowRunnerInfo dataflowRunnerInfo = DataflowRunnerInfo.getDataflowRunnerInfo();
    return String.format(
        "%s/%s:%s",
        dataflowRunnerInfo.getContainerImageBaseRepository(),
        getDefaultContainerImageNameForJob(options),
        getDefaultContainerVersion(options));
  }

  /**
   * Construct the default Dataflow container image name based on pipeline type and Java version.
   */
  static String getDefaultContainerImageNameForJob(DataflowPipelineOptions options) {
    Environments.JavaVersion javaVersion = Environments.getJavaVersion();
    if (useUnifiedWorker(options)) {
      return String.format("beam_%s_sdk", javaVersion.name());
    } else if (options.isStreaming()) {
      return String.format("beam-%s-streaming", javaVersion.legacyName());
    } else {
      return String.format("beam-%s-batch", javaVersion.legacyName());
    }
  }

  /**
   * Construct the default Dataflow container image name based on pipeline type and Java version.
   */
  static String getDefaultContainerVersion(DataflowPipelineOptions options) {
    DataflowRunnerInfo dataflowRunnerInfo = DataflowRunnerInfo.getDataflowRunnerInfo();
    ReleaseInfo releaseInfo = ReleaseInfo.getReleaseInfo();
    if (releaseInfo.isDevSdkVersion()) {
      if (useUnifiedWorker(options)) {
        return dataflowRunnerInfo.getFnApiDevContainerVersion();
      }
      return dataflowRunnerInfo.getLegacyDevContainerVersion();
    }
    return releaseInfo.getSdkVersion();
  }

  static boolean useUnifiedWorker(DataflowPipelineOptions options) {
    return hasExperiment(options, "beam_fn_api")
        || hasExperiment(options, "use_runner_v2")
        || hasExperiment(options, "use_unified_worker")
        || hasExperiment(options, "use_portable_job_submission");
  }

  static void verifyDoFnSupported(
      DoFn<?, ?> fn, boolean streaming, DataflowPipelineOptions options) {
    if (!streaming && DoFnSignatures.usesMultimapState(fn)) {
      throw new UnsupportedOperationException(
          String.format(
              "%s does not currently support %s in batch mode",
              DataflowRunner.class.getSimpleName(), MultimapState.class.getSimpleName()));
    }
    if (streaming && DoFnSignatures.requiresTimeSortedInput(fn)) {
      throw new UnsupportedOperationException(
          String.format(
              "%s does not currently support @RequiresTimeSortedInput in streaming mode.",
              DataflowRunner.class.getSimpleName()));
    }
    boolean isUnifiedWorker = useUnifiedWorker(options);

    if (DoFnSignatures.usesMultimapState(fn) && isUnifiedWorker) {
      throw new UnsupportedOperationException(
          String.format(
              "%s does not currently support %s running using streaming on unified worker",
              DataflowRunner.class.getSimpleName(), MultimapState.class.getSimpleName()));
    }
    if (DoFnSignatures.usesSetState(fn) && streaming && isUnifiedWorker) {
      throw new UnsupportedOperationException(
          String.format(
              "%s does not currently support %s when using streaming on unified worker",
              DataflowRunner.class.getSimpleName(), SetState.class.getSimpleName()));
    }
    if (DoFnSignatures.usesMapState(fn) && streaming && isUnifiedWorker) {
      throw new UnsupportedOperationException(
          String.format(
              "%s does not currently support %s when using streaming on unified worker",
              DataflowRunner.class.getSimpleName(), MapState.class.getSimpleName()));
    }
    if (DoFnSignatures.usesBundleFinalizer(fn) && !isUnifiedWorker) {
      throw new UnsupportedOperationException(
          String.format(
              "%s does not currently support %s when not using unified worker because it uses "
                  + "BundleFinalizers in its implementation. Set the `--experiments=use_runner_v2` "
                  + "option to use this DoFn.",
              DataflowRunner.class.getSimpleName(), fn.getClass().getSimpleName()));
    }
  }

  static void verifyStateSupportForWindowingStrategy(WindowingStrategy strategy) {
    // https://github.com/apache/beam/issues/18478
    if (strategy.needsMerge()) {
      throw new UnsupportedOperationException(
          String.format(
              "%s does not currently support state or timers with merging windows",
              DataflowRunner.class.getSimpleName()));
    }
  }
}
