
package org.apache.beam.runners.direct;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.direct.DirectRunner.DirectPipelineResult;
import org.apache.beam.runners.direct.TestStreamEvaluatorFactory.DirectTestStreamFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.util.construction.PTransformMatchers;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;


@SuppressWarnings({
  "nullness" 
})
public class DirectRunner extends PipelineRunner<DirectPipelineResult> {

  enum Enforcement {
    ENCODABILITY {
      @Override
      public boolean appliesTo(PCollection<?> collection, DirectGraph graph) {
        return true;
      }
    },
    IMMUTABILITY {
      @Override
      public boolean appliesTo(PCollection<?> collection, DirectGraph graph) {
        return !ImmutabilityEnforcementFactory.isReadTransform(graph.getProducer(collection))
            && CONTAINS_UDF.contains(
                PTransformTranslation.urnForTransform(
                    graph.getProducer(collection).getTransform()));
      }
    };

    
    private static final Set<String> CONTAINS_UDF =
        ImmutableSet.of(
            PTransformTranslation.READ_TRANSFORM_URN, PTransformTranslation.PAR_DO_TRANSFORM_URN);

    public abstract boolean appliesTo(PCollection<?> collection, DirectGraph graph);

    static Set<Enforcement> enabled(DirectOptions options) {
      EnumSet<Enforcement> enabled = EnumSet.noneOf(Enforcement.class);
      if (options.isEnforceEncodability()) {
        enabled.add(ENCODABILITY);
      }
      if (options.isEnforceImmutability()) {
        enabled.add(IMMUTABILITY);
      }
      return Collections.unmodifiableSet(enabled);
    }

    static BundleFactory bundleFactoryFor(Set<Enforcement> enforcements, DirectGraph graph) {
      BundleFactory bundleFactory =
          enforcements.contains(Enforcement.ENCODABILITY)
              ? CloningBundleFactory.create()
              : ImmutableListBundleFactory.create();
      if (enforcements.contains(Enforcement.IMMUTABILITY)) {
        bundleFactory = ImmutabilityCheckingBundleFactory.create(bundleFactory, graph);
      }
      return bundleFactory;
    }

    private static Map<String, Collection<ModelEnforcementFactory>> defaultModelEnforcements(
        Set<Enforcement> enabledEnforcements) {
      ImmutableMap.Builder<String, Collection<ModelEnforcementFactory>> enforcements =
          ImmutableMap.builder();
      ImmutableList.Builder<ModelEnforcementFactory> enabledParDoEnforcements =
          ImmutableList.builder();
      if (enabledEnforcements.contains(Enforcement.IMMUTABILITY)) {
        enabledParDoEnforcements.add(ImmutabilityEnforcementFactory.create());
      }
      Collection<ModelEnforcementFactory> parDoEnforcements = enabledParDoEnforcements.build();
      enforcements.put(PTransformTranslation.PAR_DO_TRANSFORM_URN, parDoEnforcements);
      return enforcements.build();
    }
  }

  private DirectOptions options;
  private final Set<Enforcement> enabledEnforcements;
  private Supplier<Clock> clockSupplier = new NanosOffsetClockSupplier();
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));

  
  public static DirectRunner fromOptions(PipelineOptions options) {
    return new DirectRunner(options.as(DirectOptions.class));
  }

  private DirectRunner(DirectOptions options) {
    this.options = options;
    this.enabledEnforcements = Enforcement.enabled(options);
  }

  Supplier<Clock> getClockSupplier() {
    return clockSupplier;
  }

  void setClockSupplier(Supplier<Clock> supplier) {
    this.clockSupplier = supplier;
  }

  @Override
  public DirectPipelineResult run(Pipeline pipeline) {
    try {
      options =
          MAPPER
              .readValue(MAPPER.writeValueAsBytes(options), PipelineOptions.class)
              .as(DirectOptions.class);
    } catch (IOException e) {
      throw new IllegalArgumentException(
          "PipelineOptions specified failed to serialize to JSON.", e);
    }

    performRewrites(pipeline);
    MetricsEnvironment.setMetricsSupported(true);
    try {
      DirectGraphVisitor graphVisitor = new DirectGraphVisitor();
      pipeline.traverseTopologically(graphVisitor);

      @SuppressWarnings("rawtypes")
      KeyedPValueTrackingVisitor keyedPValueVisitor = KeyedPValueTrackingVisitor.create();
      pipeline.traverseTopologically(keyedPValueVisitor);

      DisplayDataValidator.validatePipeline(pipeline);
      DisplayDataValidator.validateOptions(options);

      ExecutorService metricsPool =
          Executors.newCachedThreadPool(
              new ThreadFactoryBuilder()
                  .setThreadFactory(MoreExecutors.platformThreadFactory())
                  .setDaemon(false) 
                  .setNameFormat("direct-metrics-counter-committer")
                  .build());
      DirectGraph graph = graphVisitor.getGraph();
      EvaluationContext context =
          EvaluationContext.create(
              clockSupplier.get(),
              Enforcement.bundleFactoryFor(enabledEnforcements, graph),
              graph,
              keyedPValueVisitor.getKeyedPValues(),
              metricsPool);

      TransformEvaluatorRegistry registry =
          TransformEvaluatorRegistry.javaSdkNativeRegistry(context, options);
      PipelineExecutor executor =
          ExecutorServiceParallelExecutor.create(
              options.getTargetParallelism(),
              registry,
              Enforcement.defaultModelEnforcements(enabledEnforcements),
              context,
              metricsPool);
      executor.start(graph, RootProviderRegistry.javaNativeRegistry(context, options));

      DirectPipelineResult result = new DirectPipelineResult(executor, context);
      if (options.isBlockOnRun()) {
        try {
          result.waitUntilFinish();
        } catch (UserCodeException userException) {
          throw new PipelineExecutionException(userException.getCause());
        } catch (Throwable t) {
          if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
          }
          throw new RuntimeException(t);
        }
      }
      return result;
    } finally {
      MetricsEnvironment.setMetricsSupported(false);
    }
  }

  
  @VisibleForTesting
  void performRewrites(Pipeline pipeline) {
    
    
    pipeline.replaceAll(sideInputUsingTransformOverrides());

    
    
    
    pipeline.traverseTopologically(new DirectWriteViewVisitor());

    
    pipeline.replaceAll(groupByKeyOverrides());

    
    
    SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReadsIfNecessary(pipeline);
  }

  @SuppressWarnings("rawtypes")
  private List<PTransformOverride> sideInputUsingTransformOverrides() {
    DirectTestOptions testOptions = options.as(DirectTestOptions.class);
    ImmutableList.Builder<PTransformOverride> builder = ImmutableList.builder();
    if (testOptions.isRunnerDeterminedSharding()) {
      builder.add(
          PTransformOverride.of(
              PTransformMatchers.writeWithRunnerDeterminedSharding(),
              new WriteWithShardingFactory())); 
    }
    builder
        .add(PTransformOverride.of(MultiStepCombine.matcher(), MultiStepCombine.Factory.create()))
        .add(
            PTransformOverride.of(
                PTransformMatchers.urnEqualTo(PTransformTranslation.TEST_STREAM_TRANSFORM_URN),
                new DirectTestStreamFactory(this))); 
    return builder.build();
  }

  @SuppressWarnings("rawtypes")
  private List<PTransformOverride> groupByKeyOverrides() {
    ImmutableList.Builder<PTransformOverride> builder = ImmutableList.builder();
    builder =
        builder
            
            
            .add(
                PTransformOverride.of(
                    PTransformMatchers.splittableParDo(), new ParDoMultiOverrideFactory()))
            
            .add(
                PTransformOverride.of(
                    PTransformMatchers.stateOrTimerParDo(), new ParDoMultiOverrideFactory()))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.urnEqualTo(
                        PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN),
                    new SplittableParDoViaKeyedWorkItems.OverrideFactory()))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.urnEqualTo(SplittableParDo.SPLITTABLE_GBKIKWI_URN),
                    new DirectGBKIntoKeyedWorkItemsOverrideFactory())) 
            .add(
                PTransformOverride.of(
                    PTransformMatchers.urnEqualTo(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN),
                    new DirectGroupByKeyOverrideFactory())); 
    return builder.build();
  }

  
  public static class DirectPipelineResult implements PipelineResult {
    private final PipelineExecutor executor;
    private final EvaluationContext evaluationContext;
    private State state;

    private DirectPipelineResult(PipelineExecutor executor, EvaluationContext evaluationContext) {
      this.executor = executor;
      this.evaluationContext = evaluationContext;
      
      this.state = State.RUNNING;
    }

    @Override
    public State getState() {
      if (this.state == State.RUNNING) {
        this.state = executor.getPipelineState();
      }
      return state;
    }

    @Override
    public MetricResults metrics() {
      return evaluationContext.getMetrics();
    }

    
    @Override
    public State waitUntilFinish() {
      return waitUntilFinish(Duration.ZERO);
    }

    @Override
    public State cancel() {
      this.state = executor.getPipelineState();
      if (!this.state.isTerminal()) {
        executor.stop();
        this.state = executor.getPipelineState();
      }
      return executor.getPipelineState();
    }

    
    @Override
    public State waitUntilFinish(Duration duration) {
      if (this.state.isTerminal()) {
        return this.state;
      }
      final State endState;
      try {
        endState = executor.waitUntilFinish(duration);
      } catch (UserCodeException uce) {
        
        
        
        throw new Pipeline.PipelineExecutionException(uce.getCause());
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        if (e instanceof RuntimeException) {
          throw (RuntimeException) e;
        }
        throw new RuntimeException(e);
      }
      if (endState != null) {
        this.state = endState;
      }
      return endState;
    }
  }

  
  private static class NanosOffsetClockSupplier implements Supplier<Clock> {
    @Override
    public Clock get() {
      return NanosOffsetClock.create();
    }
  }
}
