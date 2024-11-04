package org.apache.beam.sdk;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables.transform;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler.BadRecordErrorHandler;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Collections2;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.SetMultimap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({
  "nullness" 
})
public class Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
  
  public static class PipelineExecutionException extends RuntimeException {
    
    public PipelineExecutionException(Throwable cause) {
      super(cause);
    }
  }
  public static Pipeline create() {
    Pipeline pipeline = new Pipeline(PipelineOptionsFactory.create());
    LOG.debug("Creating {}", pipeline);
    return pipeline;
  }

  
  public static Pipeline create(PipelineOptions options) {
    PipelineRunner.fromOptions(options);

    Pipeline pipeline = new Pipeline(options);
    LOG.debug("Creating {}", pipeline);
    return pipeline;
  }

  
  public PBegin begin() {
    return PBegin.in(this);
  }

  
  public <OutputT extends POutput> OutputT apply(PTransform<? super PBegin, OutputT> root) {
    return begin().apply(root);
  }

  
  public <OutputT extends POutput> OutputT apply(
      String name, PTransform<? super PBegin, OutputT> root) {
    return begin().apply(name, root);
  }

  @Internal
  public static Pipeline forTransformHierarchy(
      TransformHierarchy transforms, PipelineOptions options) {
    return new Pipeline(transforms, options);
  }

  @Internal
  public PipelineOptions getOptions() {
    return defaultOptions;
  }

  
  @Internal
  public void replaceAll(List<PTransformOverride> overrides) {
    for (PTransformOverride override : overrides) {
      replace(override);
    }
    checkNoMoreMatches(overrides);
  }

  private void checkNoMoreMatches(final List<PTransformOverride> overrides) {
    traverseTopologically(
        new PipelineVisitor.Defaults() {
          final SetMultimap<Node, PTransformOverride> matched = HashMultimap.create();

          @Override
          public CompositeBehavior enterCompositeTransform(Node node) {
            if (!node.isRootNode()) {
              checkForMatches(node);
            }
            if (matched.containsKey(node)) {
              return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
            } else {
              return CompositeBehavior.ENTER_TRANSFORM;
            }
          }

          @Override
          public void leaveCompositeTransform(Node node) {
            if (node.isRootNode() && !matched.isEmpty()) {
              LOG.info(
                  "Found nodes that matched overrides. Matches: {}. The match usually should be empty unless there are runner specific replacement transforms.",
                  matched);
            }
          }

          @Override
          public void visitPrimitiveTransform(Node node) {
            checkForMatches(node);
          }

          private void checkForMatches(Node node) {
            for (PTransformOverride override : overrides) {
              if (override
                  .getMatcher()
                  .matchesDuringValidation(node.toAppliedPTransform(getPipeline()))) {
                matched.put(node, override);
              }
            }
          }
        });
  }

  private void replace(final PTransformOverride override) {
    final Set<Node> matches = new HashSet<>();
    final Set<Node> freedNodes = new HashSet<>();
    traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public CompositeBehavior enterCompositeTransform(Node node) {
            if (!node.isRootNode() && freedNodes.contains(node.getEnclosingNode())) {
              
              freedNodes.add(node);
              return CompositeBehavior.ENTER_TRANSFORM;
            }
            if (!node.isRootNode()
                && override.getMatcher().matches(node.toAppliedPTransform(getPipeline()))) {
              matches.add(node);
              
              freedNodes.add(node);
            }
            return CompositeBehavior.ENTER_TRANSFORM;
          }

          @Override
          public void visitPrimitiveTransform(Node node) {
            if (freedNodes.contains(node.getEnclosingNode())) {
              freedNodes.add(node);
            } else if (override.getMatcher().matches(node.toAppliedPTransform(getPipeline()))) {
              matches.add(node);
              freedNodes.add(node);
            }
          }
        });
    for (Node freedNode : freedNodes) {
      usedFullNames.remove(freedNode.getFullName());
    }
    for (Node match : matches) {
      applyReplacement(match, override.getOverrideFactory());
    }
  }

  
  public PipelineResult run() {
    return run(defaultOptions);
  }

  
  public PipelineResult run(PipelineOptions options) {
    PipelineRunner<? extends PipelineResult> runner = PipelineRunner.fromOptions(options);
    
    
    LOG.debug("Running {} via {}", this, runner);
    try {
      validate(options);
      validateErrorHandlers();
      return runner.run(this);
    } catch (UserCodeException e) {
      
      
      
      
      throw new PipelineExecutionException(e.getCause());
    }
  }

  
  public CoderRegistry getCoderRegistry() {
    if (coderRegistry == null) {
      coderRegistry = CoderRegistry.createDefault();
    }
    return coderRegistry;
  }

  public SchemaRegistry getSchemaRegistry() {
    if (schemaRegistry == null) {
      schemaRegistry = SchemaRegistry.createDefault();
    }
    return schemaRegistry;
  }

  public <OutputT extends POutput> BadRecordErrorHandler<OutputT> registerBadRecordErrorHandler(
      PTransform<PCollection<BadRecord>, OutputT> sinkTransform) {
    BadRecordErrorHandler<OutputT> errorHandler = new BadRecordErrorHandler<>(sinkTransform, this);
    errorHandlers.add(errorHandler);
    return errorHandler;
  }

  @Deprecated
  public void setCoderRegistry(CoderRegistry coderRegistry) {
    this.coderRegistry = coderRegistry;
  }

  
  @Internal
  public interface PipelineVisitor {
    
    void enterPipeline(Pipeline p);

    
    CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node);

    
    void leaveCompositeTransform(TransformHierarchy.Node node);

    
    void visitPrimitiveTransform(TransformHierarchy.Node node);

    
    void visitValue(PValue value, TransformHierarchy.Node producer);

    
    void leavePipeline(Pipeline pipeline);

    
    enum CompositeBehavior {
      ENTER_TRANSFORM,
      DO_NOT_ENTER_TRANSFORM
    }

    
    class Defaults implements PipelineVisitor {

      private @Nullable Pipeline pipeline;

      protected Pipeline getPipeline() {
        if (pipeline == null) {
          throw new IllegalStateException(
              "Illegal access to pipeline after visitor traversal was completed");
        }
        return pipeline;
      }

      @Override
      public void enterPipeline(Pipeline pipeline) {
        this.pipeline = checkNotNull(pipeline);
      }

      @Override
      public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
        return CompositeBehavior.ENTER_TRANSFORM;
      }

      @Override
      public void leaveCompositeTransform(TransformHierarchy.Node node) {}

      @Override
      public void visitPrimitiveTransform(TransformHierarchy.Node node) {}

      @Override
      public void visitValue(PValue value, TransformHierarchy.Node producer) {}

      @Override
      public void leavePipeline(Pipeline pipeline) {
        this.pipeline = null;
      }
    }
  }

  
  @Internal
  public void traverseTopologically(PipelineVisitor visitor) {
    visitor.enterPipeline(this);
    transforms.visit(visitor);
    visitor.leavePipeline(this);
  }

  
  @Internal
  public static <InputT extends PInput, OutputT extends POutput> OutputT applyTransform(
      InputT input, PTransform<? super InputT, OutputT> transform) {
    return input.getPipeline().applyInternal(transform.getName(), input, transform);
  }

  
  @Internal
  public static <InputT extends PInput, OutputT extends POutput> OutputT applyTransform(
      String name, InputT input, PTransform<? super InputT, OutputT> transform) {
    return input.getPipeline().applyInternal(name, input, transform);
  }

  private final TransformHierarchy transforms;
  private final Set<String> usedFullNames = new HashSet<>();

  
  private @Nullable CoderRegistry coderRegistry;

  
  private @Nullable SchemaRegistry schemaRegistry;

  private final Multimap<String, PTransform<?, ?>> instancePerName = ArrayListMultimap.create();
  private final PipelineOptions defaultOptions;

  private final List<ErrorHandler<?, ?>> errorHandlers = new ArrayList<>();

  private Pipeline(TransformHierarchy transforms, PipelineOptions options) {
    CoderTranslation.verifyModelCodersRegistered();
    this.transforms = transforms;
    this.defaultOptions = options;
  }

  protected Pipeline(PipelineOptions options) {
    this(new TransformHierarchy(ResourceHints.fromOptions(options)), options);
  }

  @Override
  public String toString() {
    return "Pipeline#" + hashCode();
  }

  
  private <InputT extends PInput, OutputT extends POutput> OutputT applyInternal(
      String name, InputT input, PTransform<? super InputT, OutputT> transform) {
    String namePrefix = transforms.getCurrent().getFullName();
    String uniqueName = uniquifyInternal(namePrefix, name);

    final String builtName = buildName(namePrefix, name);
    instancePerName.put(builtName, transform);

    LOG.debug("Adding {} to {}", transform, this);
    transforms.pushNode(uniqueName, input, transform);
    try {
      transforms.finishSpecifyingInput();
      OutputT output = transform.expand(input);
      transforms.setOutput(output);

      return output;
    } finally {
      transforms.popNode();
    }
  }

  private <
          InputT extends PInput,
          OutputT extends POutput,
          TransformT extends PTransform<? super InputT, OutputT>>
      void applyReplacement(
          Node original,
          PTransformOverrideFactory<InputT, OutputT, TransformT> replacementFactory) {
    PTransformReplacement<InputT, OutputT> replacement =
        replacementFactory.getReplacementTransform(
            (AppliedPTransform<InputT, OutputT, TransformT>) original.toAppliedPTransform(this));
    if (replacement.getTransform() == original.getTransform()) {
      return;
    }
    InputT originalInput = replacement.getInput();
    Map<TupleTag<?>, PCollection<?>> originalOutputs = original.getOutputs();

    LOG.debug("Replacing {} with {}", original, replacement);
    transforms.replaceNode(original, originalInput, replacement.getTransform());
    try {
      OutputT newOutput = replacement.getTransform().expand(originalInput);
      Map<PCollection<?>, ReplacementOutput> originalToReplacement =
          replacementFactory.mapOutputs(original.getOutputs(), newOutput);
      
      transforms.setOutput(newOutput);
      transforms.replaceOutputs(originalToReplacement);
      checkState(
          ImmutableSet.copyOf(originalOutputs.values())
              .equals(ImmutableSet.copyOf(transforms.getCurrent().getOutputs().values())),
          "After replacing %s with %s, outputs were not rewired correctly:"
              + " Original outputs %s became %s.",
          original,
          transforms.getCurrent(),
          originalOutputs,
          transforms.getCurrent().getOutputs());
    } finally {
      transforms.popNode();
    }
  }

  @VisibleForTesting
  void validate(PipelineOptions options) {
    this.traverseTopologically(new ValidateVisitor(options));
    final Collection<Map.Entry<String, Collection<PTransform<?, ?>>>> errors =
        Collections2.filter(instancePerName.asMap().entrySet(), Predicates.not(new IsUnique<>()));
    if (!errors.isEmpty()) {
      switch (options.getStableUniqueNames()) {
        case OFF:
          break;
        case WARNING:
          LOG.warn(
              "The following transforms do not have stable unique names: {}",
              Joiner.on(", ").join(transform(errors, new KeysExtractor())));
          break;
        case ERROR: 
          throw new IllegalStateException(
              String.format(
                      "Pipeline update will not be possible because the following transforms do"
                          + " not have stable unique names: %s.",
                      Joiner.on(", ").join(transform(errors, new KeysExtractor())))
                  + "\n\n"
                  + "Conflicting instances:\n"
                  + Joiner.on("\n")
                      .join(transform(errors, new UnstableNameToMessage(instancePerName)))
                  + "\n\nYou can fix it adding a name when you call apply(): "
                  + "pipeline.apply(<name>, <transform>).");
        default:
          throw new IllegalArgumentException(
              "Unrecognized value for stable unique names: " + options.getStableUniqueNames());
      }
    }
  }

  
  private String uniquifyInternal(String namePrefix, String origName) {
    String name = origName;
    int suffixNum = 2;
    while (true) {
      String candidate = buildName(namePrefix, name);
      if (usedFullNames.add(candidate)) {
        return candidate;
      }
      
      name = origName + suffixNum++;
    }
  }

  
  private String buildName(String namePrefix, String name) {
    return namePrefix.isEmpty() ? name : namePrefix + "/" + name;
  }

  private static class ValidateVisitor extends PipelineVisitor.Defaults {

    private final PipelineOptions options;

    public ValidateVisitor(PipelineOptions options) {
      this.options = options;
    }

    @Override
    public CompositeBehavior enterCompositeTransform(Node node) {
      if (node.getTransform() != null) {
        node.getTransform().validate(options, node.getInputs(), node.getOutputs());
      }
      return CompositeBehavior.ENTER_TRANSFORM;
    }

    @Override
    public void visitPrimitiveTransform(Node node) {
      node.getTransform().validate(options, node.getInputs(), node.getOutputs());
    }
  }

  private static class TransformToMessage implements Function<PTransform<?, ?>, String> {
    @Override
    public String apply(final PTransform<?, ?> transform) {
      return "    - " + transform;
    }
  }

  private static class UnstableNameToMessage
      implements Function<Map.Entry<String, Collection<PTransform<?, ?>>>, String> {
    private final Multimap<String, PTransform<?, ?>> instances;

    private UnstableNameToMessage(final Multimap<String, PTransform<?, ?>> instancePerName) {
      this.instances = instancePerName;
    }

    @SuppressFBWarnings(
        value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
        justification = "https://github.com/google/guava/issues/920")
    @Override
    public String apply(@Nonnull final Map.Entry<String, Collection<PTransform<?, ?>>> input) {
      final Collection<PTransform<?, ?>> values = instances.get(input.getKey());
      return "- name="
          + input.getKey()
          + ":\n"
          + Joiner.on("\n").join(transform(values, new TransformToMessage()));
    }
  }

  private static class KeysExtractor
      implements Function<Map.Entry<String, Collection<PTransform<?, ?>>>, String> {
    @SuppressFBWarnings(
        value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
        justification = "https://github.com/google/guava/issues/920")
    @Override
    public String apply(@Nonnull final Map.Entry<String, Collection<PTransform<?, ?>>> input) {
      return input.getKey();
    }
  }

  private static class IsUnique<K, V> implements Predicate<Map.Entry<K, Collection<V>>> {
    @SuppressFBWarnings(
        value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
        justification = "https://github.com/google/guava/issues/920")
    @Override
    public boolean apply(@Nonnull final Map.Entry<K, Collection<V>> input) {
      return input != null && input.getValue().size() == 1;
    }
  }

  private void validateErrorHandlers() {
    for (ErrorHandler<?, ?> errorHandler : errorHandlers) {
      if (!errorHandler.isClosed()) {
        throw new IllegalStateException(
            "One or more ErrorHandlers aren't closed, and this pipeline "
                + "cannot be run. See the ErrorHandler documentation for expected usage");
      }
    }
  }
}
