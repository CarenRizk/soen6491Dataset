package org.apache.beam.sdk.io;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Verify.verify;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.CreateOptions.StandardCreateOptions;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.MoveOptions.StandardMoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Ordering;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.TreeMultimap;

@SuppressWarnings({
  "nullness", 
  "rawtypes"
})
public class FileSystems {

  public static final String DEFAULT_SCHEME = "file";
  private static final Pattern FILE_SCHEME_PATTERN =
      Pattern.compile("(?<scheme>[a-zA-Z][-a-zA-Z0-9+.]*):/.*");
  private static final Pattern GLOB_PATTERN = Pattern.compile("[*?{}]");

  private static final ConcurrentHashMap<String, FileSystem> SCHEME_TO_FILESYSTEM =
      new ConcurrentHashMap<>(ImmutableMap.of(DEFAULT_SCHEME, new LocalFileSystem()));

  private static final Object FILESYSTEM_REVISION_LOCK = new Object();
  private static KV<Long, Integer> FILESYSTEM_REVISION;

  public static boolean hasGlobWildcard(String spec) {
    return GLOB_PATTERN.matcher(spec).find();
  }

  public static List<MatchResult> match(List<String> specs) throws IOException {
    return getFileSystemInternal(getOnlyScheme(specs)).match(specs);
  }

  // Optimized by LLM: Consolidated match methods with EmptyMatchTreatment
  public static List<MatchResult> match(List<String> specs, EmptyMatchTreatment emptyMatchTreatment) throws IOException {
    List<MatchResult> matches = getFileSystemInternal(getOnlyScheme(specs)).match(specs);
    List<MatchResult> res = Lists.newArrayListWithExpectedSize(matches.size());
    for (int i = 0; i < matches.size(); i++) {
      res.add(maybeAdjustEmptyMatchResult(specs.get(i), matches.get(i), emptyMatchTreatment));
    }
    return res;
  }

  public static MatchResult match(String spec) throws IOException {
    List<MatchResult> matches = match(List.of(spec)); // Optimized by LLM: Used List.of() instead of Collections.singletonList()
    verify(
        matches.size() == 1,
        "FileSystem implementation for %s did not return exactly one MatchResult: %s",
        spec,
        matches);
    return matches.get(0);
  }

  public static MatchResult match(String spec, EmptyMatchTreatment emptyMatchTreatment) throws IOException {
    MatchResult res = match(spec);
    return maybeAdjustEmptyMatchResult(spec, res, emptyMatchTreatment);
  }

  // Optimized by LLM: Extracted logic to create MatchResult
  private static MatchResult createMatchResult(String spec, MatchResult res, EmptyMatchTreatment emptyMatchTreatment) throws IOException {
    if (res.status() == Status.NOT_FOUND
        || (res.status() == Status.OK && res.metadata().isEmpty())) {
      boolean notFoundAllowed =
          emptyMatchTreatment == EmptyMatchTreatment.ALLOW
              || (hasGlobWildcard(spec)
                  && emptyMatchTreatment == EmptyMatchTreatment.ALLOW_IF_WILDCARD);
      return notFoundAllowed
          ? MatchResult.create(Status.OK, Collections.emptyList())
          : MatchResult.create(
              Status.NOT_FOUND, new FileNotFoundException("No files matched spec: " + spec));
    }
    return res;
  }

  private static MatchResult maybeAdjustEmptyMatchResult(
      String spec, MatchResult res, EmptyMatchTreatment emptyMatchTreatment) throws IOException {
    return createMatchResult(spec, res, emptyMatchTreatment);
  }

  public static Metadata matchSingleFileSpec(String spec) throws IOException {
    List<MatchResult> matches = FileSystems.match(List.of(spec)); // Optimized by LLM: Used List.of() instead of Collections.singletonList()
    MatchResult matchResult = Iterables.getOnlyElement(matches);
    if (matchResult.status() == Status.NOT_FOUND) {
      throw new FileNotFoundException(String.format("File spec %s not found", spec));
    } else if (matchResult.status() != Status.OK) {
      throw new IOException(
          String.format("Error matching file spec %s: status %s", spec, matchResult.status()));
    } else {
      List<Metadata> metadata = matchResult.metadata();
      if (metadata.size() != 1) {
        throw new IOException(
            String.format(
                "Expecting spec %s to match exactly one file, but matched %s: %s",
                spec, metadata.size(), metadata));
      }
      return metadata.get(0);
    }
  }

  public static List<MatchResult> matchResources(List<ResourceId> resourceIds) throws IOException {
    return match(FluentIterable.from(resourceIds).transform(ResourceId::toString).toList());
  }

  public static WritableByteChannel create(ResourceId resourceId, String mimeType) throws IOException {
    return create(resourceId, StandardCreateOptions.builder().setMimeType(mimeType).build());
  }

  public static WritableByteChannel create(ResourceId resourceId, CreateOptions createOptions) throws IOException {
    return getFileSystemInternal(resourceId.getScheme()).create(resourceId, createOptions);
  }

  public static ReadableByteChannel open(ResourceId resourceId) throws IOException {
    return getFileSystemInternal(resourceId.getScheme()).open(resourceId);
  }

  public static void copy(
      List<ResourceId> srcResourceIds, List<ResourceId> destResourceIds, MoveOptions... moveOptions)
      throws IOException {
    validateSrcDestLists(srcResourceIds, destResourceIds);
    if (srcResourceIds.isEmpty()) {
      return;
    }
    FileSystem fileSystem = getFileSystemInternal(srcResourceIds.iterator().next().getScheme());
    FilterResult filtered = filterFiles(fileSystem, srcResourceIds, destResourceIds, moveOptions);
    if (!filtered.resultSources.isEmpty()) {
      fileSystem.copy(filtered.resultSources, filtered.resultDestinations);
    }
  }

  public static void rename(
      List<ResourceId> srcResourceIds, List<ResourceId> destResourceIds, MoveOptions... moveOptions)
      throws IOException {
    validateSrcDestLists(srcResourceIds, destResourceIds);
    if (srcResourceIds.isEmpty()) {
      return;
    }
    renameInternal(
        getFileSystemInternal(srcResourceIds.iterator().next().getScheme()),
        srcResourceIds,
        destResourceIds,
        moveOptions);
  }

  @VisibleForTesting
  static void renameInternal(
      FileSystem fileSystem,
      List<ResourceId> srcResourceIds,
      List<ResourceId> destResourceIds,
      MoveOptions... moveOptions)
      throws IOException {
    try {
      fileSystem.rename(srcResourceIds, destResourceIds, moveOptions);
    } catch (UnsupportedOperationException e) {
      FilterResult filtered = filterFiles(fileSystem, srcResourceIds, destResourceIds, moveOptions);
      if (!filtered.resultSources.isEmpty()) {
        fileSystem.rename(filtered.resultSources, filtered.resultDestinations);
      }
      if (!filtered.filteredExistingSrcs.isEmpty()) {
        fileSystem.delete(filtered.filteredExistingSrcs);
      }
    }
  }

  public static void delete(Collection<ResourceId> resourceIds, MoveOptions... moveOptions) throws IOException {
    if (resourceIds.isEmpty()) {
      return;
    }

    Collection<ResourceId> resourceIdsToDelete;
    if (Sets.newHashSet(moveOptions).contains(MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES)) {
      resourceIdsToDelete =
          FluentIterable.from(matchResources(Lists.newArrayList(resourceIds)))
              .filter(matchResult -> !matchResult.status().equals(Status.NOT_FOUND))
              .transformAndConcat(
                  new Function<MatchResult, Iterable<Metadata>>() {
                    @SuppressFBWarnings(
                        value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
                        justification = "https://github.com/google/guava/issues/920")
                    @Nonnull
                    @Override
                    public Iterable<Metadata> apply(@Nonnull MatchResult input) {
                      try {
                        return Lists.newArrayList(input.metadata());
                      } catch (IOException e) {
                        throw new RuntimeException(
                            String.format("Failed to get metadata from MatchResult: %s.", input),
                            e);
                      }
                    }
                  })
              .transform(
                  new Function<Metadata, ResourceId>() {
                    @SuppressFBWarnings(
                        value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION",
                        justification = "https://github.com/google/guava/issues/920")
                    @Nonnull
                    @Override
                    public ResourceId apply(@Nonnull Metadata input) {
                      return input.resourceId();
                    }
                  })
              .toList();
    } else {
      resourceIdsToDelete = resourceIds;
    }
    if (resourceIdsToDelete.isEmpty()) {
      return;
    }
    getFileSystemInternal(resourceIdsToDelete.iterator().next().getScheme()).delete(resourceIdsToDelete);
  }

  private static class FilterResult {
    public List<ResourceId> resultSources = new ArrayList<>(); // Optimized by LLM: Used diamond operator for type inference
    public List<ResourceId> resultDestinations = new ArrayList<>(); // Optimized by LLM: Used diamond operator for type inference
    public List<ResourceId> filteredExistingSrcs = new ArrayList<>(); // Optimized by LLM: Used diamond operator for type inference
  };

  private static FilterResult filterFiles(
      FileSystem fileSystem,
      List<ResourceId> srcResourceIds,
      List<ResourceId> destResourceIds,
      MoveOptions... moveOptions)
      throws IOException {
    FilterResult result = new FilterResult();
    if (moveOptions.length == 0 || srcResourceIds.isEmpty()) {
      result.resultSources = srcResourceIds;
      result.resultDestinations = destResourceIds;
      return result;
    }
    Set<MoveOptions> moveOptionSet = Sets.newHashSet(moveOptions);
    final boolean ignoreMissingSrc = moveOptionSet.contains(StandardMoveOptions.IGNORE_MISSING_FILES);
    final boolean skipExistingDest = moveOptionSet.contains(StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS);
    final int size = srcResourceIds.size();

    List<ResourceId> matchResources = new ArrayList<>();
    if (ignoreMissingSrc) {
      matchResources.addAll(srcResourceIds);
    }
    if (skipExistingDest) {
      matchResources.addAll(destResourceIds);
    }
    List<MatchResult> matchResults =
        fileSystem.match(FluentIterable.from(matchResources).transform(ResourceId::toString).toList());
    List<MatchResult> matchSrcResults = ignoreMissingSrc ? matchResults.subList(0, size) : null;
    List<MatchResult> matchDestResults =
        skipExistingDest ? matchResults.subList(matchResults.size() - size, matchResults.size()) : null;

    for (int i = 0; i < size; ++i) {
      if (matchSrcResults != null && matchSrcResults.get(i).status().equals(Status.NOT_FOUND)) {
        continue;
      }
      if (matchDestResults != null
          && matchSrcResults != null
          && matchDestResults.get(i).status().equals(Status.OK)
          && checksumMatch(
              matchDestResults.get(i).metadata().get(0),
              matchSrcResults.get(i).metadata().get(0))) {
        result.filteredExistingSrcs.add(srcResourceIds.get(i));
        continue;
      }
      result.resultSources.add(srcResourceIds.get(i));
      result.resultDestinations.add(destResourceIds.get(i));
    }
    return result;
  }

  private static boolean checksumMatch(MatchResult.Metadata first, MatchResult.Metadata second) {
    return first.checksum() != null && first.checksum().equals(second.checksum());
  }

  private static void validateSrcDestLists(
      List<ResourceId> srcResourceIds, List<ResourceId> destResourceIds) {
    checkArgument(
        srcResourceIds.size() == destResourceIds.size(),
        "Number of source resource ids %s must equal number of destination resource ids %s",
        srcResourceIds.size(),
        destResourceIds.size());

    if (srcResourceIds.isEmpty()) {
      return;
    }

    Set<String> schemes =
        FluentIterable.from(srcResourceIds)
            .append(destResourceIds)
            .transform(ResourceId::getScheme)
            .toSet();
    checkArgument(
        schemes.size() == 1,
        String.format(
            "Expect srcResourceIds and destResourceIds have the same scheme, but received %s.",
            Joiner.on(", ").join(schemes)));
  }

  private static String getOnlyScheme(List<String> specs) {
    checkArgument(!specs.isEmpty(), "Expect specs are not empty.");
    Set<String> schemes = FluentIterable.from(specs).transform(FileSystems::parseScheme).toSet();
    return Iterables.getOnlyElement(schemes);
  }

  private static String parseScheme(String spec) {
    Matcher matcher = FILE_SCHEME_PATTERN.matcher(spec);
    if (!matcher.matches()) {
      return DEFAULT_SCHEME;
    } else {
      return matcher.group("scheme").toLowerCase();
    }
  }

  @VisibleForTesting
  static FileSystem getFileSystemInternal(String scheme) {
    String lowerCaseScheme = scheme.toLowerCase();
    FileSystem rval = SCHEME_TO_FILESYSTEM.get(lowerCaseScheme);
    if (rval == null) {
      throw new IllegalArgumentException("No filesystem found for scheme " + scheme);
    }
    return rval;
  }

  @Internal
  public static void setDefaultPipelineOptions(PipelineOptions options) {
    checkNotNull(options, "options");
    long id = options.getOptionsId();
    int nextRevision = options.revision();

    while (true) {
      KV<Long, Integer> revision;
      synchronized (FILESYSTEM_REVISION_LOCK) { // Optimized by LLM: Used synchronized block instead of AtomicReference
        revision = FILESYSTEM_REVISION;
        if (revision != null && revision.getKey().equals(id) && revision.getValue() >= nextRevision) {
          return;
        }
      }

      if (FILESYSTEM_REVISION == null || FILESYSTEM_REVISION.getKey() != id) {
        synchronized (FILESYSTEM_REVISION_LOCK) {
          FILESYSTEM_REVISION = KV.of(id, nextRevision);
          Set<FileSystemRegistrar> registrars =
              Sets.newTreeSet(ReflectHelpers.ObjectsClassComparator.INSTANCE);
          registrars.addAll(
              Lists.newArrayList(
                  ServiceLoader.load(FileSystemRegistrar.class, ReflectHelpers.findClassLoader())));

          SCHEME_TO_FILESYSTEM.putAll(verifySchemesAreUnique(options, registrars));
          return;
        }
      }
    }
  }

  @VisibleForTesting
  static Map<String, FileSystem> verifySchemesAreUnique(
      PipelineOptions options, Set<FileSystemRegistrar> registrars) {
    Multimap<String, FileSystem> fileSystemsBySchemes =
        TreeMultimap.create(Ordering.<String>natural(), Ordering.arbitrary());

    for (FileSystemRegistrar registrar : registrars) {
      for (FileSystem fileSystem : registrar.fromOptions(options)) {
        fileSystemsBySchemes.put(fileSystem.getScheme(), fileSystem);
      }
    }
    for (Entry<String, Collection<FileSystem>> entry : fileSystemsBySchemes.asMap().entrySet()) {
      if (entry.getValue().size() > 1) {
        String conflictingFileSystems =
            Joiner.on(", ")
                .join(
                    FluentIterable.from(entry.getValue())
                        .transform(input -> input.getClass().getName())
                        .toSortedList(Ordering.natural()));
        throw new IllegalStateException(
            String.format(
                "Scheme: [%s] has conflicting filesystems: [%s]",
                entry.getKey(), conflictingFileSystems));
      }
    }

    ImmutableMap.Builder<String, FileSystem> schemeToFileSystem = ImmutableMap.builder();
    for (Entry<String, FileSystem> entry : fileSystemsBySchemes.entries()) {
      schemeToFileSystem.put(entry.getKey(), entry.getValue());
    }
    return schemeToFileSystem.build();
  }

  public static ResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
    return getFileSystemInternal(parseScheme(singleResourceSpec))
        .matchNewResource(singleResourceSpec, isDirectory);
  }

  public static ResourceId matchNewDirectory(String singleResourceSpec, String... baseNames) {
    ResourceId currentDir = matchNewResource(singleResourceSpec, true);
    for (String dir : baseNames) {
      currentDir = currentDir.resolve(dir, StandardResolveOptions.RESOLVE_DIRECTORY);
    }
    return currentDir;
  }
}