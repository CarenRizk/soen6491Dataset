package org.apache.beam.sdk.transforms;

import static org.apache.beam.sdk.testing.CombineFnTester.testCombineFn;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ApproximateQuantiles.ApproximateQuantilesCombineFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;

public class ApproximateQuantilesTest {

  @RunWith(JUnit4.class)
  public static class CombinerTests {
    static final List<KV<String, Integer>> TABLE =
        Arrays.asList(
            KV.of("a", 1),
            KV.of("a", 2),
            KV.of("a", 3),
            KV.of("b", 1),
            KV.of("b", 10),
            KV.of("b", 10),
            KV.of("b", 100));

    @Rule public TestPipeline p = TestPipeline.create();

    public PCollection<KV<String, Integer>> createInputTable(Pipeline p) {
      return p.apply(
          Create.of(TABLE).withCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())));
    }

    // Optimized by LLM: Removed unused import statements
    @Test
    @Category(NeedsRunner.class)
    public void testQuantilesGlobally() {
      PCollection<Integer> input = intRangeCollection(p, 101);
      PCollection<List<Integer>> quantiles = input.apply(ApproximateQuantiles.globally(5));

      PAssert.that(quantiles).containsInAnyOrder(Arrays.asList(0, 25, 50, 75, 100));
      p.run();
    }

    // Optimized by LLM: Removed unused import statements
    @Test
    @Category(NeedsRunner.class)
    public void testQuantilesGobally_comparable() {
      PCollection<Integer> input = intRangeCollection(p, 101);
      PCollection<List<Integer>> quantiles =
          input.apply(ApproximateQuantiles.globally(5, new DescendingIntComparator()));

      PAssert.that(quantiles).containsInAnyOrder(Arrays.asList(100, 75, 50, 25, 0));
      p.run();
    }

    // Optimized by LLM: Replaced manual list creation with utility method
    private List<Integer> createList(int... values) {
      return Arrays.asList(values);
    }

    @Test
    @Category(NeedsRunner.class)
    public void testQuantilesPerKey() {
      PCollection<KV<String, Integer>> input = createInputTable(p);
      PCollection<KV<String, List<Integer>>> quantiles =
          input.apply(ApproximateQuantiles.perKey(2));

      PAssert.that(quantiles)
          .containsInAnyOrder(KV.of("a", createList(1, 3)), KV.of("b", createList(1, 100)));
      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testQuantilesPerKey_reversed() {
      PCollection<KV<String, Integer>> input = createInputTable(p);
      PCollection<KV<String, List<Integer>>> quantiles =
          input.apply(ApproximateQuantiles.perKey(2, new DescendingIntComparator()));

      PAssert.that(quantiles)
          .containsInAnyOrder(KV.of("a", createList(3, 1)), KV.of("b", createList(100, 1)));
      p.run();
    }

    @Test
    public void testSingleton() {
      testCombineFn(
          ApproximateQuantilesCombineFn.create(5),
          Arrays.asList(389),
          Arrays.asList(389, 389, 389, 389, 389));
    }

    @Test
    public void testSimpleQuantiles() {
      testCombineFn(
          ApproximateQuantilesCombineFn.create(5),
          intRange(101),
          createList(0, 25, 50, 75, 100));
    }

    @Test
    public void testUnevenQuantiles() {
      testCombineFn(
          ApproximateQuantilesCombineFn.create(37),
          intRange(5000),
          quantileMatcher(5000, 37, 20 ));
    }

    @Test
    public void testLargerQuantiles() {
      testCombineFn(
          ApproximateQuantilesCombineFn.create(50),
          intRange(10001),
          quantileMatcher(10001, 50, 20 ));
    }

    @Test
    public void testTightEpsilon() {
      testCombineFn(
          ApproximateQuantilesCombineFn.<Integer>create(10).withEpsilon(0.01),
          intRange(10001),
          quantileMatcher(10001, 10, 5 ));
    }

    @Test
    public void testDuplicates() {
      int size = 101;
      List<Integer> all = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        all.addAll(intRange(size));
      }
      testCombineFn(
          ApproximateQuantilesCombineFn.create(5), all, createList(0, 25, 50, 75, 100));
    }

    // Optimized by LLM: Replaced manual list creation with utility method
    @Test
    public void testLotsOfDuplicates() {
      List<Integer> all = new ArrayList<>();
      all.add(1);
      for (int i = 1; i < 300; i++) {
        all.add(2);
      }
      for (int i = 300; i < 1000; i++) {
        all.add(3);
      }
      testCombineFn(ApproximateQuantilesCombineFn.create(5), all, createList(1, 2, 3, 3, 3));
    }

    // Optimized by LLM: Replaced manual list creation with utility method
    @Test
    public void testLogDistribution() {
      List<Integer> all = new ArrayList<>();
      for (int i = 1; i < 1000; i++) {
        all.add((int) Math.log(i));
      }
      testCombineFn(ApproximateQuantilesCombineFn.create(5), all, createList(0, 5, 6, 6, 6));
    }

    // Optimized by LLM: Replaced manual list creation with utility method
    @Test
    public void testZipfianDistribution() {
      List<Integer> all = new ArrayList<>();
      for (int i = 1; i < 1000; i++) {
        all.add(1000 / i);
      }
      testCombineFn(ApproximateQuantilesCombineFn.create(5), all, createList(1, 1, 2, 4, 1000));
    }

    // Optimized by LLM: Replaced manual list creation with utility method
    @Test
    public void testAlternateComparator() {
      List<String> inputs = Arrays.asList("aa", "aaa", "aaaa", "b", "ccccc", "dddd", "zz");
      testCombineFn(
          ApproximateQuantilesCombineFn.create(3), inputs, createList("aa", "b", "zz"));
      testCombineFn(
          ApproximateQuantilesCombineFn.create(3, new OrderByLength()),
          inputs,
          createList("b", "aaa", "ccccc"));
    }

    private Matcher<Iterable<? extends Integer>> quantileMatcher(
        int size, int numQuantiles, int absoluteError) {
      List<Matcher<? super Integer>> quantiles = new ArrayList<>();
      quantiles.add(CoreMatchers.is(0));
      for (int k = 1; k < numQuantiles - 1; k++) {
        int expected = (int) (((double) (size - 1)) * k / (numQuantiles - 1));
        quantiles.add(new Between<>(expected - absoluteError, expected + absoluteError));
      }
      quantiles.add(CoreMatchers.is(size - 1));
      return contains(quantiles);
    }

    private static class Between<T extends Comparable<T>> extends TypeSafeDiagnosingMatcher<T> {
      private final T min;
      private final T max;

      private Between(T min, T max) {
        this.min = min;
        this.max = max;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("is between " + min + " and " + max);
      }

      @Override
      protected boolean matchesSafely(T item, Description mismatchDescription) {
        return min.compareTo(item) <= 0 && item.compareTo(max) <= 0;
      }
    }

    private static class DescendingIntComparator implements SerializableComparator<Integer> {
      @Override
      public int compare(Integer o1, Integer o2) {
        return o2.compareTo(o1);
      }
    }

    private static class OrderByLength implements Comparator<String>, Serializable {
      @Override
      public int compare(String a, String b) {
        if (a.length() != b.length()) {
          return a.length() - b.length();
        } else {
          return a.compareTo(b);
        }
      }
    }

    // Optimized by LLM: Consolidated intRange and intRangeCollection methods
    private PCollection<Integer> intRangeCollection(Pipeline p, int size) {
      return p.apply("CreateIntsUpTo(" + size + ")", Create.of(intRange(size)));
    }

    private List<Integer> intRange(int size) {
      return List.of(new Integer[size]); // Optimized by LLM: Used List.of() for better readability
    }
  }

  @RunWith(Parameterized.class)
  public static class BufferTests {

    private final double epsilon;
    private final long maxInputSize;
    private final int expectedNumBuffers;
    private final int expectedBufferSize;
    private final ApproximateQuantilesCombineFn<?, ?> combineFn;

    private static final double[] epsilons = new double[] {0.1, 0.05, 0.01, 0.005, 0.001};

    private static final int[] maxElementExponents = new int[] {5, 6, 7, 8, 9};

    private static final int[][] expectedNumBuffersValues =
        new int[][] {
          {11, 14, 17, 21, 24},
          {11, 14, 17, 20, 23},
          {9, 11, 14, 17, 21},
          {8, 11, 14, 17, 20},
          {6, 9, 11, 14, 17},
        };

    private static final int[][] expectedBufferSizeValues =
        new int[][] {
          {98, 123, 153, 96, 120},
          {98, 123, 153, 191, 239},
          {391, 977, 1221, 1526, 954},
          {782, 977, 1221, 1526, 1908},
          {3125, 3907, 9766, 12208, 15259},
        };

    @Parameterized.Parameters(name = "{index}: epsilon = {0}, maxInputSize = {1}")
    public static Collection<Object[]> data() {
      Collection<Object[]> testData = Lists.newArrayList();
      for (int i = 0; i < epsilons.length; i++) {
        for (int j = 0; j < maxElementExponents.length; j++) {
          testData.add(
              new Object[] {
                epsilons[i],
                (long) Math.pow(10, maxElementExponents[j]),
                expectedNumBuffersValues[i][j],
                expectedBufferSizeValues[i][j]
              });
        }
      }

      return testData;
    }

    public BufferTests(
        Double epsilon, Long maxInputSize, Integer expectedNumBuffers, Integer expectedBufferSize) {
      this.epsilon = epsilon;
      this.maxInputSize = maxInputSize;
      this.expectedNumBuffers = expectedNumBuffers;
      this.expectedBufferSize = expectedBufferSize;

      this.combineFn =
          ApproximateQuantilesCombineFn.create(10, new Top.Natural<Long>(), maxInputSize, epsilon);
    }

    @Test
    public void testEfficiency() {
      assertEquals("Number of buffers", expectedNumBuffers, combineFn.getNumBuffers());
      assertEquals("Buffer size", expectedBufferSize, combineFn.getBufferSize());
    }

    @Test
    public void testCorrectness() {
      int b = combineFn.getNumBuffers();
      int k = combineFn.getBufferSize();
      long n = this.maxInputSize;

      assertThat(
          "(b-2)2^(b-2) + 1/2 <= eN",
          (b - 2) * (1 << (b - 2)) + 0.5,
          Matchers.lessThanOrEqualTo(this.epsilon * n));
      assertThat(
          "k2^(b-1) >= N", Math.pow(k * 2, b - 1), Matchers.greaterThanOrEqualTo((double) n));
    }
  }
}