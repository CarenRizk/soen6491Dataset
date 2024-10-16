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
package org.apache.beam.sdk.transforms;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.testing.CombineFnTester;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ApproximateUnique.ApproximateUniqueCombineFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;

/** Tests for the ApproximateUnique transform. */
public class ApproximateUniqueTest implements Serializable {
  // implements Serializable just to make it easy to use anonymous inner DoFn subclasses

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static class VerifyEstimateFn implements SerializableFunction<Long, Void> {
    private final long uniqueCount;
    private final int sampleSize;

    private VerifyEstimateFn(final long uniqueCount, final int sampleSize) {
      this.uniqueCount = uniqueCount;
      this.sampleSize = sampleSize;
    }

    @Override
    public Void apply(final Long estimate) {
      verifyEstimate(uniqueCount, sampleSize, estimate);
      return null;
    }
  }

  /**
   * Checks that the estimation error, i.e., the difference between {@code uniqueCount} and {@code
   * estimate} is less than {@code 2 / sqrt(sampleSize}).
   */
  private static void verifyEstimate(
      final long uniqueCount, final int sampleSize, final long estimate) {
    if (uniqueCount < sampleSize) {
      assertEquals(
          "Number of hashes is less than the sample size. " + "Estimate should be exact",
          uniqueCount,
          estimate);
    }

    final double error = 100.0 * Math.abs(estimate - uniqueCount) / uniqueCount;
    final double maxError = 100.0 * 2 / Math.sqrt(sampleSize);

    assertTrue(
        "Estimate="
            + estimate
            + " Actual="
            + uniqueCount
            + " Error="
            + error
            + "%, MaxError="
            + maxError
            + "%.",
        error < maxError);

    assertTrue(
        "Estimate="
            + estimate
            + " Actual="
            + uniqueCount
            + " Error="
            + error
            + "%, MaxError="
            + maxError
            + "%.",
        error < maxError);
  }

  private static Matcher<Long> estimateIsWithinRangeFor(
      final long uniqueCount, final int sampleSize) {
    if (uniqueCount <= sampleSize) {
      return is(uniqueCount);
    } else {
      long maxError = (long) Math.ceil(2.0 * uniqueCount / Math.sqrt(sampleSize));
      return both(lessThan(uniqueCount + maxError)).and(greaterThan(uniqueCount - maxError));
    }
  }

  private static class VerifyEstimatePerKeyFn
      implements SerializableFunction<Iterable<KV<Long, Long>>, Void> {

    private final int sampleSize;

    private VerifyEstimatePerKeyFn(final int sampleSize) {
      this.sampleSize = sampleSize;
    }

    @Override
    public Void apply(final Iterable<KV<Long, Long>> estimatePerKey) {
      for (final KV<Long, Long> result : estimatePerKey) {
        verifyEstimate(result.getKey(), sampleSize, result.getValue());
      }
      return null;
    }
  }

  /** Tests for ApproximateUnique with duplicates. */
  @RunWith(Parameterized.class)
  public static class ApproximateUniqueWithDuplicatesTest extends ApproximateUniqueTest {

    @Parameterized.Parameter public int elementCount;

    @Parameterized.Parameter(1)
    public int uniqueCount;

    @Parameterized.Parameter(2)
    public int sampleSize;

    @Parameterized.Parameters(name = "total_{0}_unique_{1}_sample_{2}")
    public static Iterable<Object[]> data() throws IOException {
      return ImmutableList.<Object[]>builder()
          .add(
              new Object[] {100, 100, 100},
              new Object[] {1000, 1000, 100},
              new Object[] {1500, 1000, 100},
              new Object[] {10000, 1000, 100})
          .build();
    }

    private void runApproximateUniqueWithDuplicates(
        final int elementCount, final int uniqueCount, final int sampleSize) {

      assert elementCount >= uniqueCount;
      final List<Double> elements = Lists.newArrayList();
      for (int i = 0; i < elementCount; i++) {
        elements.add(1.0 / (i % uniqueCount + 1));
      }
      Collections.shuffle(elements);

      final PCollection<Double> input = p.apply(Create.of(elements));
      final PCollection<Long> estimate = input.apply(ApproximateUnique.globally(sampleSize));

      PAssert.thatSingleton(estimate).satisfies(new VerifyEstimateFn(uniqueCount, sampleSize));

      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testApproximateUniqueWithDuplicates() {
      runApproximateUniqueWithDuplicates(elementCount, uniqueCount, sampleSize);
    }
  }
}
