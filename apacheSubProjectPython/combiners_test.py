
"""Unit tests for our libraries of combine PTransforms."""

import itertools
import random
import time
import unittest

import hamcrest as hc
import pytest

import apache_beam as beam
import combiners as combine
from apache_beam.metrics import Metrics
from apache_beam.metrics import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import equal_to_per_window
from apache_beam.transforms import WindowInto
from apache_beam.transforms import trigger
from apache_beam.transforms import window
from apache_beam.transforms.core import CombineGlobally
from apache_beam.transforms.core import Create
from apache_beam.transforms.core import Map
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display_test import DisplayDataItemMatcher
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.ptransform import PTransform
from apache_beam.transforms.trigger import AfterAll
from apache_beam.transforms.trigger import AfterCount
from apache_beam.transforms.trigger import AfterWatermark
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import GlobalWindows
from apache_beam.transforms.window import TimestampCombiner
from apache_beam.transforms.window import TimestampedValue
from apache_beam.typehints import TypeCheckError
from apache_beam.utils.timestamp import Timestamp


class SortedConcatWithCounters(beam.CombineFn):
  """CombineFn for incrementing three different counters:
     counter, distribution, gauge,
     at the same time concatenating words."""
  def __init__(self):
    beam.CombineFn.__init__(self)
    self.word_counter = Metrics.counter(self.__class__, 'word_counter')
    self.word_lengths_counter = Metrics.counter(self.__class__, 'word_lengths')
    self.word_lengths_dist = Metrics.distribution(
        self.__class__, 'word_len_dist')
    self.last_word_len = Metrics.gauge(self.__class__, 'last_word_len')

  def create_accumulator(self):
    return ''

  def add_input(self, acc, element):
    self.word_counter.inc(1)
    self.word_lengths_counter.inc(len(element))
    self.word_lengths_dist.update(len(element))
    self.last_word_len.set(len(element))

    return acc + element

  def merge_accumulators(self, accs):
    return ''.join(accs)

  def extract_output(self, acc):
    return ''.join(sorted(acc))


class CombineTest(unittest.TestCase):


  def test_empty_global_top(self):
    with TestPipeline() as p:
      assert_that(p | beam.Create([]) | combine.Top.Largest(10), equal_to([[]]))

  def test_sharded_top(self):
    elements = list(range(100))
    random.shuffle(elements)

    with TestPipeline() as pipeline:
      shards = [
          pipeline | 'Shard%s' % shard >> beam.Create(elements[shard::7])
          for shard in range(7)
      ]
      assert_that(
          shards | beam.Flatten() | combine.Top.Largest(10),
          equal_to([[99, 98, 97, 96, 95, 94, 93, 92, 91, 90]]))

  def test_top_key(self):
    self.assertEqual(['aa', 'bbb', 'c', 'dddd'] | combine.Top.Of(3, key=len),
                     [['dddd', 'bbb', 'aa']])
    self.assertEqual(['aa', 'bbb', 'c', 'dddd']
                     | combine.Top.Of(3, key=len, reverse=True),
                     [['c', 'aa', 'bbb']])

    self.assertEqual(['xc', 'zb', 'yd', 'wa']
                     | combine.Top.Largest(3, key=lambda x: x[-1]),
                     [['yd', 'xc', 'zb']])
    self.assertEqual(['xc', 'zb', 'yd', 'wa']
                     | combine.Top.Smallest(3, key=lambda x: x[-1]),
                     [['wa', 'zb', 'xc']])

    self.assertEqual([('a', x) for x in [1, 2, 3, 4, 1, 1]]
                     | combine.Top.LargestPerKey(3, key=lambda x: -x),
                     [('a', [1, 1, 1])])
    self.assertEqual([('a', x) for x in [1, 2, 3, 4, 1, 1]]
                     | combine.Top.SmallestPerKey(3, key=lambda x: -x),
                     [('a', [4, 3, 2])])

  def test_sharded_top_combine_fn(self):
    def test_combine_fn(combine_fn, shards, expected):
      accumulators = [
          combine_fn.add_inputs(combine_fn.create_accumulator(), shard)
          for shard in shards
      ]
      final_accumulator = combine_fn.merge_accumulators(accumulators)
      self.assertEqual(combine_fn.extract_output(final_accumulator), expected)

    test_combine_fn(combine.TopCombineFn(3), [range(10), range(10)], [9, 9, 8])
    test_combine_fn(
        combine.TopCombineFn(5), [range(1000), range(100), range(1001)],
        [1000, 999, 999, 998, 998])

  def test_combine_per_key_top_display_data(self):
    def individual_test_per_key_dd(combineFn):
      transform = beam.CombinePerKey(combineFn)
      dd = DisplayData.create_from(transform)
      expected_items = [
          DisplayDataItemMatcher('combine_fn', combineFn.__class__),
          DisplayDataItemMatcher('n', combineFn._n),
          DisplayDataItemMatcher('compare', combineFn._compare.__name__)
      ]
      hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

    individual_test_per_key_dd(combine.Largest(5))
    individual_test_per_key_dd(combine.Smallest(3))
    individual_test_per_key_dd(combine.TopCombineFn(8))
    individual_test_per_key_dd(combine.Largest(5))

  def test_combine_sample_display_data(self):
    def individual_test_per_key_dd(sampleFn, n):
      trs = [sampleFn(n)]
      for transform in trs:
        dd = DisplayData.create_from(transform)
        hc.assert_that(
            dd.items,
            hc.contains_inanyorder(DisplayDataItemMatcher('n', transform._n)))

    individual_test_per_key_dd(combine.Sample.FixedSizePerKey, 5)
    individual_test_per_key_dd(combine.Sample.FixedSizeGlobally, 5)

  def test_combine_globally_display_data(self):
    transform = beam.CombineGlobally(combine.Smallest(5))
    dd = DisplayData.create_from(transform)
    expected_items = [
        DisplayDataItemMatcher('combine_fn', combine.Smallest),
        DisplayDataItemMatcher('n', 5),
        DisplayDataItemMatcher('compare', 'gt')
    ]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_basic_combiners_display_data(self):
    transform = beam.CombineGlobally(
        combine.TupleCombineFn(max, combine.MeanCombineFn(), sum))
    dd = DisplayData.create_from(transform)
    expected_items = [
        DisplayDataItemMatcher('combine_fn', combine.TupleCombineFn),
        DisplayDataItemMatcher('combiners', "['max', 'MeanCombineFn', 'sum']"),
        DisplayDataItemMatcher('merge_accumulators_batch_size', 333),
    ]
    hc.assert_that(dd.items, hc.contains_inanyorder(*expected_items))

  def test_top_shorthands(self):
    with TestPipeline() as pipeline:

      pcoll = pipeline | 'start' >> Create([6, 3, 1, 1, 9, 1, 5, 2, 0, 6])
      result_top = pcoll | 'top' >> beam.CombineGlobally(combine.Largest(5))
      result_bot = pcoll | 'bot' >> beam.CombineGlobally(combine.Smallest(4))
      assert_that(result_top, equal_to([[9, 6, 6, 5, 3]]), label='assert:top')
      assert_that(result_bot, equal_to([[0, 1, 1, 1]]), label='assert:bot')

      pcoll = pipeline | 'start-perkey' >> Create(
          [('a', x) for x in [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]])
      result_ktop = pcoll | 'top-perkey' >> beam.CombinePerKey(
          combine.Largest(5))
      result_kbot = pcoll | 'bot-perkey' >> beam.CombinePerKey(
          combine.Smallest(4))
      assert_that(result_ktop, equal_to([('a', [9, 6, 6, 5, 3])]), label='ktop')
      assert_that(result_kbot, equal_to([('a', [0, 1, 1, 1])]), label='kbot')

  def test_top_no_compact(self):
    class TopCombineFnNoCompact(combine.TopCombineFn):
      def compact(self, accumulator):
        return accumulator

    with TestPipeline() as pipeline:
      pcoll = pipeline | 'Start' >> Create([6, 3, 1, 1, 9, 1, 5, 2, 0, 6])
      result_top = pcoll | 'Top' >> beam.CombineGlobally(
          TopCombineFnNoCompact(5, key=lambda x: x))
      result_bot = pcoll | 'Bot' >> beam.CombineGlobally(
          TopCombineFnNoCompact(4, reverse=True))
      assert_that(result_top, equal_to([[9, 6, 6, 5, 3]]), label='Assert:Top')
      assert_that(result_bot, equal_to([[0, 1, 1, 1]]), label='Assert:Bot')

      pcoll = pipeline | 'Start-Perkey' >> Create(
          [('a', x) for x in [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]])
      result_ktop = pcoll | 'Top-PerKey' >> beam.CombinePerKey(
          TopCombineFnNoCompact(5, key=lambda x: x))
      result_kbot = pcoll | 'Bot-PerKey' >> beam.CombinePerKey(
          TopCombineFnNoCompact(4, reverse=True))
      assert_that(result_ktop, equal_to([('a', [9, 6, 6, 5, 3])]), label='KTop')
      assert_that(result_kbot, equal_to([('a', [0, 1, 1, 1])]), label='KBot')


  def test_per_key_sample(self):
    with TestPipeline() as pipeline:
      pcoll = pipeline | 'start-perkey' >> Create(
          sum(([(i, 1), (i, 1), (i, 2), (i, 2)] for i in range(9)), []))
      result = pcoll | 'sample' >> combine.Sample.FixedSizePerKey(3)

      def matcher():
        def match(actual):
          for _, samples in actual:
            equal_to([3])([len(samples)])
            num_ones = sum(1 for x in samples if x == 1)
            num_twos = sum(1 for x in samples if x == 2)
            equal_to([1, 2])([num_ones, num_twos])

        return match

      assert_that(result, matcher())

  def test_tuple_combine_fn(self):
    with TestPipeline() as p:
      result = (
          p
          | Create([('a', 100, 0.0), ('b', 10, -1), ('c', 1, 100)])
          | beam.CombineGlobally(
              combine.TupleCombineFn(max, combine.MeanCombineFn(),
                                     sum)).without_defaults())
      assert_that(result, equal_to([('c', 111.0 / 3, 99.0)]))

  def test_tuple_combine_fn_without_defaults(self):
    with TestPipeline() as p:
      result = (
          p
          | Create([1, 1, 2, 3])
          | beam.CombineGlobally(
              combine.TupleCombineFn(
                  min, combine.MeanCombineFn(),
                  max).with_common_input()).without_defaults())
      assert_that(result, equal_to([(1, 7.0 / 4, 3)]))

  def test_empty_tuple_combine_fn(self):
    with TestPipeline() as p:
      result = (
          p
          | Create([(), (), ()])
          | beam.CombineGlobally(combine.TupleCombineFn()))
      assert_that(result, equal_to([()]))

  def test_tuple_combine_fn_batched_merge(self):
    num_combine_fns = 10
    max_num_accumulators_in_memory = 30
    merge_accumulators_batch_size = (
        max_num_accumulators_in_memory // num_combine_fns - 1)
    num_accumulator_tuples_to_merge = 20

    class CountedAccumulator:
      count = 0
      oom = False

      def __init__(self):
        if CountedAccumulator.count > max_num_accumulators_in_memory:
          CountedAccumulator.oom = True
        else:
          CountedAccumulator.count += 1

    class CountedAccumulatorCombineFn(beam.CombineFn):
      def create_accumulator(self):
        return CountedAccumulator()

      def merge_accumulators(self, accumulators):
        CountedAccumulator.count += 1
        for _ in accumulators:
          CountedAccumulator.count -= 1

    combine_fn = combine.TupleCombineFn(
        *[CountedAccumulatorCombineFn() for _ in range(num_combine_fns)],
        merge_accumulators_batch_size=merge_accumulators_batch_size)
    combine_fn.merge_accumulators(
        combine_fn.create_accumulator()
        for _ in range(num_accumulator_tuples_to_merge))
    assert not CountedAccumulator.oom



  def test_to_set(self):
    pipeline = TestPipeline()
    the_list = [6, 3, 1, 1, 9, 1, 5, 2, 0, 6]
    timestamp = 0
    pcoll = pipeline | 'start' >> Create(the_list)
    result = pcoll | 'to set' >> combine.ToSet()

    timestamped = pcoll | Map(lambda x: TimestampedValue(x, timestamp))
    windowed = timestamped | 'window' >> WindowInto(FixedWindows(60))
    result_windowed = (
        windowed
        | 'to set wo defaults' >> combine.ToSet().without_defaults())

    def matcher(expected):
      def match(actual):
        equal_to(expected[0])(actual[0])

      return match

    assert_that(result, matcher(set(the_list)))
    assert_that(
        result_windowed, matcher(set(the_list)), label='to-set-wo-defaults')

  def test_combine_globally_with_default(self):
    with TestPipeline() as p:
      assert_that(p | Create([]) | CombineGlobally(sum), equal_to([0]))

  def test_combine_globally_without_default(self):
    with TestPipeline() as p:
      result = p | Create([]) | CombineGlobally(sum).without_defaults()
      assert_that(result, equal_to([]))

  def test_combine_globally_with_default_side_input(self):
    class SideInputCombine(PTransform):
      def expand(self, pcoll):
        side = pcoll | CombineGlobally(sum).as_singleton_view()
        main = pcoll.pipeline | Create([None])
        return main | Map(lambda _, s: s, side)

    with TestPipeline() as p:
      result1 = p | 'i1' >> Create([]) | 'c1' >> SideInputCombine()
      result2 = p | 'i2' >> Create([1, 2, 3, 4]) | 'c2' >> SideInputCombine()
      assert_that(result1, equal_to([0]), label='r1')
      assert_that(result2, equal_to([10]), label='r2')

  def test_hot_key_fanout(self):
    with TestPipeline() as p:
      result = (
          p
          | beam.Create(itertools.product(['hot', 'cold'], range(10)))
          | beam.CombinePerKey(combine.MeanCombineFn()).with_hot_key_fanout(
              lambda key: (key == 'hot') * 5))
      assert_that(result, equal_to([('hot', 4.5), ('cold', 4.5)]))

  def test_hot_key_fanout_sharded(self):
    with TestPipeline() as p:
      elements = [(None, e) for e in range(1000)]
      random.shuffle(elements)
      shards = [
          p | "Shard%s" % shard >> beam.Create(elements[shard::20])
          for shard in range(20)
      ]
      result = (
          shards
          | beam.Flatten()
          | beam.CombinePerKey(combine.MeanCombineFn()).with_hot_key_fanout(
              lambda key: random.randrange(0, 5)))
      assert_that(result, equal_to([(None, 499.5)]))

  def test_global_fanout(self):
    with TestPipeline() as p:
      result = (
          p
          | beam.Create(range(100))
          | beam.CombineGlobally(combine.MeanCombineFn()).with_fanout(11))
      assert_that(result, equal_to([49.5]))


  def test_combining_with_sliding_windows_and_fanout_raises_error(self):
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    with self.assertRaises(ValueError):
      with TestPipeline(options=options) as p:
        _ = (
            p
            | beam.Create([
                window.TimestampedValue(0, Timestamp(seconds=1666707510)),
                window.TimestampedValue(1, Timestamp(seconds=1666707511)),
                window.TimestampedValue(2, Timestamp(seconds=1666707512)),
                window.TimestampedValue(3, Timestamp(seconds=1666707513)),
                window.TimestampedValue(5, Timestamp(seconds=1666707515)),
                window.TimestampedValue(6, Timestamp(seconds=1666707516)),
                window.TimestampedValue(7, Timestamp(seconds=1666707517)),
                window.TimestampedValue(8, Timestamp(seconds=1666707518))
            ])
            | beam.WindowInto(window.SlidingWindows(10, 5))
            | beam.CombineGlobally(beam.combiners.ToListCombineFn()).
            without_defaults().with_fanout(7))

  def test_MeanCombineFn_combine(self):
    with TestPipeline() as p:
      input = (
          p
          | beam.Create([('a', 1), ('a', 1), ('a', 4), ('b', 1), ('b', 13)]))
      global_mean = (
          input
          | beam.Values()
          | beam.CombineGlobally(combine.MeanCombineFn()))

      mean_per_key = (input | beam.CombinePerKey(combine.MeanCombineFn()))

      expected_mean_per_key = [('a', 2), ('b', 7)]
      assert_that(global_mean, equal_to([4]), label='global mean')
      assert_that(
          mean_per_key, equal_to(expected_mean_per_key), label='mean per key')

  def test_MeanCombineFn_combine_empty(self):

    with TestPipeline() as p:
      input = (p | beam.Create([]))

      global_mean = (
          input
          | beam.Values()
          | beam.CombineGlobally(combine.MeanCombineFn())
          | beam.Map(str))

      mean_per_key = (input | beam.CombinePerKey(combine.MeanCombineFn()))

      assert_that(global_mean, equal_to(['nan']), label='global mean')
      assert_that(mean_per_key, equal_to([]), label='mean per key')



  def test_custormized_counters_in_combine_fn(self):
    p = TestPipeline()
    input = (
        p
        | beam.Create([('key1', 'a'), ('key1', 'ab'), ('key1', 'abc'),
                       ('key2', 'uvxy'), ('key2', 'uvxyz')]))

    global_concat = (
        input
        | beam.Values()
        | beam.CombineGlobally(SortedConcatWithCounters()))

    concat_per_key = (input | beam.CombinePerKey(SortedConcatWithCounters()))

    expected_concat_per_key = [('key1', 'aaabbc'), ('key2', 'uuvvxxyyz')]
    assert_that(
        global_concat, equal_to(['aaabbcuuvvxxyyz']), label='global concat')
    assert_that(
        concat_per_key,
        equal_to(expected_concat_per_key),
        label='concat per key')

    result = p.run()
    result.wait_until_finish()

    word_counter_filter = MetricsFilter().with_name('word_counter')
    query_result = result.metrics().query(word_counter_filter)
    if query_result['counters']:
      word_counter = query_result['counters'][0]
      self.assertEqual(word_counter.result, 5)

    word_lengths_filter = MetricsFilter().with_name('word_lengths')
    query_result = result.metrics().query(word_lengths_filter)
    if query_result['counters']:
      word_lengths = query_result['counters'][0]
      self.assertEqual(word_lengths.result, 15)

    word_len_dist_filter = MetricsFilter().with_name('word_len_dist')
    query_result = result.metrics().query(word_len_dist_filter)
    if query_result['distributions']:
      word_len_dist = query_result['distributions'][0]
      self.assertEqual(word_len_dist.result.mean, 3)

    last_word_len_filter = MetricsFilter().with_name('last_word_len')
    query_result = result.metrics().query(last_word_len_filter)
    if query_result['gauges']:
      last_word_len = query_result['gauges'][0]
      self.assertIn(last_word_len.result.value, [1, 2, 3, 4, 5])

  def test_custormized_counters_in_combine_fn_empty(self):
    p = TestPipeline()
    input = p | beam.Create([])

    global_concat = (
        input
        | beam.Values()
        | beam.CombineGlobally(SortedConcatWithCounters()))

    concat_per_key = (input | beam.CombinePerKey(SortedConcatWithCounters()))

    assert_that(global_concat, equal_to(['']), label='global concat')
    assert_that(concat_per_key, equal_to([]), label='concat per key')

    result = p.run()
    result.wait_until_finish()

    word_counter_filter = MetricsFilter().with_name('word_counter')
    query_result = result.metrics().query(word_counter_filter)
    if query_result['counters']:
      word_counter = query_result['counters'][0]
      self.assertEqual(word_counter.result, 0)

    word_lengths_filter = MetricsFilter().with_name('word_lengths')
    query_result = result.metrics().query(word_lengths_filter)
    if query_result['counters']:
      word_lengths = query_result['counters'][0]
      self.assertEqual(word_lengths.result, 0)

    word_len_dist_filter = MetricsFilter().with_name('word_len_dist')
    query_result = result.metrics().query(word_len_dist_filter)
    if query_result['distributions']:
      word_len_dist = query_result['distributions'][0]
      self.assertEqual(word_len_dist.result.count, 0)

    last_word_len_filter = MetricsFilter().with_name('last_word_len')
    query_result = result.metrics().query(last_word_len_filter)

    self.assertFalse(query_result['gauges'])


class LatestTest(unittest.TestCase):

  def test_globally_empty(self):
    l = []
    with TestPipeline() as p:
      pc = p | Create(l) | Map(lambda x: x)
      latest = pc | combine.Latest.Globally()
      assert_that(latest, equal_to([None]))

  def test_per_key(self):
    l = [
        window.TimestampedValue(('a', 1), 300),
        window.TimestampedValue(('b', 3), 100),
        window.TimestampedValue(('a', 2), 200)
    ]
    with TestPipeline() as p:
      pc = p | Create(l) | Map(lambda x: x)
      latest = pc | combine.Latest.PerKey()
      assert_that(latest, equal_to([('a', 1), ('b', 3)]))

  def test_per_key_empty(self):
    l = []
    with TestPipeline() as p:
      pc = p | Create(l) | Map(lambda x: x)
      latest = pc | combine.Latest.PerKey()
      assert_that(latest, equal_to([]))


class LatestCombineFnTest(unittest.TestCase):
  def setUp(self):
    self.fn = combine.LatestCombineFn()

  def test_create_accumulator(self):
    accumulator = self.fn.create_accumulator()
    self.assertEqual(accumulator, (None, window.MIN_TIMESTAMP))

  def test_add_input(self):
    accumulator = self.fn.create_accumulator()
    element = (1, 100)
    new_accumulator = self.fn.add_input(accumulator, element)
    self.assertEqual(new_accumulator, (1, 100))

  def test_merge_accumulators(self):
    accumulators = [(2, 400), (5, 100), (9, 200)]
    merged_accumulator = self.fn.merge_accumulators(accumulators)
    self.assertEqual(merged_accumulator, (2, 400))

  def test_extract_output(self):
    accumulator = (1, 100)
    output = self.fn.extract_output(accumulator)
    self.assertEqual(output, 1)

  def test_with_input_types_decorator_violation(self):
    l_int = [1, 2, 3]
    l_dict = [{'a': 3}, {'g': 5}, {'r': 8}]
    l_3_tuple = [(12, 31, 41), (12, 34, 34), (84, 92, 74)]

    with self.assertRaises(TypeCheckError):
      with TestPipeline() as p:
        pc = p | Create(l_int)
        _ = pc | beam.CombineGlobally(self.fn)

    with self.assertRaises(TypeCheckError):
      with TestPipeline() as p:
        pc = p | Create(l_dict)
        _ = pc | beam.CombineGlobally(self.fn)

    with self.assertRaises(TypeCheckError):
      with TestPipeline() as p:
        pc = p | Create(l_3_tuple)
        _ = pc | beam.CombineGlobally(self.fn)


@pytest.mark.it_validatesrunner
class CombineValuesTest(unittest.TestCase):
  def test_gbk_immediately_followed_by_combine(self):
    def merge(vals):
      return "".join(vals)

    with TestPipeline() as p:
      result = (
          p \
          | Create([("key1", "foo"), ("key2", "bar"), ("key1", "foo")],
                    reshuffle=False) \
          | beam.GroupByKey() \
          | beam.CombineValues(merge) \
          | beam.MapTuple(lambda k, v: '{}: {}'.format(k, v)))

      assert_that(result, equal_to(['key1: foofoo', 'key2: bar']))





class CombineGloballyTest(unittest.TestCase):
  def test_combine_globally_for_unbounded_source_with_default(self):
    with self.assertLogs() as captured_logs:
      with TestPipeline() as p:
        _ = (
            p
            | PeriodicImpulse(
                start_timestamp=time.time(),
                stop_timestamp=time.time() + 4,
                fire_interval=1,
                apply_windowing=False,
            )
            | beam.Map(lambda x: ('c', 1))
            | beam.WindowInto(
                window.GlobalWindows(),
                trigger=trigger.Repeatedly(trigger.AfterCount(2)),
                accumulation_mode=trigger.AccumulationMode.DISCARDING,
            )
            | beam.combiners.Count.Globally())
    self.assertIn('unbounded collections', '\n'.join(captured_logs.output))

  def test_combine_globally_for_unbounded_source_without_defaults(self):
    with TestPipeline() as p:
      _ = (
          p
          | PeriodicImpulse(
              start_timestamp=time.time(),
              stop_timestamp=time.time() + 4,
              fire_interval=1,
              apply_windowing=False,
          )
          | beam.Map(lambda x: 1)
          | beam.WindowInto(
              window.GlobalWindows(),
              trigger=trigger.Repeatedly(trigger.AfterCount(2)),
              accumulation_mode=trigger.AccumulationMode.DISCARDING,
          )
          | beam.CombineGlobally(sum).without_defaults())


if __name__ == '__main__':
  unittest.main()
