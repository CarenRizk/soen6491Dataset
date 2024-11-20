import itertools
from typing import TypeVar
from typing import Union

from apache_beam.transforms import core
from apache_beam.transforms import ptransform
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp

__all__ = [
    'Combine',
]

T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')
TimestampType = Union[int, float, Timestamp, Duration]


class Combine:
    @staticmethod
    def globally(combiner):
        return Combine.globally_with_fn(IterableCombineFn.of(combiner), DisplayData.item("combineFn", combiner.__class__).with_label("Combiner"))

    @staticmethod
    def globally_with_fn(fn, fn_display_data):
        return Combine.Globally(fn, fn_display_data, True, 0, [])

    @staticmethod
    def per_key(fn):
        return Combine.per_key_with_fn(IterableCombineFn.of(fn), DisplayData.item("combineFn", fn.__class__).with_label("Combiner"))

    @staticmethod
    def per_key_with_fn(fn, fn_display_data):
        return Combine.PerKey(fn, fn_display_data, False)

    class Globally(ptransform.PTransform):
        def __init__(self, fn, fn_display_data, insert_default, fanout, side_inputs):
            self.fn = fn
            self.fn_display_data = fn_display_data
            self.insert_default = insert_default
            self.fanout = fanout
            self.side_inputs = side_inputs

        def expand(self, input):
            # Placeholder for PCollection
            return input

    class PerKey(ptransform.PTransform):
        def __init__(self, fn, fn_display_data, few_keys):
            self.fn = fn
            self.fn_display_data = fn_display_data
            self.few_keys = few_keys
            self.side_inputs = []

        def expand(self, input):
            # Placeholder for PCollection
            return input


class IterableCombineFn(core.CombineFn):
    @staticmethod
    def of(combiner, buffer_size=20):
        return IterableCombineFn(combiner, buffer_size)

    def __init__(self, combiner, buffer_size):
        self.combiner = combiner
        self.buffer_size = buffer_size

    def create_accumulator(self):
        return []

    def add_input(self, accumulator, input):
        accumulator.append(input)
        if len(accumulator) > self.buffer_size:
            return self.merge_to_singleton(accumulator)
        else:
            return accumulator

    def merge_accumulators(self, accumulators):
        return self.merge_to_singleton(itertools.chain.from_iterable(accumulators))

    def extract_output(self, accumulator):
        return self.combiner(accumulator)

    def merge_to_singleton(self, values):
        return [self.combiner(values)]

    def compact(self, accumulator):
        return accumulator if len(accumulator) <= 1 else self.merge_to_singleton(accumulator)