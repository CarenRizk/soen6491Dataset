
"""Tests for schemas."""


import typing
import unittest

import apache_beam as beam
import numpy as np
import pandas as pd
from apache_beam.coders import RowCoder
from apache_beam.coders.typecoders import registry as coders_registry
from apache_beam.dataframe import transforms
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.typehints import row_type
from apache_beam.typehints import typehints
from apache_beam.typehints.native_type_compatibility import match_is_named_tuple

import schemas

Simple = typing.NamedTuple(
    'Simple', [('name', str), ('id', int), ('height', float)])
coders_registry.register_coder(Simple, RowCoder)
Animal = typing.NamedTuple(
    'Animal', [('animal', str), ('max_speed', typing.Optional[float])])
coders_registry.register_coder(Animal, RowCoder)


def matches_df(expected):
  def check_df_pcoll_equal(actual):
    actual = pd.concat(actual)
    sorted_actual = actual.sort_values(by=list(actual.columns)).reset_index(
        drop=True)
    sorted_expected = expected.sort_values(
        by=list(expected.columns)).reset_index(drop=True)
    pd.testing.assert_frame_equal(sorted_actual, sorted_expected)

  return check_df_pcoll_equal


COLUMNS: typing.List[typing.Tuple[typing.List[typing.Any],
                                  typing.Any,
                                  str,
                                  typing.Any]] = [
                                      ([375, 24, 0, 10, 16],
                                       np.int32,
                                       'i32',
                                       np.int32),
                                      ([375, 24, 0, 10, 16],
                                       np.int64,
                                       'i64',
                                       np.int64),
                                      ([375, 24, None, 10, 16],
                                       pd.Int32Dtype(),
                                       'i32_nullable',
                                       typing.Optional[np.int32]),
                                      ([375, 24, None, 10, 16],
                                       pd.Int64Dtype(),
                                       'i64_nullable',
                                       typing.Optional[np.int64]),
                                      ([375., 24., None, 10., 16.],
                                       np.float64,
                                       'f64',
                                       typing.Optional[np.float64]),
                                      ([375., 24., None, 10., 16.],
                                       np.float32,
                                       'f32',
                                       typing.Optional[np.float32]),
                                      ([True, False, True, True, False],
                                       bool,
                                       'bool',
                                       bool),
                                      (['Falcon', 'Ostrich', None, 3.14, 0],
                                       object,
                                       'any',
                                       typing.Any),
                                      ([True, False, True, None, False],
                                       pd.BooleanDtype(),
                                       'bool_nullable',
                                       typing.Optional[bool]),
                                      ([
                                          'Falcon',
                                          'Ostrich',
                                          None,
                                          'Aardvark',
                                          'Elephant'
                                      ],
                                       pd.StringDtype(),
                                       'strdtype',
                                       typing.Optional[str]),
                                  ]

NICE_TYPES_DF = pd.DataFrame(columns=[name for _, _, name, _ in COLUMNS])
for arr, dtype, name, _ in COLUMNS:
  NICE_TYPES_DF[name] = pd.Series(arr, dtype=dtype, name=name).astype(dtype)

NICE_TYPES_PROXY = NICE_TYPES_DF[:0]

SERIES_TESTS = [(pd.Series(arr, dtype=dtype, name=name), arr, beam_type)
                for (arr, dtype, name, beam_type) in COLUMNS]

_TEST_ARRAYS: typing.List[typing.List[typing.Any]] = [
    arr for (arr, _, _, _) in COLUMNS
]
DF_RESULT = list(zip(*_TEST_ARRAYS))
BEAM_SCHEMA = typing.NamedTuple(  # type: ignore
    'BEAM_SCHEMA', [(name, beam_type) for _, _, name, beam_type in COLUMNS])
INDEX_DF_TESTS = [(
    NICE_TYPES_DF.set_index([name for _, _, name, _ in COLUMNS[:i]]),
    DF_RESULT,
    BEAM_SCHEMA) for i in range(1, len(COLUMNS) + 1)]

NOINDEX_DF_TESTS = [(NICE_TYPES_DF, DF_RESULT, BEAM_SCHEMA)]

PD_VERSION = tuple(map(int, pd.__version__.split('.')[0:3]))




class SchemasTest(unittest.TestCase):
  def test_simple_df(self):
    expected = pd.DataFrame({
        'name': list(str(i) for i in range(5)),
        'id': list(range(5)),
        'height': list(float(i) for i in range(5))
    },
                            columns=['name', 'id', 'height'])

    expected.name = expected.name.astype(pd.StringDtype())

    with TestPipeline() as p:
      res = (
          p
          | beam.Create(
              [Simple(name=str(i), id=i, height=float(i)) for i in range(5)])
          | schemas.BatchRowsAsDataFrame(min_batch_size=10, max_batch_size=10))
      assert_that(res, matches_df(expected))

  def test_simple_df_with_beam_row(self):
    expected = pd.DataFrame({
        'name': list(str(i) for i in range(5)),
        'id': list(range(5)),
        'height': list(float(i) for i in range(5))
    },
                            columns=['name', 'id', 'height'])
    expected.name = expected.name.astype(pd.StringDtype())

    with TestPipeline() as p:
      res = (
          p
          | beam.Create([(str(i), i, float(i)) for i in range(5)])
          | beam.Select(
              name=lambda r: str(r[0]),
              id=lambda r: int(r[1]),
              height=lambda r: float(r[2]))
          | schemas.BatchRowsAsDataFrame(min_batch_size=10, max_batch_size=10))
      assert_that(res, matches_df(expected))

  def test_generate_proxy(self):
    expected = pd.DataFrame({
        'animal': pd.Series(dtype=pd.StringDtype()),
        'max_speed': pd.Series(dtype=np.float64)
    })

    pd.testing.assert_frame_equal(schemas.generate_proxy(Animal), expected)

  def test_generate_proxy_beam_typehint(self):
    expected = pd.Series(dtype=pd.Int32Dtype())

    actual = schemas.generate_proxy(typehints.Optional[np.int32])

    pd.testing.assert_series_equal(actual, expected)

  def test_nice_types_proxy_roundtrip(self):
    roundtripped = schemas.generate_proxy(
        schemas.element_type_from_dataframe(NICE_TYPES_PROXY))
    self.assertTrue(roundtripped.equals(NICE_TYPES_PROXY))

  @unittest.skipIf(
      PD_VERSION == (1, 2, 1),
      "Can't roundtrip bytes in pandas 1.2.1"
      "https://github.com/pandas-dev/pandas/issues/39474")
  def test_bytes_proxy_roundtrip(self):
    proxy = pd.DataFrame({'bytes': []})
    proxy.bytes = proxy.bytes.astype(bytes)

    roundtripped = schemas.generate_proxy(
        schemas.element_type_from_dataframe(proxy))

    self.assertEqual(roundtripped.bytes.dtype.kind, 'S')

  def test_batch_with_df_transform(self):
    with TestPipeline() as p:
      res = (
          p
          | beam.Create([
              Animal('Falcon', 380.0),
              Animal('Falcon', 370.0),
              Animal('Parrot', 24.0),
              Animal('Parrot', 26.0)
          ])
          | schemas.BatchRowsAsDataFrame()
          | transforms.DataframeTransform(
              lambda df: df.groupby('animal').mean(),
              proxy=schemas.generate_proxy(Animal),
              include_indexes=True))
      assert_that(res, equal_to([('Falcon', 375.), ('Parrot', 25.)]))

    with TestPipeline() as p:
      with beam.dataframe.allow_non_parallel_operations():
        res = (
            p
            | beam.Create([
                Animal('Falcon', 380.0),
                Animal('Falcon', 370.0),
                Animal('Parrot', 24.0),
                Animal('Parrot', 26.0)
            ])
            | schemas.BatchRowsAsDataFrame()
            | transforms.DataframeTransform(
                lambda df: df.groupby('animal').mean().reset_index(),
                proxy=schemas.generate_proxy(Animal)))
        assert_that(res, equal_to([('Falcon', 375.), ('Parrot', 25.)]))

  def assert_typehints_equal(self, left, right):
    def maybe_drop_rowtypeconstraint(typehint):
      if isinstance(typehint, row_type.RowTypeConstraint):
        return typehint.user_type
      else:
        return typehint

    left = maybe_drop_rowtypeconstraint(typehints.normalize(left))
    right = maybe_drop_rowtypeconstraint(typehints.normalize(right))

    if match_is_named_tuple(left):
      self.assertTrue(match_is_named_tuple(right))
      self.assertEqual(left.__annotations__, right.__annotations__)
    else:
      self.assertEqual(left, right)



  def test_unbatch_include_index_unnamed_index_raises(self):
    df = pd.DataFrame({'foo': [1, 2, 3, 4]})
    proxy = df[:0]

    with TestPipeline() as p:
      pc = p | beam.Create([df[::2], df[1::2]])

      with self.assertRaisesRegex(ValueError, 'unnamed'):
        _ = pc | schemas.UnbatchPandas(proxy, include_indexes=True)

  def test_unbatch_include_index_nonunique_index_raises(self):
    df = pd.DataFrame({'foo': [1, 2, 3, 4]})
    df.index = pd.MultiIndex.from_arrays([[1, 2, 3, 4], [4, 3, 2, 1]],
                                         names=['bar', 'bar'])
    proxy = df[:0]

    with TestPipeline() as p:
      pc = p | beam.Create([df[::2], df[1::2]])

      with self.assertRaisesRegex(ValueError, 'bar'):
        _ = pc | schemas.UnbatchPandas(proxy, include_indexes=True)

  def test_unbatch_include_index_column_conflict_raises(self):
    df = pd.DataFrame({'foo': [1, 2, 3, 4]})
    df.index = pd.Index([4, 3, 2, 1], name='foo')
    proxy = df[:0]

    with TestPipeline() as p:
      pc = p | beam.Create([df[::2], df[1::2]])

      with self.assertRaisesRegex(ValueError, 'foo'):
        _ = pc | schemas.UnbatchPandas(proxy, include_indexes=True)

  def test_unbatch_datetime(self):

    s = pd.Series(
        pd.date_range(
            '1/1/2000', periods=100, freq='m', tz='America/Los_Angeles'))
    proxy = s[:0]

    with TestPipeline() as p:
      res = (
          p | beam.Create([s[::2], s[1::2]])
          | schemas.UnbatchPandas(proxy, include_indexes=True))

      assert_that(res, equal_to(list(s)))


if __name__ == '__main__':
  unittest.main()
