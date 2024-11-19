import unittest

import mock

from apache_beam.examples.snippets.util import assert_matches_stdout
from apache_beam.testing.test_pipeline import TestPipeline

import flatten


def check_flatten(actual):
  expected = '''[START flatten_result]
🍓
🥕
🍆
🍅
🥔
🍎
🍐
🍊
[END flatten_result]'''.splitlines()[1:-1]
  assert_matches_stdout(actual, expected)

if __name__ == '__main__':
  unittest.main()
