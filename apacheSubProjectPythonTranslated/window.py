import abc
from typing import Any
from typing import Iterable
from typing import Optional

from apache_beam.coders import coders
from apache_beam.portability import python_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.transforms import timeutil
from apache_beam.utils import urns
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.timestamp import TimestampTypes  # pylint: disable=unused-import

__all__ = [
    'TimestampCombiner',
    'WindowFn',
    'BoundedWindow',
    'IntervalWindow',
    'TimestampedValue',
    'GlobalWindow',
    'NonMergingWindowFn',
    'GlobalWindows',
    'FixedWindows',
    'SlidingWindows',
    'Sessions',
]

class TimestampCombiner(object):
    """Determines how output timestamps of grouping operations are assigned."""

    OUTPUT_AT_EOW = beam_runner_api_pb2.OutputTime.END_OF_WINDOW
    OUTPUT_AT_EARLIEST = beam_runner_api_pb2.OutputTime.EARLIEST_IN_PANE
    OUTPUT_AT_LATEST = beam_runner_api_pb2.OutputTime.LATEST_IN_PANE
    OUTPUT_AT_EARLIEST_TRANSFORMED = 'OUTPUT_AT_EARLIEST_TRANSFORMED'

    @staticmethod
    def get_impl(
        timestamp_combiner: beam_runner_api_pb2.OutputTime.Enum,
        window_fn: 'WindowFn') -> timeutil.TimestampCombinerImpl:
        if timestamp_combiner == TimestampCombiner.OUTPUT_AT_EOW:
            return timeutil.OutputAtEndOfWindowImpl()
        elif timestamp_combiner == TimestampCombiner.OUTPUT_AT_EARLIEST:
            return timeutil.OutputAtEarliestInputTimestampImpl()
        elif timestamp_combiner == TimestampCombiner.OUTPUT_AT_LATEST:
            return timeutil.OutputAtLatestInputTimestampImpl()
        elif timestamp_combiner == TimestampCombiner.OUTPUT_AT_EARLIEST_TRANSFORMED:
            return timeutil.OutputAtEarliestTransformedInputTimestampImpl(window_fn)
        else:
            raise ValueError('Invalid TimestampCombiner: %s.' % timestamp_combiner)

class WindowFn(urns.RunnerApiFn, metaclass=abc.ABCMeta):
    """An abstract windowing function defining a basic assign and merge."""
    class AssignContext(object):
        """Context passed to WindowFn.assign()."""
        def __init__(
            self,
            timestamp: TimestampTypes,
            element: Optional[Any] = None,
            window: Optional['BoundedWindow'] = None) -> None:
            self.timestamp = Timestamp.of(timestamp)
            self.element = element
            self.window = window

    @abc.abstractmethod
    def assign(self,
               assign_context: 'AssignContext') -> Iterable['BoundedWindow']:
        """Associates windows to an element.

        Arguments:
          assign_context: Instance of AssignContext.

        Returns:
          An iterable of BoundedWindow.
        """
        raise NotImplementedError

    class MergeContext(object):
        """Context passed to WindowFn.merge() to perform merging, if any."""
        def __init__(self, windows: Iterable['BoundedWindow']) -> None:
            self.windows = list(windows)

        def merge(
            self,
            to_be_merged: Iterable['BoundedWindow'],
            merge_result: 'BoundedWindow') -> None:
            raise NotImplementedError

    @abc.abstractmethod
    def merge(self, merge_context: 'WindowFn.MergeContext') -> None:
        """Returns a window that is the result of merging a set of windows."""
        raise NotImplementedError

    def is_merging(self) -> bool:
        """Returns whether this WindowFn merges windows."""
        return True

    @abc.abstractmethod
    def get_window_coder(self) -> coders.Coder:
        raise NotImplementedError

    def get_transformed_output_time(
        self, window: 'BoundedWindow', input_timestamp: Timestamp) -> Timestamp:  # pylint: disable=unused-argument
        """Given input time and output window, returns output time for window.

        If TimestampCombiner.OUTPUT_AT_EARLIEST_TRANSFORMED is used in the
        Windowing, the output timestamp for the given window will be the earliest
        of the timestamps returned by get_transformed_output_time() for elements
        of the window.

        Arguments:
          window: Output window of element.
          input_timestamp: Input timestamp of element as a timeutil.Timestamp
            object.

        Returns:
          Transformed timestamp.
        """
        return input_timestamp

    urns.RunnerApiFn.register_pickle_urn(python_urns.PICKLED_WINDOWFN)

class BoundedWindow(object):
    """A window for timestamps in range (-infinity, end).
    """