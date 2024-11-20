import logging
from collections import namedtuple
from typing import Callable
from typing import List
from typing import TYPE_CHECKING

import apache_beam as beam
from apache_beam.io import filesystem
from apache_beam.io import filesystems
from apache_beam.io.filesystem import BeamIOError

if TYPE_CHECKING:
    pass

_LOGGER = logging.getLogger(__name__)

FileMetadata = namedtuple("FileMetadata", "mime_type compression_type")

CreateFileMetadataFn = Callable[[str, str], FileMetadata]


class EmptyMatchTreatment(object):
    """How to treat empty matches in ``MatchAll`` and ``MatchFiles`` transforms.

    If empty matches are disallowed, an error will be thrown if a pattern does not
    match any files."""

    ALLOW = 'ALLOW'
    DISALLOW = 'DISALLOW'
    ALLOW_IF_WILDCARD = 'ALLOW_IF_WILDCARD'

    @staticmethod
    def allow_empty_match(pattern, setting):
        if setting == EmptyMatchTreatment.ALLOW:
            return True
        elif setting == EmptyMatchTreatment.ALLOW_IF_WILDCARD and '*' in pattern:
            return True
        elif setting == EmptyMatchTreatment.DISALLOW:
            return False
        else:
            raise ValueError(setting)


class _MatchAllFn(beam.DoFn):
    def __init__(self, empty_match_treatment):
        self._empty_match_treatment = empty_match_treatment

    def process(self, file_pattern: str) -> List[filesystem.FileMetadata]:
        match_results = filesystems.FileSystems.match([file_pattern])
        match_result = match_results[0]

        if (not match_result.metadata_list and
                not EmptyMatchTreatment.allow_empty_match(file_pattern,
                                                          self._empty_match_treatment)):
            raise BeamIOError(
                'Empty match for pattern %s. Disallowed.' % file_pattern)

        return match_result.metadata_list


class MatchFiles(beam.PTransform):
    """Matches a file pattern using ``FileSystems.match``.

    This ``PTransform`` returns a ``PCollection`` of matching files in the form
    of ``FileMetadata`` objects."""
    def __init__(self,
                 file_pattern: str,
                 empty_match_treatment=EmptyMatchTreatment.ALLOW_IF_WILDCARD):
        self._file_pattern = file_pattern
        self._empty_match_treatment = empty_match_treatment

    def expand(self, pcoll) -> beam.PCollection[filesystem.FileMetadata]:
        return pcoll.pipeline | beam.Create([self._file_pattern]) | MatchAll(
            empty_match_treatment=self._empty_match_treatment)


class MatchAll(beam.PTransform):
    """Matches file patterns from the input PCollection via ``FileSystems.match``.

    This ``PTransform`` returns a ``PCollection`` of matching files in the form
    of ``FileMetadata`` objects."""
    def __init__(self, empty_match_treatment=EmptyMatchTreatment.ALLOW):
        self._empty_match_treatment = empty_match_treatment

    def expand(
            self,
            pcoll: beam.PCollection,
    ) -> beam.PCollection[filesystem.FileMetadata]:
        return pcoll | beam.ParDo(_MatchAllFn(self._empty_match_treatment))


class ReadableFile(object):
    """A utility class for accessing files."""
    def __init__(self, metadata, compression=None):
        self.metadata = metadata