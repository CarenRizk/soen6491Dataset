import logging

from apache_beam.io.filesystem import CompressionTypes
from apache_beam.transforms import PTransform

_LOGGER = logging.getLogger(__name__)

class TextIO:
    DEFAULT_BUNDLE_SIZE_BYTES = 64 * 1024 * 1024

    @staticmethod
    def read():
        return TextIO.Read()

    @staticmethod
    def write():
        return TextIO.Write()

    class Read(PTransform):
        def __init__(self):
            self.filepattern = None
            self.match_configuration = None
            self.hint_matches_many_files = False
            self.compression = CompressionTypes.AUTO
            self.delimiter = None
            self.skip_header_lines = 0

        def from_(self, filepattern):
            assert filepattern is not None, "filepattern can not be null"
            self.filepattern = filepattern
            return self

        def with_match_configuration(self, match_configuration):
            self.match_configuration = match_configuration
            return self

        def with_compression(self, compression):
            self.compression = compression
            return self

        def with_delimiter(self, delimiter):
            assert delimiter is not None, "delimiter can not be null"
            self.delimiter = delimiter
            return self

        def with_skip_header_lines(self, skip_header_lines):
            self.skip_header_lines = skip_header_lines
            return self

        def expand(self, input):
            assert self.filepattern is not None, "need to set the filepattern of a TextIO.Read transform"
            return input.apply("Read", ReadFromText(self.filepattern, self.match_configuration, self.compression, self.delimiter, self.skip_header_lines))

    class Write(PTransform):
        def __init__(self):
            self.filename_prefix = None
            self.filename_suffix = None
            self.temp_directory = None
            self.delimiter = None
            self.header = None
            self.footer = None
            self.num_shards = None

        def to(self, filename_prefix):
            self.filename_prefix = filename_prefix
            return self

        def with_suffix(self, filename_suffix):
            self.filename_suffix = filename_suffix
            return self

        def with_temp_directory(self, temp_directory):
            self.temp_directory = temp_directory
            return self

        def with_delimiter(self, delimiter):
            self.delimiter = delimiter
            return self

        def with_header(self, header):
            self.header = header
            return self

        def with_footer(self, footer):
            self.footer = footer
            return self

        def with_num_shards(self, num_shards):
            self.num_shards = num_shards
            return self

        def expand(self, input):
            assert self.filename_prefix is not None, "Need to set the filename prefix of a TextIO.Write transform."
            return input.apply("Write", WriteToText(self.filename_prefix, self.filename_suffix, self.temp_directory, self.delimiter, self.header, self.footer, self.num_shards))

# Placeholder for ReadFromText
# Placeholder for WriteToText