
"""Tests for transforms defined in apache_beam.io.fileio."""


import csv
import io
import json
import logging
import os
import unittest
import uuid
import warnings

import pytest
from hamcrest.library.text import stringmatches

import apache_beam as beam
import fileio
from apache_beam.io.filebasedsink_test import _TestCaseWithTempDirCleanUp
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.test_utils import compute_hash
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import matches_all
from apache_beam.transforms import trigger
from window import FixedWindows
from window import GlobalWindow
from window import IntervalWindow
from apache_beam.utils.timestamp import Timestamp

warnings.filterwarnings(
    'ignore', category=FutureWarning, module='apache_beam.io.fileio_test')


def _get_file_reader(readable_file):
  return io.TextIOWrapper(readable_file.open())


class MatchTest(_TestCaseWithTempDirCleanUp):
  def test_basic_two_files(self):
    files = []
    tempdir = '%s%s' % (self._new_tempdir(), os.sep)

    files.append(self._create_temp_file(dir=tempdir))
    files.append(self._create_temp_file(dir=tempdir))

    with TestPipeline() as p:
      files_pc = (
          p
          | fileio.MatchFiles(FileSystems.join(tempdir, '*'))
          | beam.Map(lambda x: x.path))

      assert_that(files_pc, equal_to(files))

  def test_match_all_two_directories(self):
    files = []
    directories = []

    for _ in range(2):
      d = '%s%s' % (self._new_tempdir(), os.sep)
      directories.append(d)

      files.append(self._create_temp_file(dir=d))
      files.append(self._create_temp_file(dir=d))

    with TestPipeline() as p:
      files_pc = (
          p
          | beam.Create([FileSystems.join(d, '*') for d in directories])
          | fileio.MatchAll()
          | beam.Map(lambda x: x.path))

      assert_that(files_pc, equal_to(files))

  def test_match_files_one_directory_failure1(self):
    directories = [
        '%s%s' % (self._new_tempdir(), os.sep),
        '%s%s' % (self._new_tempdir(), os.sep)
    ]

    files = []
    files.append(self._create_temp_file(dir=directories[0]))
    files.append(self._create_temp_file(dir=directories[0]))

    with self.assertRaises(beam.io.filesystem.BeamIOError):
      with TestPipeline() as p:
        files_pc = (
            p
            | beam.Create([FileSystems.join(d, '*') for d in directories])
            | fileio.MatchAll(fileio.EmptyMatchTreatment.DISALLOW)
            | beam.Map(lambda x: x.path))

        assert_that(files_pc, equal_to(files))

  def test_match_files_one_directory_failure2(self):
    directories = [
        '%s%s' % (self._new_tempdir(), os.sep),
        '%s%s' % (self._new_tempdir(), os.sep)
    ]

    files = []
    files.append(self._create_temp_file(dir=directories[0]))
    files.append(self._create_temp_file(dir=directories[0]))

    with TestPipeline() as p:
      files_pc = (
          p
          | beam.Create([FileSystems.join(d, '*') for d in directories])
          | fileio.MatchAll(fileio.EmptyMatchTreatment.ALLOW_IF_WILDCARD)
          | beam.Map(lambda x: x.path))

      assert_that(files_pc, equal_to(files))


class ReadTest(_TestCaseWithTempDirCleanUp):
  def test_basic_file_name_provided(self):
    content = 'TestingMyContent\nIn multiple lines\nhaha!'
    dir = '%s%s' % (self._new_tempdir(), os.sep)
    self._create_temp_file(dir=dir, content=content)

    with TestPipeline() as p:
      content_pc = (
          p
          | beam.Create([FileSystems.join(dir, '*')])
          | fileio.MatchAll()
          | fileio.ReadMatches()
          | beam.FlatMap(lambda f: f.read().decode('utf-8').splitlines()))

      assert_that(content_pc, equal_to(content.splitlines()))

  def test_csv_file_source(self):
    content = 'name,year,place\ngoogle,1999,CA\nspotify,2006,sweden'
    rows = [r.split(',') for r in content.split('\n')]

    dir = '%s%s' % (self._new_tempdir(), os.sep)
    self._create_temp_file(dir=dir, content=content)

    with TestPipeline() as p:
      content_pc = (
          p
          | beam.Create([FileSystems.join(dir, '*')])
          | fileio.MatchAll()
          | fileio.ReadMatches()
          | beam.FlatMap(lambda rf: csv.reader(_get_file_reader(rf))))

      assert_that(content_pc, equal_to(rows))

  def test_infer_compressed_file(self):
    dir = '%s%s' % (self._new_tempdir(), os.sep)

    file_contents = b'compressed_contents!'
    import gzip
    with gzip.GzipFile(os.path.join(dir, 'compressed.gz'), 'w') as f:
      f.write(file_contents)

    file_contents2 = b'compressed_contents_bz2!'
    import bz2
    with bz2.BZ2File(os.path.join(dir, 'compressed2.bz2'), 'w') as f:
      f.write(file_contents2)

    with TestPipeline() as p:
      content_pc = (
          p
          | beam.Create([FileSystems.join(dir, '*')])
          | fileio.MatchAll()
          | fileio.ReadMatches()
          | beam.Map(lambda rf: rf.open().readline()))

      assert_that(content_pc, equal_to([file_contents, file_contents2]))

  def test_read_bz2_compressed_file_without_suffix(self):
    dir = '%s%s' % (self._new_tempdir(), os.sep)

    file_contents = b'compressed_contents!'
    import bz2
    with bz2.BZ2File(os.path.join(dir, 'compressed'), 'w') as f:
      f.write(file_contents)

    with TestPipeline() as p:
      content_pc = (
          p
          | beam.Create([FileSystems.join(dir, '*')])
          | fileio.MatchAll()
          | fileio.ReadMatches()
          | beam.Map(
              lambda rf: rf.open(compression_type=CompressionTypes.BZIP2).read(
                  len(file_contents))))

      assert_that(content_pc, equal_to([file_contents]))

  def test_read_gzip_compressed_file_without_suffix(self):
    dir = '%s%s' % (self._new_tempdir(), os.sep)

    file_contents = b'compressed_contents!'
    import gzip
    with gzip.GzipFile(os.path.join(dir, 'compressed'), 'w') as f:
      f.write(file_contents)

    with TestPipeline() as p:
      content_pc = (
          p
          | beam.Create([FileSystems.join(dir, '*')])
          | fileio.MatchAll()
          | fileio.ReadMatches()
          | beam.Map(
              lambda rf: rf.open(compression_type=CompressionTypes.GZIP).read(
                  len(file_contents))))

      assert_that(content_pc, equal_to([file_contents]))

  def test_string_filenames_and_skip_directory(self):
    content = 'thecontent\n'
    files = []
    tempdir = '%s%s' % (self._new_tempdir(), os.sep)

    files.append(self._create_temp_file(dir=tempdir, content=content))
    files.append(self._create_temp_file(dir=tempdir, content=content))

    with TestPipeline() as p:
      contents_pc = (
          p
          | beam.Create(files + ['%s/' % tempdir])
          | fileio.ReadMatches()
          | beam.FlatMap(lambda x: x.read().decode('utf-8').splitlines()))

      assert_that(contents_pc, equal_to(content.splitlines() * 2))

  def test_fail_on_directories(self):
    content = 'thecontent\n'
    files = []
    tempdir = '%s%s' % (self._new_tempdir(), os.sep)

    files.append(self._create_temp_file(dir=tempdir, content=content))
    files.append(self._create_temp_file(dir=tempdir, content=content))

    with self.assertRaises(beam.io.filesystem.BeamIOError):
      with TestPipeline() as p:
        _ = (
            p
            | beam.Create(files + ['%s/' % tempdir])
            | fileio.ReadMatches(skip_directories=False)
            | beam.Map(lambda x: x.read_utf8()))


class MatchIntegrationTest(unittest.TestCase):

  INPUT_FILE = 'gs://dataflow-samples/shakespeare/kinglear.txt'
  KINGLEAR_CHECKSUM = 'f418b25f1507f5a901257026b035ac2857a7ab87'
  INPUT_FILE_LARGE = (
      'gs://dataflow-samples/wikipedia_edits/wiki_data-00000000000*.json')

  WIKI_FILES = [
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000000.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000001.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000002.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000003.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000004.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000005.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000006.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000007.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000008.json',
      'gs://dataflow-samples/wikipedia_edits/wiki_data-000000000009.json',
  ]

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)

  @pytest.mark.it_postcommit
  def test_transform_on_gcs(self):
    args = self.test_pipeline.get_full_options_as_args()

    with beam.Pipeline(argv=args) as p:
      matches_pc = (
          p
          | beam.Create([self.INPUT_FILE, self.INPUT_FILE_LARGE])
          | fileio.MatchAll()
          | 'GetPath' >> beam.Map(lambda metadata: metadata.path))

      assert_that(
          matches_pc,
          equal_to([self.INPUT_FILE] + self.WIKI_FILES),
          label='Matched Files')

      checksum_pc = (
          p
          | 'SingleFile' >> beam.Create([self.INPUT_FILE])
          | 'MatchOneAll' >> fileio.MatchAll()
          | fileio.ReadMatches()
          | 'ReadIn' >> beam.Map(lambda x: x.read_utf8().split('\n'))
          | 'Checksums' >> beam.Map(compute_hash))

      assert_that(
          checksum_pc,
          equal_to([self.KINGLEAR_CHECKSUM]),
          label='Assert Checksums')


class MatchContinuouslyTest(_TestCaseWithTempDirCleanUp):
  def test_with_deduplication(self):
    files = []
    tempdir = '%s%s' % (self._new_tempdir(), os.sep)

    files.append(self._create_temp_file(dir=tempdir))
    files.append(FileSystems.join(tempdir, 'extra'))

    interval = 0.2
    start = Timestamp.now()
    stop = start + interval + 0.1

    def _create_extra_file(element):
      writer = FileSystems.create(FileSystems.join(tempdir, 'extra'))
      writer.close()
      return element.path

    with TestPipeline() as p:
      match_continiously = (
          p
          | fileio.MatchContinuously(
              file_pattern=FileSystems.join(tempdir, '*'),
              interval=interval,
              start_timestamp=start,
              stop_timestamp=stop)
          | beam.Map(_create_extra_file))

      assert_that(match_continiously, equal_to(files))

  def test_without_deduplication(self):
    interval = 0.2
    start = Timestamp.now()
    stop = start + interval + 0.1

    files = []
    tempdir = '%s%s' % (self._new_tempdir(), os.sep)

    file = self._create_temp_file(dir=tempdir)
    files += [file, file]
    files.append(FileSystems.join(tempdir, 'extra'))

    def _create_extra_file(element):
      writer = FileSystems.create(FileSystems.join(tempdir, 'extra'))
      writer.close()
      return element.path

    with TestPipeline() as p:
      match_continiously = (
          p
          | fileio.MatchContinuously(
              file_pattern=FileSystems.join(tempdir, '*'),
              interval=interval,
              has_deduplication=False,
              start_timestamp=start,
              stop_timestamp=stop)
          | beam.Map(_create_extra_file))

      assert_that(match_continiously, equal_to(files))

  def test_match_updated_files(self):
    files = []
    tempdir = '%s%s' % (self._new_tempdir(), os.sep)

    def _create_extra_file(element):
      writer = FileSystems.create(FileSystems.join(tempdir, 'extra'))
      writer.close()
      return element.path

    files.append(self._create_temp_file(dir=tempdir))
    writer = FileSystems.create(FileSystems.join(tempdir, 'extra'))
    writer.close()

    files.append(FileSystems.join(tempdir, 'extra'))
    files.append(FileSystems.join(tempdir, 'extra'))

    interval = 0.2
    start = Timestamp.now()
    stop = start + interval + 0.1

    with TestPipeline() as p:
      match_continiously = (
          p
          | fileio.MatchContinuously(
              file_pattern=FileSystems.join(tempdir, '*'),
              interval=interval,
              start_timestamp=start,
              stop_timestamp=stop,
              match_updated_files=True)
          | beam.Map(_create_extra_file))

      assert_that(match_continiously, equal_to(files))


class WriteFilesTest(_TestCaseWithTempDirCleanUp):

  SIMPLE_COLLECTION = [
      {
          'project': 'beam', 'foundation': 'apache'
      },
      {
          'project': 'prometheus', 'foundation': 'cncf'
      },
      {
          'project': 'flink', 'foundation': 'apache'
      },
      {
          'project': 'grpc', 'foundation': 'cncf'
      },
      {
          'project': 'spark', 'foundation': 'apache'
      },
      {
          'project': 'kubernetes', 'foundation': 'cncf'
      },
      {
          'project': 'spark', 'foundation': 'apache'
      },
      {
          'project': 'knative', 'foundation': 'cncf'
      },
      {
          'project': 'linux', 'foundation': 'linux'
      },
  ]

  LARGER_COLLECTION = ['{:05d}'.format(i) for i in range(200)]

  CSV_HEADERS = ['project', 'foundation']

  SIMPLE_COLLECTION_VALIDATION_SET = {(elm['project'], elm['foundation'])
                                      for elm in SIMPLE_COLLECTION}

  class CsvSink(fileio.TextSink):
    def __init__(self, headers):
      self.headers = headers

    def write(self, record):
      self._fh.write(','.join([record[h] for h in self.headers]).encode('utf8'))
      self._fh.write('\n'.encode('utf8'))

  class JsonSink(fileio.TextSink):
    def write(self, record):
      self._fh.write(json.dumps(record).encode('utf8'))
      self._fh.write('\n'.encode('utf8'))

  def test_write_to_single_file_batch(self):

    dir = self._new_tempdir()

    with TestPipeline() as p:
      _ = (
          p
          | beam.Create(WriteFilesTest.SIMPLE_COLLECTION)
          | "Serialize" >> beam.Map(json.dumps)
          | beam.io.fileio.WriteToFiles(path=dir))

    with TestPipeline() as p:
      result = (
          p
          | fileio.MatchFiles(FileSystems.join(dir, '*'))
          | fileio.ReadMatches()
          | beam.FlatMap(lambda f: f.read_utf8().strip().split('\n'))
          | beam.Map(json.loads))

      assert_that(result, equal_to([row for row in self.SIMPLE_COLLECTION]))



  @unittest.skip('https://github.com/apache/beam/issues/21269')
  def test_find_orphaned_files(self):
    dir = self._new_tempdir()

    write_transform = beam.io.fileio.WriteToFiles(path=dir)

    def write_orphaned_file(temp_dir, writer_key):
      temp_dir_path = FileSystems.join(dir, temp_dir)

      file_prefix_dir = FileSystems.join(
          temp_dir_path, str(abs(hash(writer_key))))

      file_name = '%s_%s' % (file_prefix_dir, uuid.uuid4())
      with FileSystems.create(file_name) as f:
        f.write(b'Hello y\'all')

      return file_name

    with TestPipeline() as p:
      _ = (
          p
          | beam.Create(WriteFilesTest.SIMPLE_COLLECTION)
          | "Serialize" >> beam.Map(json.dumps)
          | write_transform)

      temp_dir_path = FileSystems.mkdirs(
          FileSystems.join(dir, write_transform._temp_directory.get()))
      write_orphaned_file(
          write_transform._temp_directory.get(), (None, GlobalWindow()))
      f2 = write_orphaned_file(
          write_transform._temp_directory.get(), ('other-dest', GlobalWindow()))

    temp_dir_path = FileSystems.join(dir, write_transform._temp_directory.get())
    leftovers = FileSystems.match(['%s%s*' % (temp_dir_path, os.sep)])
    found_files = [m.path for m in leftovers[0].metadata_list]
    self.assertListEqual(found_files, [f2])


  def record_dofn(self):
    class RecordDoFn(beam.DoFn):
      def process(self, element):
        WriteFilesTest.all_records.append(element)

    return RecordDoFn()



  def test_shard_naming(self):
    namer = fileio.default_file_naming(prefix='/path/to/file', suffix='.txt')
    self.assertEqual(
        namer(GlobalWindow(), None, None, None, None, None),
        '/path/to/file.txt')
    self.assertEqual(
        namer(GlobalWindow(), None, 1, 5, None, None),
        '/path/to/file-00001-of-00005.txt')
    self.assertEqual(
        namer(GlobalWindow(), None, 1, 5, 'gz', None),
        '/path/to/file-00001-of-00005.txt.gz')
    self.assertEqual(
        namer(IntervalWindow(0, 100), None, 1, 5, None, None),
        '/path/to/file'
        '-1970-01-01T00:00:00-1970-01-01T00:01:40-00001-of-00005.txt')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
