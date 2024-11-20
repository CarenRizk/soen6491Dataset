from apache_beam import Create
from apache_beam import ParDo
from apache_beam import Reshuffle
from apache_beam.pvalue import PBegin
from apache_beam.transforms import PTransform


class SpannerIO:

    DEFAULT_BATCH_SIZE_BYTES = 1024 * 1024  # 1MB
    DEFAULT_MAX_NUM_MUTATIONS = 5000
    DEFAULT_MAX_NUM_ROWS = 500
    DEFAULT_GROUPING_FACTOR = 1000
    METRICS_CACHE_SIZE = 100

    @staticmethod
    def read():
        return Read()

    @staticmethod
    def readAll():
        return ReadAll()

    @staticmethod
    def createTransaction():
        return CreateTransaction()

    @staticmethod
    def write():
        return Write()

    @staticmethod
    def readChangeStream():
        return ReadChangeStream()

class ReadAll(PTransform):

    def __init__(self):
        self.spanner_config = SpannerConfig.create()
        self.timestamp_bound = TimestampBound.strong()
        self.batching = True

    def withSpannerConfig(self, spannerConfig):
        self.spanner_config = spannerConfig
        return self

    def withTransaction(self, transaction):
        self.transaction = transaction
        return self

    def withTimestampBound(self, timestampBound):
        self.timestamp_bound = timestampBound
        return self

    def withBatching(self, batching):
        self.batching = batching
        return self

    def expand(self, input):
        if input.isBounded() == PCollection.IsBounded.UNBOUNDED:
            LOG.warn("SpannerIO.ReadAll({}) is being applied to an unbounded input. "
                      "This is not supported and can lead to runtime failures.", self.getName())

        readTransform = BatchSpannerRead.create(self.spanner_config, self.transaction, self.timestamp_bound) if self.batching else NaiveSpannerRead.create(self.spanner_config, self.transaction, self.timestamp_bound)
        return input.apply("Reshuffle", Reshuffle.viaRandomKey()).apply("Read from Cloud Spanner", readTransform)

class Read(PTransform):

    def __init__(self):
        self.spanner_config = SpannerConfig.create()
        self.read_operation = ReadOperation.create()
        self.timestamp_bound = None
        self.transaction = None
        self.partition_options = None
        self.batching = False
        self.type_descriptor = None
        self.toBeamRowFn = None
        self.fromBeamRowFn = None

    def withSpannerConfig(self, spannerConfig):
        self.spanner_config = spannerConfig
        return self

    def withReadOperation(self, operation):
        self.read_operation = operation
        return self

    def withTimestampBound(self, timestampBound):
        self.timestamp_bound = timestampBound
        return self

    def withTransaction(self, transaction):
        self.transaction = transaction
        return self

    def withBatching(self, batching):
        self.batching = batching
        return self

    def expand(self, input):
        self.spanner_config.validate()
        if self.timestamp_bound is None:
            raise ValueError("SpannerIO.read() requires timestamp to be set with withTimestampBound or withTimestamp method")

        if self.read_operation.query is not None and self.read_operation.table is not None:
            raise ValueError("Both query and table cannot be specified at the same time for SpannerIO.read().")

        if self.read_operation.table is not None and not self.read_operation.columns:
            raise ValueError("For a read operation SpannerIO.read() requires a list of columns to set with withColumns method")

        readAll = ReadAll().withSpannerConfig(self.spanner_config).withTimestampBound(self.timestamp_bound).withBatching(self.batching).withTransaction(self.transaction)
        rows = input.apply(Create.of(self.read_operation)).apply("Execute query", readAll)

        return rows

class CreateTransaction(PTransform):

    def __init__(self):
        self.spanner_config = SpannerConfig.create()
        self.timestamp_bound = None

    def withSpannerConfig(self, spannerConfig):
        self.spanner_config = spannerConfig
        return self

    def withTimestampBound(self, timestampBound):
        self.timestamp_bound = timestampBound
        return self

    def expand(self, input):
        self.spanner_config.validate()
        collection = input.getPipeline().apply(Create.of(1))

        if isinstance(input, PCollection):
            collection = collection.apply(Wait.on(input))
        elif not isinstance(input, PBegin):
            raise RuntimeError("input must be PBegin or PCollection")

        return collection.apply("Create transaction", ParDo.of(CreateTransactionFn(self.spanner_config, self.timestamp_bound))).apply("As PCollectionView", View.asSingleton())

class Write(PTransform):

    def __init__(self):
        self.spanner_config = SpannerConfig.create()
        self.batch_size_bytes = SpannerIO.DEFAULT_BATCH_SIZE_BYTES
        self.max_num_mutations = SpannerIO.DEFAULT_MAX_NUM_MUTATIONS
        self.max_num_rows = SpannerIO.DEFAULT_MAX_NUM_ROWS
        self.failure_mode = FailureMode.FAIL_FAST

    def withSpannerConfig(self, spannerConfig):
        self.spanner_config = spannerConfig
        return self

    def withBatchSizeBytes(self, batchSizeBytes):
        self.batch_size_bytes = batchSizeBytes
        return self

    def expand(self, input):
        self.spanner_config.validate()
        return input.apply("To mutation group", ParDo.of(ToMutationGroupFn())).apply("Write mutations to Cloud Spanner", WriteGrouped(self))

class ReadChangeStream(PTransform):

    def __init__(self):
        self.spanner_config = SpannerConfig.create()
        self.change_stream_name = DEFAULT_CHANGE_STREAM_NAME
        self.inclusive_start_at = DEFAULT_INCLUSIVE_START_AT
        self.inclusive_end_at = DEFAULT_INCLUSIVE_END_AT

    def withSpannerConfig(self, spannerConfig):
        self.spanner_config = spannerConfig
        return self

    def withChangeStreamName(self, changeStreamName):
        self.change_stream_name = changeStreamName
        return self

    def expand(self, input):
        # Implementation for reading change streams
        pass

# Placeholder for SpannerConfig, ReadOperation, CreateTransactionFn, ToMutationGroupFn, WriteGrouped, FailureMode, and other dependencies.