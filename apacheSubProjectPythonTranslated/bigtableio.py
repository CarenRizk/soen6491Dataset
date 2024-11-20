import logging

import apache_beam as beam
from apache_beam.internal.metrics.metric import ServiceCallMetric
from apache_beam.io.gcp import resource_identifiers
from apache_beam.metrics import Metrics
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.metric import Lineage
from apache_beam.transforms import PTransform
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.transforms.external import BeamJarExpansionService
from google.cloud.bigtable import Client
from google.cloud.bigtable.batcher import MutationsBatcher

_LOGGER = logging.getLogger(__name__)

FLUSH_COUNT = 1000
MAX_ROW_BYTES = 5242880  # 5MB

class _BigTableWriteFn(beam.DoFn):
    """ Creates the connector can call and add_row to the batcher using each
    row in beam pipe line
    Args:
        project_id(str): GCP Project ID
        instance_id(str): GCP Instance ID
        table_id(str): GCP Table ID
    """
    def __init__(self, project_id, instance_id, table_id):
        """ Constructor of the Write connector of Bigtable
        Args:
            project_id(str): GCP Project of to write the Rows
            instance_id(str): GCP Instance to write the Rows
            table_id(str): GCP Table to write the `DirectRows`
        """
        super().__init__()
        self.beam_options = {
            'project_id': project_id,
            'instance_id': instance_id,
            'table_id': table_id
        }
        self.table = None
        self.batcher = None
        self.service_call_metric = None
        self.written = Metrics.counter(self.__class__, 'Written Row')

    def __getstate__(self):
        return self.beam_options

    def __setstate__(self, options):
        self.beam_options = options
        self.table = None
        self.batcher = None
        self.service_call_metric = None
        self.written = Metrics.counter(self.__class__, 'Written Row')

    def write_mutate_metrics(self, status_list):
        for status in status_list:
            code = status.code if status else None
            grpc_status_string = (
                ServiceCallMetric.bigtable_error_code_to_grpc_status_string(code))
            self.service_call_metric.call(grpc_status_string)

    def start_service_call_metrics(self, project_id, instance_id, table_id):
        resource = resource_identifiers.BigtableTable(
            project_id, instance_id, table_id)
        labels = {
            monitoring_infos.SERVICE_LABEL: 'BigTable',
            monitoring_infos.METHOD_LABEL: 'google.bigtable.v2.MutateRows',
            monitoring_infos.RESOURCE_LABEL: resource,
            monitoring_infos.BIGTABLE_PROJECT_ID_LABEL: (
                self.beam_options['project_id']),
            monitoring_infos.INSTANCE_ID_LABEL: self.beam_options['instance_id'],
            monitoring_infos.TABLE_ID_LABEL: self.beam_options['table_id']
        }
        return ServiceCallMetric(
            request_count_urn=monitoring_infos.API_REQUEST_COUNT_URN,
            base_labels=labels)

    def start_bundle(self):
        if self.table is None:
            client = Client(project=self.beam_options['project_id'])
            instance = client.instance(self.beam_options['instance_id'])
            self.table = instance.table(self.beam_options['table_id'])
        self.service_call_metric = self.start_service_call_metrics(
            self.beam_options['project_id'],
            self.beam_options['instance_id'],
            self.beam_options['table_id'])
        self.batcher = MutationsBatcher(
            self.table,
            batch_completed_callback=self.write_mutate_metrics,
            flush_count=FLUSH_COUNT,
            max_row_bytes=MAX_ROW_BYTES)

    def process(self, row):
        self.written.inc()
        self.batcher.mutate(row)

    def finish_bundle(self):
        if self.batcher:
            self.batcher.close()
            self.batcher = None
            Lineage.sinks().add(
                'bigtable',
                self.beam_options['project_id'],
                self.beam_options['instance_id'],
                self.beam_options['table_id'])

    def display_data(self):
        return {
            'projectId': DisplayDataItem(
                self.beam_options['project_id'], label='Bigtable Project Id'),
            'instanceId': DisplayDataItem(
                self.beam_options['instance_id'], label='Bigtable Instance Id'),
            'tableId': DisplayDataItem(
                self.beam_options['table_id'], label='Bigtable Table Id')
        }


class WriteToBigTable(beam.PTransform):
    """A transform that writes rows to a Bigtable table.

    Takes an input PCollection of `DirectRow` objects containing un-committed
    mutations. For more information about this row object, visit
    https://cloud.google.com/python/docs/reference/bigtable/latest/row#class-googlecloudbigtablerowdirectrowrowkey-tablenone

    If flag `use_cross_language` is set to true, this transform will use the
    multi-language transforms framework to inject the Java native write transform
    into the pipeline.
    """
    URN = "beam:schematransform:org.apache.beam:bigtable_write:v1"

    def __init__(
            self,
            project_id,
            instance_id,
            table_id,
            use_cross_language=False,
            expansion_service=None):
        """Initialize an WriteToBigTable transform.

        :param table_id:
          The ID of the table to write to.
        :param instance_id:
          The ID of the instance where the table resides.
        :param project_id:
          The GCP project ID.
        :param use_cross_language:
          If set to True, will use the Java native transform via cross-language.
        :param expansion_service:
          The address of the expansion service in the case of using cross-language.
          If no expansion service is provided, will attempt to run the default GCP
          expansion service.
        """
        super().__init__()
        self._table_id = table_id
        self._instance_id = instance_id
        self._project_id = project_id
        self._use_cross_language = use_cross_language
        if use_cross_language:
            self._expansion_service = (
                expansion_service or BeamJarExpansionService(
                    'sdks:java:io:google-cloud-platform:expansion-service:build'))