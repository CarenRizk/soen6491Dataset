from apache_beam.pipeline import PTransformOverride
import logging
import time
from typing import List

from apache_beam.pipeline import PTransformOverride
from apache_beam.runners.dataflow.ptransform_overrides import NativeReadPTransformOverride
from apache_beam.runners.runner import PipelineRunner
from apache_beam.runners.runner import PipelineState


# Placeholder for Java imports

class DataflowRunner(PipelineRunner):
    """A runner that creates job graphs and submits them for remote execution.

    Every execution of the run() method will submit an independent job for
    remote execution that consists of the nodes reachable from the passed-in
    node argument or entire graph if the node is None. The run() method returns
    after the service creates the job, and the job status is reported as RUNNING.
    """

    _PTRANSFORM_OVERRIDES = [
        NativeReadPTransformOverride(),
    ]  # type: List[PTransformOverride]

    def __init__(self, cache=None):
        self._default_environment = None

    def is_fnapi_compatible(self):
        return False

    @staticmethod
    def poll_for_job_completion(
            runner, result, duration, state_update_callback=None):
        """Polls for the specified job to finish running (successfully or not).

        Updates the result with the new job information before returning.

        Args:
          runner: DataflowRunner instance to use for polling job state.
          result: DataflowPipelineResult instance used for job information.
          duration (int): The time to wait (in milliseconds) for job to finish.
            If it is set to :data:`None`, it will wait indefinitely until the job
            is finished.
        """
        if result.state == PipelineState.DONE:
            return

        last_message_time = None
        current_seen_messages = set()

        last_error_rank = float('-inf')
        last_error_msg = None
        last_job_state = None
        final_countdown_timer_secs = 50.0
        sleep_secs = 5.0

        def rank_error(msg):
            if 'work item was attempted' in msg:
                return -1
            elif 'Traceback' in msg:
                return 1
            return 0

        if duration:
            start_secs = time.time()
            duration_secs = duration // 1000

        job_id = result.job_id()
        while True:
            response = runner.dataflow_client.get_job(job_id)
            if response.currentState is not None:
                if response.currentState != last_job_state:
                    if state_update_callback:
                        state_update_callback(response.currentState)
                    logging.info('Job %s is in state %s', job_id, response.currentState)
                    last_job_state = response.currentState
                if str(response.currentState) != 'JOB_STATE_RUNNING':
                    if (final_countdown_timer_secs <= 0.0 or last_error_msg is not None or
                            str(response.currentState) == 'JOB_STATE_DONE' or
                            str(response.currentState) == 'JOB_STATE_CANCELLED' or
                            str(response.currentState) == 'JOB_STATE_UPDATED' or
                            str(response.currentState) == 'JOB_STATE_DRAINED'):
                        break

                    if (str(response.currentState) not in ('JOB_STATE_PENDING',
                                                           'JOB_STATE_QUEUED')):
                        sleep_secs = 1.0  # poll faster during the final countdown
                        final_countdown_timer_secs -= sleep_secs

            time.sleep(sleep_secs)

            page_token = None
            while True:
                messages, page_token = runner.dataflow_client.list_messages(
                    job_id, page_token=page_token, start_time=last_message_time)
                for m in messages:
                    message = '%s: %s: %s' % (m.time, m.messageImportance, m.messageText)

                    if not last_message_time or m.time > last_message_time:
                        last_message_time = m.time
                        current_seen_messages = set()

                    if message in current_seen_messages:
                        continue
                    else:
                        current_seen_messages.add(message)
                    if m.messageImportance is None:
                        continue
                    message_importance = str(m.messageImportance)
                    if (message_importance == 'JOB_MESSAGE_DEBUG' or
                            message_importance == 'JOB_MESSAGE_DETAILED'):
                        logging.debug(message)
                    elif message_importance == 'JOB_MESSAGE_BASIC':
                        logging.info(message)
                    elif message_importance == 'JOB_MESSAGE_WARNING':
                        logging.warning(message)
                    elif message_importance == 'JOB_MESSAGE_ERROR':
                        logging.error(message)
                        if rank_error(m.messageText) >= last_error_rank:
                            last_error_rank = rank_error(m.messageText)
                            last_error_msg = m.messageText
                    else:
                        logging.info(message)
                if not page_token:
                    break

            if duration:
                passed_secs = time.time() - start_secs
                if passed_secs > duration_secs:
                    logging.warning(
                        'Timing out on waiting for job %s after %d seconds',
                        job_id,
                        passed_secs)
                    break

        result._job = response
        runner.last_error_msg = last_error_msg

    @staticmethod
    def _only_element(iterable):
        element, = iterable
        return element

    @staticmethod
    def side_input_visitor(deterministic_key_coders=True):
        from apache_beam.pipeline import PipelineVisitor
        from apache_beam.transforms.core import ParDo

        class SideInputVisitor(PipelineVisitor):
            """Ensures input `PCollection` used as a side inputs has a `KV` type.

            TODO(BEAM-115): Once Python SDK is compatible with the new Runner API,
            we could directly replace the coder instead of mutating the element type.
            """
            def visit_transform(self, transform_node):
                if isinstance(transform_node.transform, ParDo):
                    # Placeholder for additional logic
                    pass

    # Placeholder for additional methods and logic as needed.