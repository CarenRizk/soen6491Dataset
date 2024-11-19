import abc
import contextlib
import logging
import os
import re
import shutil
import tempfile
import unicodedata
import uuid
from collections import defaultdict
from typing import TYPE_CHECKING
from typing import Any
from typing import Dict
from typing import FrozenSet
from typing import Iterable
from typing import List
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple
from typing import Type
from typing import Union

from google.protobuf import message

from apache_beam import pvalue
from apache_beam.internal import pickler
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import CrossLanguageOptions
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import TypeOptions
from apache_beam.options.pipeline_options_validator import PipelineOptionsValidator
from apache_beam.portability import common_urns
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.runners import PipelineRunner
from apache_beam.runners import common
from apache_beam.runners import create_runner
from apache_beam.transforms import ParDo
from apache_beam.transforms import ptransform
from apache_beam.transforms.display import DisplayData
from apache_beam.transforms.display import HasDisplayData
from apache_beam.transforms.resources import merge_resource_hints
from apache_beam.transforms.resources import resource_hints_from_options
from apache_beam.transforms.sideinputs import get_sideinput_index
from apache_beam.typehints import TypeCheckError
from apache_beam.typehints import typehints
from apache_beam.utils import proto_utils
from apache_beam.utils import subprocess_server
from apache_beam.utils.annotations import deprecated
from apache_beam.utils.interactive_utils import alter_label_if_ipython
from apache_beam.utils.interactive_utils import is_in_ipython

if TYPE_CHECKING:
  from types import TracebackType
  from apache_beam.runners.pipeline_context import PipelineContext
  from apache_beam.runners.runner import PipelineResult
  from apache_beam.transforms import environments

__all__ = ['Pipeline', 'PTransformOverride']


class Pipeline(HasDisplayData):
  """A pipeline object that manages a DAG of
  :class:`~apache_beam.pvalue.PValue` s and their
  :class:`~apache_beam.transforms.ptransform.PTransform` s.

  Conceptually the :class:`~apache_beam.pvalue.PValue` s are the DAG's nodes and
  the :class:`~apache_beam.transforms.ptransform.PTransform` s computing
  the :class:`~apache_beam.pvalue.PValue` s are the edges.

  All the transforms applied to the pipeline must have distinct full labels.
  If same transform instance needs to be applied then the right shift operator
  should be used to designate new names
  (e.g. ``input | "label" >> my_transform``).
  """
  @classmethod
  def runner_implemented_transforms(cls):

    return frozenset([
        common_urns.primitives.GROUP_BY_KEY.urn,
        common_urns.primitives.IMPULSE.urn,
    ])

  def __init__(
      self,
      runner: Optional[Union[str, PipelineRunner]] = None,
      options: Optional[PipelineOptions] = None,
      argv: Optional[List[str]] = None,
      display_data: Optional[Dict[str, Any]] = None):
    """Initialize a pipeline object.

    Args:
      runner (~apache_beam.runners.runner.PipelineRunner): An object of
        type :class:`~apache_beam.runners.runner.PipelineRunner` that will be
        used to execute the pipeline. For registered runners, the runner name
        can be specified, otherwise a runner object must be supplied.
      options (~apache_beam.options.pipeline_options.PipelineOptions):
        A configured
        :class:`~apache_beam.options.pipeline_options.PipelineOptions` object
        containing arguments that should be used for running the Beam job.
      argv (List[str]): a list of arguments (such as :data:`sys.argv`)
        to be used for building a
        :class:`~apache_beam.options.pipeline_options.PipelineOptions` object.
        This will only be used if argument **options** is :data:`None`.
      display_data (Dict[str, Any]): a dictionary of static data associated
        with this pipeline that can be displayed when it runs.

    Raises:
      ValueError: if either the runner or options argument is not
        of the expected type.
    """
    logging.basicConfig()

    if options is not None:
      if isinstance(options, PipelineOptions):
        self._options = options
      else:
        raise ValueError(
            'Parameter options, if specified, must be of type PipelineOptions. '
            'Received : %r' % options)
    elif argv is not None:
      if isinstance(argv, list):
        self._options = PipelineOptions(argv)
      else:
        raise ValueError(
            'Parameter argv, if specified, must be a list. Received : %r' %
            argv)
    else:
      self._options = PipelineOptions([])

    FileSystems.set_options(self._options)

    pickle_library = self._options.view_as(SetupOptions).pickle_library
    pickler.set_library(pickle_library)

    if runner is None:
      runner = self._options.view_as(StandardOptions).runner
      if runner is None:
        runner = StandardOptions.DEFAULT_RUNNER
        logging.info((
            'Missing pipeline option (runner). Executing pipeline '
            'using the default runner: %s.'),
                     runner)

    if isinstance(runner, str):
      runner = create_runner(runner)
    elif not isinstance(runner, PipelineRunner):
      raise TypeError(
          'Runner %s is not a PipelineRunner object or the '
          'name of a registered runner.' % runner)

    errors = PipelineOptionsValidator(self._options, runner).validate()
    if errors:
      raise ValueError(
          'Pipeline has validations errors: \n' + '\n'.join(errors))

    if runner.is_fnapi_compatible():
      experiments = (self._options.view_as(DebugOptions).experiments or [])
      if not 'beam_fn_api' in experiments:
        experiments.append('beam_fn_api')
        self._options.view_as(DebugOptions).experiments = experiments

    self.local_tempdir = tempfile.mkdtemp(prefix='beam-pipeline-temp')

    self.runner = runner
    self.transforms_stack = [AppliedPTransform(None, None, '', None)]
    self.applied_labels = set() 
    self._root_transform().resource_hints = resource_hints_from_options(options)
    self.component_id_map = ComponentIdMap()

    self.contains_external_transforms = False

    self._display_data = display_data or {}
    self._error_handlers = []

  def display_data(self):
    return self._display_data

  @property
  def options(self):
    return self._options

  @property
  def allow_unsafe_triggers(self):
    return self._options.view_as(TypeOptions).allow_unsafe_triggers

  def _register_error_handler(self, error_handler):
    self._error_handlers.append(error_handler)

  def _current_transform(self):

    """Returns the transform currently on the top of the stack."""
    return self.transforms_stack[-1]

  def _root_transform(self):

    """Returns the root transform of the transform stack."""
    return self.transforms_stack[0]

  def _remove_labels_recursively(self, applied_transform):
    for part in applied_transform.parts:
      if part.full_label in self.applied_labels:
        self.applied_labels.remove(part.full_label)
        self._remove_labels_recursively(part)

  def _replace(self, override):
    assert isinstance(override, PTransformOverride)

    output_map = {}  # type: Dict[pvalue.PValue, pvalue.PValue]
    output_replacements = {
    }  # type: Dict[AppliedPTransform, List[Tuple[pvalue.PValue, Optional[str]]]]
    input_replacements = {
    }  # type: Dict[AppliedPTransform, Mapping[str, Union[pvalue.PBegin, pvalue.PCollection]]]
    side_input_replacements = {
    }  # type: Dict[AppliedPTransform, List[pvalue.AsSideInput]]

    class TransformUpdater(PipelineVisitor):  # pylint: disable=used-before-assignment
      """"A visitor that replaces the matching PTransforms."""
      def __init__(self, pipeline):
        self.pipeline = pipeline

      def _replace_if_needed(self, original_transform_node):
        if override.matches(original_transform_node):
          assert isinstance(original_transform_node, AppliedPTransform)
          replacement_transform = (
              override.get_replacement_transform_for_applied_ptransform(
                  original_transform_node))
          if replacement_transform is original_transform_node.transform:
            return
          replacement_transform.side_inputs = tuple(
              original_transform_node.transform.side_inputs)

          replacement_transform_node = AppliedPTransform(
              original_transform_node.parent,
              replacement_transform,
              original_transform_node.full_label,
              original_transform_node.main_inputs)

          replacement_transform_node.resource_hints = (
              original_transform_node.resource_hints)

          if original_transform_node.parent:
            assert isinstance(original_transform_node.parent, AppliedPTransform)
            parent_parts = original_transform_node.parent.parts
            parent_parts[parent_parts.index(original_transform_node)] = (
                replacement_transform_node)
          else:
            roots = self.pipeline.transforms_stack[0].parts
            assert original_transform_node in roots
            roots[roots.index(original_transform_node)] = (
                replacement_transform_node)

          inputs = override.get_replacement_inputs(original_transform_node)
          if len(inputs) > 1:
            transform_input = inputs
          elif len(inputs) == 1:
            transform_input = inputs[0]
          elif len(inputs) == 0:
            transform_input = pvalue.PBegin(self.pipeline)
          try:
            self.pipeline.transforms_stack.append(replacement_transform_node)
            self.pipeline._remove_labels_recursively(original_transform_node)

            new_output = replacement_transform.expand(transform_input)
            assert isinstance(
                new_output, (dict, pvalue.PValue, pvalue.DoOutputsTuple))

            if isinstance(new_output, pvalue.PValue):
              new_output.element_type = None
              self.pipeline._infer_result_type(
                  replacement_transform, inputs, new_output)

            if isinstance(new_output, dict):
              for new_tag, new_pcoll in new_output.items():
                replacement_transform_node.add_output(new_pcoll, new_tag)
            elif isinstance(new_output, pvalue.DoOutputsTuple):
              replacement_transform_node.add_output(
                  new_output, new_output._main_tag)
            else:
              replacement_transform_node.add_output(new_output, new_output.tag)

            if isinstance(new_output, pvalue.PValue):
              if not new_output.producer:
                new_output.producer = replacement_transform_node
              output_map[original_transform_node.outputs[new_output.tag]] = \
                  new_output
            elif isinstance(new_output, (pvalue.DoOutputsTuple, tuple)):
              for pcoll in new_output:
                if not pcoll.producer:
                  pcoll.producer = replacement_transform_node
                output_map[original_transform_node.outputs[pcoll.tag]] = pcoll
            elif isinstance(new_output, dict):
              for tag, pcoll in new_output.items():
                if not pcoll.producer:
                  pcoll.producer = replacement_transform_node
                output_map[original_transform_node.outputs[tag]] = pcoll
          finally:
            self.pipeline.transforms_stack.pop()

      def enter_composite_transform(self, transform_node):
        self._replace_if_needed(transform_node)

      def visit_transform(self, transform_node):
        self._replace_if_needed(transform_node)

    self.visit(TransformUpdater(self))

    for old, new in output_map.items():
      if new.element_type == typehints.Any:
        new.element_type = old.element_type

    class InputOutputUpdater(PipelineVisitor):  # pylint: disable=used-before-assignment
      """"A visitor that records input and output values to be replaced.

      Input and output values that should be updated are recorded in maps
      input_replacements and output_replacements respectively.

      We cannot update input and output values while visiting since that results
      in validation errors.
      """
      def __init__(self, pipeline):
        self.pipeline = pipeline

      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        replace_output = False
        for tag in transform_node.outputs:
          if transform_node.outputs[tag] in output_map:
            replace_output = True
            break

        replace_input = False
        for input in transform_node.inputs:
          if input in output_map:
            replace_input = True
            break

        replace_side_inputs = False
        for side_input in transform_node.side_inputs:
          if side_input.pvalue in output_map:
            replace_side_inputs = True
            break

        if replace_output:
          output_replacements[transform_node] = []
          for original, replacement in output_map.items():
            for tag, output in transform_node.outputs.items():
              if output == original:
                output_replacements[transform_node].append((tag, replacement))

        if replace_input:
          new_inputs = {
              tag: input if not input in output_map else output_map[input]
              for (tag, input) in transform_node.main_inputs.items()
          }
          input_replacements[transform_node] = new_inputs

        if replace_side_inputs:
          new_side_inputs = []
          for side_input in transform_node.side_inputs:
            if side_input.pvalue in output_map:
              side_input.pvalue = output_map[side_input.pvalue]
              new_side_inputs.append(side_input)
            else:
              new_side_inputs.append(side_input)
          side_input_replacements[transform_node] = new_side_inputs

    self.visit(InputOutputUpdater(self))

    for transform, output_replacement in output_replacements.items():
      for tag, output in output_replacement:
        transform.replace_output(output, tag=tag)

    for transform, input_replacement in input_replacements.items():
      transform.replace_inputs(input_replacement)

    for transform, side_input_replacement in side_input_replacements.items():
      transform.replace_side_inputs(side_input_replacement)

  def _check_replacement(self, override):
    class ReplacementValidator(PipelineVisitor):
      def visit_transform(self, transform_node):
        if override.matches(transform_node):
          raise RuntimeError(
              'Transform node %r was not replaced as expected.' %
              transform_node)

    self.visit(ReplacementValidator())

  def replace_all(self, replacements):

    """ Dynamically replaces PTransforms in the currently populated hierarchy.

    Currently this only works for replacements where input and output types
    are exactly the same.

    TODO: Update this to also work for transform overrides where input and
    output types are different.

    Args:
      replacements (List[~apache_beam.pipeline.PTransformOverride]): a list of
        :class:`~apache_beam.pipeline.PTransformOverride` objects.
    """
    for override in replacements:
      assert isinstance(override, PTransformOverride)
      self._replace(override)
    for override in replacements:
      self._check_replacement(override)

  def run(self, test_runner_api='AUTO'):

    """Runs the pipeline. Returns whatever our runner returns after running."""

    for error_handler in self._error_handlers:
      error_handler.verify_closed()

    self.contains_external_transforms = (
        ExternalTransformFinder.contains_external_transforms(self))

    try:
      if test_runner_api == 'AUTO':
        is_fnapi_compatible = self.runner.is_fnapi_compatible() or (
            self.runner.__class__.__name__ == 'SwitchingDirectRunner' and
            not self._options.view_as(StandardOptions).streaming)


        test_runner_api = (
            not is_fnapi_compatible and
            not self.contains_external_transforms and
            self.runner.__class__.__name__ != 'InteractiveRunner')

      if test_runner_api and self._verify_runner_api_compatible():
        return Pipeline.from_runner_api(
            self.to_runner_api(use_fake_coders=True),
            self.runner,
            self._options).run(False)

      if (self._options.view_as(TypeOptions).runtime_type_check and
          self._options.view_as(TypeOptions).performance_runtime_type_check):
        raise RuntimeError(
            'You cannot turn on runtime_type_check '
            'and performance_runtime_type_check simultaneously. '
            'Pick one or the other.')

      if self._options.view_as(TypeOptions).runtime_type_check:
        from apache_beam.typehints import typecheck
        self.visit(typecheck.TypeCheckVisitor())

      if self._options.view_as(TypeOptions).performance_runtime_type_check:
        from apache_beam.typehints import typecheck
        self.visit(typecheck.PerformanceTypeCheckVisitor())

      if self._options.view_as(SetupOptions).save_main_session:
        tmpdir = tempfile.mkdtemp()
        try:
          pickler.dump_session(os.path.join(tmpdir, 'main_session.pickle'))
        finally:
          shutil.rmtree(tmpdir)
      return self.runner.run_pipeline(self, self._options)
    finally:
      if not is_in_ipython():
        shutil.rmtree(self.local_tempdir, ignore_errors=True)

  def __enter__(self):
    self._extra_context = contextlib.ExitStack()
    self._extra_context.enter_context(
        subprocess_server.JavaJarServer.beam_services(
            self._options.view_as(CrossLanguageOptions).beam_services))
    self._extra_context.enter_context(
        subprocess_server.SubprocessServer.cache_subprocesses())
    return self

  def __exit__(
      self,
      exc_type,  # type: Optional[Type[BaseException]]
      exc_val,  # type: Optional[BaseException]
      exc_tb  # type: Optional[TracebackType]
  ):

    try:
      if not exc_type:
        self.result = self.run()
        if not self._options.view_as(StandardOptions).no_wait_until_finish:
          self.result.wait_until_finish()
        else:
          logging.info(
              'Job execution continues without waiting for completion.'
              ' Use "wait_until_finish" in PipelineResult to block'
              ' until finished.')
    finally:
      self._extra_context.__exit__(exc_type, exc_val, exc_tb)

  def visit(self, visitor):

    """Visits depth-first every node of a pipeline's DAG.

    Runner-internal implementation detail; no backwards-compatibility guarantees

    Args:
      visitor (~apache_beam.pipeline.PipelineVisitor):
        :class:`~apache_beam.pipeline.PipelineVisitor` object whose callbacks
        will be called for each node visited. See
        :class:`~apache_beam.pipeline.PipelineVisitor` comments.

    Raises:
      TypeError: if node is specified and is not a
        :class:`~apache_beam.pvalue.PValue`.
      ~apache_beam.error.PipelineError: if node is specified and does not
        belong to this pipeline instance.
    """

    visited = set()  # type: Set[pvalue.PValue]
    self._root_transform().visit(visitor, self, visited)

  def apply(
      self,
      transform,  # type: ptransform.PTransform
      pvalueish=None,  # type: Optional[pvalue.PValue]
      label=None  # type: Optional[str]
  ):

    """Applies a custom transform using the pvalueish specified.

    Args:
      transform (~apache_beam.transforms.ptransform.PTransform): the
        :class:`~apache_beam.transforms.ptransform.PTransform` to apply.
      pvalueish (~apache_beam.pvalue.PCollection): the input for the
        :class:`~apache_beam.transforms.ptransform.PTransform` (typically a
        :class:`~apache_beam.pvalue.PCollection`).
      label (str): label of the
        :class:`~apache_beam.transforms.ptransform.PTransform`.

    Raises:
      TypeError: if the transform object extracted from the
        argument list is not a
        :class:`~apache_beam.transforms.ptransform.PTransform`.
      RuntimeError: if the transform object was already applied to
        this pipeline and needs to be cloned in order to apply again.
    """
    if isinstance(transform, ptransform._NamedPTransform):
      return self.apply(
          transform.transform, pvalueish, label or transform.label)

    if not isinstance(transform, ptransform.PTransform):
      raise TypeError("Expected a PTransform object, got %s" % transform)

    if label:
      old_label, transform.label = transform.label, label
      try:
        return self.apply(transform, pvalueish)
      finally:
        transform.label = old_label

    if self._current_transform() is self._root_transform():
      alter_label_if_ipython(transform, pvalueish)

    full_label = '/'.join(
        [self._current_transform().full_label, transform.label]).lstrip('/')
    if full_label in self.applied_labels:
      auto_unique_labels = self._options.view_as(
          StandardOptions).auto_unique_labels
      if auto_unique_labels:
        logging.warning(
            'Using --auto_unique_labels could cause data loss when '
            'updating a pipeline or reloading the job state. '
            'This is not recommended for streaming jobs.')
        unique_label = self._generate_unique_label(transform)
        return self.apply(transform, pvalueish, unique_label)
      else:
        raise RuntimeError(
            'A transform with label "%s" already exists in the pipeline. '
            'To apply a transform with a specified label, write '
            'pvalue | "label" >> transform or use the option '
            '"auto_unique_labels" to automatically generate unique '
            'transform labels. Note "auto_unique_labels" '
            'could cause data loss when updating a pipeline or '
            'reloading the job state. This is not recommended for '
            'streaming jobs.' % full_label)
    self.applied_labels.add(full_label)

    pvalueish, inputs = transform._extract_input_pvalues(pvalueish)
    try:
      if not isinstance(inputs, dict):
        inputs = {str(ix): input for (ix, input) in enumerate(inputs)}
    except TypeError:
      raise NotImplementedError(
          'Unable to extract PValue inputs from %s; either %s does not accept '
          'inputs of this format, or it does not properly override '
          '_extract_input_pvalues' % (pvalueish, transform))
    for t, leaf_input in inputs.items():
      if not isinstance(leaf_input, pvalue.PValue) or not isinstance(t, str):
        raise NotImplementedError(
            '%s does not properly override _extract_input_pvalues, '
            'returned %s from %s' % (transform, inputs, pvalueish))

    current = AppliedPTransform(
        self._current_transform(), transform, full_label, inputs)
    self._current_transform().add_part(current)

    try:
      self.transforms_stack.append(current)

      type_options = self._options.view_as(TypeOptions)
      if type_options.pipeline_type_check:
        transform.type_check_inputs(pvalueish)

      pvalueish_result = self.runner.apply(transform, pvalueish, self._options)

      if type_options is not None and type_options.pipeline_type_check:
        transform.type_check_outputs(pvalueish_result)

      for tag, result in ptransform.get_named_nested_pvalues(pvalueish_result):
        assert isinstance(result, (pvalue.PValue, pvalue.DoOutputsTuple))

        if result.producer is None:
          result.producer = current

        self._infer_result_type(transform, tuple(inputs.values()), result)

        assert isinstance(result.producer.inputs, tuple)
        if isinstance(result, pvalue.DoOutputsTuple):
          current.add_output(result, result._main_tag)
          continue

        base = tag
        counter = 0
        while tag in current.outputs:
          counter += 1
          tag = '%s_%d' % (base, counter)

        current.add_output(result, tag)

      if (type_options is not None and
          type_options.type_check_strictness == 'ALL_REQUIRED' and
          transform.get_type_hints().output_types is None):
        ptransform_name = '%s(%s)' % (transform.__class__.__name__, full_label)
        raise TypeCheckError(
            'Pipeline type checking is enabled, however no '
            'output type-hint was found for the '
            'PTransform %s' % ptransform_name)
    finally:
      self.transforms_stack.pop()
    return pvalueish_result

  def _generate_unique_label(
      self,
      transform  # type: str
  ):

    """
    Given a transform, generate a unique label for it based on current label.
    """
    unique_suffix = uuid.uuid4().hex[:6]
    return '%s_%s' % (transform.label, unique_suffix)


  def _infer_result_type(
      self,
      transform,  # type: ptransform.PTransform
      inputs,  # type: Sequence[Union[pvalue.PBegin, pvalue.PCollection]]
      result_pcollection  # type: Union[pvalue.PValue, pvalue.DoOutputsTuple]
  ):
    type_options = self._options.view_as(TypeOptions)
    if type_options is None or not type_options.pipeline_type_check:
      return
    if (isinstance(result_pcollection, pvalue.PCollection) and
        (not result_pcollection.element_type
         or result_pcollection.element_type == typehints.Any)):
      input_element_types_tuple = tuple(i.element_type for i in inputs)
      input_element_type = (
          input_element_types_tuple[0] if len(input_element_types_tuple) == 1
          else typehints.Union[input_element_types_tuple])
      type_hints = transform.get_type_hints()
      declared_output_type = type_hints.simple_output_type(transform.label)
      if declared_output_type:
        input_types = type_hints.input_types
        if input_types and input_types[0]:
          declared_input_type = input_types[0][0]
          result_element_type = typehints.bind_type_variables(
              declared_output_type,
              typehints.match_type_variables(
                  declared_input_type, input_element_type))
        else:
          result_element_type = declared_output_type
      else:
        result_element_type = transform.infer_output_type(input_element_type)
      result_pcollection.element_type = typehints.bind_type_variables(
          result_element_type, {'*': typehints.Any})
    elif isinstance(result_pcollection, pvalue.DoOutputsTuple):
      for pcoll in result_pcollection:
        if pcoll.element_type is None:
          pcoll.element_type = typehints.Any

  def __reduce__(self):
    return str, ('Pickled pipeline stub.', )

  def _verify_runner_api_compatible(self):
    if self._options.view_as(TypeOptions).runtime_type_check:
      return False

    class Visitor(PipelineVisitor):  # pylint: disable=used-before-assignment
      ok = True  # Really a nonlocal.

      def enter_composite_transform(self, transform_node):
        pass

      def visit_transform(self, transform_node):
        try:
          pickler.loads(
              pickler.dumps(transform_node.transform, enable_trace=False),
              enable_trace=False)
        except Exception:
          Visitor.ok = False

      def visit_value(self, value, _):
        if isinstance(value, pvalue.PDone):
          Visitor.ok = False

    self.visit(Visitor())
    return Visitor.ok

  def to_runner_api(
      self,
      return_context=False,  # type: bool
      context=None,  # type: Optional[PipelineContext]
      use_fake_coders=False,  # type: bool
      default_environment=None  # type: Optional[environments.Environment]
  ):

    """For internal use only; no backwards-compatibility guarantees."""
    from apache_beam.runners import pipeline_context
    if context is None:
      context = pipeline_context.PipelineContext(
          use_fake_coders=use_fake_coders,
          component_id_map=self.component_id_map,
          default_environment=default_environment)
    elif default_environment is not None:
      raise ValueError(
          'Only one of context or default_environment may be specified.')

    deterministic_key_coders = not self._options.view_as(
        TypeOptions).allow_non_deterministic_key_coders

    class ForceKvInputTypes(PipelineVisitor):
      def enter_composite_transform(self, transform_node):
        self.visit_transform(transform_node)

      def visit_transform(self, transform_node):
        if not transform_node.transform:
          return
        if transform_node.transform.runner_api_requires_keyed_input():
          pcoll = transform_node.inputs[0]
          pcoll.element_type = typehints.coerce_to_kv_type(
              pcoll.element_type, transform_node.full_label)
          pcoll.requires_deterministic_key_coder = (
              deterministic_key_coders and transform_node.full_label)
          if len(transform_node.outputs) == 1:
            output, = transform_node.outputs.values()
            if not output.element_type:
              output.element_type = transform_node.transform.infer_output_type(
                  pcoll.element_type)
            if (isinstance(output.element_type,
                           typehints.TupleHint.TupleConstraint) and
                len(output.element_type.tuple_types) == 2 and
                pcoll.element_type.tuple_types[0] ==
                output.element_type.tuple_types[0]):
              output.requires_deterministic_key_coder = (
                  deterministic_key_coders and transform_node.full_label)
        for side_input in transform_node.transform.side_inputs:
          if side_input.requires_keyed_input():
            side_input.pvalue.element_type = typehints.coerce_to_kv_type(
                side_input.pvalue.element_type,
                transform_node.full_label,
                side_input_producer=side_input.pvalue.producer.full_label)
            side_input.pvalue.requires_deterministic_key_coder = (
                deterministic_key_coders and transform_node.full_label)

    self.visit(ForceKvInputTypes())

    root_transform_id = context.transforms.get_id(self._root_transform())
    proto = beam_runner_api_pb2.Pipeline(
        root_transform_ids=[root_transform_id],
        components=context.to_runner_api(),
        requirements=context.requirements(),
        display_data=DisplayData('', self._display_data).to_proto())
    proto.components.transforms[root_transform_id].unique_name = (
        root_transform_id)
    self.merge_compatible_environments(proto)
    if return_context:
      return proto, context  # type: ignore  # too complicated for now
    else:
      return proto

  @staticmethod
  def merge_compatible_environments(proto):
    """Tries to minimize the number of distinct environments by merging
    those that are compatible (currently defined as identical).

    Mutates proto as contexts may have references to proto.components.
    """
    common.merge_common_environments(proto, inplace=True)

  @staticmethod
  def from_runner_api(
      proto,  # type: beam_runner_api_pb2.Pipeline
      runner,  # type: PipelineRunner
      options,  # type: PipelineOptions
      return_context=False,  # type: bool
  ):

    """For internal use only; no backwards-compatibility guarantees."""
    p = Pipeline(
        runner=runner,
        options=options,
        display_data={str(ix): d
                      for ix, d in enumerate(proto.display_data)})
    from apache_beam.runners import pipeline_context
    context = pipeline_context.PipelineContext(
        proto.components, requirements=proto.requirements)
    if proto.root_transform_ids:
      root_transform_id, = proto.root_transform_ids
      p.transforms_stack = [context.transforms.get_by_id(root_transform_id)]
    else:
      p.transforms_stack = [AppliedPTransform(None, None, '', None)]
    p.applied_labels = {
        t.unique_name
        for t in proto.components.transforms.values()
    }
    for id in proto.components.pcollections:
      pcollection = context.pcollections.get_by_id(id)
      pcollection.pipeline = p
      if not pcollection.producer:
        raise ValueError('No producer for %s' % id)

    from apache_beam.io.iobase import Read
    from apache_beam.transforms.core import Create
    has_pbegin = [Read, Create]
    for id in proto.components.transforms:
      transform = context.transforms.get_by_id(id)
      if not transform.inputs and transform.transform.__class__ in has_pbegin:
        transform.main_inputs = {'None': pvalue.PBegin(p)}

    if return_context:
      return p, context  # type: ignore  # too complicated for now
    else:
      return p


class PipelineVisitor(object):
  """For internal use only; no backwards-compatibility guarantees.

  Visitor pattern class used to traverse a DAG of transforms
  (used internally by Pipeline for bookkeeping purposes).
  """
  def visit_value(self, value, producer_node):

    """Callback for visiting a PValue in the pipeline DAG.

    Args:
      value: PValue visited (typically a PCollection instance).
      producer_node: AppliedPTransform object whose transform produced the
        pvalue.
    """
    pass

  def visit_transform(self, transform_node):

    """Callback for visiting a transform leaf node in the pipeline DAG."""
    pass

  def enter_composite_transform(self, transform_node):

    """Callback for entering traversal of a composite transform node."""
    pass

  def leave_composite_transform(self, transform_node):

    """Callback for leaving traversal of a composite transform node."""
    pass


class ExternalTransformFinder(PipelineVisitor):
  """Looks for any external transforms in the pipeline and if found records
  it.
  """
  def __init__(self):
    self._contains_external_transforms = False

  @staticmethod
  def contains_external_transforms(pipeline):
    visitor = ExternalTransformFinder()
    pipeline.visit(visitor)
    return visitor._contains_external_transforms

  def _perform_exernal_transform_test(self, transform):
    if not transform:
      return
    from apache_beam.transforms import ExternalTransform
    if isinstance(transform, ExternalTransform):
      self._contains_external_transforms = True

  def visit_transform(self, transform_node):
    self._perform_exernal_transform_test(transform_node.transform)

  def enter_composite_transform(self, transform_node):
    self._perform_exernal_transform_test(transform_node.transform)


class AppliedPTransform(object):
  """For internal use only; no backwards-compatibility guarantees.

  A transform node representing an instance of applying a PTransform
  (used internally by Pipeline for bookeeping purposes).
  """
  def __init__(
      self,
      parent,  # type:  Optional[AppliedPTransform]
      transform,  # type: Optional[ptransform.PTransform]
      full_label,  # type: str
      main_inputs,  # type: Optional[Mapping[str, Union[pvalue.PBegin, pvalue.PCollection]]]
      environment_id=None,  # type: Optional[str]
      annotations=None, # type: Optional[Dict[str, bytes]]
  ):
    self.parent = parent
    self.transform = transform
    self.full_label = full_label
    self.main_inputs = dict(main_inputs or {})

    self.side_inputs = tuple() if transform is None else transform.side_inputs
    self.outputs = {}  # type: Dict[Union[str, int, None], pvalue.PValue]
    self.parts = []  # type: List[AppliedPTransform]
    self.environment_id = environment_id if environment_id else None  # type: Optional[str]
    self.resource_hints = dict(
        transform.get_resource_hints()) if transform else {
        }  # type: Dict[str, bytes]

    if annotations is None and transform:

      def annotation_to_bytes(key, a: Any) -> bytes:
        if isinstance(a, bytes):
          return a
        elif isinstance(a, str):
          return a.encode('ascii')
        elif isinstance(a, message.Message):
          return a.SerializeToString()
        else:
          raise TypeError(
              'Unknown annotation type %r (type %s) for %s' % (a, type(a), key))

      annotations = {
          key: annotation_to_bytes(key, a)
          for key,
          a in transform.annotations().items()
      }
    self.annotations = annotations

  @property
  def inputs(self):
    return tuple(self.main_inputs.values())

  def __repr__(self):
    return "%s(%s, %s)" % (
        self.__class__.__name__, self.full_label, type(self.transform).__name__)

  def replace_output(
      self,
      output,  # type: Union[pvalue.PValue, pvalue.DoOutputsTuple]
      tag=None  # type: Union[str, int, None]
  ):

    """Replaces the output defined by the given tag with the given output.

    Args:
      output: replacement output
      tag: tag of the output to be replaced.
    """
    if isinstance(output, pvalue.DoOutputsTuple):
      self.replace_output(output[output._main_tag])
    elif isinstance(output, pvalue.PValue):
      self.outputs[tag] = output
    elif isinstance(output, dict):
      for output_tag, out in output.items():
        self.outputs[output_tag] = out
    else:
      raise TypeError("Unexpected output type: %s" % output)

    from apache_beam.transforms import external
    if isinstance(self.transform, external.ExternalTransform):
      self.transform.replace_named_outputs(self.named_outputs())

  def replace_inputs(self, main_inputs):
    self.main_inputs = main_inputs

    from apache_beam.transforms import external
    if isinstance(self.transform, external.ExternalTransform):
      self.transform.replace_named_inputs(self.named_inputs())

  def replace_side_inputs(self, side_inputs):
    self.side_inputs = side_inputs

    from apache_beam.transforms import external
    if isinstance(self.transform, external.ExternalTransform):
      self.transform.replace_named_inputs(self.named_inputs())

  def add_output(
      self,
      output,  # type: Union[pvalue.DoOutputsTuple, pvalue.PValue]
      tag  # type: Union[str, int, None]
  ):
    if isinstance(output, pvalue.DoOutputsTuple):
      self.add_output(output[tag], tag)
    elif isinstance(output, pvalue.PValue):
      assert tag not in self.outputs
      self.outputs[tag] = output
    else:
      raise TypeError("Unexpected output type: %s" % output)

  def add_part(self, part):
    assert isinstance(part, AppliedPTransform)
    part._merge_outer_resource_hints()
    self.parts.append(part)

  def is_composite(self):

    """Returns whether this is a composite transform.

    A composite transform has parts (inner transforms) or isn't the
    producer for any of its outputs. (An example of a transform that
    is not a producer is one that returns its inputs instead.)
    """
    return bool(self.parts) or all(
        pval.producer is not self for pval in self.outputs.values())

  def visit(
      self,
      visitor,  # type: PipelineVisitor
      pipeline,  # type: Pipeline
      visited  # type: Set[pvalue.PValue]
  ):

    """Visits all nodes reachable from the current node."""

    for in_pval in self.inputs:
      if in_pval not in visited and not isinstance(in_pval, pvalue.PBegin):
        if in_pval.producer is not None:
          in_pval.producer.visit(visitor, pipeline, visited)
          assert in_pval in visited, in_pval

    for side_input in self.side_inputs:
      if isinstance(side_input, pvalue.AsSideInput) \
          and side_input.pvalue not in visited:
        pval = side_input.pvalue  # Unpack marker-object-wrapped pvalue.
        if pval.producer is not None:
          pval.producer.visit(visitor, pipeline, visited)
          assert pval in visited

    if self.is_composite():
      visitor.enter_composite_transform(self)
      for part in self.parts:
        part.visit(visitor, pipeline, visited)
      visitor.leave_composite_transform(self)
    else:
      visitor.visit_transform(self)

    for out_pval in self.outputs.values():
      if isinstance(out_pval, pvalue.DoOutputsTuple):
        pvals = (v for v in out_pval)
      else:
        pvals = (out_pval, )
      for v in pvals:
        if v not in visited:
          visited.add(v)
          visitor.visit_value(v, self)

  def named_inputs(self):
    if self.transform is None:
      assert not self.main_inputs and not self.side_inputs
      return {}
    else:
      named_inputs = self.transform._named_inputs(
          self.main_inputs, self.side_inputs)
      if not self.parts:
        for name, pc_out in self.outputs.items():
          if pc_out.producer is not self and pc_out not in named_inputs.values(
          ):
            named_inputs[f'__implicit_input_{name}'] = pc_out
      return named_inputs

  def named_outputs(self):
    if self.transform is None:
      assert not self.outputs
      return {}
    else:
      return self.transform._named_outputs(self.outputs)

  def to_runner_api(self, context):
    from apache_beam.transforms import external
    if isinstance(self.transform, external.ExternalTransform):
      return self.transform.to_runner_api_transform(context, self.full_label)

    def transform_to_runner_api(
        transform,  # type: Optional[ptransform.PTransform]
        context  # type: PipelineContext
    ):
      if transform is None:
        return None
      else:
        if isinstance(transform, ParDo):
          return transform.to_runner_api(
              context,
              has_parts=bool(self.parts),
              named_inputs=self.named_inputs())
        return transform.to_runner_api(context, has_parts=bool(self.parts))

    try:
      transform_spec = transform_to_runner_api(self.transform, context)
    except Exception as exn:
      raise RuntimeError(f'Unable to translate {self.full_label}') from exn
    environment_id = self.environment_id
    transform_urn = transform_spec.urn if transform_spec else None
    if (not environment_id and
        (transform_urn not in Pipeline.runner_implemented_transforms())):
      environment_id = context.get_environment_id_for_resource_hints(
          self.resource_hints)

    return beam_runner_api_pb2.PTransform(
        unique_name=self.full_label,
        spec=transform_spec,
        subtransforms=[
            context.transforms.get_id(part, label=part.full_label)
            for part in self.parts
        ],
        inputs={
            tag: context.pcollections.get_id(pc)
            for tag,
            pc in sorted(self.named_inputs().items())
        },
        outputs={
            tag: context.pcollections.get_id(out)
            for tag,
            out in sorted(self.named_outputs().items())
        },
        environment_id=environment_id,
        annotations=self.annotations,
        display_data=DisplayData.create_from(self.transform).to_proto()
        if self.transform else None)

  @staticmethod
  def from_runner_api(
      proto,  # type: beam_runner_api_pb2.PTransform
      context  # type: PipelineContext
  ):

    if common_urns.primitives.PAR_DO.urn == proto.spec.urn:
      pardo_payload = (
          proto_utils.parse_Bytes(
              proto.spec.payload, beam_runner_api_pb2.ParDoPayload))
      side_input_tags = list(pardo_payload.side_inputs.keys())
    else:
      pardo_payload = None
      side_input_tags = []

    main_inputs = {
        tag: context.pcollections.get_by_id(id)
        for (tag, id) in proto.inputs.items() if tag not in side_input_tags
    }

    transform = ptransform.PTransform.from_runner_api(proto, context)
    if transform and proto.environment_id:
      resource_hints = context.environments.get_by_id(
          proto.environment_id).resource_hints()
      if resource_hints:
        transform._resource_hints = dict(resource_hints)

    indexed_side_inputs = [
        (get_sideinput_index(tag), context.pcollections.get_by_id(id)) for tag,
        id in proto.inputs.items() if tag in side_input_tags
    ]
    side_inputs = [si for _, si in sorted(indexed_side_inputs)]

    result = AppliedPTransform(
        parent=None,
        transform=transform,
        full_label=proto.unique_name,
        main_inputs=main_inputs,
        environment_id=None,
        annotations=proto.annotations)

    if result.transform and result.transform.side_inputs:
      for si, pcoll in zip(result.transform.side_inputs, side_inputs):
        si.pvalue = pcoll
      result.side_inputs = tuple(result.transform.side_inputs)
    result.parts = []
    for transform_id in proto.subtransforms:
      part = context.transforms.get_by_id(transform_id)
      part.parent = result
      result.add_part(part)
    result.outputs = {
        None if tag == 'None' else tag: context.pcollections.get_by_id(id)
        for tag,
        id in proto.outputs.items()
    }
    if proto.spec.urn == common_urns.primitives.PAR_DO.urn:
      result.transform.output_tags = set(proto.outputs.keys()).difference(
          {'None'})
    if not result.parts:
      for tag, pcoll_id in proto.outputs.items():
        if pcoll_id not in proto.inputs.values():
          pc = context.pcollections.get_by_id(pcoll_id)
          pc.producer = result
          pc.tag = None if tag == 'None' else tag
    return result

  def _merge_outer_resource_hints(self):
    if (self.parent is not None and self.parent.resource_hints):
      self.resource_hints = merge_resource_hints(
          outer_hints=self.parent.resource_hints,
          inner_hints=self.resource_hints)
    if self.resource_hints:
      for part in self.parts:
        part._merge_outer_resource_hints()


class PTransformOverride(metaclass=abc.ABCMeta):
  """For internal use only; no backwards-compatibility guarantees.

  Gives a matcher and replacements for matching PTransforms.

  TODO: Update this to support cases where input and/our output types are
  different.
  """
  @abc.abstractmethod
  def matches(self, applied_ptransform):

    """Determines whether the given AppliedPTransform matches.

    Note that the matching will happen *after* Runner API proto translation.
    If matching is done via type checks, to/from_runner_api[_parameter] methods
    must be implemented to preserve the type (and other data) through proto
    serialization.

    Consider URN-based translation instead.

    Args:
      applied_ptransform: AppliedPTransform to be matched.

    Returns:
      a bool indicating whether the given AppliedPTransform is a match.
    """
    raise NotImplementedError

  def get_replacement_transform_for_applied_ptransform(
      self, applied_ptransform):

    """Provides a runner specific override for a given `AppliedPTransform`.

    Args:
      applied_ptransform: `AppliedPTransform` containing the `PTransform` to be
        replaced.

    Returns:
      A `PTransform` that will be the replacement for the `PTransform` inside
      the `AppliedPTransform` given as an argument.
    """
    return self.get_replacement_transform(applied_ptransform.transform)

  @deprecated(
      since='2.24', current='get_replacement_transform_for_applied_ptransform')
  def get_replacement_transform(self, ptransform):

    """Provides a runner specific override for a given PTransform.

    Args:
      ptransform: PTransform to be replaced.

    Returns:
      A PTransform that will be the replacement for the PTransform given as an
      argument.
    """
    raise NotImplementedError

  def get_replacement_inputs(self, applied_ptransform):

    """Provides inputs that will be passed to the replacement PTransform.

    Args:
      applied_ptransform: Original AppliedPTransform containing the PTransform
        to be replaced.

    Returns:
      An iterable of PValues that will be passed to the expand() method of the
      replacement PTransform.
    """
    return tuple(applied_ptransform.inputs) + tuple(
        side_input.pvalue for side_input in applied_ptransform.side_inputs)


class ComponentIdMap(object):
  """A utility for assigning unique component ids to Beam components.

  Component ID assignments are only guaranteed to be unique and consistent
  within the scope of a ComponentIdMap instance.
  """
  def __init__(self, namespace="ref"):
    self.namespace = namespace
    self._counters = defaultdict(lambda: 0)  # type: Dict[type, int]
    self._obj_to_id = {}  # type: Dict[Any, str]

  def get_or_assign(self, obj=None, obj_type=None, label=None):
    if obj not in self._obj_to_id:
      self._obj_to_id[obj] = self._unique_ref(obj, obj_type, label)

    return self._obj_to_id[obj]

  def _normalize(self, str_value):
    str_value = unicodedata.normalize('NFC', str_value)
    return re.sub(r'[^a-zA-Z0-9-_]+', '-', str_value)

  def _unique_ref(self, obj=None, obj_type=None, label=None):
    prefix = self._normalize(
        '%s_%s_%s' %
        (self.namespace, obj_type.__name__, label or type(obj).__name__))[0:100]
    self._counters[obj_type] += 1
    return '%s_%d' % (prefix, self._counters[obj_type])
