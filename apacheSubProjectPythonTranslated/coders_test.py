
import base64
import logging
import unittest

import proto
import pytest
from apache_beam import typehints
from apache_beam.coders import proto2_coder_test_messages_pb2 as test_message
from apache_beam.coders.avro_record import AvroRecord
from apache_beam.coders.typecoders import registry as coders_registry

import coders


class PickleCoderTest(unittest.TestCase):
  def test_basics(self):
    v = ('a' * 10, 'b' * 90)
    pickler = coders.PickleCoder()
    self.assertEqual(v, pickler.decode(pickler.encode(v)))
    pickler = coders.Base64PickleCoder()
    self.assertEqual(v, pickler.decode(pickler.encode(v)))
    self.assertEqual(
        coders.Base64PickleCoder().encode(v),
        base64.b64encode(coders.PickleCoder().encode(v)))

  def test_equality(self):
    self.assertEqual(coders.PickleCoder(), coders.PickleCoder())
    self.assertEqual(coders.Base64PickleCoder(), coders.Base64PickleCoder())
    self.assertNotEqual(coders.Base64PickleCoder(), coders.PickleCoder())
    self.assertNotEqual(coders.Base64PickleCoder(), object())


class CodersTest(unittest.TestCase):
  def test_str_utf8_coder(self):
    real_coder = coders_registry.get_coder(bytes)
    expected_coder = coders.BytesCoder()
    self.assertEqual(real_coder.encode(b'abc'), expected_coder.encode(b'abc'))
    self.assertEqual(b'abc', real_coder.decode(real_coder.encode(b'abc')))


class ProtoCoderTest(unittest.TestCase):
  def test_proto_coder(self):
    ma = test_message.MessageA()
    mb = ma.field2.add()
    mb.field1 = True
    ma.field1 = 'hello world'
    expected_coder = coders.ProtoCoder(ma.__class__)
    real_coder = coders_registry.get_coder(ma.__class__)
    self.assertEqual(real_coder.encode(ma), expected_coder.encode(ma))
    self.assertEqual(ma, real_coder.decode(real_coder.encode(ma)))
    self.assertEqual(ma.__class__, real_coder.to_type_hint())


class DeterministicProtoCoderTest(unittest.TestCase):
  def test_deterministic_proto_coder(self):
    ma = test_message.MessageA()
    mb = ma.field2.add()
    mb.field1 = True
    ma.field1 = 'hello world'
    expected_coder = coders.DeterministicProtoCoder(ma.__class__)
    real_coder = (
        coders_registry.get_coder(
            ma.__class__).as_deterministic_coder(step_label='unused'))
    self.assertTrue(real_coder.is_deterministic())
    self.assertEqual(real_coder.encode(ma), expected_coder.encode(ma))
    self.assertEqual(ma, real_coder.decode(real_coder.encode(ma)))

  def test_deterministic_proto_coder_determinism(self):
    for _ in range(10):
      keys = list(range(20))
      mm_forward = test_message.MessageWithMap()
      for key in keys:
        mm_forward.field1[str(key)].field1 = str(key)
      mm_reverse = test_message.MessageWithMap()
      for key in reversed(keys):
        mm_reverse.field1[str(key)].field1 = str(key)
      coder = coders.DeterministicProtoCoder(mm_forward.__class__)
      self.assertEqual(coder.encode(mm_forward), coder.encode(mm_reverse))


class ProtoPlusMessageB(proto.Message):
  field1 = proto.Field(proto.BOOL, number=1)


class ProtoPlusMessageA(proto.Message):
  field1 = proto.Field(proto.STRING, number=1)
  field2 = proto.RepeatedField(ProtoPlusMessageB, number=2)


class ProtoPlusMessageWithMap(proto.Message):
  field1 = proto.MapField(proto.STRING, ProtoPlusMessageA, number=1)


class ProtoPlusCoderTest(unittest.TestCase):
  def test_proto_plus_coder(self):
    ma = ProtoPlusMessageA()
    ma.field2 = [ProtoPlusMessageB(field1=True)]
    ma.field1 = 'hello world'
    expected_coder = coders.ProtoPlusCoder(ma.__class__)
    real_coder = coders_registry.get_coder(ma.__class__)
    self.assertTrue(issubclass(ma.__class__, proto.Message))
    self.assertTrue(real_coder.is_deterministic())
    self.assertEqual(real_coder.encode(ma), expected_coder.encode(ma))
    self.assertEqual(ma, real_coder.decode(real_coder.encode(ma)))

  def test_proto_plus_coder_determinism(self):
    for _ in range(10):
      keys = list(range(20))
      mm_forward = ProtoPlusMessageWithMap()
      for key in keys:
        mm_forward.field1[str(key)] = ProtoPlusMessageA(field1=str(key))  # pylint: disable=E1137
      mm_reverse = ProtoPlusMessageWithMap()
      for key in reversed(keys):
        mm_reverse.field1[str(key)] = ProtoPlusMessageA(field1=str(key))  # pylint: disable=E1137
      coder = coders.ProtoPlusCoder(ProtoPlusMessageWithMap)
      self.assertEqual(coder.encode(mm_forward), coder.encode(mm_reverse))


class AvroTestCoder(coders.AvroGenericCoder):
  SCHEMA = """
  {
    "type": "record", "name": "testrecord",
    "fields": [
      {"name": "name", "type": "string"},
      {"name": "age", "type": "int"}
    ]
  }
  """

  def __init__(self):
    super().__init__(self.SCHEMA)


class AvroTestRecord(AvroRecord):
  pass


coders_registry.register_coder(AvroTestRecord, AvroTestCoder)


class AvroCoderTest(unittest.TestCase):
  def test_avro_record_coder(self):
    real_coder = coders_registry.get_coder(AvroTestRecord)
    expected_coder = AvroTestCoder()
    self.assertEqual(
        real_coder.encode(
            AvroTestRecord({
                "name": "Daenerys targaryen", "age": 23
            })),
        expected_coder.encode(
            AvroTestRecord({
                "name": "Daenerys targaryen", "age": 23
            })))
    self.assertEqual(
        AvroTestRecord({
            "name": "Jon Snow", "age": 23
        }),
        real_coder.decode(
            real_coder.encode(AvroTestRecord({
                "name": "Jon Snow", "age": 23
            }))))


class DummyClass(object):
  """A class with no registered coder."""
  def __init__(self):
    pass

  def __eq__(self, other):
    if isinstance(other, self.__class__):
      return True
    return False

  def __hash__(self):
    return hash(type(self))


class FallbackCoderTest(unittest.TestCase):
  def test_default_fallback_path(self):
    """Test fallback path picks a matching coder if no coder is registered."""

    coder = coders_registry.get_coder(DummyClass)
    self.assertEqual(DummyClass(), coder.decode(coder.encode(DummyClass())))


class NullableCoderTest(unittest.TestCase):
  def test_determinism(self):
    deterministic = coders_registry.get_coder(typehints.Optional[int])
    deterministic.as_deterministic_coder('label')

    complex_deterministic = coders_registry.get_coder(
        typehints.Optional[DummyClass])
    complex_deterministic.as_deterministic_coder('label')

    nondeterministic = coders.NullableCoder(coders.Base64PickleCoder())
    with pytest.raises(ValueError):
      nondeterministic.as_deterministic_coder('label')


class LengthPrefixCoderTest(unittest.TestCase):
  def test_to_type_hint(self):
    coder = coders.LengthPrefixCoder(coders.BytesCoder())
    assert coder.to_type_hint() is bytes


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
