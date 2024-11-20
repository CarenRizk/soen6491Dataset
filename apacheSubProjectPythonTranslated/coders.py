from typing import TypeVar

T = TypeVar('T')

class Coder:
    """Base class for coders."""
    class Context:
        OUTER = None  # Placeholder for Context.OUTER
        NESTED = None  # Placeholder for Context.NESTED

        def __init__(self, is_whole_stream):
            self.is_whole_stream = is_whole_stream

        def nested(self):
            return Coder.Context.NESTED

        def __eq__(self, obj):
            if not isinstance(obj, Coder.Context):
                return False
            return self.is_whole_stream == obj.is_whole_stream  # Optimized by LLM

        def __hash__(self):
            return hash(self.is_whole_stream)  # Optimized by LLM

        def __str__(self):
            return "OUTER" if self.is_whole_stream else "NESTED"

    def encode(self, value, out_stream):
        """Encodes the given object into a byte string."""
        raise NotImplementedError('Encode not implemented: %s.' % self)

    def encode_with_context(self, value, out_stream, context):
        self.encode(value, out_stream)

    def decode(self, in_stream):
        """Decodes the given byte string into the corresponding object."""
        raise NotImplementedError('Decode not implemented: %s.' % self)

    def decode_with_context(self, in_stream, context):
        return self.decode(in_stream)

    def get_coder_arguments(self):
        """Returns the coder arguments."""
        raise NotImplementedError('getCoderArguments not implemented: %s.' % self)

    def verify_deterministic(self):
        """Verifies if the coder is deterministic."""
        raise NotImplementedError('verifyDeterministic not implemented: %s.' % self)

    @staticmethod
    def verify_deterministic_static(target, message, coders):
        for coder in coders:
            try:
                coder.verify_deterministic()
            except Coder.NonDeterministicException as e:  # Optimized by LLM
                raise Coder.NonDeterministicException(target, message, e)

    @staticmethod
    def verify_deterministic_static_varargs(target, message, *coders):
        Coder.verify_deterministic_static(target, message, list(coders))

    def consistent_with_equals(self):
        return False

    def structural_value(self, value):
        if value is not None and self.consistent_with_equals():
            return value
        else:
            try:
                os = bytearray()
                self.encode(value, os, Coder.Context.OUTER)
                return os
            except ValueError as exn:  # Optimized by LLM
                raise ValueError(
                    "Unable to encode element '" + str(value) + "' with coder '" + str(self) + "'.", exn)

    def is_register_byte_size_observer_cheap(self, value):
        return False

    def register_byte_size_observer(self, value, observer):
        observer.update(self.get_encoded_element_byte_size(value))

    def get_encoded_element_byte_size(self, value):
        try:
            os = bytearray()
            self.encode(value, os)
            return len(os)
        except ValueError as exn:  # Optimized by LLM
            raise ValueError(
                "Unable to encode element '" + str(value) + "' with coder '" + str(self) + "'.", exn)

    def get_encoded_type_descriptor(self):
        return TypeDescriptor.of(type(self)).resolve_type(TypeDescriptor[T]())

    class NonDeterministicException(Exception):
        def __init__(self, coder, reason, e=None):
            super().__init__(e)
            self.coder = coder
            self.reasons = [reason]

        def get_reasons(self):
            return self.reasons

        def get_message(self):
            reasons_str = "\n\t".join(self.reasons)
            return str(self.coder) + " is not deterministic because:\n\t" + reasons_str