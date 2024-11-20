from typing import Any
from typing import NamedTuple
from typing import Optional
from typing import TypeVar

# Placeholder for <Java import>

T = TypeVar('T', bound=NamedTuple)

class ByteArrayWrapper:
    def __init__(self, array: bytes):
        self.array = array

    @staticmethod
    def wrap(array: bytes) -> 'ByteArrayWrapper':
        return ByteArrayWrapper(array)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ByteArrayWrapper):
            return False
        return self.array == other.array

    def __hash__(self) -> int:
        return hash(self.array)

    def __str__(self) -> str:
        return str(self.array)

class Schema:
    def __init__(self, fields: List['Field'], options: Optional['Options'] = None):
        self.fields = fields
        self.options = options if options is not None else Options.none()
        self.field_indices = {field.get_name(): index for index, field in enumerate(fields)}
        self.hash_code = hash((tuple(self.field_indices.items()), tuple(self.fields), self.options))
        self.uuid = UUID.randomUUID()

    @staticmethod
    def builder() -> 'Builder':
        return Schema.Builder()

    def get_fields(self) -> List['Field']:
        return self.fields

    def get_field_count(self) -> int:
        return len(self.fields)

    def get_field(self, index: int) -> 'Field':
        return self.fields[index]

    def get_field_by_name(self, name: str) -> 'Field':
        return self.fields[self.field_indices[name]]

    def has_field(self, field_name: str) -> bool:
        return field_name in self.field_indices

    def index_of(self, field_name: str) -> int:
        return self.field_indices[field_name]

    def __eq__(self, other: Any) -> bool:
        if self is other:
            return True
        if not isinstance(other, Schema):
            return False
        return self.hash_code == other.hash_code and self.fields == other.fields and self.options == other.options

    def __hash__(self) -> int:
        return self.hash_code

    def __str__(self) -> str:
        return f"Schema(fields={self.fields}, options={self.options}, uuid={self.uuid})"

    class Builder:
        def __init__(self):
            self.fields = []

        def add_field(self, field: 'Field') -> 'Builder':
            self.fields.append(field)
            return self

        def build(self) -> 'Schema':
            return Schema(self.fields)

class Field:
    def __init__(self, name: str, field_type: 'FieldType', options: Optional['Options'] = None):
        self.name = name
        self.field_type = field_type
        self.options = options if options is not None else Options.none()

    @staticmethod
    def of(name: str, field_type: 'FieldType') -> 'Field':
        return Field(name, field_type)

    def get_name(self) -> str:
        return self.name

    def get_type(self) -> 'FieldType':
        return self.field_type

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Field):
            return False
        return self.name == other.name and self.field_type == other.field_type and self.options == other.options

    def __hash__(self) -> int:
        return hash((self.name, self.field_type, self.options))

class FieldType:
    @staticmethod
    def of(type_name: 'TypeName') -> 'FieldType':
        return FieldType(type_name)

class Options:
    @staticmethod
    def none() -> 'Options':
        return Options()

    def to_builder(self) -> 'Options.Builder':
        return Options.Builder()

    class Builder:
        def __init__(self):
            self.options = {}

        def build(self) -> 'Options':
            return Options()

# Placeholder for <Java import>