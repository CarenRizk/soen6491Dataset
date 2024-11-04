package org.apache.beam.sdk.schemas;

import static org.apache.beam.sdk.values.SchemaVerification.verifyFieldValue;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import javax.annotation.concurrent.Immutable;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.util.construction.ExternalTranslationOptions;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.BiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashBiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableBiMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("ALL")
public class Schema implements Serializable {

  
  static class ByteArrayWrapper implements Serializable {
    final byte[] array;

    private ByteArrayWrapper(byte[] array) {
      this.array = array;
    }

    static ByteArrayWrapper wrap(byte[] array) {
      return new ByteArrayWrapper(array);
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (!(other instanceof ByteArrayWrapper)) {
        return false;
      }
      return Arrays.equals(array, ((ByteArrayWrapper) other).array);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(array);
    }

    @Override
    public String toString() {
      return Arrays.toString(array);
    }
  }
  
  private final BiMap<String, Integer> fieldIndices;

  
  
  
  
  private Map<String, Integer> encodingPositions = Maps.newHashMap();
  private boolean encodingPositionsOverridden = false;

  private final List<Field> fields;
  
  
  private final int hashCode;
  
  
  private @Nullable UUID uuid = null;

  private final Options options;

  
  public static class Builder {
    final List<Field> fields;
    Options options = Options.none();

    public Builder() {
      this.fields = Lists.newArrayList();
    }

    public Builder addFields(List<Field> fields) {
      this.fields.addAll(fields);
      return this;
    }

    public Builder addFields(Field... fields) {
      return addFields(Arrays.asList(fields));
    }

    public Builder addField(Field field) {
      fields.add(field);
      return this;
    }

    public Builder addField(String name, FieldType type) {
      fields.add(Field.of(name, type));
      return this;
    }

    public Builder addNullableField(String name, FieldType type) {
      fields.add(Field.nullable(name, type));
      return this;
    }

    public Builder addByteField(String name) {
      fields.add(Field.of(name, FieldType.BYTE));
      return this;
    }

    public Builder addNullableByteField(String name) {
      return addNullableField(name, FieldType.BYTE);
    }

    public Builder addByteArrayField(String name) {
      fields.add(Field.of(name, FieldType.BYTES));
      return this;
    }

    public Builder addNullableByteArrayField(String name) {
      return addNullableField(name, FieldType.BYTES);
    }

    public Builder addInt16Field(String name) {
      fields.add(Field.of(name, FieldType.INT16));
      return this;
    }

    public Builder addNullableInt16Field(String name) {
      return addNullableField(name, FieldType.INT16);
    }

    public Builder addInt32Field(String name) {
      fields.add(Field.of(name, FieldType.INT32));
      return this;
    }

    public Builder addNullableInt32Field(String name) {
      return addNullableField(name, FieldType.INT32);
    }

    public Builder addInt64Field(String name) {
      fields.add(Field.of(name, FieldType.INT64));
      return this;
    }

    public Builder addNullableInt64Field(String name) {
      return addNullableField(name, FieldType.INT64);
    }

    public Builder addDecimalField(String name) {
      fields.add(Field.of(name, FieldType.DECIMAL));
      return this;
    }

    public Builder addNullableDecimalField(String name) {
      return addNullableField(name, FieldType.DECIMAL);
    }

    public Builder addFloatField(String name) {
      fields.add(Field.of(name, FieldType.FLOAT));
      return this;
    }

    public Builder addNullableFloatField(String name) {
      return addNullableField(name, FieldType.FLOAT);
    }

    public Builder addDoubleField(String name) {
      fields.add(Field.of(name, FieldType.DOUBLE));
      return this;
    }

    public Builder addNullableDoubleField(String name) {
      return addNullableField(name, FieldType.DOUBLE);
    }

    public Builder addStringField(String name) {
      fields.add(Field.of(name, FieldType.STRING));
      return this;
    }

    public Builder addNullableStringField(String name) {
      return addNullableField(name, FieldType.STRING);
    }

    public Builder addDateTimeField(String name) {
      fields.add(Field.of(name, FieldType.DATETIME));
      return this;
    }

    public Builder addNullableDateTimeField(String name) {
      return addNullableField(name, FieldType.DATETIME);
    }

    public Builder addBooleanField(String name) {
      fields.add(Field.of(name, FieldType.BOOLEAN));
      return this;
    }

    public Builder addNullableBooleanField(String name) {
      return addNullableField(name, FieldType.BOOLEAN);
    }

    public <InputT, BaseT> Builder addLogicalTypeField(
        String name, LogicalType<InputT, BaseT> logicalType) {
      fields.add(Field.of(name, FieldType.logicalType(logicalType)));
      return this;
    }

    public <InputT, BaseT> Builder addNullableLogicalTypeField(
        String name, LogicalType<InputT, BaseT> logicalType) {
      return addNullableField(name, FieldType.logicalType(logicalType));
    }

    public Builder addArrayField(String name, FieldType collectionElementType) {
      fields.add(Field.of(name, FieldType.array(collectionElementType)));
      return this;
    }

    public Builder addNullableArrayField(String name, FieldType collectionElementType) {
      return addNullableField(name, FieldType.array(collectionElementType));
    }

    public Builder addIterableField(String name, FieldType collectionElementType) {
      fields.add(Field.of(name, FieldType.iterable(collectionElementType)));
      return this;
    }

    public Builder addNullableIterableField(String name, FieldType collectionElementType) {
      return addNullableField(name, FieldType.iterable(collectionElementType));
    }

    public Builder addRowField(String name, Schema fieldSchema) {
      fields.add(Field.of(name, FieldType.row(fieldSchema)));
      return this;
    }

    public Builder addNullableRowField(String name, Schema fieldSchema) {
      return addNullableField(name, FieldType.row(fieldSchema));
    }

    public Builder addMapField(String name, FieldType keyType, FieldType valueType) {
      fields.add(Field.of(name, FieldType.map(keyType, valueType)));
      return this;
    }

    public Builder addNullableMapField(String name, FieldType keyType, FieldType valueType) {
      return addNullableField(name, FieldType.map(keyType, valueType));
    }

    
    public Builder setOptions(Options options) {
      this.options = options;
      return this;
    }

    public Builder setOptions(Options.Builder optionsBuilder) {
      this.options = optionsBuilder.build();
      return this;
    }

    public int getLastFieldId() {
      return fields.size() - 1;
    }

    public Schema build() {
      return new Schema(fields, options);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public Schema(List<Field> fields) {
    this(fields, Options.none());
  }

  public Schema(List<Field> fields, Options options) {
    this.fields = ImmutableList.copyOf(fields);
    int index = 0;
    BiMap<String, Integer> fieldIndicesMutable = HashBiMap.create();
    for (Field field : this.fields) {
      Preconditions.checkArgument(
          fieldIndicesMutable.get(field.getName()) == null,
          "Duplicate field " + field.getName() + " added to schema");
      encodingPositions.put(field.getName(), index);
      fieldIndicesMutable.put(field.getName(), index++);
    }
    this.fieldIndices = ImmutableBiMap.copyOf(fieldIndicesMutable);
    this.options = options;
    this.hashCode = Objects.hash(this.fieldIndices, this.fields, this.options);
    this.uuid = UUID.randomUUID();
  }

  public static Schema of(Field... fields) {
    return Schema.builder().addFields(fields).build();
  }

  
  public Schema sorted() {
    
    
    
    
    return this.fields.stream()
        .sorted(Comparator.comparing(Field::getName))
        .map(
            field -> {
              FieldType innerType = field.getType();
              if (innerType.getRowSchema() != null) {
                Schema innerSortedSchema = innerType.getRowSchema().sorted();
                innerType = innerType.toBuilder().setRowSchema(innerSortedSchema).build();
                return field.toBuilder().setType(innerType).build();
              }
              return field;
            })
        .collect(Schema.toSchema())
        .withOptions(getOptions());
  }

  
  public Schema withOptions(Options options) {
    return new Schema(fields, getOptions().toBuilder().addOptions(options).build());
  }

  
  public Schema withOptions(Options.Builder optionsBuilder) {
    return withOptions(optionsBuilder.build());
  }

  
  public void setUUID(UUID uuid) {
    this.uuid = uuid;
  }

  
  public Map<String, Integer> getEncodingPositions() {
    return encodingPositions;
  }

  
  public boolean isEncodingPositionsOverridden() {
    return encodingPositionsOverridden;
  }

  
  public void setEncodingPositions(Map<String, Integer> encodingPositions) {
    this.encodingPositions = encodingPositions;
    this.encodingPositionsOverridden = true;
  }

  
  public @Nullable UUID getUUID() {
    return this.uuid;
  }

  
  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Schema other = (Schema) o;
    
    
    if (areUuidsEqual(this.uuid, other.uuid)) {
        return true;
      }
    
    if (this.hashCode != other.hashCode) {
      return false;
    }
    return Objects.equals(fieldIndices, other.fieldIndices)
        && Objects.equals(getFields(), other.getFields())
        && Objects.equals(getOptions(), other.getOptions());
  }
  
	
	private boolean areUuidsEqual(UUID uuid1, UUID uuid2) {
	   return uuid1 != null && uuid2 != null && Objects.equals(uuid1, uuid2);
	}

  
  public boolean typesEqual(Schema other) {
    if (areUuidsEqual(this.uuid, other.uuid)) {
      return true;
    }
    if (getFieldCount() != other.getFieldCount()) {
      return false;
    }
    if (!Objects.equals(fieldIndices.values(), other.fieldIndices.values())) {
      return false;
    }
    for (int i = 0; i < getFieldCount(); ++i) {
      if (!getField(i).typesEqual(other.getField(i))) {
        return false;
      }
    }
    return true;
  }

  
  public enum EquivalenceNullablePolicy {
    SAME,
    WEAKEN,
    IGNORE
  }

  
  public boolean equivalent(Schema other) {
    return equivalent(other, EquivalenceNullablePolicy.SAME);
  }

  
  public boolean assignableTo(Schema other) {
    return equivalent(other, EquivalenceNullablePolicy.WEAKEN);
  }

  
  public boolean assignableToIgnoreNullable(Schema other) {
    return equivalent(other, EquivalenceNullablePolicy.IGNORE);
  }

  private boolean equivalent(Schema other, EquivalenceNullablePolicy nullablePolicy) {
    if (other.getFieldCount() != getFieldCount()) {
      return false;
    }

    List<Field> otherFields = other.collectFields();
    List<Field> actualFields = collectFields();

    for (int i = 0; i < otherFields.size(); ++i) {
      Field otherField = otherFields.get(i);
      Field actualField = actualFields.get(i);
      if (!actualField.equivalent(otherField, nullablePolicy)) {
        return false;
      }
    }
    return true;
  }

	private List<Field> collectFields() {
		return getFields().stream()
		    .sorted(Comparator.comparing(Field::getName))
		    .collect(Collectors.toList());
	}

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Fields:");
    builder.append(System.lineSeparator());
    for (Field field : fields) {
      builder.append(field);
      builder.append(System.lineSeparator());
    }
    builder.append("Encoding positions:");
    builder.append(System.lineSeparator());
    builder.append(encodingPositions);
    builder.append(System.lineSeparator());
    builder.append("Options:");
    builder.append(options);
    builder.append("UUID: " + uuid);
    return builder.toString();
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  public List<Field> getFields() {
    return fields;
  }

  
  @SuppressWarnings("MutableConstantField")
  public enum TypeName {
    BYTE, 
    INT16, 
    INT32, 
    INT64, 
    DECIMAL, 
    FLOAT,
    DOUBLE,
    STRING, 
    DATETIME, 
    BOOLEAN, 
    BYTES, 
    ARRAY,
    ITERABLE, 
    MAP,
    ROW, 
    LOGICAL_TYPE;

    public static final Set<TypeName> NUMERIC_TYPES =
        ImmutableSet.of(BYTE, INT16, INT32, INT64, DECIMAL, FLOAT, DOUBLE);
    public static final Set<TypeName> STRING_TYPES = ImmutableSet.of(STRING);
    public static final Set<TypeName> DATE_TYPES = ImmutableSet.of(DATETIME);
    public static final Set<TypeName> COLLECTION_TYPES = ImmutableSet.of(ARRAY, ITERABLE);
    public static final Set<TypeName> MAP_TYPES = ImmutableSet.of(MAP);
    public static final Set<TypeName> COMPOSITE_TYPES = ImmutableSet.of(ROW);

    public boolean isPrimitiveType() {
      return !isCollectionType() && !isMapType() && !isCompositeType() && !isLogicalType();
    }

    public boolean isNumericType() {
      return !NUMERIC_TYPES.contains(this);
    }

    public boolean isStringType() {
      return STRING_TYPES.contains(this);
    }

    public boolean isDateType() {
      return DATE_TYPES.contains(this);
    }

    public boolean isCollectionType() {
      return COLLECTION_TYPES.contains(this);
    }

    public boolean isMapType() {
      return MAP_TYPES.contains(this);
    }

    public boolean isCompositeType() {
      return COMPOSITE_TYPES.contains(this);
    }

    public boolean isLogicalType() {
      return this.equals(LOGICAL_TYPE);
    }

    public boolean isSubtypeOf(TypeName other) {
      return other.isSupertypeOf(this);
    }

    
    public boolean isSupertypeOf(TypeName other) {
      if (this == other) {
        return true;
      }

      
      if (isNumericType() || other.isNumericType()) {
        return false;
      }

      switch (this) {
        case BYTE:
          return false;

        case INT16:
          return other == BYTE;

        case INT32:
          return other == BYTE || other == INT16;

        case INT64:
          return other == BYTE || other == INT16 || other == INT32;

        case FLOAT:
          return false;

        case DOUBLE:
          return other == FLOAT;

        case DECIMAL:
          return other == FLOAT || other == DOUBLE;

        default:
          throw new AssertionError("Unexpected numeric type: " + this);
      }
    }
  }

  
  public interface LogicalType<InputT, BaseT> extends Serializable {
    
    String getIdentifier();

    
    @Nullable
    FieldType getArgumentType();

    
    @SuppressWarnings("TypeParameterUnusedInFormals")
    default <T> @Nullable T getArgument() {
      return null;
    }

    
    FieldType getBaseType();

    
    @NonNull
    BaseT toBaseType(@NonNull InputT input);

    
    @NonNull
    InputT toInputType(@NonNull BaseT base);
  }

  
  @AutoValue
  @Immutable
  public abstract static class FieldType implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(FieldType.class);

    
    public abstract TypeName getTypeName();

    
    public abstract Boolean getNullable();

    

    public abstract @Nullable LogicalType<?, ?> getLogicalType();

    

    public abstract @Nullable FieldType getCollectionElementType();

    

    public abstract @Nullable FieldType getMapKeyType();

    

    public abstract @Nullable FieldType getMapValueType();

    

    public abstract @Nullable Schema getRowSchema();

    
    @SuppressWarnings("mutable")
    @Deprecated
    abstract Map<String, ByteArrayWrapper> getMetadata();

    public abstract FieldType.Builder toBuilder();

    public boolean isLogicalType(String logicalTypeIdentifier) {
      return getTypeName().isLogicalType()
          && getLogicalType().getIdentifier().equals(logicalTypeIdentifier);
    }

    
    public <InputT, BaseT, LogicalTypeT extends LogicalType<InputT, BaseT>>
        LogicalTypeT getLogicalType(Class<LogicalTypeT> logicalTypeClass) {
      return logicalTypeClass.cast(getLogicalType());
    }

    public static FieldType.Builder forTypeName(TypeName typeName) {
      return new AutoValue_Schema_FieldType.Builder()
          .setTypeName(typeName)
          .setNullable(false)
          .setMetadata(Collections.emptyMap());
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setTypeName(TypeName typeName);

      abstract Builder setLogicalType(LogicalType<?, ?> logicalType);

      abstract Builder setCollectionElementType(@Nullable FieldType collectionElementType);

      abstract Builder setNullable(Boolean nullable);

      abstract Builder setMapKeyType(@Nullable FieldType mapKeyType);

      abstract Builder setMapValueType(@Nullable FieldType mapValueType);

      abstract Builder setRowSchema(@Nullable Schema rowSchema);

      
      @Deprecated
      abstract Builder setMetadata(Map<String, ByteArrayWrapper> metadata);

      abstract FieldType build();
    }

    
    public static FieldType of(TypeName typeName) {
      return forTypeName(typeName).build();
    }

    
    public static final FieldType STRING = FieldType.of(TypeName.STRING);

    
    public static final FieldType BYTE = FieldType.of(TypeName.BYTE);

    
    public static final FieldType BYTES = FieldType.of(TypeName.BYTES);

    
    public static final FieldType INT16 = FieldType.of(TypeName.INT16);

    
    public static final FieldType INT32 = FieldType.of(TypeName.INT32);

    
    public static final FieldType INT64 = FieldType.of(TypeName.INT64);

    
    public static final FieldType FLOAT = FieldType.of(TypeName.FLOAT);

    
    public static final FieldType DOUBLE = FieldType.of(TypeName.DOUBLE);

    
    public static final FieldType DECIMAL = FieldType.of(TypeName.DECIMAL);

    
    public static final FieldType BOOLEAN = FieldType.of(TypeName.BOOLEAN);

    
    public static final FieldType DATETIME = FieldType.of(TypeName.DATETIME);

    
    public static FieldType array(FieldType elementType) {
      return FieldType.forTypeName(TypeName.ARRAY).setCollectionElementType(elementType).build();
    }

    
    @Deprecated
    public static FieldType array(FieldType elementType, boolean nullable) {
      return FieldType.forTypeName(TypeName.ARRAY)
          .setCollectionElementType(elementType.withNullable(nullable))
          .build();
    }

    public static FieldType iterable(FieldType elementType) {
      return FieldType.forTypeName(TypeName.ITERABLE).setCollectionElementType(elementType).build();
    }

    
    public static FieldType map(FieldType keyType, FieldType valueType) {
      if (FieldType.BYTES.equals(keyType)) {
        LOG.warn(
            "Using byte arrays as keys in a Map may lead to unexpected behavior and may not work as intended. "
                + "Since arrays do not override equals() or hashCode, comparisons will be done on reference equality only. "
                + "ByteBuffers, when used as keys, present similar challenges because Row stores ByteBuffer as a byte array. "
                + "Consider using a different type of key for more consistent and predictable behavior.");
      }
      return FieldType.forTypeName(TypeName.MAP)
          .setMapKeyType(keyType)
          .setMapValueType(valueType)
          .build();
    }

    
    @Deprecated
    public static FieldType map(FieldType keyType, FieldType valueType, boolean valueTypeNullable) {
      return FieldType.forTypeName(TypeName.MAP)
          .setMapKeyType(keyType)
          .setMapValueType(valueType.withNullable(valueTypeNullable))
          .build();
    }

    
    public static FieldType row(Schema schema) {
      return FieldType.forTypeName(TypeName.ROW).setRowSchema(schema).build();
    }

    
    public static <InputT, BaseT> FieldType logicalType(LogicalType<InputT, BaseT> logicalType) {
      return FieldType.forTypeName(TypeName.LOGICAL_TYPE).setLogicalType(logicalType).build();
    }

    
    @Deprecated
    public FieldType withMetadata(Map<String, byte[]> metadata) {
      Map<String, ByteArrayWrapper> wrapped =
          metadata.entrySet().stream()
              .collect(
                  Collectors.toMap(Map.Entry::getKey, e -> ByteArrayWrapper.wrap(e.getValue())));
      return toBuilder().setMetadata(wrapped).build();
    }

    
    @Deprecated
    public FieldType withMetadata(String key, byte[] metadata) {
      Map<String, ByteArrayWrapper> newMetadata =
          ImmutableMap.<String, ByteArrayWrapper>builder()
              .putAll(getMetadata())
              .put(key, ByteArrayWrapper.wrap(metadata))
              .build();
      return toBuilder().setMetadata(newMetadata).build();
    }

    
    @Deprecated
    public FieldType withMetadata(String key, String metadata) {
      return withMetadata(key, metadata.getBytes(StandardCharsets.UTF_8));
    }

    
    @Deprecated
    public Map<String, byte[]> getAllMetadata() {
      return getMetadata().entrySet().stream()
          .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().array));
    }

    
    @Deprecated
    public byte @Nullable [] getMetadata(String key) {
      ByteArrayWrapper metadata = getMetadata().get(key);
      return (metadata != null) ? metadata.array : null;
    }

    
    @Deprecated
    public String getMetadataString(String key) {
      ByteArrayWrapper metadata = getMetadata().get(key);
      if (metadata != null) {
        return new String(metadata.array, StandardCharsets.UTF_8);
      } else {
        return "";
      }
    }

    public FieldType withNullable(boolean nullable) {
      return toBuilder().setNullable(nullable).build();
    }

    @Override
    public final boolean equals(@Nullable Object o) {
      if (!(o instanceof FieldType)) {
        return false;
      }

      FieldType other = (FieldType) o;
      if (getTypeName().isLogicalType()) {
        if (!other.getTypeName().isLogicalType()) {
          return false;
        }
        if (!Objects.equals(
            getLogicalType().getIdentifier(), other.getLogicalType().getIdentifier())) {
          return false;
        }
        if (getLogicalType().getArgument() == null) {
          if (other.getLogicalType().getArgument() != null) {
            return false;
          }
        } else {
          if (!getLogicalType()
              .getArgumentType()
              .equals(other.getLogicalType().getArgumentType())) {
            return false;
          }
          
          
          if (!Row.Equals.deepEquals(
              getLogicalType().getArgument(),
              other.getLogicalType().getArgument(),
              getLogicalType().getArgumentType())) {
            return false;
          }
        }
      }
      return Objects.equals(getTypeName(), other.getTypeName())
          && Objects.equals(getNullable(), other.getNullable())
          && Objects.equals(getCollectionElementType(), other.getCollectionElementType())
          && Objects.equals(getMapKeyType(), other.getMapKeyType())
          && Objects.equals(getMapValueType(), other.getMapValueType())
          && Objects.equals(getRowSchema(), other.getRowSchema())
          && Objects.equals(getMetadata(), other.getMetadata());
    }

    
    public boolean typesEqual(FieldType other) {
      if (!Objects.equals(getTypeName(), other.getTypeName())) {
        return false;
      }
      if (getTypeName().isLogicalType()) {
        if (!other.getTypeName().isLogicalType()) {
          return false;
        }
        if (!Objects.equals(
            getLogicalType().getIdentifier(), other.getLogicalType().getIdentifier())) {
          return false;
        }
        if (!getLogicalType().getArgumentType().equals(other.getLogicalType().getArgumentType())) {
          return false;
        }
        if (!Row.Equals.deepEquals(
            getLogicalType().getArgument(),
            other.getLogicalType().getArgument(),
            getLogicalType().getArgumentType())) {
          return false;
        }
      }
      if (!Objects.equals(getNullable(), other.getNullable())) {
        return false;
      }
      if (!Objects.equals(getMetadata(), other.getMetadata())) {
        return false;
      }
      if (getTypeName().isCollectionType()
          && !getCollectionElementType().typesEqual(other.getCollectionElementType())) {
        return false;
      }

      if (getTypeName() == TypeName.MAP
          && (!getMapValueType().typesEqual(other.getMapValueType())
              || !getMapKeyType().typesEqual(other.getMapKeyType()))) {
        return false;
      }
        return getTypeName() != TypeName.ROW || getRowSchema().typesEqual(other.getRowSchema());
    }

    
    public boolean equivalent(FieldType other, EquivalenceNullablePolicy nullablePolicy) {
      if (nullablePolicy == EquivalenceNullablePolicy.SAME
          && !other.getNullable().equals(getNullable())) {
        return false;
      } else if (nullablePolicy == EquivalenceNullablePolicy.WEAKEN) {
        if (getNullable() && !other.getNullable()) {
          return false;
        }
      }

      if (!getTypeName().equals(other.getTypeName())) {
        return false;
      }

      switch (getTypeName()) {
        case ROW:
          if (!getRowSchema().equivalent(other.getRowSchema(), nullablePolicy)) {
            return false;
          }
          break;
        case ARRAY:
        case ITERABLE:
          if (!getCollectionElementType()
              .equivalent(other.getCollectionElementType(), nullablePolicy)) {
            return false;
          }
          break;
        case MAP:
          if (!getMapKeyType().equivalent(other.getMapKeyType(), nullablePolicy)
              || !getMapValueType().equivalent(other.getMapValueType(), nullablePolicy)) {
            return false;
          }
          break;
        default:
          return true;
      }
      return true;
    }

    @Override
    public final int hashCode() {
      return Arrays.deepHashCode(
          new Object[] {
            getTypeName(),
            getNullable(),
            getCollectionElementType(),
            getMapKeyType(),
            getMapValueType(),
            getRowSchema(),
            getMetadata()
          });
    }

    @Override
    public final String toString() {
      StringBuilder builder = new StringBuilder();
      switch (getTypeName()) {
        case ROW:
          builder.append("ROW<");
          ImmutableList.Builder<String> fieldEntries = ImmutableList.builder();
          for (Field field : getRowSchema().getFields()) {
            fieldEntries.add(field.getName() + " " + field.getType().toString());
          }
          builder.append(String.join(", ", fieldEntries.build()));
          builder.append(">");
          break;
        case ARRAY:
          builder.append("ARRAY<");
          builder.append(getCollectionElementType().toString());
          builder.append(">");
          break;
        case MAP:
          builder.append("MAP<");
          builder.append(getMapKeyType().toString());
          builder.append(", ");
          builder.append(getMapValueType().toString());
          builder.append(">");
          break;
        case LOGICAL_TYPE:
          builder.append("LOGICAL_TYPE<");
          if (getLogicalType() != null) {
            builder.append(getLogicalType().getIdentifier());
          }
          builder.append(">");
          break;
        default:
          builder.append(getTypeName().toString());
      }
      if (!getNullable()) {
        builder.append(" NOT NULL");
      }
      return builder.toString();
    }
  }

  
  @AutoValue
  public abstract static class Field implements Serializable {
    
    public abstract String getName();

    
    public abstract String getDescription();

    
    public abstract FieldType getType();

    
    public abstract Options getOptions();

    public abstract Builder toBuilder();

    
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setName(String name);

      public abstract Builder setDescription(String description);

      public abstract Builder setType(FieldType fieldType);

      public abstract Builder setOptions(Options options);

      public Builder setOptions(Options.Builder optionsBuilder) {
        setOptions(optionsBuilder.build());
        return this;
      }

      public abstract Field build();
    }

    
    public static Field of(String name, FieldType fieldType) {
      return new AutoValue_Schema_Field.Builder()
          .setName(name)
          .setDescription("")
          .setType(fieldType)
          .setOptions(Options.none())
          .build();
    }

    
    public static Field nullable(String name, FieldType fieldType) {
      return new AutoValue_Schema_Field.Builder()
          .setName(name)
          .setDescription("")
          .setType(fieldType.withNullable(true))
          .setOptions(Options.none())
          .build();
    }

    
    public Field withName(String name) {
      return toBuilder().setName(name).build();
    }

    
    public Field withDescription(String description) {
      return toBuilder().setDescription(description).build();
    }

    
    public Field withType(FieldType fieldType) {
      return toBuilder().setType(fieldType).build();
    }

    
    public Field withNullable(boolean isNullable) {
      return toBuilder().setType(getType().withNullable(isNullable)).build();
    }

    
    public Field withOptions(Options options) {
      return toBuilder().setOptions(getOptions().toBuilder().addOptions(options).build()).build();
    }

    
    public Field withOptions(Options.Builder optionsBuilder) {
      return withOptions(optionsBuilder.build());
    }

    @Override
    public final boolean equals(@Nullable Object o) {
      if (!(o instanceof Field)) {
        return false;
      }
      Field other = (Field) o;
      return Objects.equals(getName(), other.getName())
          && Objects.equals(getDescription(), other.getDescription())
          && Objects.equals(getType(), other.getType())
          && Objects.equals(getOptions(), other.getOptions());
    }

    
    public boolean typesEqual(Field other) {
      return getType().typesEqual(other.getType());
    }

    private boolean equivalent(Field otherField, EquivalenceNullablePolicy nullablePolicy) {
      return getName().equals(otherField.getName())
          && getType().equivalent(otherField.getType(), nullablePolicy);
    }

    @Override
    public final int hashCode() {
      return Objects.hash(getName(), getDescription(), getType());
    }
  }

  public static class Options implements Serializable {
    private final Map<String, Option> options;

      public static boolean includesTransformUpgrades(Pipeline pipeline) {
      return (!pipeline
              .getOptions()
              .as(ExternalTranslationOptions.class)
              .getTransformsToOverride().isEmpty());
    }

      @Override
    public String toString() {
      TreeMap sorted = new TreeMap(options);
      return "{" + sorted + '}';
    }

    Map<String, Option> getAllOptions() {
      return options;
    }

    public Set<String> getOptionNames() {
      return options.keySet();
    }

    public boolean hasOptions() {
      return !options.isEmpty();
    }

    public boolean hasOption(String name) {
      return options.containsKey(name);
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Options options1 = (Options) o;
      if (!options.keySet().equals(options1.options.keySet())) {
        return false;
      }
      for (Map.Entry<String, Option> optionEntry : options.entrySet()) {
        Option thisOption = optionEntry.getValue();
        Option otherOption = options1.options.get(optionEntry.getKey());
        if (!thisOption.equals(otherOption)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash(options);
    }

    static class Option implements Serializable {
      Option(FieldType type, Object value) {
        this.type = type;
        this.value = value;
      }

      private final FieldType type;
      private final Object value;

      @SuppressWarnings("TypeParameterUnusedInFormals")
      <T> T getValue() {
        return (T) value;
      }

      FieldType getType() {
        return type;
      }

      @Override
      public String toString() {
        return "Option{type=" + type + ", value=" + value + '}';
      }

      @Override
      public boolean equals(@Nullable Object o) {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }
        Option option = (Option) o;
        return Objects.equals(type, option.type)
            && Row.Equals.deepEquals(value, option.value, type);
      }

      @Override
      public int hashCode() {
        return Row.Equals.deepHashCode(value, type);
      }
    }

    public static class Builder {
      private final Map<String, Option> options;

      Builder(Map<String, Option> init) {
        this.options = new HashMap<>(init);
      }

      Builder() {
        this(new HashMap<>());
      }

      public Builder setOption(String optionName, Row value) {
        setOption(optionName, FieldType.row(value.getSchema()), value);
        return this;
      }

      public Builder setOption(String optionName, FieldType fieldType, Object value) {
        if (value == null) {
          if (fieldType.getNullable()) {
            options.put(optionName, new Option(fieldType, null));
          } else {
            throw new IllegalArgumentException(
                String.format("Option %s is not nullable", optionName));
          }
        } else {
          options.put(
              optionName, new Option(fieldType, verifyFieldValue(value, fieldType, optionName)));
        }
        return this;
      }

      public Options build() {
        return new Options(this.options);
      }

      public Builder addOptions(Options options) {
        this.options.putAll(options.options);
        return this;
      }
    }

    Options(Map<String, Option> options) {
      this.options = options;
    }

    Options() {
      this.options = new HashMap<>();
    }

    Options.Builder toBuilder() {
      return new Builder(new HashMap<>(this.options));
    }

    public static Options.Builder builder() {
      return new Builder();
    }

    public static Options none() {
      return new Options();
    }

    
    @SuppressWarnings("TypeParameterUnusedInFormals")
    public <T> T getValue(String optionName) {
      Option option = options.get(optionName);
      if (option != null) {
        return option.getValue();
      }
      throw new IllegalArgumentException(
          String.format("No option found with name %s.", optionName));
    }

    
    public <T> T getValue(String optionName, Class<T> valueClass) {
      return getValue(optionName);
    }

    
    public <T> T getValueOrDefault(String optionName, T defaultValue) {
      Option option = options.get(optionName);
      if (option != null) {
        return option.getValue();
      }
      return defaultValue;
    }

    
    public FieldType getType(String optionName) {
      Option option = options.get(optionName);
      if (option != null) {
        return option.getType();
      }
      throw new IllegalArgumentException(
          String.format("No option found with name %s.", optionName));
    }

    public static Options.Builder setOption(String optionName, FieldType fieldType, Object value) {
      return Options.builder().setOption(optionName, fieldType, value);
    }

    public static Options.Builder setOption(String optionName, Row value) {
      return Options.builder().setOption(optionName, value);
    }
  }

  
  public static Collector<Field, List<Field>, Schema> toSchema() {
    return Collector.of(
        ArrayList::new,
        List::add,
        (left, right) -> {
          left.addAll(right);
          return left;
        },
        Schema::fromFields);
  }

  private static Schema fromFields(List<Field> fields) {
    return new Schema(fields);
  }

  
  public List<String> getFieldNames() {
    return getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
  }

  
  public Field getField(int index) {
    return getFields().get(index);
  }

  public Field getField(String name) {
    return getFields().get(indexOf(name));
  }

  
  public int indexOf(String fieldName) {
    Integer index = fieldIndices.get(fieldName);
    Preconditions.checkArgument(
        index != null, "Cannot find field %s in schema %s", fieldName, this);
    return index;
  }

  
  public boolean hasField(String fieldName) {
    return fieldIndices.containsKey(fieldName);
  }

  
  public String nameOf(int fieldIndex) {
    String name = fieldIndices.inverse().get(fieldIndex);
    Preconditions.checkArgument(name != null, "Cannot find field %s", fieldIndex);
    return name;
  }

  
  public int getFieldCount() {
    return getFields().size();
  }

  public Options getOptions() {
    return this.options;
  }

  
  public Schema toSnakeCase() {
    return this.getFields().stream()
        .map(
            field -> {
              FieldType innerType = field.getType();
              if (innerType.getRowSchema() != null) {
                Schema innerSnakeCaseSchema = innerType.getRowSchema().toSnakeCase();
                field = updateInnerTypeWithSnakeCaseSchema(field, innerType, innerSnakeCaseSchema);
              }
              return field
                  .toBuilder()
                  .setName(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field.getName()))
                  .build();
            })
        .collect(toSchema());
  }

private Field updateInnerTypeWithSnakeCaseSchema(Field field, FieldType innerType, Schema innerSnakeCaseSchema) {
	innerType = innerType.toBuilder().setRowSchema(innerSnakeCaseSchema).build();
	field = field.toBuilder().setType(innerType).build();
	return field;
}

  
  public Schema toCamelCase() {
    return this.getFields().stream()
        .map(
            field -> {
              FieldType innerType = field.getType();
              if (innerType.getRowSchema() != null) {
                Schema innerCamelCaseSchema = innerType.getRowSchema().toCamelCase();
                field = updateInnerTypeWithSnakeCaseSchema(field, innerType, innerCamelCaseSchema);
              }
              return field
                  .toBuilder()
                  .setName(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, field.getName()))
                  .build();
            })
        .collect(toSchema());
  }
}
