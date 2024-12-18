package org.apache.beam.sdk.schemas;

import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.Options;
import org.apache.beam.sdk.schemas.logicaltypes.PassThroughLogicalType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


public class SchemaTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCreate() {
    Schema schema =
        Schema.builder()
            .addByteField("f_byte")
            .addInt16Field("f_int16")
            .addInt32Field("f_int32")
            .addInt64Field("f_int64")
            .addDecimalField("f_decimal")
            .addFloatField("f_float")
            .addDoubleField("f_double")
            .addStringField("f_string")
            .addDateTimeField("f_datetime")
            .addBooleanField("f_boolean")
            .build();
    assertEquals(10, schema.getFieldCount());

    assertEquals(0, schema.indexOf("f_byte"));
    assertEquals("f_byte", schema.getField(0).getName());
    assertEquals(FieldType.BYTE, schema.getField(0).getType());

    assertEquals(1, schema.indexOf("f_int16"));
    assertEquals("f_int16", schema.getField(1).getName());
    assertEquals(FieldType.INT16, schema.getField(1).getType());

    assertEquals(2, schema.indexOf("f_int32"));
    assertEquals("f_int32", schema.getField(2).getName());
    assertEquals(FieldType.INT32, schema.getField(2).getType());

    assertEquals(3, schema.indexOf("f_int64"));
    assertEquals("f_int64", schema.getField(3).getName());
    assertEquals(FieldType.INT64, schema.getField(3).getType());

    assertEquals(4, schema.indexOf("f_decimal"));
    assertEquals("f_decimal", schema.getField(4).getName());
    assertEquals(FieldType.DECIMAL, schema.getField(4).getType());

    assertEquals(5, schema.indexOf("f_float"));
    assertEquals("f_float", schema.getField(5).getName());
    assertEquals(FieldType.FLOAT, schema.getField(5).getType());

    assertEquals(6, schema.indexOf("f_double"));
    assertEquals("f_double", schema.getField(6).getName());
    assertEquals(FieldType.DOUBLE, schema.getField(6).getType());

    assertEquals(7, schema.indexOf("f_string"));
    assertEquals("f_string", schema.getField(7).getName());
    assertEquals(FieldType.STRING, schema.getField(7).getType());

    assertEquals(8, schema.indexOf("f_datetime"));
    assertEquals("f_datetime", schema.getField(8).getName());
    assertEquals(FieldType.DATETIME, schema.getField(8).getType());

    assertEquals(9, schema.indexOf("f_boolean"));
    assertEquals("f_boolean", schema.getField(9).getName());
    assertEquals(FieldType.BOOLEAN, schema.getField(9).getType());
  }

  @Test
  public void testNestedSchema() {
    Schema nestedSchema = Schema.of(Field.of("f1_str", FieldType.STRING));
    Schema schema = Schema.of(Field.of("nested", FieldType.row(nestedSchema)));
    Field inner = schema.getField("nested").getType().getRowSchema().getField("f1_str");
    assertEquals("f1_str", inner.getName());
    assertEquals(FieldType.STRING, inner.getType());
  }

  @Test
  public void testArraySchema() {
    FieldType arrayType = FieldType.array(FieldType.STRING);
    createSchemaAndValidateField(arrayType);
  }

private void createSchemaAndValidateField(FieldType arrayType) {
	Schema schema = Schema.of(Field.of("f_array", arrayType));
    Field field = schema.getField("f_array");
    assertEquals("f_array", field.getName());
    assertEquals(arrayType, field.getType());
}

  @Test
  public void testArrayOfRowSchema() {
    Schema nestedSchema = Schema.of(Field.of("f1_str", FieldType.STRING));
    FieldType arrayType = FieldType.array(FieldType.row(nestedSchema));
    createSchemaAndValidateField(arrayType);
  }

  @Test
  public void testNestedArraySchema() {
    FieldType arrayType = FieldType.array(FieldType.array(FieldType.STRING));
    createSchemaAndValidateField(arrayType);
  }

  @Test
  public void testIterableSchema() {
    FieldType iterableType = FieldType.iterable(FieldType.STRING);
    Schema schema = Schema.of(Field.of("f_iter", iterableType));
    Field field = schema.getField("f_iter");
    assertEquals("f_iter", field.getName());
    assertEquals(iterableType, field.getType());
  }

  @Test
  public void testIterableOfRowSchema() {
    Schema nestedSchema = Schema.of(Field.of("f1_str", FieldType.STRING));
    FieldType iterableType = FieldType.iterable(FieldType.row(nestedSchema));
    Schema schema = Schema.of(Field.of("f_iter", iterableType));
    Field field = schema.getField("f_iter");
    assertEquals("f_iter", field.getName());
    assertEquals(iterableType, field.getType());
  }

  @Test
  public void testNestedIterableSchema() {
    FieldType iterableType = FieldType.iterable(FieldType.iterable(FieldType.STRING));
    Schema schema = Schema.of(Field.of("f_iter", iterableType));
    Field field = schema.getField("f_iter");
    assertEquals("f_iter", field.getName());
    assertEquals(iterableType, field.getType());
  }

  @Test
  public void testWrongName() {
    Schema schema = Schema.of(Field.of("f_byte", FieldType.BYTE));
    thrown.expect(IllegalArgumentException.class);
    schema.getField("f_string");
  }

  @Test
  public void testWrongIndex() {
    Schema schema = Schema.of(Field.of("f_byte", FieldType.BYTE));
    thrown.expect(IndexOutOfBoundsException.class);
    schema.getField(1);
  }

  @Test
  public void testCollector() {
    Schema schema =
        Stream.of(
                Schema.Field.of("f_int", FieldType.INT32),
                Schema.Field.of("f_string", FieldType.STRING))
            .collect(toSchema());

    assertEquals(2, schema.getFieldCount());

    assertEquals("f_int", schema.getField(0).getName());
    assertEquals(FieldType.INT32, schema.getField(0).getType());
    assertEquals("f_string", schema.getField(1).getName());
    assertEquals(FieldType.STRING, schema.getField(1).getType());
  }

  @Test
  public void testToSnakeCase() {
    Schema innerSchema =
        Schema.builder()
            .addStringField("myFirstNestedStringField")
            .addStringField("mySecondNestedStringField")
            .build();
    Schema schema =
        Schema.builder()
            .addStringField("myFirstStringField")
            .addStringField("mySecondStringField")
            .addRowField("myRowField", innerSchema)
            .build();

    Schema expectedInnerSnakeCaseSchema =
        Schema.builder()
            .addStringField("my_first_nested_string_field")
            .addStringField("my_second_nested_string_field")
            .build();
    Schema expectedSnakeCaseSchema =
        Schema.builder()
            .addStringField("my_first_string_field")
            .addStringField("my_second_string_field")
            .addRowField("my_row_field", expectedInnerSnakeCaseSchema)
            .build();

    assertEquals(
        expectedInnerSnakeCaseSchema,
        schema.toSnakeCase().getField("my_row_field").getType().getRowSchema());
    assertEquals(expectedSnakeCaseSchema, schema.toSnakeCase());
  }

  @Test
  public void testToCamelCase() {
    Schema innerSchema =
        Schema.builder()
            .addStringField("my_first_nested_string_field")
            .addStringField("my_second_nested_string_field")
            .build();
    Schema schema =
        Schema.builder()
            .addStringField("my_first_string_field")
            .addStringField("my_second_string_field")
            .addRowField("my_row_field", innerSchema)
            .build();

    Schema expectedInnerCamelCaseSchema =
        Schema.builder()
            .addStringField("myFirstNestedStringField")
            .addStringField("mySecondNestedStringField")
            .build();
    Schema expectedCamelCaseSchema =
        Schema.builder()
            .addStringField("myFirstStringField")
            .addStringField("mySecondStringField")
            .addRowField("myRowField", expectedInnerCamelCaseSchema)
            .build();

    assertTrue(schema.toCamelCase().hasField("myRowField"));
    assertEquals(
        expectedInnerCamelCaseSchema,
        schema.toCamelCase().getField("myRowField").getType().getRowSchema());
    assertEquals(expectedCamelCaseSchema, schema.toCamelCase());
  }

  @Test
  public void testSorted() {
    Options testOptions =
        Options.builder()
            .setOption("test_str_option", FieldType.STRING, "test_str")
            .setOption("test_bool_option", FieldType.BOOLEAN, true)
            .build();

    Schema unorderedSchema =
        Schema.builder()
            .addStringField("d")
            .addInt32Field("c")
            .addStringField("b")
            .addByteField("a")
            .build()
            .withOptions(testOptions);

    Schema unorderedSchemaAfterSorting = unorderedSchema.sorted();

    Schema sortedSchema =
        Schema.builder()
            .addByteField("a")
            .addStringField("b")
            .addInt32Field("c")
            .addStringField("d")
            .build()
            .withOptions(testOptions);

    assertEquals(true, unorderedSchema.equivalent(unorderedSchemaAfterSorting));
    assertEquals(
        true,
        Objects.equals(unorderedSchemaAfterSorting.getFields(), sortedSchema.getFields())
            && Objects.equals(unorderedSchemaAfterSorting.getOptions(), sortedSchema.getOptions())
            && Objects.equals(
                unorderedSchemaAfterSorting.getEncodingPositions(),
                sortedSchema.getEncodingPositions()));
  }

  @Test
  public void testNestedSorted() {
    Schema unsortedNestedSchema =
        Schema.builder().addStringField("bb").addInt32Field("aa").addStringField("cc").build();
    Schema unsortedSchema =
        Schema.builder()
            .addStringField("d")
            .addInt32Field("c")
            .addRowField("e", unsortedNestedSchema)
            .addStringField("b")
            .addByteField("a")
            .build();

    Schema sortedSchema = unsortedSchema.sorted();

    Schema expectedInnerSortedSchema =
        Schema.builder().addInt32Field("aa").addStringField("bb").addStringField("cc").build();
    Schema expectedSortedSchema =
        Schema.builder()
            .addByteField("a")
            .addStringField("b")
            .addInt32Field("c")
            .addStringField("d")
            .addRowField("e", expectedInnerSortedSchema)
            .build();

    assertTrue(unsortedSchema.equivalent(sortedSchema));
    assertEquals(expectedSortedSchema.getFields(), sortedSchema.getFields());
    assertEquals(expectedSortedSchema.getEncodingPositions(), sortedSchema.getEncodingPositions());
    assertEquals(expectedInnerSortedSchema, sortedSchema.getField("e").getType().getRowSchema());
    assertEquals(
        expectedInnerSortedSchema.getEncodingPositions(),
        sortedSchema.getField("e").getType().getRowSchema().getEncodingPositions());
  }

  @Test
  public void testSortedMethodIncludesAllSchemaFields() {
    
    
    

    
    
    List<String> attributesAccountedForInSorted =
        Arrays.asList(
            "fieldIndices",
            "encodingPositions",
            "encodingPositionsOverridden",
            "fields",
            "hashCode",
            "uuid",
            "options");

    
    List<String> currentAttributes =
        Arrays.stream(Schema.class.getDeclaredFields())
            .filter(field -> !field.isSynthetic())
            .map(java.lang.reflect.Field::getName)
            .collect(Collectors.toList());

    List<String> differences = new ArrayList<>(currentAttributes);
    differences.removeAll(attributesAccountedForInSorted);

    assertEquals(
        String.format(
            "Detected attributes %s in Schema object that are not accounted for in Schema::sorted(). "
                + "If appropriate, sorted() should copy over these attributes as well. Either way, update this test after checking.",
            differences.toString()),
        currentAttributes,
        attributesAccountedForInSorted);
  }

  @Test
  public void testEquivalent() {
    final Schema expectedNested1 =
        Schema.builder().addStringField("yard1").addInt64Field("yard2").build();

    final Schema expectedSchema1 =
        Schema.builder()
            .addStringField("field1")
            .addInt64Field("field2")
            .addRowField("field3", expectedNested1)
            .addArrayField("field4", FieldType.row(expectedNested1))
            .addMapField("field5", FieldType.STRING, FieldType.row(expectedNested1))
            .build();

    final Schema expectedNested2 =
        Schema.builder().addInt64Field("yard2").addStringField("yard1").build();

    final Schema expectedSchema2 =
        Schema.builder()
            .addMapField("field5", FieldType.STRING, FieldType.row(expectedNested2))
            .addArrayField("field4", FieldType.row(expectedNested2))
            .addRowField("field3", expectedNested2)
            .addInt64Field("field2")
            .addStringField("field1")
            .build();

    assertNotEquals(expectedSchema1, expectedSchema2);
    assertTrue(expectedSchema1.equivalent(expectedSchema2));
  }

  @Test
  public void testPrimitiveNotEquivalent() {
    Schema schema1 = Schema.builder().addInt64Field("foo").build();
    Schema schema2 = Schema.builder().addStringField("foo").build();
    schema1 = assertSchemasAreNotEquivalent(schema1, schema2);
    schema2 = Schema.builder().addInt64Field("bar").build();
    schema1 = assertSchemasAreNotEquivalent(schema1, schema2);
    schema2 = Schema.builder().addNullableField("foo", FieldType.INT64).build();
    assertNotEquals(schema1, schema2);
    assertFalse(schema1.equivalent(schema2));
  }

private Schema assertSchemasAreNotEquivalent(Schema schema1, Schema schema2) {
	assertNotEquals(schema1, schema2);
    assertFalse(schema1.equivalent(schema2));

    schema1 = Schema.builder().addInt64Field("foo").build();
	return schema1;
}

  @Test
  public void testFieldsWithDifferentMetadataAreEquivalent() {
    Field foo = Field.of("foo", FieldType.STRING);
    Field fooWithMetadata = Field.of("foo", FieldType.STRING.withMetadata("key", "value"));

    Schema schema1 = Schema.builder().addField(foo).build();
    Schema schema2 = Schema.builder().addField(foo).build();
    assertEquals(schema1, schema2);
    assertTrue(schema1.equivalent(schema2));

    schema1 = Schema.builder().addField(foo).build();
    schema2 = Schema.builder().addField(fooWithMetadata).build();
    assertNotEquals(schema1, schema2);
    assertTrue(schema1.equivalent(schema2));
  }

  @Test
  public void testNestedNotEquivalent() {
    Schema nestedSchema1 = Schema.builder().addInt64Field("foo").build();
    Schema nestedSchema2 = Schema.builder().addStringField("foo").build();

    Schema schema1 = Schema.builder().addRowField("foo", nestedSchema1).build();
    Schema schema2 = Schema.builder().addRowField("foo", nestedSchema2).build();
    assertNotEquals(schema1, schema2);
    assertFalse(schema1.equivalent(schema2));
  }

  @Test
  public void testArrayNotEquivalent() {
    Schema schema1 = Schema.builder().addArrayField("foo", FieldType.BOOLEAN).build();
    Schema schema2 = Schema.builder().addArrayField("foo", FieldType.DATETIME).build();
    assertNotEquals(schema1, schema2);
    assertFalse(schema1.equivalent(schema2));
  }

  @Test
  public void testNestedArraysNotEquivalent() {
    Schema nestedSchema1 = Schema.builder().addInt64Field("foo").build();
    Schema nestedSchema2 = Schema.builder().addStringField("foo").build();

    Schema schema1 = Schema.builder().addArrayField("foo", FieldType.row(nestedSchema1)).build();
    Schema schema2 = Schema.builder().addArrayField("foo", FieldType.row(nestedSchema2)).build();
    assertNotEquals(schema1, schema2);
    assertFalse(schema1.equivalent(schema2));
  }

  @Test
  public void testMapNotEquivalent() {
    Schema schema1 =
        Schema.builder().addMapField("foo", FieldType.STRING, FieldType.BOOLEAN).build();
    Schema schema2 =
        Schema.builder().addMapField("foo", FieldType.DATETIME, FieldType.BOOLEAN).build();
    assertNotEquals(schema1, schema2);
    assertFalse(schema1.equivalent(schema2));

    schema1 = Schema.builder().addMapField("foo", FieldType.STRING, FieldType.BOOLEAN).build();
    schema2 = Schema.builder().addMapField("foo", FieldType.STRING, FieldType.STRING).build();
    assertNotEquals(schema1, schema2);
    assertFalse(schema1.equivalent(schema2));
  }

  @Test
  public void testNestedMapsNotEquivalent() {
    Schema nestedSchema1 = Schema.builder().addInt64Field("foo").build();
    Schema nestedSchema2 = Schema.builder().addStringField("foo").build();

    Schema schema1 =
        Schema.builder().addMapField("foo", FieldType.STRING, FieldType.row(nestedSchema1)).build();
    Schema schema2 =
        Schema.builder().addMapField("foo", FieldType.STRING, FieldType.row(nestedSchema2)).build();
    assertNotEquals(schema1, schema2);
    assertFalse(schema1.equivalent(schema2));
  }

  static class TestType extends PassThroughLogicalType<Long> {
    TestType(String id, String arg) {
      super(id, FieldType.STRING, arg, FieldType.INT64);
    }
  }

  @Test
  public void testLogicalType() {
    Schema schema1 =
        Schema.builder().addLogicalTypeField("logical", new TestType("id", "arg")).build();
    Schema schema2 =
        Schema.builder().addLogicalTypeField("logical", new TestType("id", "arg")).build();
    assertEquals(schema1, schema2); 

    Schema schema3 =
        Schema.builder()
            .addNullableField("logical", Schema.FieldType.logicalType(new TestType("id", "arg")))
            .build();
    assertNotEquals(schema1, schema3); 

    Schema schema4 =
        Schema.builder().addLogicalTypeField("logical", new TestType("id2", "arg")).build();
    assertNotEquals(schema1, schema4); 

    Schema schema5 =
        Schema.builder().addLogicalTypeField("logical", new TestType("id", "arg2")).build();
    assertNotEquals(schema1, schema5); 
  }

  @Test
  public void testTypesEquality() {
    Schema schema1 = Schema.builder().addStringField("foo").build();
    Schema schema2 = Schema.builder().addStringField("bar").build();
    assertTrue(schema1.typesEqual(schema2)); 

    Schema schema3 = Schema.builder().addNullableField("foo", FieldType.STRING).build();
    assertFalse(schema1.typesEqual(schema3)); 

    Schema schema4 = Schema.builder().addInt32Field("foo").build();
    assertFalse(schema1.typesEqual(schema4)); 
  }

  @Test
  public void testIllegalIndexOf() {
    Schema schema = Schema.builder().addStringField("foo").build();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Cannot find field bar in schema " + schema);

    schema.indexOf("bar");
  }

  @Test
  public void testIllegalNameOf() {
    Schema schema = Schema.builder().addStringField("foo").build();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Cannot find field 1");

    schema.nameOf(1);
  }

  @Test
  public void testFieldTypeToString() {
    assertEquals("STRING NOT NULL", FieldType.STRING.toString());
    assertEquals("INT64", FieldType.INT64.withNullable(true).toString());
    assertEquals("ARRAY<INT32 NOT NULL> NOT NULL", FieldType.array(FieldType.INT32).toString());
    assertEquals(
        "MAP<INT16 NOT NULL, FLOAT> NOT NULL",
        FieldType.map(FieldType.INT16, FieldType.FLOAT.withNullable(true)).toString());
    assertEquals(
        "ROW<field1 BYTES NOT NULL, time DATETIME>",
        FieldType.row(
                Schema.builder()
                    .addByteArrayField("field1")
                    .addField("time", FieldType.DATETIME.withNullable(true))
                    .build())
            .withNullable(true)
            .toString());
  }
}
