package org.apache.beam.sdk.coders;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class CoderTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testContextEqualsAndHashCode() {
    assertEquals(Context.NESTED, new Context(false));
    assertEquals(Context.OUTER, new Context(true));
    assertNotEquals(Context.NESTED, Context.OUTER);

    assertEquals(Context.NESTED.hashCode(), new Context(false).hashCode());
    assertEquals(Context.OUTER.hashCode(), new Context(true).hashCode());
    
    
    assertNotEquals(Context.NESTED.hashCode(), Context.OUTER.hashCode());
  }

  @Test
  public void testContextToString() {
    assertEquals("Context{NESTED}", Context.NESTED.toString());
    assertEquals("Context{OUTER}", Context.OUTER.toString());
  }

  @Test
  public void testNonDeterministicExceptionRequiresReason() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Reasons must not be empty");
    new NonDeterministicException(VoidCoder.of(), Collections.emptyList());
  }

  @Test
  public void testNonDeterministicException() {
    NonDeterministicException rootCause =
        new NonDeterministicException(VoidCoder.of(), "Root Cause");
    NonDeterministicException exception =
        new NonDeterministicException(StringUtf8Coder.of(), "Problem", rootCause);
    assertEquals(rootCause, exception.getCause());
    assertThat(exception.getReasons(), contains("Problem"));
    assertThat(exception.toString(), containsString("Problem"));
    assertThat(exception.toString(), containsString("is not deterministic"));
  }

  @Test
  public void testNonDeterministicExceptionMultipleReasons() {
    NonDeterministicException rootCause =
        new NonDeterministicException(VoidCoder.of(), "Root Cause");
    NonDeterministicException exception =
        new NonDeterministicException(
            StringUtf8Coder.of(), Arrays.asList("Problem1", "Problem2"), rootCause);

    String expectedMessage =
        "StringUtf8Coder is not deterministic because:\n\tProblem1\n\tProblem2";

    assertThat(exception.getMessage(), equalTo(expectedMessage));
  }

  @Test
  public void testTypeIsPreserved() throws Exception {
    assertThat(VoidCoder.of().getEncodedTypeDescriptor(), equalTo(TypeDescriptor.of(Void.class)));
  }
}
