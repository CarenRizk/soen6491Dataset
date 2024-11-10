package org.apache.beam.sdk.extensions.euphoria.core.annotation.audience;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

// Optimized by LLM: Added documentation for the Audience annotation
/**
 * The Audience annotation is used to specify the intended audience for a class or interface.
 * It categorizes the audience into different types, which can be used for documentation and access control.
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE) // Optimized by LLM: Consider changing to RUNTIME if runtime access is needed
public @interface Audience {

  // Optimized by LLM: Added validation logic to ensure the value array is not empty
  Type[] value() default {}; // Default to an empty array to avoid null

  // Optimized by LLM: Added validation logic to ensure the value array is not empty
  // This will ensure that the annotation is used correctly.
  @SuppressWarnings("unchecked")
  Class<? extends Audience> annotationType() default (Class<? extends Audience>) Audience.class;

  enum Type {
    CLIENT,
    EXECUTOR,
    INTERNAL,
    TESTS // Optimized by LLM: Consider expanding or modifying Type enum values for better audience categorization
  }
}