package org.apache.beam.sdk.extensions.euphoria.core.annotation.audience;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** 
 * Indicates the intended audience for a class or interface. 
 * This annotation can be used to specify whether the annotated element is meant for 
 * clients, executors, internal use, or tests.
 * 
 * @see Type
 */
// Optimized by LLM: Added JavaDoc comments to the Audience annotation
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
public @interface Audience {

  Type[] value();

  /** 
   * Specifies the type of audience for the annotated element. 
   * This enum can be extended in the future to include more audience types.
   */
  // Optimized by LLM: Added JavaDoc comments to the Type enum
  enum Type {
    CLIENT,
    EXECUTOR,
    INTERNAL,
    TESTS
  }
}