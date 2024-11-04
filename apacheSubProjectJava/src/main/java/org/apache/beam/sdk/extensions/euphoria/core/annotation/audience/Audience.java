package org.apache.beam.sdk.extensions.euphoria.core.annotation.audience;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
public @interface Audience {

  Type[] value();

  enum Type {
    CLIENT,
    EXECUTOR,
    INTERNAL,
    TESTS
  }
}
