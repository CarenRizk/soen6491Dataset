package org.apache.beam.sdk.coders;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects; // Optimized by LLM: Suggestion 2
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.CountingOutputStream;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class Coder<T> implements Serializable {
  @Deprecated
  public static class Context {
    public static final Context OUTER = new Context(true);
    public static final Context NESTED = new Context(false);
    public final boolean isWholeStream;

    public Context(boolean isWholeStream) {
      this.isWholeStream = isWholeStream;
    }

    public Context nested() {
      return NESTED;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (!(obj instanceof Context)) {
        return false;
      }
      return Objects.equals(isWholeStream, ((Context) obj).isWholeStream); // Optimized by LLM: Suggestion 2
    }

    @Override
    public int hashCode() {
      return Objects.hash(isWholeStream); // Optimized by LLM: Suggestion 2
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(Context.class)
          .addValue(isWholeStream ? "OUTER" : "NESTED")
          .toString();
    }
  }
  public abstract void encode(T value, OutputStream outStream) throws CoderException, IOException;

  @Deprecated
  public void encode(T value, OutputStream outStream, Context context)
      throws CoderException, IOException {
    encode(value, outStream);
  }

  public abstract T decode(InputStream inStream) throws CoderException, IOException;

  @Deprecated
  public T decode(InputStream inStream, Context context) throws CoderException, IOException {
    return decode(inStream);
  }

  public abstract List<? extends Coder<?>> getCoderArguments();

  public abstract void verifyDeterministic() throws Coder.NonDeterministicException;

  public static void verifyDeterministic(Coder<?> target, String message, Iterable<Coder<?>> coders)
      throws NonDeterministicException {
    for (Coder<?> coder : coders) {
      try {
        coder.verifyDeterministic();
      } catch (NonDeterministicException e) {
        throw new NonDeterministicException(target, message, e);
      }
    }
  }

  public static void verifyDeterministic(Coder<?> target, String message, Coder<?>... coders)
      throws NonDeterministicException {
    verifyDeterministic(target, message, Arrays.asList(coders));
  }

  public boolean consistentWithEquals() {
    return false;
  }

  // Optimized by LLM: Suggestion 4
  private String constructErrorMessage(T value) {
    return "Unable to encode element '" + value + "' with coder '" + this + "'.";
  }

  public Object structuralValue(T value) {
    if (value != null && consistentWithEquals()) {
      return value;
    } else {
      try {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        encode(value, os, Context.OUTER);
        return new StructuralByteArray(os.toByteArray());
      } catch (Exception exn) {
        throw new IllegalArgumentException(constructErrorMessage(value), exn); // Optimized by LLM: Suggestion 4
      }
    }
  }
  public boolean isRegisterByteSizeObserverCheap(T value) {
    return false;
  }

  public void registerByteSizeObserver(T value, ElementByteSizeObserver observer) throws Exception {
    observer.update(getEncodedElementByteSize(value));
  }

  protected long getEncodedElementByteSize(T value) {
    try (CountingOutputStream os = new CountingOutputStream(ByteStreams.nullOutputStream())) {
      encode(value, os);
      return os.getCount();
    } catch (Exception exn) {
      throw new IllegalArgumentException(constructErrorMessage(value), exn); // Optimized by LLM: Suggestion 4
    }
  }

  public TypeDescriptor<T> getEncodedTypeDescriptor() {
    return (TypeDescriptor<T>)
        TypeDescriptor.of(getClass()).resolveType(new TypeDescriptor<T>() {}.getType());
  }

  public static class NonDeterministicException extends Exception {
    private final Coder<?> coder;
    private final List<String> reasons;

    public NonDeterministicException(
        Coder<?> coder, String reason, @Nullable NonDeterministicException e) {
      this(coder, List.of(reason), e); // Optimized by LLM: Suggestion 6
    }

    public NonDeterministicException(Coder<?> coder, String reason) {
      this(coder, List.of(reason), null); // Optimized by LLM: Suggestion 6
    }

    public NonDeterministicException(Coder<?> coder, List<String> reasons) {
      this(coder, reasons, null);
    }

    public NonDeterministicException(
        Coder<?> coder, List<String> reasons, @Nullable NonDeterministicException cause) {
      super(cause);
      checkArgument(!reasons.isEmpty(), "Reasons must not be empty.");
      this.reasons = reasons;
      this.coder = coder;
    }

    public Iterable<String> getReasons() {
      return reasons;
    }

    @Override
    public String getMessage() {
      String reasonsStr = Joiner.on("\n\t").join(reasons);
      return coder + " is not deterministic because:\n\t" + reasonsStr;
    }
  }
}