package org.apache.beam.sdk.io;

import java.util.Arrays;
import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TextIO_ReadFiles extends TextIO.ReadFiles {

  private final long desiredBundleSizeBytes;

  private final byte @Nullable [] delimiter;

  private final int skipHeaderLines;

  private AutoValue_TextIO_ReadFiles(
      long desiredBundleSizeBytes,
      byte @Nullable [] delimiter,
      int skipHeaderLines) {
    this.desiredBundleSizeBytes = desiredBundleSizeBytes;
    this.delimiter = delimiter;
    this.skipHeaderLines = skipHeaderLines;
  }

  @Override
  long getDesiredBundleSizeBytes() {
    return desiredBundleSizeBytes;
  }

  @SuppressWarnings("mutable")
  @Override
  byte @Nullable [] getDelimiter() {
    return delimiter;
  }

  @Override
  int getSkipHeaderLines() {
    return skipHeaderLines;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TextIO.ReadFiles) {
      TextIO.ReadFiles that = (TextIO.ReadFiles) o;
      return this.desiredBundleSizeBytes == that.getDesiredBundleSizeBytes()
          && Arrays.equals(this.delimiter, (that instanceof AutoValue_TextIO_ReadFiles) ? ((AutoValue_TextIO_ReadFiles) that).delimiter : that.getDelimiter())
          && this.skipHeaderLines == that.getSkipHeaderLines();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (int) ((desiredBundleSizeBytes >>> 32) ^ desiredBundleSizeBytes);
    h$ *= 1000003;
    h$ ^= Arrays.hashCode(delimiter);
    h$ *= 1000003;
    h$ ^= skipHeaderLines;
    return h$;
  }

  @Override
  TextIO.ReadFiles.Builder toBuilder() {
    return new AutoValue_TextIO_ReadFiles.Builder(this);
  }

  static final class Builder extends TextIO.ReadFiles.Builder {
    private long desiredBundleSizeBytes;
    private byte @Nullable [] delimiter;
    private int skipHeaderLines;
    private byte set$0;
    Builder() {
    }
    Builder(TextIO.ReadFiles source) {
      this.desiredBundleSizeBytes = source.getDesiredBundleSizeBytes();
      this.delimiter = source.getDelimiter();
      this.skipHeaderLines = source.getSkipHeaderLines();
      set$0 = (byte) 3;
    }
    @Override
    TextIO.ReadFiles.Builder setDesiredBundleSizeBytes(long desiredBundleSizeBytes) {
      this.desiredBundleSizeBytes = desiredBundleSizeBytes;
      set$0 |= (byte) 1;
      return this;
    }
    @Override
    TextIO.ReadFiles.Builder setDelimiter(byte @Nullable [] delimiter) {
      this.delimiter = delimiter;
      return this;
    }
    @Override
    TextIO.ReadFiles.Builder setSkipHeaderLines(int skipHeaderLines) {
      this.skipHeaderLines = skipHeaderLines;
      set$0 |= (byte) 2;
      return this;
    }
    @Override
    TextIO.ReadFiles build() {
      if (set$0 != 3) {
        StringBuilder missing = new StringBuilder();
        if ((set$0 & 1) == 0) {
          missing.append(" desiredBundleSizeBytes");
        }
        if ((set$0 & 2) == 0) {
          missing.append(" skipHeaderLines");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_TextIO_ReadFiles(
          this.desiredBundleSizeBytes,
          this.delimiter,
          this.skipHeaderLines);
    }
  }

}
