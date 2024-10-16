package org.apache.beam.sdk.io;

import java.util.Arrays;
import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TextIO_ReadAll extends TextIO.ReadAll {

  private final FileIO.MatchConfiguration matchConfiguration;

  private final Compression compression;

  private final byte @Nullable [] delimiter;

  private final int skipHeaderLines;

  private AutoValue_TextIO_ReadAll(
      FileIO.MatchConfiguration matchConfiguration,
      Compression compression,
      byte @Nullable [] delimiter,
      int skipHeaderLines) {
    this.matchConfiguration = matchConfiguration;
    this.compression = compression;
    this.delimiter = delimiter;
    this.skipHeaderLines = skipHeaderLines;
  }

  @Override
  FileIO.MatchConfiguration getMatchConfiguration() {
    return matchConfiguration;
  }

  @Override
  Compression getCompression() {
    return compression;
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
    if (o instanceof TextIO.ReadAll) {
      TextIO.ReadAll that = (TextIO.ReadAll) o;
      return this.matchConfiguration.equals(that.getMatchConfiguration())
          && this.compression.equals(that.getCompression())
          && Arrays.equals(this.delimiter, (that instanceof AutoValue_TextIO_ReadAll) ? ((AutoValue_TextIO_ReadAll) that).delimiter : that.getDelimiter())
          && this.skipHeaderLines == that.getSkipHeaderLines();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= matchConfiguration.hashCode();
    h$ *= 1000003;
    h$ ^= compression.hashCode();
    h$ *= 1000003;
    h$ ^= Arrays.hashCode(delimiter);
    h$ *= 1000003;
    h$ ^= skipHeaderLines;
    return h$;
  }

  @Override
  TextIO.ReadAll.Builder toBuilder() {
    return new AutoValue_TextIO_ReadAll.Builder(this);
  }

  static final class Builder extends TextIO.ReadAll.Builder {
    private FileIO.@Nullable MatchConfiguration matchConfiguration;
    private @Nullable Compression compression;
    private byte @Nullable [] delimiter;
    private int skipHeaderLines;
    private byte set$0;
    Builder() {
    }
    Builder(TextIO.ReadAll source) {
      this.matchConfiguration = source.getMatchConfiguration();
      this.compression = source.getCompression();
      this.delimiter = source.getDelimiter();
      this.skipHeaderLines = source.getSkipHeaderLines();
      set$0 = (byte) 1;
    }
    @Override
    TextIO.ReadAll.Builder setMatchConfiguration(FileIO.MatchConfiguration matchConfiguration) {
      if (matchConfiguration == null) {
        throw new NullPointerException("Null matchConfiguration");
      }
      this.matchConfiguration = matchConfiguration;
      return this;
    }
    @Override
    TextIO.ReadAll.Builder setCompression(Compression compression) {
      if (compression == null) {
        throw new NullPointerException("Null compression");
      }
      this.compression = compression;
      return this;
    }
    @Override
    TextIO.ReadAll.Builder setDelimiter(byte @Nullable [] delimiter) {
      this.delimiter = delimiter;
      return this;
    }
    @Override
    TextIO.ReadAll.Builder setSkipHeaderLines(int skipHeaderLines) {
      this.skipHeaderLines = skipHeaderLines;
      set$0 |= (byte) 1;
      return this;
    }
    @Override
    TextIO.ReadAll build() {
      if (set$0 != 1
          || this.matchConfiguration == null
          || this.compression == null) {
        StringBuilder missing = new StringBuilder();
        if (this.matchConfiguration == null) {
          missing.append(" matchConfiguration");
        }
        if (this.compression == null) {
          missing.append(" compression");
        }
        if ((set$0 & 1) == 0) {
          missing.append(" skipHeaderLines");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_TextIO_ReadAll(
          this.matchConfiguration,
          this.compression,
          this.delimiter,
          this.skipHeaderLines);
    }
  }

}
