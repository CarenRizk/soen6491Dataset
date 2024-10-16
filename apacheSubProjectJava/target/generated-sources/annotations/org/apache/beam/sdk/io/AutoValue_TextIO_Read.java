package org.apache.beam.sdk.io;

import java.util.Arrays;
import javax.annotation.Generated;
import org.apache.beam.sdk.options.ValueProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TextIO_Read extends TextIO.Read {

  private final @Nullable ValueProvider<String> filepattern;

  private final FileIO.MatchConfiguration matchConfiguration;

  private final boolean hintMatchesManyFiles;

  private final Compression compression;

  private final byte @Nullable [] delimiter;

  private final int skipHeaderLines;

  private AutoValue_TextIO_Read(
      @Nullable ValueProvider<String> filepattern,
      FileIO.MatchConfiguration matchConfiguration,
      boolean hintMatchesManyFiles,
      Compression compression,
      byte @Nullable [] delimiter,
      int skipHeaderLines) {
    this.filepattern = filepattern;
    this.matchConfiguration = matchConfiguration;
    this.hintMatchesManyFiles = hintMatchesManyFiles;
    this.compression = compression;
    this.delimiter = delimiter;
    this.skipHeaderLines = skipHeaderLines;
  }

  @Override
  @Nullable ValueProvider<String> getFilepattern() {
    return filepattern;
  }

  @Override
  FileIO.MatchConfiguration getMatchConfiguration() {
    return matchConfiguration;
  }

  @Override
  boolean getHintMatchesManyFiles() {
    return hintMatchesManyFiles;
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
    if (o instanceof TextIO.Read) {
      TextIO.Read that = (TextIO.Read) o;
      return (this.filepattern == null ? that.getFilepattern() == null : this.filepattern.equals(that.getFilepattern()))
          && this.matchConfiguration.equals(that.getMatchConfiguration())
          && this.hintMatchesManyFiles == that.getHintMatchesManyFiles()
          && this.compression.equals(that.getCompression())
          && Arrays.equals(this.delimiter, (that instanceof AutoValue_TextIO_Read) ? ((AutoValue_TextIO_Read) that).delimiter : that.getDelimiter())
          && this.skipHeaderLines == that.getSkipHeaderLines();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (filepattern == null) ? 0 : filepattern.hashCode();
    h$ *= 1000003;
    h$ ^= matchConfiguration.hashCode();
    h$ *= 1000003;
    h$ ^= hintMatchesManyFiles ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= compression.hashCode();
    h$ *= 1000003;
    h$ ^= Arrays.hashCode(delimiter);
    h$ *= 1000003;
    h$ ^= skipHeaderLines;
    return h$;
  }

  @Override
  TextIO.Read.Builder toBuilder() {
    return new AutoValue_TextIO_Read.Builder(this);
  }

  static final class Builder extends TextIO.Read.Builder {
    private @Nullable ValueProvider<String> filepattern;
    private FileIO.@Nullable MatchConfiguration matchConfiguration;
    private boolean hintMatchesManyFiles;
    private @Nullable Compression compression;
    private byte @Nullable [] delimiter;
    private int skipHeaderLines;
    private byte set$0;
    Builder() {
    }
    Builder(TextIO.Read source) {
      this.filepattern = source.getFilepattern();
      this.matchConfiguration = source.getMatchConfiguration();
      this.hintMatchesManyFiles = source.getHintMatchesManyFiles();
      this.compression = source.getCompression();
      this.delimiter = source.getDelimiter();
      this.skipHeaderLines = source.getSkipHeaderLines();
      set$0 = (byte) 3;
    }
    @Override
    TextIO.Read.Builder setFilepattern(ValueProvider<String> filepattern) {
      this.filepattern = filepattern;
      return this;
    }
    @Override
    TextIO.Read.Builder setMatchConfiguration(FileIO.MatchConfiguration matchConfiguration) {
      if (matchConfiguration == null) {
        throw new NullPointerException("Null matchConfiguration");
      }
      this.matchConfiguration = matchConfiguration;
      return this;
    }
    @Override
    TextIO.Read.Builder setHintMatchesManyFiles(boolean hintMatchesManyFiles) {
      this.hintMatchesManyFiles = hintMatchesManyFiles;
      set$0 |= (byte) 1;
      return this;
    }
    @Override
    TextIO.Read.Builder setCompression(Compression compression) {
      if (compression == null) {
        throw new NullPointerException("Null compression");
      }
      this.compression = compression;
      return this;
    }
    @Override
    TextIO.Read.Builder setDelimiter(byte @Nullable [] delimiter) {
      this.delimiter = delimiter;
      return this;
    }
    @Override
    TextIO.Read.Builder setSkipHeaderLines(int skipHeaderLines) {
      this.skipHeaderLines = skipHeaderLines;
      set$0 |= (byte) 2;
      return this;
    }
    @Override
    TextIO.Read build() {
      if (set$0 != 3
          || this.matchConfiguration == null
          || this.compression == null) {
        StringBuilder missing = new StringBuilder();
        if (this.matchConfiguration == null) {
          missing.append(" matchConfiguration");
        }
        if ((set$0 & 1) == 0) {
          missing.append(" hintMatchesManyFiles");
        }
        if (this.compression == null) {
          missing.append(" compression");
        }
        if ((set$0 & 2) == 0) {
          missing.append(" skipHeaderLines");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_TextIO_Read(
          this.filepattern,
          this.matchConfiguration,
          this.hintMatchesManyFiles,
          this.compression,
          this.delimiter,
          this.skipHeaderLines);
    }
  }

}
