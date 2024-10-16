package org.apache.beam.sdk.io.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.bson.Document;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_MongoDbIO_Read extends MongoDbIO.Read {

  private final @Nullable String uri;

  private final int maxConnectionIdleTime;

  private final boolean sslEnabled;

  private final boolean sslInvalidHostNameAllowed;

  private final boolean ignoreSSLCertificate;

  private final @Nullable String database;

  private final @Nullable String collection;

  private final int numSplits;

  private final boolean bucketAuto;

  private final SerializableFunction<MongoCollection<Document>, MongoCursor<Document>> queryFn;

  private AutoValue_MongoDbIO_Read(
      @Nullable String uri,
      int maxConnectionIdleTime,
      boolean sslEnabled,
      boolean sslInvalidHostNameAllowed,
      boolean ignoreSSLCertificate,
      @Nullable String database,
      @Nullable String collection,
      int numSplits,
      boolean bucketAuto,
      SerializableFunction<MongoCollection<Document>, MongoCursor<Document>> queryFn) {
    this.uri = uri;
    this.maxConnectionIdleTime = maxConnectionIdleTime;
    this.sslEnabled = sslEnabled;
    this.sslInvalidHostNameAllowed = sslInvalidHostNameAllowed;
    this.ignoreSSLCertificate = ignoreSSLCertificate;
    this.database = database;
    this.collection = collection;
    this.numSplits = numSplits;
    this.bucketAuto = bucketAuto;
    this.queryFn = queryFn;
  }

  @Pure
  @Override
  @Nullable String uri() {
    return uri;
  }

  @Pure
  @Override
  int maxConnectionIdleTime() {
    return maxConnectionIdleTime;
  }

  @Pure
  @Override
  boolean sslEnabled() {
    return sslEnabled;
  }

  @Pure
  @Override
  boolean sslInvalidHostNameAllowed() {
    return sslInvalidHostNameAllowed;
  }

  @Pure
  @Override
  boolean ignoreSSLCertificate() {
    return ignoreSSLCertificate;
  }

  @Pure
  @Override
  @Nullable String database() {
    return database;
  }

  @Pure
  @Override
  @Nullable String collection() {
    return collection;
  }

  @Pure
  @Override
  int numSplits() {
    return numSplits;
  }

  @Pure
  @Override
  boolean bucketAuto() {
    return bucketAuto;
  }

  @Pure
  @Override
  SerializableFunction<MongoCollection<Document>, MongoCursor<Document>> queryFn() {
    return queryFn;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof MongoDbIO.Read) {
      MongoDbIO.Read that = (MongoDbIO.Read) o;
      return (this.uri == null ? that.uri() == null : this.uri.equals(that.uri()))
          && this.maxConnectionIdleTime == that.maxConnectionIdleTime()
          && this.sslEnabled == that.sslEnabled()
          && this.sslInvalidHostNameAllowed == that.sslInvalidHostNameAllowed()
          && this.ignoreSSLCertificate == that.ignoreSSLCertificate()
          && (this.database == null ? that.database() == null : this.database.equals(that.database()))
          && (this.collection == null ? that.collection() == null : this.collection.equals(that.collection()))
          && this.numSplits == that.numSplits()
          && this.bucketAuto == that.bucketAuto()
          && this.queryFn.equals(that.queryFn());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (uri == null) ? 0 : uri.hashCode();
    h$ *= 1000003;
    h$ ^= maxConnectionIdleTime;
    h$ *= 1000003;
    h$ ^= sslEnabled ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= sslInvalidHostNameAllowed ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= ignoreSSLCertificate ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= (database == null) ? 0 : database.hashCode();
    h$ *= 1000003;
    h$ ^= (collection == null) ? 0 : collection.hashCode();
    h$ *= 1000003;
    h$ ^= numSplits;
    h$ *= 1000003;
    h$ ^= bucketAuto ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= queryFn.hashCode();
    return h$;
  }

  @Override
  MongoDbIO.Read.Builder builder() {
    return new AutoValue_MongoDbIO_Read.Builder(this);
  }

  static final class Builder extends MongoDbIO.Read.Builder {
    private @Nullable String uri;
    private int maxConnectionIdleTime;
    private boolean sslEnabled;
    private boolean sslInvalidHostNameAllowed;
    private boolean ignoreSSLCertificate;
    private @Nullable String database;
    private @Nullable String collection;
    private int numSplits;
    private boolean bucketAuto;
    private @Nullable SerializableFunction<MongoCollection<Document>, MongoCursor<Document>> queryFn;
    private byte set$0;
    Builder() {
    }
    Builder(MongoDbIO.Read source) {
      this.uri = source.uri();
      this.maxConnectionIdleTime = source.maxConnectionIdleTime();
      this.sslEnabled = source.sslEnabled();
      this.sslInvalidHostNameAllowed = source.sslInvalidHostNameAllowed();
      this.ignoreSSLCertificate = source.ignoreSSLCertificate();
      this.database = source.database();
      this.collection = source.collection();
      this.numSplits = source.numSplits();
      this.bucketAuto = source.bucketAuto();
      this.queryFn = source.queryFn();
      set$0 = (byte) 0x3f;
    }
    @Override
    MongoDbIO.Read.Builder setUri(String uri) {
      this.uri = uri;
      return this;
    }
    @Override
    MongoDbIO.Read.Builder setMaxConnectionIdleTime(int maxConnectionIdleTime) {
      this.maxConnectionIdleTime = maxConnectionIdleTime;
      set$0 |= (byte) 1;
      return this;
    }
    @Override
    MongoDbIO.Read.Builder setSslEnabled(boolean sslEnabled) {
      this.sslEnabled = sslEnabled;
      set$0 |= (byte) 2;
      return this;
    }
    @Override
    MongoDbIO.Read.Builder setSslInvalidHostNameAllowed(boolean sslInvalidHostNameAllowed) {
      this.sslInvalidHostNameAllowed = sslInvalidHostNameAllowed;
      set$0 |= (byte) 4;
      return this;
    }
    @Override
    MongoDbIO.Read.Builder setIgnoreSSLCertificate(boolean ignoreSSLCertificate) {
      this.ignoreSSLCertificate = ignoreSSLCertificate;
      set$0 |= (byte) 8;
      return this;
    }
    @Override
    MongoDbIO.Read.Builder setDatabase(String database) {
      this.database = database;
      return this;
    }
    @Override
    MongoDbIO.Read.Builder setCollection(String collection) {
      this.collection = collection;
      return this;
    }
    @Override
    MongoDbIO.Read.Builder setNumSplits(int numSplits) {
      this.numSplits = numSplits;
      set$0 |= (byte) 0x10;
      return this;
    }
    @Override
    MongoDbIO.Read.Builder setBucketAuto(boolean bucketAuto) {
      this.bucketAuto = bucketAuto;
      set$0 |= (byte) 0x20;
      return this;
    }
    @Override
    MongoDbIO.Read.Builder setQueryFn(SerializableFunction<MongoCollection<Document>, MongoCursor<Document>> queryFn) {
      if (queryFn == null) {
        throw new NullPointerException("Null queryFn");
      }
      this.queryFn = queryFn;
      return this;
    }
    @Override
    MongoDbIO.Read build() {
      if (set$0 != 0x3f
          || this.queryFn == null) {
        StringBuilder missing = new StringBuilder();
        if ((set$0 & 1) == 0) {
          missing.append(" maxConnectionIdleTime");
        }
        if ((set$0 & 2) == 0) {
          missing.append(" sslEnabled");
        }
        if ((set$0 & 4) == 0) {
          missing.append(" sslInvalidHostNameAllowed");
        }
        if ((set$0 & 8) == 0) {
          missing.append(" ignoreSSLCertificate");
        }
        if ((set$0 & 0x10) == 0) {
          missing.append(" numSplits");
        }
        if ((set$0 & 0x20) == 0) {
          missing.append(" bucketAuto");
        }
        if (this.queryFn == null) {
          missing.append(" queryFn");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_MongoDbIO_Read(
          this.uri,
          this.maxConnectionIdleTime,
          this.sslEnabled,
          this.sslInvalidHostNameAllowed,
          this.ignoreSSLCertificate,
          this.database,
          this.collection,
          this.numSplits,
          this.bucketAuto,
          this.queryFn);
    }
  }

}
