/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.mongodb;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongoCmdOptions;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test on the MongoDbIO. */
@RunWith(JUnit4.class)
public class MongoDbIOTest {
  private static final Logger LOG = LoggerFactory.getLogger(MongoDbIOTest.class);

  @ClassRule public static final TemporaryFolder MONGODB_LOCATION = new TemporaryFolder();
  private static final String DATABASE_NAME = "beam";
  private static final String COLLECTION_NAME = "test";
  private static final String VIEW_NAME = "test_view";

  private static final MongodStarter mongodStarter = MongodStarter.getDefaultInstance();
  private static MongodExecutable mongodExecutable;
  private static MongodProcess mongodProcess;
  private static MongoClient client;
  private static MongoDatabase database;

  private static int port;

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() throws Exception {
    port = NetworkTestHelper.getAvailableLocalPort();
    LOG.info("Starting MongoDB embedded instance on {}", port);
    MongodConfig mongodConfig =
        MongodConfig.builder()
            .version(Version.Main.PRODUCTION)
            .isConfigServer(false)
            .replication(new Storage(MONGODB_LOCATION.getRoot().getPath(), null, 0))
            .net(new Net("localhost", port, Network.localhostIsIPv6()))
            .cmdOptions(
                MongoCmdOptions.builder()
                    .syncDelay(10)
                    .useNoPrealloc(true)
                    .useSmallFiles(true)
                    .useNoJournal(true)
                    .isVerbose(false)
                    .build())
            .build();
    mongodExecutable = mongodStarter.prepare(mongodConfig);
    mongodProcess = mongodExecutable.start();
    client = new MongoClient("localhost", port);
    database = client.getDatabase(DATABASE_NAME);

    LOG.info("Insert test data");
    List<Document> documents = createDocuments(1000, false);
    MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);
    collection.insertMany(documents);

    database.createView(VIEW_NAME, COLLECTION_NAME, Collections.emptyList());
  }

  @AfterClass
  public static void afterClass() {
    LOG.info("Stopping MongoDB instance");
    client.close();
    mongodProcess.stop();
    mongodExecutable.stop();
  }

  @Test
  public void testSplitIntoFilters() {
    // A single split will result in two filters
    ArrayList<Document> documents = new ArrayList<>();
    documents.add(new Document("_id", 56));
    List<String> filters = MongoDbIO.BoundedMongoDbSource.splitKeysToFilters(documents);
    assertEquals(2, filters.size());
    assertEquals("{ $and: [ {\"_id\":{$lte:ObjectId(\"56\")}} ]}", filters.get(0));
    assertEquals("{ $and: [ {\"_id\":{$gt:ObjectId(\"56\")}} ]}", filters.get(1));

    // Add two more splits; now we should have 4 filters
    documents.add(new Document("_id", 109));
    documents.add(new Document("_id", 256));
    filters = MongoDbIO.BoundedMongoDbSource.splitKeysToFilters(documents);
    assertEquals(4, filters.size());
    assertEquals("{ $and: [ {\"_id\":{$lte:ObjectId(\"56\")}} ]}", filters.get(0));
    assertEquals(
        "{ $and: [ {\"_id\":{$gt:ObjectId(\"56\"),$lte:ObjectId(\"109\")}} ]}", filters.get(1));
    assertEquals(
        "{ $and: [ {\"_id\":{$gt:ObjectId(\"109\"),$lte:ObjectId(\"256\")}} ]}", filters.get(2));
    assertEquals("{ $and: [ {\"_id\":{$gt:ObjectId(\"256\")}} ]}", filters.get(3));
  }

  @Test
  public void testSplitIntoBucket() {
    // a single split should result in two buckets
    ArrayList<Document> documents = new ArrayList<>();
    documents.add(new Document("_id", new ObjectId("52cc8f6254c5317943000005")));
    List<BsonDocument> buckets = MongoDbIO.BoundedMongoDbSource.splitKeysToMatch(documents);
    assertEquals(2, buckets.size());
    assertEquals(
        "{\"$match\": {\"_id\": {\"$lte\": {\"$oid\": \"52cc8f6254c5317943000005\"}}}}",
        buckets.get(0).toString());
    assertEquals(
        "{\"$match\": {\"_id\": {\"$gt\": {\"$oid\": \"52cc8f6254c5317943000005\"}}}}",
        buckets.get(1).toString());

    // add more splits and verify the buckets
    documents.add(new Document("_id", new ObjectId("52cc8f6254c5317943000007")));
    documents.add(new Document("_id", new ObjectId("54242e9e54c531ef8800001f")));
    buckets = MongoDbIO.BoundedMongoDbSource.splitKeysToMatch(documents);
    assertEquals(4, buckets.size());
    assertEquals(
        "{\"$match\": {\"_id\": {\"$lte\": {\"$oid\": \"52cc8f6254c5317943000005\"}}}}",
        buckets.get(0).toString());
    assertEquals(
        "{\"$match\": {\"_id\": {\"$gt\": {\"$oid\": \"52cc8f6254c5317943000005\"}, \"$lte\": {\"$oid\": \"52cc8f6254c5317943000007\"}}}}",
        buckets.get(1).toString());
    assertEquals(
        "{\"$match\": {\"_id\": {\"$gt\": {\"$oid\": \"52cc8f6254c5317943000007\"}, \"$lte\": {\"$oid\": \"54242e9e54c531ef8800001f\"}}}}",
        buckets.get(2).toString());
    assertEquals(
        "{\"$match\": {\"_id\": {\"$gt\": {\"$oid\": \"54242e9e54c531ef8800001f\"}}}}",
        buckets.get(3).toString());
  }

  private static List<Document> createDocuments(final int n, boolean addId) {
    final String[] scientists =
        new String[] {
          "Einstein",
          "Darwin",
          "Copernicus",
          "Pasteur",
          "Curie",
          "Faraday",
          "Newton",
          "Bohr",
          "Galilei",
          "Maxwell"
        };
    final String[] country =
        new String[] {
          "Germany",
          "England",
          "Poland",
          "France",
          "France",
          "England",
          "England",
          "Denmark",
          "Florence",
          "Scotland"
        };
    List<Document> documents = new ArrayList<>();
    for (int i = 1; i <= n; i++) {
      int index = i % scientists.length;
      Document document = new Document();
      if (addId) {
        document.append("_id", i);
      }
      document.append("scientist", scientists[index]);
      document.append("country", country[index]);
      documents.add(document);
    }
    return documents;
  }

  private static int countElements(final String collectionName) {
    return Iterators.size(database.getCollection(collectionName).find().iterator());
  }

  static class DocumentToKVFn extends SimpleFunction<Document, KV<String, Void>> {
    @Override
    public KV<String, Void> apply(Document input) {
      return KV.of(input.getString("scientist"), null);
    }
  }
}