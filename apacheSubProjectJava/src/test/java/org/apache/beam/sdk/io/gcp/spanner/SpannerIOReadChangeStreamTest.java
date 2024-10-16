
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
package org.apache.beam.sdk.io.gcp.spanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import com.google.auth.Credentials;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Options.RpcPriority;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.MetadataSpannerConfigFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class SpannerIOReadChangeStreamTest {

  private static final String TEST_PROJECT = "my-project";
  private static final String TEST_INSTANCE = "my-instance";
  private static final String TEST_DATABASE = "my-database";
  private static final String TEST_METADATA_INSTANCE = "my-metadata-instance";
  private static final String TEST_METADATA_DATABASE = "my-metadata-database";
  private static final String TEST_METADATA_TABLE = "my-metadata-table";
  private static final String TEST_CHANGE_STREAM = "my-change-stream";

  @Rule public final transient TestPipeline testPipeline = TestPipeline.create();

  private SpannerConfig spannerConfig;
  private SpannerIO.ReadChangeStream readChangeStream;

  @Before
  public void setUp() throws Exception {
	MockitoAnnotations.openMocks(this); // Ensure this line is present
    spannerConfig =
        SpannerConfig.create()
            .withProjectId(TEST_PROJECT)
            .withInstanceId(TEST_INSTANCE)
            .withDatabaseId(TEST_DATABASE);

    Timestamp startTimestamp = Timestamp.now();
    Timestamp endTimestamp =
        Timestamp.ofTimeSecondsAndNanos(
            startTimestamp.getSeconds() + 10, startTimestamp.getNanos());
    readChangeStream =
        SpannerIO.readChangeStream()
            .withSpannerConfig(spannerConfig)
            .withChangeStreamName(TEST_CHANGE_STREAM)
            .withMetadataInstance(TEST_METADATA_INSTANCE)
            .withMetadataDatabase(TEST_METADATA_DATABASE)
            .withMetadataTable(TEST_METADATA_TABLE)
            .withRpcPriority(RpcPriority.MEDIUM)
            .withInclusiveStartAt(startTimestamp)
            .withInclusiveEndAt(endTimestamp);
  }

  @Test
  public void testSetPipelineCredential() {
      // Create an instance of TestCredential to use in assertions
      TestCredential testCredential = new TestCredential();

      // Set the credential in the pipeline options directly, without mocking
      testPipeline.getOptions().as(GcpOptions.class).setGcpCredential(testCredential);

      // Build Spanner configurations
      SpannerConfig changeStreamSpannerConfig = readChangeStream.buildChangeStreamSpannerConfig();
      SpannerConfig metadataSpannerConfig =
          MetadataSpannerConfigFactory.create(
              changeStreamSpannerConfig, TEST_METADATA_INSTANCE, TEST_METADATA_DATABASE);
      
      // Assert that initially the credentials are null
      assertNull(changeStreamSpannerConfig.getCredentials());
      assertNull(metadataSpannerConfig.getCredentials());

      // Build configurations with the credentials set
      SpannerConfig changeStreamSpannerConfigWithCredential =
          SpannerIO.buildSpannerConfigWithCredential(
              changeStreamSpannerConfig, testPipeline.getOptions());
      SpannerConfig metadataSpannerConfigWithCredential =
          SpannerIO.buildSpannerConfigWithCredential(
              metadataSpannerConfig, testPipeline.getOptions());
      
      // Assert that the credentials have been correctly set
      assertEquals(testCredential, changeStreamSpannerConfigWithCredential.getCredentials().get());
      assertEquals(testCredential, metadataSpannerConfigWithCredential.getCredentials().get());
  }

  @Test
  public void testSetSpannerConfigCredential() {
      // Create an instance of TestCredential to use in assertions
      TestCredential testCredential = new TestCredential();

      // Directly use the actual TestCredential without mocking
      spannerConfig = spannerConfig.withCredentials(testCredential); // Ensure this method exists in SpannerConfig
      readChangeStream = readChangeStream.withSpannerConfig(spannerConfig);

      SpannerConfig changeStreamSpannerConfig = readChangeStream.buildChangeStreamSpannerConfig();
      SpannerConfig metadataSpannerConfig =
          MetadataSpannerConfigFactory.create(
              changeStreamSpannerConfig, TEST_METADATA_INSTANCE, TEST_METADATA_DATABASE);

      // Assert that the expected credentials are set in both configurations
      assertEquals(testCredential, changeStreamSpannerConfig.getCredentials().get());
      assertEquals(testCredential, metadataSpannerConfig.getCredentials().get());

      // Test building configurations with credentials
      SpannerConfig changeStreamSpannerConfigWithCredential =
          SpannerIO.buildSpannerConfigWithCredential(
              changeStreamSpannerConfig, testPipeline.getOptions());
      SpannerConfig metadataSpannerConfigWithCredential =
          SpannerIO.buildSpannerConfigWithCredential(
              metadataSpannerConfig, testPipeline.getOptions());

      // Assert the credentials in the built configurations
      assertEquals(testCredential, changeStreamSpannerConfigWithCredential.getCredentials().get());
      assertEquals(testCredential, metadataSpannerConfigWithCredential.getCredentials().get());
  }
}
