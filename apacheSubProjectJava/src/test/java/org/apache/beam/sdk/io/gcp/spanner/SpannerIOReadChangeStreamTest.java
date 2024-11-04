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
	MockitoAnnotations.openMocks(this); 
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
      
      TestCredential testCredential = new TestCredential();

      
      testPipeline.getOptions().as(GcpOptions.class).setGcpCredential(testCredential);

      
      SpannerConfig changeStreamSpannerConfig = readChangeStream.buildChangeStreamSpannerConfig();
      SpannerConfig metadataSpannerConfig =
          MetadataSpannerConfigFactory.create(
              changeStreamSpannerConfig, TEST_METADATA_INSTANCE, TEST_METADATA_DATABASE);
      
      
      assertNull(changeStreamSpannerConfig.getCredentials());
      assertNull(metadataSpannerConfig.getCredentials());

      
      SpannerConfig changeStreamSpannerConfigWithCredential =
          SpannerIO.buildSpannerConfigWithCredential(
              changeStreamSpannerConfig, testPipeline.getOptions());
      SpannerConfig metadataSpannerConfigWithCredential =
          SpannerIO.buildSpannerConfigWithCredential(
              metadataSpannerConfig, testPipeline.getOptions());
      
      
      assertEquals(testCredential, changeStreamSpannerConfigWithCredential.getCredentials().get());
      assertEquals(testCredential, metadataSpannerConfigWithCredential.getCredentials().get());
  }

  @Test
  public void testSetSpannerConfigCredential() {
      
      TestCredential testCredential = new TestCredential();

      
      spannerConfig = spannerConfig.withCredentials(testCredential); 
      readChangeStream = readChangeStream.withSpannerConfig(spannerConfig);

      SpannerConfig changeStreamSpannerConfig = readChangeStream.buildChangeStreamSpannerConfig();
      SpannerConfig metadataSpannerConfig =
          MetadataSpannerConfigFactory.create(
              changeStreamSpannerConfig, TEST_METADATA_INSTANCE, TEST_METADATA_DATABASE);

      
      assertEquals(testCredential, changeStreamSpannerConfig.getCredentials().get());
      assertEquals(testCredential, metadataSpannerConfig.getCredentials().get());

      
      SpannerConfig changeStreamSpannerConfigWithCredential =
          SpannerIO.buildSpannerConfigWithCredential(
              changeStreamSpannerConfig, testPipeline.getOptions());
      SpannerConfig metadataSpannerConfigWithCredential =
          SpannerIO.buildSpannerConfigWithCredential(
              metadataSpannerConfig, testPipeline.getOptions());

      
      assertEquals(testCredential, changeStreamSpannerConfigWithCredential.getCredentials().get());
      assertEquals(testCredential, metadataSpannerConfigWithCredential.getCredentials().get());
  }
}
