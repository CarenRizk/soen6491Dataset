package org.apache.beam.sdk.io.gcp.spanner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap; // Optimized by LLM: Suggestion 1
import javax.annotation.concurrent.GuardedBy;
import org.mockito.Matchers;

class FakeServiceFactory implements ServiceFactory<Spanner, SpannerOptions>, Serializable {

  private static final Object lock = new Object(); // Optimized by LLM: Suggestion 5

  @GuardedBy("lock")
  private static final List<Spanner> mockSpanners = new ArrayList<>();

  @GuardedBy("lock")
  private static final List<DatabaseClient> mockDatabaseClients = new ArrayList<>();

  @GuardedBy("lock")
  private static final List<BatchClient> mockBatchClients = new ArrayList<>();

  @GuardedBy("lock")
  private static final List<InstanceAdminClient> mockAdminClients = new ArrayList<>();

  @GuardedBy("lock")
  private static final List<Instance> mockInstances = new ArrayList<>();

  @GuardedBy("lock")
  private static int count = 0;

  private final int index;

  public FakeServiceFactory() {
    synchronized (lock) {
      index = count++;
      addMocks(); // Optimized by LLM: Suggestion 2
    }
    setupMockReturns(); // Optimized by LLM: Suggestion 2
  }

  // Optimized by LLM: Suggestion 2
  private void addMocks() {
    mockSpanners.add(mock(Spanner.class, withSettings().serializable()));
    mockDatabaseClients.add(mock(DatabaseClient.class, withSettings().serializable()));
    mockBatchClients.add(mock(BatchClient.class, withSettings().serializable()));
    mockAdminClients.add(mock(InstanceAdminClient.class, withSettings().serializable()));
    mockInstances.add(mock(Instance.class, withSettings().serializable()));
  }

  // Optimized by LLM: Suggestion 2
  private void setupMockReturns() {
    when(mockAdminClient().getInstance(Matchers.any(String.class))).thenReturn(mockInstance());
    when(mockSpanner().getDatabaseClient(Matchers.any(DatabaseId.class)))
        .thenReturn(mockDatabaseClient());
    when(mockSpanner().getBatchClient(Matchers.any(DatabaseId.class)))
        .thenReturn(mockBatchClient());
    when(mockSpanner().getInstanceAdminClient()).thenReturn(mockAdminClient());
  }

  DatabaseClient mockDatabaseClient() {
    synchronized (lock) {
      return mockDatabaseClients.get(index);
    }
  }

  BatchClient mockBatchClient() {
    synchronized (lock) {
      return mockBatchClients.get(index);
    }
  }

  Spanner mockSpanner() {
    synchronized (lock) {
      return mockSpanners.get(index);
    }
  }

  InstanceAdminClient mockAdminClient() {
    synchronized (lock) {
      return mockAdminClients.get(index);
    }
  }

  Instance mockInstance() {
    synchronized (lock) {
      return mockInstances.get(index);
    }
  }

  @Override
  public Spanner create(SpannerOptions serviceOptions) {
    return mockSpanner();
  }
}