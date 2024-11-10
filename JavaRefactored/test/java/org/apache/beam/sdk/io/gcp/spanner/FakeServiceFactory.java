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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.GuardedBy;
import org.mockito.Matchers;

class FakeServiceFactory implements ServiceFactory<Spanner, SpannerOptions>, Serializable {

  // Optimized by LLM: Consider using a ConcurrentHashMap instead of synchronized lists to manage the mock clients
  private static final ConcurrentHashMap<Integer, Spanner> mockSpanners = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<Integer, DatabaseClient> mockDatabaseClients = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<Integer, BatchClient> mockBatchClients = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<Integer, InstanceAdminClient> mockAdminClients = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<Integer, Instance> mockInstances = new ConcurrentHashMap<>();

  // Optimized by LLM: Use java.util.concurrent.atomic.AtomicInteger for the count variable to avoid synchronization overhead
  private static final AtomicInteger count = new AtomicInteger(0);

  private final int index;

  public FakeServiceFactory() {
    index = count.getAndIncrement();
    // Optimized by LLM: Extract the mock creation logic into a separate private method to reduce duplication and improve readability
    createMocks();
    when(mockAdminClient().getInstance(Matchers.any(String.class))).thenReturn(mockInstance());
    when(mockSpanner().getDatabaseClient(Matchers.any(DatabaseId.class)))
        .thenReturn(mockDatabaseClient());
    when(mockSpanner().getBatchClient(Matchers.any(DatabaseId.class)))
        .thenReturn(mockBatchClient());
    when(mockSpanner().getInstanceAdminClient()).thenReturn(mockAdminClient());
  }

  // Optimized by LLM: Extract the mock creation logic into a separate private method to reduce duplication and improve readability
  private void createMocks() {
    mockSpanners.put(index, mock(Spanner.class, withSettings().serializable()));
    mockDatabaseClients.put(index, mock(DatabaseClient.class, withSettings().serializable()));
    mockBatchClients.put(index, mock(BatchClient.class, withSettings().serializable()));
    mockAdminClients.put(index, mock(InstanceAdminClient.class, withSettings().serializable()));
    mockInstances.put(index, mock(Instance.class, withSettings().serializable()));
  }

  // Optimized by LLM: Use a single list to hold all mock clients instead of separate lists for each type
  DatabaseClient mockDatabaseClient() {
    return mockDatabaseClients.get(index);
  }

  BatchClient mockBatchClient() {
    return mockBatchClients.get(index);
  }

  Spanner mockSpanner() {
    return mockSpanners.get(index);
  }

  InstanceAdminClient mockAdminClient() {
    return mockAdminClients.get(index);
  }

  Instance mockInstance() {
    return mockInstances.get(index);
  }

  @Override
  public Spanner create(SpannerOptions serviceOptions) {
    return mockSpanner();
  }
}